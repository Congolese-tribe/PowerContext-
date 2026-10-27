# Copyright (c) 2026 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Literal evidence selection, scoped identities, and complete wire budgets."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from powercontext.artifacts import ArtifactRef
from powercontext.builtin.artifacts.memory import MemoryHit
from powercontext.builtin.runtime import ContextAssembly, PrepareContextRequest
from powercontext.builtin.runtime.errors import PreparedContextInvariantError
from powercontext.builtin.runtime.prepared_context import PreparedContextBuilder, PreparedMemoryCandidates
from powercontext.builtin.runtime.prepared_text import TRUST_POLICY

_REF = ArtifactRef(family="memory", artifact_id="memory", revision=3)


def _group(texts: tuple[str, ...], *, scope: str = "project:alpha") -> PreparedMemoryCandidates:
    return PreparedMemoryCandidates(
        scope_id=scope,
        memory_ref=_REF,
        hits=tuple(
            MemoryHit(
                memory_ref=_REF,
                entry_id=f"entry-{index}",
                entry_version_id=f"version-{index}",
                text=text,
                score=1 / (index + 1),
                matched_by=("fts",),
            )
            for index, text in enumerate(texts)
        ),
    )


def _build(group, query="cobalt sensor", budget=1400, **options):
    return PreparedContextBuilder().build_scopes_result(
        request=PrepareContextRequest(query=query, max_bytes=budget, evidence_selection=True, **options),
        current_scope_id="project:alpha",
        memory_candidates=(group,),
    )


@pytest.mark.parametrize("value", ["true", "false", 0, 1, None, 1.0])
def test_selection_flag_requires_a_real_boolean(value) -> None:
    with pytest.raises(ValidationError):
        PrepareContextRequest(query="sensor", evidence_selection=value)
    assert PrepareContextRequest(query="sensor").evidence_selection is False


def test_changing_query_changes_late_original_evidence() -> None:
    text = "Routine office notes. " * 100 + "The cobalt sensor detects mercury. "
    text += "Unrelated scheduling details. " * 100 + "The amber valve regulates helium."
    group = _group((text,))
    first = _build(group, "cobalt sensor mercury", budget=1100)
    second = _build(group, "amber valve helium", budget=1100)
    assert any("cobalt sensor detects mercury" in fragment.text for fragment in first.fragments)
    assert any("amber valve regulates helium" in fragment.text for fragment in second.fragments)
    assert first.fragments != second.fragments
    for result in (first, second):
        for fragment in result.fragments:
            assert fragment.text == text[fragment.start : fragment.end]
            assert type(fragment.start) is type(fragment.end) is int


def test_multiple_disjoint_spans_keep_literal_text_and_aligned_origins() -> None:
    text = "Cobalt sensor measures pressure. " + "Unrelated discussion. " * 80 + "Cobalt sensor also measures velocity."
    result = _build(_group((text,)), "cobalt sensor pressure velocity", budget=1100)
    assert len(result.fragments) >= 2
    assert any("pressure" in item.text for item in result.fragments)
    assert any("velocity" in item.text for item in result.fragments)
    assert result.context.content is not None
    items = json.loads(result.context.content.splitlines()[-2])["items"]
    assert len(items) == len(result.origins) == len(result.fragments)
    for item, fragment, origin in zip(items, result.fragments, result.origins, strict=True):
        assert origin == fragment.address
        assert item == {
            "citation": {
                "scope_id": "project:alpha",
                "memory_ref": _REF.model_dump(mode="json"),
                "entry_id": fragment.address.entry_id,
                "entry_version_id": fragment.address.entry_version_id,
            },
            "content": text[fragment.start : fragment.end],
            "start": fragment.start,
            "end": fragment.end,
        }


def test_identical_content_and_entry_ids_in_different_scopes_keep_both_identities() -> None:
    group = _group(("Cobalt sensor records pressure.",))
    other = _group(("Cobalt sensor records pressure.",), scope="project:beta")
    result = PreparedContextBuilder().build_scopes_result(
        request=PrepareContextRequest(query="cobalt sensor", evidence_selection=True, max_bytes=2000),
        current_scope_id="project:alpha",
        memory_candidates=(group, other, group),
    )
    assert len(result.fragments) == 2
    assert {item.address.memory.scope_id for item in result.fragments} == {"project:alpha", "project:beta"}
    assert tuple(item.address for item in result.fragments) == result.origins


@pytest.mark.parametrize("budget", [512, 900, 1200, 1400, 1800])
def test_unicode_quoting_and_control_characters_pay_full_rendering_cost(budget: int) -> None:
    text = '海洋观测 station records "温度" and \\pressure\x00.\n' * 30
    result = _build(_group((text,)), "海洋观测 station 温度 pressure", budget=budget)
    content = result.context.content or ""
    assert result.context.content_bytes == len(content.encode("utf-8")) <= budget
    if content:
        assert content.startswith(TRUST_POLICY + "\n\nBEGIN_POWERCONTEXT_PREPARED_CONTEXT_V1\n")
        assert content.endswith("\nEND_POWERCONTEXT_PREPARED_CONTEXT_V1")
        payload = json.loads(content.splitlines()[-2])
        assert payload["trust"] == "untrusted_history"
        for fragment, item in zip(result.fragments, payload["items"], strict=True):
            assert item["content"] == fragment.text == text[fragment.start : fragment.end]


def test_exact_budget_boundary_includes_citation_and_envelope() -> None:
    group = _group(("The cobalt sensor records pressure.",))
    complete = _build(group, budget=2000)
    size = complete.context.content_bytes
    assert size > 512
    assert _build(group, budget=size).context == complete.context
    empty = _build(group, budget=size - 1)
    assert empty.context.content is None and empty.context.content_bytes == 0
    assert empty.origins == empty.fragments == ()


def test_query_can_select_inside_a_single_oversized_sentence() -> None:
    text = "miscellaneous notes " * 500 + "zirconium detector threshold " + "routine administration " * 500
    result = _build(_group((text,)), "zirconium detector threshold", budget=1200)
    assert result.fragments
    assert "zirconium detector threshold" in "".join(fragment.text for fragment in result.fragments)
    assert any(fragment.start > len(text) // 4 for fragment in result.fragments)


def test_whitespace_in_original_evidence_is_not_discarded() -> None:
    evidence = "RESULTS Cobalt sensor pressure is stable.   \n\n"
    text = "Administrative details. " + evidence + "Routine scheduling information."
    start = text.index(evidence)
    result = _build(_group((text,)), "cobalt sensor pressure", budget=1200)
    assert any(fragment.start <= start and fragment.end >= start + len(evidence) for fragment in result.fragments)


def test_empty_and_unmatched_input_is_legal() -> None:
    for group in (_group(()), _group(("Routine administration.",))):
        result = _build(group, "zirconium detector")
        assert result.context.content is None
        assert result.context.content_bytes == 0
        assert result.fragments == result.origins == ()


def test_evidence_mode_is_memory_only_even_with_code_and_assembly_options() -> None:
    group = _group(("Cobalt sensor records pressure.",))
    baseline = _build(group)
    combined = _build(group, include_code=True, assembly=ContextAssembly(sections=()))
    assert combined == baseline


def test_budget_probe_observes_the_same_fragment_selection() -> None:
    group = _group(("Routine notes. " * 100 + "Cobalt sensor records pressure.",))
    request = PrepareContextRequest(query="cobalt sensor", evidence_selection=True, max_bytes=1200)
    builder = PreparedContextBuilder()
    result = builder.build_scopes_result(request=request, current_scope_id=group.scope_id, memory_candidates=(group,))
    budget = builder.probe_budget(request=request, current_scope_id=group.scope_id, memory_candidates=(group,))
    assert budget.delivered_items == len(result.fragments)
    assert budget.unused_bytes == request.max_bytes - result.context.content_bytes
    assert budget.truncated_items == result.omissions.truncated_items


def test_missing_or_mismatched_memory_revision_is_rejected() -> None:
    group = _group(("Cobalt sensor records pressure.",))
    for reference in (None, _REF.model_copy(update={"revision": 4})):
        with pytest.raises(PreparedContextInvariantError):
            _build(PreparedMemoryCandidates(scope_id=group.scope_id, memory_ref=reference, hits=group.hits))


def test_disabled_selection_preserves_ordinary_prepare_output() -> None:
    group = _group(("Cobalt sensor records pressure. " * 100,))
    builder = PreparedContextBuilder()
    defaults = builder.build_scopes_result(
        request=PrepareContextRequest(query="cobalt sensor", max_bytes=1200),
        current_scope_id=group.scope_id,
        memory_candidates=(group,),
    )
    explicit = builder.build_scopes_result(
        request=PrepareContextRequest(query="cobalt sensor", max_bytes=1200, evidence_selection=False),
        current_scope_id=group.scope_id,
        memory_candidates=(group,),
    )
    assert defaults == explicit
    assert defaults.fragments == ()
