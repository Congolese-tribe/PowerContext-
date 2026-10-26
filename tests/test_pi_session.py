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

"""Pi raw branch parsing and captured event identity contracts."""

import asyncio
import json

import pytest

from powercontext import PI_EVENT_SOURCE_DEFINITION, PiEventInput, PiEventSource, PiSessionInput, parse_pi_session
from powercontext.sources import (
    TEXT_EVIDENCE_PROJECTION_KEY,
    SourceDefinitionRegistry,
    SourceMaterialization,
    TextEvidence,
)


def archive(records, session_id="session", leaf_id=None):
    header = {"type": "session", "version": 3, "id": session_id}
    return PiSessionInput(
        content=("\ufeff" + "\n".join(json.dumps(row, ensure_ascii=False) for row in [header, *records])).encode(),
        leaf_id=leaf_id,
    )


def message(event_id, parent, content, role="assistant", **fields):
    return {
        "type": "message",
        "id": event_id,
        "parentId": parent,
        "message": {"role": role, "content": content, **fields},
    }


def test_raw_branch_keeps_evidence_before_summaries_and_through_control_nodes() -> None:
    records = [
        message("root", None, " 原文\r\n下一行 \u2028保留 ", "user"),
        message("other", "root", "other branch"),
        {"type": "model_change", "id": "control", "parentId": "root"},
        message("hidden", "control", [{"type": "thinking", "thinking": "private"}, {"type": "image"}]),
        message(
            "call",
            "hidden",
            [
                {"type": "text", "text": "before "},
                {"type": "toolCall", "name": "read", "id": "c1", "arguments": {"z": [2, 1], "a": "中文"}},
                {"type": "text", "text": " after"},
            ],
        ),
        message(
            "result",
            "call",
            [{"type": "text", "text": "一"}, {"type": "text", "text": "二"}],
            "toolResult",
            toolName="read",
            toolCallId="c1",
        ),
        {"type": "compaction", "id": "summary", "parentId": "result", "summary": "compact"},
        {"type": "branch_summary", "id": "branch", "parentId": "summary", "summary": "branch"},
        message("bash", "branch", [], "bashExecution", command="printf literal", output="output\n"),
        {
            "type": "custom_message",
            "id": "custom",
            "parentId": "bash",
            "content": [{"type": "text", "text": "custom"}, {"type": "thinking", "thinking": "hidden"}],
        },
        {"type": "label", "id": "leaf", "parentId": "custom"},
    ]
    events = parse_pi_session(archive(records))
    assert [event.content for event in events] == [
        "user:  原文\r\n下一行 \u2028保留 ",
        'assistant: before \n[tool_call read c1] {"a":"中文","z":[2,1]}\n after',
        "toolResult: [tool_result read c1] 一\n二",
        "compaction: compact",
        "branch_summary: branch",
        "bashExecution: $ printf literal\noutput\n",
        "custom: custom",
    ]
    by_id = {record["id"]: record for record in records}
    assert all(event.record == by_id[event.event_id] for event in events)
    assert events[1].parent_id == "hidden"
    assert parse_pi_session(archive(records, leaf_id="leaf")) == events
    assert [event.event_id for event in parse_pi_session(archive(records, leaf_id="other"))] == ["root", "other"]


@pytest.mark.parametrize(
    "bad_record",
    [
        {"type": "label", "id": "bad"},
        {"type": "label", "id": "bad", "parentId": "missing"},
        {"type": "label", "id": "bad", "parentId": "bad"},
        {"type": "label", "id": "bad", "parentId": "future"},
        {"type": "label", "id": "root", "parentId": None},
        {"type": "label", "id": 1, "parentId": "root"},
        {"type": "label", "id": "", "parentId": "root"},
        {"type": "label", "id": "bad", "parentId": []},
        {"type": "session", "id": "bad", "parentId": None},
        [],
    ],
)
def test_invalid_unselected_nodes_fail_before_returning_selected_events(bad_record) -> None:
    with pytest.raises(ValueError):
        parse_pi_session(archive([message("root", None, "valid"), bad_record], leaf_id="root"))


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"\xff",
        b"{}",
        b"null",
        b"[]",
        b'{"type":"session","version":3,"id":""}',
        b'{"type":"session","version":3.0,"id":"s"}',
        b'{"type":"session","version":3,"id":"s"}\n{broken',
        b'{"type":"session","version":3,"id":"s","n":NaN}',
    ],
)
def test_invalid_archive_fails(raw) -> None:
    with pytest.raises(ValueError):
        parse_pi_session(PiSessionInput(content=raw))


def test_empty_archive_and_explicit_leaf() -> None:
    assert parse_pi_session(archive([])) == ()
    with pytest.raises(ValueError):
        parse_pi_session(archive([], leaf_id="absent"))
    assert (
        parse_pi_session(archive([message("empty", None, ""), message("space", "empty", " \n ")]))[0].content
        == "assistant:  \n "
    )


def test_captured_event_identity_and_record_survive_serialization() -> None:
    async def scenario() -> None:
        registry = SourceDefinitionRegistry((PI_EVENT_SOURCE_DEFINITION,))
        identities = set()
        for session_id, event_id in [("a:b", "c"), ("a", "b:c"), ("会话" * 200, "节点" * 200)]:
            event = parse_pi_session(
                archive([message(event_id, None, "evidence", extra={"nested": [1, "原文"]})], session_id)
            )[0]
            source = await registry.resolve(event)
            assert isinstance(source, PiEventSource)
            assert source.materialization == SourceMaterialization.CAPTURED
            assert len(source.name) <= 256
            identities.add(source.name)
            restored = PiEventSource.model_validate_json(source.model_dump_json())
            read = await registry.read(restored)
            assert isinstance(read, PiEventInput)
            assert read == event
            evidence = TextEvidence.model_validate(registry.project(restored, TEXT_EVIDENCE_PROJECTION_KEY))
            assert evidence.source_type == "pi-event" and evidence.source_id == source.name
            assert evidence.content == event.content
            assert evidence.metadata == {
                key: getattr(event, key) for key in ("session_id", "event_id", "parent_id", "event_type", "role")
            }
            changed = event.model_copy(update={"content": "changed"})
            assert (await registry.resolve(changed)).name == source.name
            # Neither the caller's input nor a read result can mutate an existing capture.
            event.record["extra"] = "changed"
            read.record.clear()
            unchanged = await registry.read(source)
            assert isinstance(unchanged, PiEventInput)
            assert unchanged.record == restored.event.record
        assert len(identities) == 3

    asyncio.run(scenario())
