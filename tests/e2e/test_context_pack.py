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

"""Native Memory capture, prepare, and serialized context delivery over forty rounds."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from powercontext.builtin.artifacts.memory import MemoryCandidateRequest, MemoryEntryInput
from powercontext.builtin.runtime import BuiltinConfig, CaptureSource, PrepareContextRequest, open_builtin_runtime
from powercontext.builtin.runtime.context_pack import ContextPack, ContextPackDelta, apply_context_pack_delta
from powercontext.builtin.scope import ScopeDraft
from powercontext.builtin.sources import ContentSource

_FIXTURE = Path(__file__).parents[1] / "fixtures/context_pack/turns.json"


class _TurnPipeline:
    """Keep source turns verbatim; exercise actual storage and FTS without a model."""

    async def extract(self, request: MemoryCandidateRequest, /) -> tuple[MemoryEntryInput, ...]:
        return tuple(
            MemoryEntryInput(kind="conversation-turn", text=source.content, sources=(source,))
            for source in request.sources
            if isinstance(source, ContentSource)
        )


@pytest.mark.asyncio
async def test_incremental_empty_context_and_scope_authorization() -> None:
    async with open_builtin_runtime(BuiltinConfig(), candidate_pipeline=_TurnPipeline()) as runtime:
        assert runtime.scopes is not None
        scope = await runtime.scopes.create(
            ScopeDraft(title="Empty context", summary="Empty pack delivery", idempotency_key="empty-pack")
        )
        context = runtime.context.for_scope(scope.scope_id)
        authorized: list[tuple[str, ...]] = []

        async def authorize(scope_ids: tuple[str, ...]) -> None:
            authorized.append(scope_ids)

        request = PrepareContextRequest(query="remote")
        ordinary = await context.prepare(request)
        first = await context.prepare_incremental(request, authorize_scopes=authorize)
        assert ordinary.content is None
        assert isinstance(first, ContextPack)
        assert first.items == ()
        base = ContextPack.from_content("Previously held context")
        delta = await context.prepare_incremental(request, base, authorize_scopes=authorize)
        assert isinstance(delta, ContextPackDelta)
        assert apply_context_pack_delta(ContextPack.from_bytes(base.to_bytes()), delta) == first
        assert authorized == [(scope.scope_id,), (scope.scope_id,)]


@pytest.mark.asyncio
async def test_forty_round_native_delivery_cost() -> None:
    fixture = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    turns = fixture["turns"]
    config = fixture["measurement"]
    raw_full_bytes = full_pack_bytes = incremental_bytes = gzip_bytes = 0
    contents: set[bytes] = set()
    receiver: ContextPack | None = None
    rows: list[dict[str, object]] = []
    async with open_builtin_runtime(BuiltinConfig(), candidate_pipeline=_TurnPipeline()) as runtime:
        assert runtime.scopes is not None
        scope = await runtime.scopes.create(
            ScopeDraft(title="QMSum ES2004a", summary="Context pack cost", idempotency_key="pack-cost")
        )
        sources = runtime.sources.for_scope(scope.scope_id)
        memory = runtime.memory.for_scope(scope.scope_id)
        context = runtime.context.for_scope(scope.scope_id)

        async def capture(index: int) -> None:
            turn = turns[index]
            await sources.capture(
                CaptureSource(
                    source_id=f"turn-{turn['turn_index']}", content=f"{turn['speaker']}: {turn['content']}", metadata={}
                )
            )

        for index in range(config["initial_turns"]):
            await capture(index)
        while (await memory.flush()).processed:
            pass
        for round_index in range(config["rounds"]):
            if round_index in config["append_before_rounds"]:
                index = config["initial_turns"] + config["append_before_rounds"].index(round_index)
                await capture(index)
                while (await memory.flush()).processed:
                    pass
            query = config["queries"][round_index // config["query_rounds"]]
            request = PrepareContextRequest(query=query, max_bytes=config["max_bytes"])
            ordinary = await context.prepare(request)
            assert ordinary.content
            content = ordinary.content.encode()
            contents.add(content)
            full = ContextPack.from_content(ordinary.content)
            packet = await context.prepare_incremental(request, base_pack=receiver)
            wire = packet.to_bytes()
            # Drop sender objects at the wire boundary; only receiver state is retained.
            if receiver is None:
                assert isinstance(packet, ContextPack)
                receiver = ContextPack.from_bytes(wire)
            else:
                assert isinstance(packet, ContextPackDelta)
                receiver = apply_context_pack_delta(
                    ContextPack.from_bytes(receiver.to_bytes()), ContextPackDelta.from_bytes(wire)
                )
            assert receiver.to_bytes() == full.to_bytes()
            assert b"".join(receiver.items) == content
            raw_full_bytes += len(content)
            full_pack_bytes += len(full.to_bytes())
            incremental_bytes += len(wire)
            compressed = len(gzip.compress(content, compresslevel=9, mtime=0))
            gzip_bytes += compressed
            rows.append({"round": round_index, "query": query, "raw": len(content), "incremental": len(wire)})

    report = {
        "rounds": len(rows),
        "distinct_contexts": len(contents),
        "raw_full_bytes": raw_full_bytes,
        "full_pack_bytes": full_pack_bytes,
        "incremental_bytes": incremental_bytes,
        "gzip_bytes": gzip_bytes,
        "saving_vs_raw": 1 - incremental_bytes / raw_full_bytes,
        "saving_vs_full_pack": 1 - incremental_bytes / full_pack_bytes,
        "saving_vs_gzip": 1 - incremental_bytes / gzip_bytes,
        "sequence": rows,
    }
    print(json.dumps(report, sort_keys=True))
    assert report["rounds"] == 40
    assert report["distinct_contexts"] >= 4
    assert report["saving_vs_raw"] >= 0.45
