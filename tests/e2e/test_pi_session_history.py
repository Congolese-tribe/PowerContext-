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

"""Pi history recovery through Runtime ingestion and durable SQLite checkpoints."""

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from powercontext import (
    PI_EVENT_SOURCE_DEFINITION,
    PiEventInput,
    PiEventSource,
    PiSessionConnector,
    PiSessionInput,
    parse_pi_session,
)
from powercontext.builtin.persistence.sqlite import SQLiteConfig
from powercontext.builtin.runtime import (
    BuiltinConfig,
    CommitConnectorCheckpoint,
    SubmitSourceObservation,
    open_builtin_contexts,
    open_builtin_runtime,
)
from powercontext.builtin.scope import ScopeDraft
from powercontext.errors import ConnectorSubmissionRejectedError, SourceConflictError
from powercontext.sources import (
    ConnectorBinding,
    ConnectorRunResult,
    ConnectorRunStatus,
    SourceDefinitionRegistry,
    SourceObservation,
    manifest_for_definition,
    project_source_for_transport,
)
from powercontext.sources.connectors import ConnectorLifecycle


def _write(path, records):
    header = {"type": "session", "version": 3, "id": "history"}
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in [header, *records]), encoding="utf-8")


def _message(event_id, parent, text):
    return {"type": "message", "id": event_id, "parentId": parent, "message": {"role": "assistant", "content": text}}


class RuntimeSink:
    def __init__(self, runtime, failure=None):
        self.runtime, self.failure = runtime, failure
        self.registry = SourceDefinitionRegistry((PI_EVENT_SOURCE_DEFINITION,))

    async def submit(self, binding, item_id, definition_name, value, /):
        if self.failure == "reject" and value.event_id == "tail":
            raise ConnectorSubmissionRejectedError("temporary rejection")  # noqa: TRY003
        source = await self.registry.resolve(value)
        receipt = await self.runtime.ingestion.submit(
            SubmitSourceObservation(
                scope_id=binding.scope_id, observation=project_source_for_transport(self.registry, source)
            )
        )
        if self.failure == "lost-receipt" and value.event_id == "middle":
            raise RuntimeError("lost durable receipt")  # noqa: TRY003
        return receipt.source_ref


class RuntimeCheckpoints:
    def __init__(self, runtime):
        self.runtime = runtime

    async def load(self, binding, /):
        return (await self.runtime.ingestion.checkpoint(binding)).checkpoint

    async def save(self, binding, checkpoint, /, *, expected):
        await self.runtime.ingestion.commit(
            CommitConnectorCheckpoint(binding=binding, checkpoint=checkpoint, expected=expected)
        )


async def _run(config, path, leaf=None, failure=None):
    async with open_builtin_runtime(config) as runtime:
        assert runtime.scopes is not None
        scope = await runtime.scopes.create(ScopeDraft(title="Pi", summary="Raw history", idempotency_key="pi-history"))
        binding = ConnectorBinding(
            scope_id=scope.scope_id, binding_id="pi", connector_name="pi-session-history", connector_version="1"
        )
        await runtime.ingestion.register(manifest_for_definition(PI_EVENT_SOURCE_DEFINITION))
        return await ConnectorLifecycle(
            sink=RuntimeSink(runtime, failure), checkpoints=RuntimeCheckpoints(runtime)
        ).run(PiSessionConnector(path, leaf), binding)


async def _stored(config, scope_id) -> list[PiEventInput]:
    registry = SourceDefinitionRegistry((PI_EVENT_SOURCE_DEFINITION,))
    async with open_builtin_contexts(config) as contexts:
        sources = (await contexts.get(scope_id)).sources
        entries = await sources.journal.entries()
        events = []
        for entry in entries:
            source = await sources.catalog.get(entry.source)
            assert isinstance(source, SourceObservation)
            assert sources.catalog.as_ref(source) == entry.source_ref
            event = await registry.read(PiEventSource.model_validate(source.payload))
            assert isinstance(event, PiEventInput)
            events.append(event)
        return events


@pytest.mark.parametrize("failure", ["lost-receipt", "reject"])
def test_failed_run_keeps_progress_and_restart_retries_durable_events_once(tmp_path, failure):
    path = tmp_path / "session.jsonl"
    config = BuiltinConfig(database=SQLiteConfig(url=f"sqlite+aiosqlite:///{tmp_path / 'history.db'}"))
    records = [_message("root", None, "shared")]
    _write(path, records)
    initial = asyncio.run(_run(config, path))
    records.extend([_message("middle", "root", "persisted before failure"), _message("tail", "middle", "retry")])
    _write(path, records)

    async def fail_and_check():
        if failure == "lost-receipt":
            with pytest.raises(RuntimeError, match="lost durable receipt"):
                await _run(config, path, failure=failure)
        else:
            result = await _run(config, path, failure=failure)
            assert result.status is ConnectorRunStatus.INCOMPLETE
            assert result.committed_checkpoint == initial.committed_checkpoint
        async with open_builtin_runtime(config) as runtime:
            assert (await runtime.ingestion.checkpoint(initial.binding)).checkpoint == initial.committed_checkpoint
        assert [event.event_id for event in await _stored(config, initial.binding.scope_id)] == ["root", "middle"]

    asyncio.run(fail_and_check())
    # Start an independent Python process with only the database and archive paths.
    environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2] / "src"))
    resumed = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), str(tmp_path / "history.db"), str(path)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert resumed.returncode == 0, resumed.stdout + resumed.stderr
    recovered = ConnectorRunResult.model_validate_json(resumed.stdout)
    assert recovered.previous_checkpoint == initial.committed_checkpoint
    assert len(recovered.items) == 2
    assert [event.event_id for event in asyncio.run(_stored(config, initial.binding.scope_id))] == [
        "root",
        "middle",
        "tail",
    ]
    replay = asyncio.run(_run(config, path))
    assert replay.items == () and replay.committed_checkpoint == recovered.committed_checkpoint


def test_branch_history_mutation_and_malformed_append_fail_before_new_writes(tmp_path):
    path = tmp_path / "session.jsonl"
    config = BuiltinConfig(database=SQLiteConfig(url=f"sqlite+aiosqlite:///{tmp_path / 'history.db'}"))
    records = [_message("root", None, "shared"), _message("left", "root", "left"), _message("right", "root", "right")]
    _write(path, records)

    async def scenario():
        first = await _run(config, path, "left")
        second = await _run(config, path, "right")
        assert len(first.items) == 2 and len(second.items) == 1
        records.append(_message("tail", "right", "new evidence"))
        records[1]["timestamp"] = "changed metadata on unselected branch"
        _write(path, records)
        with pytest.raises(ValueError, match="Previously imported"):
            await _run(config, path, "tail")
        del records[1]["timestamp"]
        _write(path, records)
        with path.open("ab") as stream:
            stream.write(b"\n{invalid JSON}")
        with pytest.raises(ValueError):
            await _run(config, path, "tail")
        assert [event.event_id for event in await _stored(config, first.binding.scope_id)] == ["root", "left", "right"]
        _write(path, records)
        repaired = await _run(config, path, "tail")
        assert repaired.previous_checkpoint == second.committed_checkpoint and len(repaired.items) == 1
        events = await _stored(config, first.binding.scope_id)
        assert [event.event_id for event in events] == ["root", "left", "right", "tail"]
        # Direct ingestion must also reject a rewritten immutable identity.
        registry = SourceDefinitionRegistry((PI_EVENT_SOURCE_DEFINITION,))
        changed = events[1].model_copy(update={"content": "rewritten"})
        observation = project_source_for_transport(registry, await registry.resolve(changed))
        async with open_builtin_runtime(config) as runtime:
            with pytest.raises(SourceConflictError):
                await runtime.ingestion.submit(
                    SubmitSourceObservation(scope_id=first.binding.scope_id, observation=observation)
                )
        assert await _stored(config, first.binding.scope_id) == events

    asyncio.run(scenario())


def test_full_large_tool_record_reads_after_archive_removal(tmp_path):
    path = tmp_path / "session.jsonl"
    output = "中文工具原始输出\n" * 10000 + "unique end marker"
    record = {
        "type": "message",
        "id": "tool",
        "parentId": None,
        "message": {
            "role": "toolResult",
            "toolName": "read",
            "toolCallId": "call",
            "content": [{"type": "text", "text": output}],
            "details": {"raw": ["complete"]},
        },
    }
    _write(path, [record])
    expected = parse_pi_session(PiSessionInput(content=path.read_bytes()))[0]
    config = BuiltinConfig(database=SQLiteConfig(url=f"sqlite+aiosqlite:///{tmp_path / 'history.db'}"))
    result = asyncio.run(_run(config, path))
    path.unlink()
    events = asyncio.run(_stored(config, result.binding.scope_id))
    assert events == [expected]
    assert events[0].content == f"toolResult: [tool_result read call] {output}"
    assert events[0].record == record


if __name__ == "__main__":
    restart_config = BuiltinConfig(database=SQLiteConfig(url=f"sqlite+aiosqlite:///{Path(sys.argv[1]).as_posix()}"))
    sys.stdout.write(asyncio.run(_run(restart_config, Path(sys.argv[2]))).model_dump_json())
