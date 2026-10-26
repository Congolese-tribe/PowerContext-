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

"""Offline capture of complete visible evidence on a Pi v3 session branch."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from powercontext.sources.connectors import (
    ConnectorRunCompletion,
    ConnectorRunSession,
    ConnectorRunStatus,
    ConnectorSubmissionStatus,
)
from powercontext.sources.definitions import SourceDefinition, SourceProjection
from powercontext.sources.models import Source, SourceMaterialization
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY, TextEvidence


class PiSessionInput(BaseModel):
    """A complete UTF-8 JSONL archive and optional selected tree node."""

    model_config = ConfigDict(frozen=True)

    content: bytes
    leaf_id: str | None = None


class PiEventInput(BaseModel):
    """One visible event, including the complete original JSON record."""

    model_config = ConfigDict(frozen=True)

    session_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    parent_id: str | None
    event_type: str
    role: str
    content: str
    record: dict[str, JsonValue]


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _event_identity(session_id: str, event_id: str) -> str:
    # Frame both IDs before hashing: separators inside either ID cannot alias a pair.
    digest = hashlib.sha256(_json([session_id, event_id]).encode("utf-8")).hexdigest()
    return f"pi:{digest}"


def _reject_constant(value: str) -> None:
    raise ValueError(f"Non-JSON number in Pi session: {value}")  # noqa: TRY003


def _parse_archive(content: bytes) -> tuple[str, dict[str, dict[str, JsonValue]]]:
    try:
        records = [
            json.loads(line, parse_constant=_reject_constant)
            for line in content.decode("utf-8-sig").split("\n")
            if line.strip()
        ]
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("Cannot decode or parse Pi session JSONL") from error  # noqa: TRY003
    if not records or not all(isinstance(record, dict) for record in records):
        raise ValueError("Pi session must contain JSON objects and a session header")  # noqa: TRY003
    header = records[0]
    session_id = header.get("id")
    if (
        header.get("type") != "session"
        or type(header.get("version")) is not int
        or header["version"] != 3
        or not isinstance(session_id, str)
        or not session_id
    ):
        raise ValueError("Pi session requires a v3 header with a non-empty string ID")  # noqa: TRY003
    by_id: dict[str, dict[str, JsonValue]] = {}
    for record in records[1:]:
        event_id = record.get("id")
        if not isinstance(event_id, str) or not event_id or event_id in by_id:
            raise ValueError("Pi node IDs must be unique non-empty strings")  # noqa: TRY003
        if not isinstance(record.get("type"), str) or record["type"] == "session":
            raise ValueError("Pi nodes require an event type and cannot be session headers")  # noqa: TRY003
        if "parentId" not in record:
            raise ValueError("Pi node is missing parentId")  # noqa: TRY003
        parent = record["parentId"]
        # Requiring an earlier parent also rules out cycles, even on unselected branches.
        if parent is not None and (not isinstance(parent, str) or parent not in by_id):
            raise ValueError("Pi parentId must be null or refer to an earlier node")  # noqa: TRY003
        by_id[event_id] = record
    return session_id, by_id


def _visible_text(value: JsonValue, *, tool_calls: bool = False) -> str:
    if isinstance(value, str):
        return value
    if not isinstance(value, list):
        return ""
    parts: list[str] = []
    for block in value:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text" and isinstance(block.get("text"), str):
            parts.append(block["text"])
        elif tool_calls and block.get("type") == "toolCall":
            parts.append(
                f"[tool_call {block.get('name', '')} {block.get('id', '')}] {_json(block.get('arguments', {}))}"
            )
    return "\n".join(parts)


def _message_body(message: dict[str, JsonValue], role: str) -> str:
    if role == "bashExecution":
        command, output = message.get("command", ""), message.get("output", "")
        if not isinstance(command, str) or not isinstance(output, str):
            raise ValueError("Pi bash command and output must be strings")  # noqa: TRY003
        return f"$ {command}\n{output}" if command or output else ""
    body = _visible_text(message.get("content"), tool_calls=True)
    if role == "toolResult" and body:
        return f"[tool_result {message.get('toolName', '')} {message.get('toolCallId', '')}] {body}"
    return body


def _visible_event(session_id: str, record: dict[str, JsonValue]) -> PiEventInput | None:
    event_type = cast(str, record["type"])
    if event_type == "message":
        message = record.get("message")
        if not isinstance(message, dict):
            raise ValueError("Pi message must be a JSON object")  # noqa: TRY003
        role = message.get("role")
        if not isinstance(role, str) or not role or role == "system":
            return None
        body = _message_body(message, role)
    elif event_type in {"compaction", "branch_summary"}:
        role, body = event_type, record.get("summary", "")
        if not isinstance(body, str):
            raise ValueError("Pi summary must be a string")  # noqa: TRY003
    elif event_type == "custom_message":
        role, body = "custom", _visible_text(record.get("content"))
    else:
        return None
    if not body:
        return None
    return PiEventInput(
        session_id=session_id,
        event_id=cast(str, record["id"]),
        parent_id=cast(str | None, record["parentId"]),
        event_type=event_type,
        role=role,
        content=f"{role}: {body}",
        record=record,
    )


def _selected_events(
    session_id: str, by_id: dict[str, dict[str, JsonValue]], leaf_id: str | None
) -> tuple[PiEventInput, ...]:
    if leaf_id is None:
        leaf_id = next(reversed(by_id), None)
    elif leaf_id not in by_id:
        raise ValueError(f"Pi leaf does not exist: {leaf_id!r}")  # noqa: TRY003
    events: list[PiEventInput] = []
    while leaf_id is not None:
        record = by_id[leaf_id]
        event = _visible_event(session_id, record)
        if event is not None:
            events.append(event)
        leaf_id = cast(str | None, record["parentId"])
    return tuple(reversed(events))


def parse_pi_session(value: PiSessionInput) -> tuple[PiEventInput, ...]:
    """Validate the entire graph and return raw visible ancestors, root to leaf.

    Control nodes remain in the graph. Compaction and branch summaries contribute
    their own evidence without replacing any earlier event.
    """
    session_id, by_id = _parse_archive(value.content)
    return _selected_events(session_id, by_id, value.leaf_id)


class PiEventSource(Source):
    """Captured evidence independent of the session file and its current branch."""

    event: PiEventInput


class _PiEventTextProjection:
    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class: type[PiEventSource] = PiEventSource
    output_class: type[BaseModel] = TextEvidence

    def project(self, source: PiEventSource, /) -> TextEvidence:
        event = source.event
        return TextEvidence(
            source_type="pi-event",
            source_id=source.name,
            content=event.content,
            metadata={
                "session_id": event.session_id,
                "event_id": event.event_id,
                "parent_id": event.parent_id,
                "event_type": event.event_type,
                "role": event.role,
            },
        )


class _PiEventDefinition:
    input_class: type[PiEventInput] = PiEventInput
    name: str = "pi-event"
    version: str = "1"
    source_class: type[PiEventSource] = PiEventSource
    projections: tuple[SourceProjection[PiEventSource], ...] = (_PiEventTextProjection(),)

    async def resolve(self, value: PiEventInput, /) -> PiEventSource:
        return PiEventSource(
            name=_event_identity(value.session_id, value.event_id),
            materialization=SourceMaterialization.CAPTURED,
            event=value.model_copy(deep=True),
        )

    async def read(self, source: PiEventSource, /) -> PiEventInput:
        return source.event.model_copy(deep=True)


PI_EVENT_SOURCE_DEFINITION: SourceDefinition[PiEventInput, PiEventSource, PiEventInput] = _PiEventDefinition()


class _Checkpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    version: Literal[1] = 1
    sessions: dict[str, dict[str, str]] = Field(default_factory=dict)


class PiSessionConnector:
    """Import unseen branch events using only lifecycle-owned durable progress.

    Each run reads the full archive and checks all previously imported records
    in that session, including records outside the selected branch. Replaying
    durable submissions after a failed run is safe through Runtime ingestion.
    """

    name = "pi-session-history"
    version = "1"
    source_definitions = frozenset({"pi-event"})

    def __init__(self, path: str | Path, leaf_id: str | None = None) -> None:
        self.path = Path(path)
        self.leaf_id = leaf_id

    async def run(self, session: ConnectorRunSession, /) -> ConnectorRunCompletion:
        session_id, by_id = _parse_archive(self.path.read_bytes())
        events = _selected_events(session_id, by_id, self.leaf_id)
        progress = _Checkpoint() if session.checkpoint is None else _Checkpoint.model_validate(session.checkpoint)
        imported = progress.sessions.setdefault(session_id, {})
        for event_id, digest in imported.items():
            record = by_id.get(event_id)
            if record is None or hashlib.sha256(_json(record).encode("utf-8")).hexdigest() != digest:
                raise ValueError(f"Previously imported Pi event changed or disappeared: {event_id!r}")  # noqa: TRY003
        for event in events:
            if event.event_id in imported:
                continue
            outcome = await session.submit(_event_identity(session_id, event.event_id), "pi-event", event)
            if outcome.status is not ConnectorSubmissionStatus.ACCEPTED:
                return ConnectorRunCompletion(status=ConnectorRunStatus.INCOMPLETE, checkpoint=session.checkpoint)
            imported[event.event_id] = hashlib.sha256(_json(event.record).encode("utf-8")).hexdigest()
        return ConnectorRunCompletion(status=ConnectorRunStatus.COMPLETE, checkpoint=progress.model_dump(mode="json"))


__all__ = [
    "PI_EVENT_SOURCE_DEFINITION",
    "PiEventInput",
    "PiEventSource",
    "PiSessionConnector",
    "PiSessionInput",
    "parse_pi_session",
]
