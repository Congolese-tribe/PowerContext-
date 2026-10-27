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

"""Validated identities and durable state for source-supported evidence."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from powercontext.artifacts import ArtifactRef, MemoryCitation
from powercontext.sources import SourceRef


class SourceLifecycleError(ValueError):
    """Reject an invalid lifecycle registration, event, or resolution."""


class SourceLifecycleEvent(BaseModel):
    """One source's ordered availability event, independent of body revisions."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    node_id: str
    revision: StrictInt = Field(gt=0)
    sequence: StrictInt = Field(gt=0)
    available: bool
    reason: str

    @field_validator("node_id", "reason")
    @classmethod
    def require_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must contain non-whitespace text")  # noqa: TRY003
        return value


class LifecycleNode(BaseModel):
    """An immutable binding plus its last accepted event and desired state."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    node_id: str
    kind: Literal["source", "memory", "topic"]
    source_ref: SourceRef | None = None
    citation: MemoryCitation | None = None
    topic_ref: ArtifactRef | None = None
    revision: int | None = None
    parents: tuple[str, ...] = ()
    policy: Literal["all", "any"] = "all"
    available: bool = True
    event: SourceLifecycleEvent | None = None

    def reference(self) -> SourceRef | MemoryCitation | ArtifactRef:
        for value in (self.source_ref, self.citation, self.topic_ref):
            if value is not None:
                return value
        raise SourceLifecycleError("lifecycle binding has no reference")  # noqa: TRY003
