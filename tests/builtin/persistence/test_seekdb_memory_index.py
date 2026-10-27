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

"""Protocol-level hybrid SQL construction; native execution is tested separately."""

from __future__ import annotations

from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from powercontext.artifacts import ArtifactRef
from powercontext.builtin.artifacts.memory import CapabilityNotSupportedError, EmbeddingProfile, MemorySearchRequest
from powercontext.builtin.artifacts.memory.canonical import embedding_content_hash
from powercontext.builtin.persistence.seekdb.memory_index import SeekDBMemoryIndex
from powercontext.builtin.tags import TagFilter

_PROFILE = EmbeddingProfile(profile_id="test", model="offline", dimension=3, distance="l2", normalization="unit")
_MEMORY = ArtifactRef(family="memory", artifact_id="memory", revision=7)


class _Rows:
    def __init__(self, values):
        self.values = values

    def mappings(self):
        return self

    def all(self):
        return self.values


class _Connection:
    def __init__(self, revision: int = 7):
        self.revision = revision
        self.candidates = []

    async def scalar(self, statement):
        return self.revision

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        if "MATCH(" in sql:
            self.candidates.append((sql, parameters))
            common = {"memory_artifact_id": "memory", "head_revision": 1, "text": "Literal source text."}
            return _Rows([
                {**common, "channel": "fts", "entry_id": "b", "entry_version_id": "vb", "distance": None},
                {**common, "channel": "fts", "entry_id": "a", "entry_version_id": "va", "distance": None},
                {**common, "channel": "vector", "entry_id": "a", "entry_version_id": "va", "distance": 0.2},
            ])
        if "embedding_content_hash" in sql:
            digest = embedding_content_hash(
                profile_id="test",
                model="offline",
                dimension=3,
                distance="l2",
                normalization="unit",
                entry_content_hash="a" * 64,
            )
            return _Rows([("a", "va", "a" * 64, digest), ("b", "vb", "a" * 64, digest)])
        return _Rows([("a", "va", "a" * 64), ("b", "vb", "a" * 64)])


def _request(tag_filter=None):
    return MemorySearchRequest(
        query="source",
        analyzed_query="source",
        memories=(_MEMORY,),
        candidate_limit=32,
        mode="hybrid",
        query_vector=(1.0, 0.0, 0.0),
        embedding_profile=_PROFILE,
        tag_filter=tag_filter,
    )


@pytest.mark.asyncio
async def test_one_candidate_statement_preserves_channels_and_current_head_references() -> None:
    connection = _Connection()
    result = await SeekDBMemoryIndex(_PROFILE).search(cast(AsyncConnection, connection), "project", _request())
    assert len(connection.candidates) == 1
    sql, parameters = connection.candidates[0]
    assert "AGAINST" in sql and "l2_distance" in sql
    assert parameters["scope_id"] == "project"
    assert parameters["memory_artifact_ids"] == ("memory",)
    assert parameters["candidate_limit"] == 32
    assert [hit.entry_id for hit in result.fts] == ["b", "a"]
    assert [hit.entry_id for hit in result.vector] == ["a"]
    assert all(hit.memory_ref == _MEMORY for hit in (*result.fts, *result.vector))
    assert result.vector[0].distance == 0.2


@pytest.mark.parametrize("match,minimum", [("all", 2), ("any", 1)])
@pytest.mark.asyncio
async def test_hybrid_tag_filters_are_bound_inside_both_candidate_branches(match, minimum) -> None:
    connection = _Connection()
    tags = TagFilter(tags=("selected", "clinical"), match=match)
    await SeekDBMemoryIndex(_PROFILE).search(cast(AsyncConnection, connection), "project", _request(tags))
    assert len(connection.candidates) == 1
    sql, parameters = connection.candidates[0]
    assert sql.count("pc_artifact_tags") == 2
    assert parameters["tag_keys"] == tags.keys
    assert parameters["tag_minimum"] == minimum


@pytest.mark.asyncio
async def test_moved_head_is_rejected_before_candidate_search() -> None:
    connection = _Connection(revision=8)
    with pytest.raises(CapabilityNotSupportedError):
        await SeekDBMemoryIndex(_PROFILE).search(cast(AsyncConnection, connection), "project", _request())
    assert not connection.candidates
