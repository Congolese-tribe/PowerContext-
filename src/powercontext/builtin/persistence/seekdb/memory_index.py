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

"""One seekdb statement for independently bounded lexical and vector candidates."""

from __future__ import annotations

from pyobvector import VECTOR
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncConnection
from typing_extensions import override

from powercontext.builtin.artifacts.memory import (
    CapabilityNotSupportedError,
    EmbeddingProfile,
    MemorySearchChannels,
    MemorySearchRequest,
)
from powercontext.builtin.artifacts.memory.canonical import validate_embedding
from powercontext.builtin.persistence.memory_index import CompositeMemoryIndex, memory_channel_hits
from powercontext.builtin.persistence.oceanbase.memory_index import OceanBaseMemoryFTSIndex, OceanBaseMemoryVectorIndex
from powercontext.builtin.persistence.tags import memory_tag_parameters, memory_tag_sql

_HYBRID_SQL = """
SELECT candidates.*
FROM (
    SELECT 'fts' AS channel, f.memory_artifact_id, f.head_revision,
           f.entry_id, f.entry_version_id, f.text,
           NULL AS distance, -f.score AS sort_key
    FROM (
        SELECT m.memory_artifact_id, m.head_revision, m.entry_id, m.entry_version_id,
               v.text, MATCH(m.searchable_text) AGAINST (:analyzed_query) AS score
        FROM pc_memory_entry_heads AS m
        JOIN pc_memory_entry_versions AS v
          ON v.scope_id = m.scope_id
         AND v.memory_artifact_id = m.memory_artifact_id
         AND v.entry_version_id = m.entry_version_id
        WHERE m.scope_id = :scope_id
          AND m.memory_artifact_id IN :memory_artifact_ids
          AND MATCH(m.searchable_text) AGAINST (:analyzed_query)
          {tag_predicate}
        ORDER BY score DESC, m.memory_artifact_id, m.entry_id, m.entry_version_id
        LIMIT :candidate_limit
    ) AS f
    UNION ALL
    SELECT 'vector' AS channel, n.memory_artifact_id, n.head_revision,
           n.entry_id, n.entry_version_id, n.text,
           n.distance, n.distance AS sort_key
    FROM (
        SELECT m.memory_artifact_id, m.head_revision, m.entry_id, m.entry_version_id,
               v.text, l2_distance(m.embedding, :query_vector) AS distance
        FROM pc_memory_vector_entries AS m
        JOIN pc_memory_entry_versions AS v
          ON v.scope_id = m.scope_id
         AND v.memory_artifact_id = m.memory_artifact_id
         AND v.entry_version_id = m.entry_version_id
        WHERE m.scope_id = :scope_id
          AND m.memory_artifact_id IN :memory_artifact_ids
          {tag_predicate}
        ORDER BY l2_distance(m.embedding, :query_vector) APPROXIMATE
        LIMIT :candidate_limit
    ) AS n
) AS candidates
ORDER BY candidates.channel, candidates.sort_key,
         candidates.memory_artifact_id, candidates.entry_id, candidates.entry_version_id
"""


class SeekDBMemoryIndex(CompositeMemoryIndex):
    """Keep existing projection/single-channel behavior and combine hybrid reads."""

    def __init__(self, profile: EmbeddingProfile) -> None:
        self._vector = OceanBaseMemoryVectorIndex(profile)
        super().__init__(OceanBaseMemoryFTSIndex(), self._vector)
        parameters = (
            bindparam("memory_artifact_ids", expanding=True),
            bindparam("query_vector", type_=VECTOR(profile.dimension)),
        )
        self._hybrid = text(_HYBRID_SQL.format(tag_predicate="")).bindparams(*parameters)
        self._tagged_hybrid = text(_HYBRID_SQL.format(tag_predicate=memory_tag_sql("m"))).bindparams(
            *parameters, bindparam("tag_keys", expanding=True), bindparam("tag_hashes", expanding=True)
        )

    @override
    async def search(
        self, connection: AsyncConnection, scope_id: str, request: MemorySearchRequest, /
    ) -> MemorySearchChannels:
        if request.mode != "hybrid":
            return await super().search(connection, scope_id, request)
        profile = self._vector.profile
        if request.embedding_profile != profile or request.query_vector is None:
            raise CapabilityNotSupportedError("embedding-profile")
        if not await self._vector.vector_complete(connection, scope_id, request.memories, profile):
            raise CapabilityNotSupportedError("vector")
        rows = (
            (
                await connection.execute(
                    self._hybrid if request.tag_filter is None else self._tagged_hybrid,
                    {
                        "scope_id": scope_id,
                        "memory_artifact_ids": tuple(memory.artifact_id for memory in request.memories),
                        "analyzed_query": request.analyzed_query,
                        "query_vector": validate_embedding(request.query_vector, dimension=profile.dimension),
                        "candidate_limit": request.candidate_limit,
                        **memory_tag_parameters(request.tag_filter),
                    },
                )
            )
            .mappings()
            .all()
        )
        # Preserve both channel rankings. Shared fusion performs admission,
        # identity deduplication, and final limiting after this single statement.
        return MemorySearchChannels(
            fts=memory_channel_hits((row for row in rows if row["channel"] == "fts"), request.memories),
            vector=memory_channel_hits((row for row in rows if row["channel"] == "vector"), request.memories),
        )
