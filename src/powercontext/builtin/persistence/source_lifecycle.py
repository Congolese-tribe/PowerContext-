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

"""Scope-local lifecycle storage and Topic retrieval eligibility."""

from __future__ import annotations

import hashlib
from typing import Literal

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql.selectable import FromClause

from powercontext.builtin.persistence.codec import dump_model, load_model
from powercontext.builtin.persistence.database import AsyncDatabase
from powercontext.builtin.persistence.sources import SourceRepository
from powercontext.builtin.persistence.tables import SOURCE_LIFECYCLE_NODES_TABLE
from powercontext.builtin.source_lifecycle import LifecycleNode


class SourceLifecycleRepository:
    """Persist bindings and event intent before attempting native projections."""

    def __init__(self, database: AsyncDatabase, sources: SourceRepository) -> None:
        self.database = database
        self.sources = sources

    async def load(self, connection: AsyncConnection, scope_id: str) -> dict[str, LifecycleNode]:
        rows = await connection.execute(
            select(SOURCE_LIFECYCLE_NODES_TABLE.c.payload).where(SOURCE_LIFECYCLE_NODES_TABLE.c.scope_id == scope_id)
        )
        nodes = [load_model(LifecycleNode, row.payload, kind="lifecycle", name=scope_id) for row in rows]
        return {node.node_id: node for node in nodes}

    async def save(self, connection: AsyncConnection, scope_id: str, node: LifecycleNode, *, create: bool) -> None:
        table = SOURCE_LIFECYCLE_NODES_TABLE
        values = {
            "available": node.available,
            "topic_artifact_id": None if node.topic_ref is None else node.topic_ref.artifact_id,
            "topic_revision": None if node.topic_ref is None else node.topic_ref.revision,
            "payload": dump_model(node, kind="lifecycle", name=node.node_id),
        }
        if create:
            statement = insert(table).values(
                scope_id=scope_id,
                node_id=node.node_id,
                node_key=hashlib.sha256(node.node_id.encode()).digest(),
                **values,
            )
        else:
            statement = (
                update(table)
                .where(
                    table.c.scope_id == scope_id,
                    table.c.node_key == hashlib.sha256(node.node_id.encode()).digest(),
                    table.c.node_id == node.node_id,
                )
                .values(**values)
            )
        await connection.execute(statement)


def topic_lifecycle_predicate(topic: FromClause) -> ColumnElement[bool]:
    """Exclude unsupported exact Topic revisions before ranking or limits."""

    nodes = SOURCE_LIFECYCLE_NODES_TABLE
    return (
        ~select(nodes.c.node_id)
        .where(
            nodes.c.scope_id == topic.c.scope_id,
            nodes.c.topic_artifact_id == topic.c.artifact_id,
            nodes.c.topic_revision == topic.c.revision,
            nodes.c.available.is_(False),
        )
        .correlate(topic)
        .exists()
    )


def topic_lifecycle_sql(
    alias: Literal[
        "v",
        "pc_topic_memory_topic_fts",
        "pc_topic_memory_chunk_fts",
        "pc_topic_memory_vector_topics",
        "pc_topic_memory_vector_chunks",
    ],
) -> str:
    """Encode the same predicate for the native indexes' static SQL templates."""

    return (
        "NOT EXISTS (SELECT 1 FROM pc_source_lifecycle_nodes AS lifecycle "  # noqa: S608 -- fixed index aliases
        f"WHERE lifecycle.scope_id = {alias}.scope_id "
        f"AND lifecycle.topic_artifact_id = {alias}.artifact_id "
        f"AND lifecycle.topic_revision = {alias}.revision AND lifecycle.available = 0)"
    )
