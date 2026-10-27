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

"""Real seekdb: paired full-search measurements, native plans, and filtered candidates."""

from __future__ import annotations

import base64
import hashlib
import json
import struct
import zlib
from contextlib import contextmanager
from importlib.metadata import version
from pathlib import Path
from statistics import median
from time import perf_counter

import pytest
from sqlalchemy import Engine, event

from powercontext.builtin.artifacts.memory import EmbeddingProfile, MemoryEntryInput, MemorySearchRequest
from powercontext.builtin.artifacts.memory.canonical import normalize_query
from powercontext.builtin.artifacts.memory.models import MemoryQueryEmbedding
from powercontext.builtin.inference import EmbeddingResult
from powercontext.builtin.persistence.memory_index import CompositeMemoryIndex
from powercontext.builtin.persistence.seekdb import SeekDBConfig
from powercontext.builtin.persistence.seekdb.memory_index import SeekDBMemoryIndex
from powercontext.builtin.runtime import BuiltinConfig, open_builtin_contexts
from powercontext.builtin.tags import MemoryEntryTagTarget, TagFilter

pytest.importorskip("pylibseekdb")

_FIXTURE = Path(__file__).parents[1] / "fixtures/seekdb_hybrid/scifact.json"
_PROFILE = EmbeddingProfile(
    profile_id="public-scifact-tfidf-svd384-v1",
    model="fixed-public-vectors",
    dimension=384,
    distance="l2",
    normalization="unit",
)


class _FixedEmbedding:
    profile = _PROFILE

    def __init__(self, texts, vectors):
        self.vectors = dict(zip(texts, vectors, strict=True))

    async def embed(self, texts, /):
        return EmbeddingResult(vectors=tuple(self.vectors[text] for text in texts))


@contextmanager
def _candidates():
    calls = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        if ("AGAINST" in statement.upper() or "l2_distance(" in statement.lower()) and (
            "pc_memory_entry_heads" in statement or "pc_memory_vector_entries" in statement
        ):
            calls.append((statement, parameters))

    event.listen(Engine, "before_cursor_execute", capture)
    try:
        yield calls
    finally:
        event.remove(Engine, "before_cursor_execute", capture)


async def _search(memory, head, query, embedding, monkeypatch, *, frozen):
    with monkeypatch.context() as control:
        if frozen:
            # The frozen composition uses these unchanged constituent indexes.
            # Both paths still execute full Memory.search against the same live DB.
            control.setattr(SeekDBMemoryIndex, "search", CompositeMemoryIndex.search)
        with _candidates() as calls:
            start = perf_counter()
            result = await memory.search(query, memories=(head,), mode="hybrid", limit=10, query_embedding=embedding)
            milliseconds = (perf_counter() - start) * 1000
        assert len(calls) == (2 if frozen else 1)
        if not frozen:
            assert "AGAINST" in calls[0][0].upper() and "l2_distance(" in calls[0][0].lower()
    return result, milliseconds, calls


def _metrics(result, head, entries, by_text, relevant):
    identities = []
    document_ids = []
    for hit in result.hits:
        entry = entries[hit.entry_id]
        assert hit.memory_ref == head.as_ref()
        assert hit.entry_version_id == entry.entry_version_id
        assert hit.text == entry.text
        assert hit.matched_by and set(hit.matched_by) <= {"fts", "vector"}
        identities.append((hit.memory_ref.artifact_id, hit.entry_id, hit.entry_version_id))
        document_ids.append(by_text[hit.text])
    assert len(identities) == len(set(identities))
    recall = len(set(document_ids).intersection(relevant)) / len(relevant)
    reciprocal = next((1 / rank for rank, key in enumerate(document_ids, start=1) if key in relevant), 0.0)
    signature = tuple(
        (
            hit.memory_ref.artifact_id,
            hit.memory_ref.revision,
            hit.entry_id,
            hit.entry_version_id,
            hit.text,
            hit.matched_by,
            hit.score,
        )
        for hit in result.hits
    )
    return recall, reciprocal, signature


async def _filtered_candidates(contexts, model):
    memory = (await contexts.get("filtered-scope")).artifacts.memory
    texts = ("Alpha selected first.", "Alpha selected second.", "Alpha selected both.", "Alpha decoy elsewhere.")
    basis = (1.0,) + (0.0,) * 383
    model.vectors.update(dict.fromkeys((*texts, "alpha"), basis))
    head = await memory.remember(
        memory=None, entries=tuple(MemoryEntryInput(kind="fact", text=text) for text in texts[:3]), mode="append"
    )
    decoy = await memory.remember(memory=None, entries=(MemoryEntryInput(kind="fact", text=texts[3]),), mode="append")
    assert head is not None and decoy is not None
    entries = {entry.text: entry for entry in await memory.entries(head)}
    for text, labels in zip(texts[:3], (("red",), ("blue",), ("red", "blue")), strict=True):
        target = MemoryEntryTagTarget(artifact_id=head.artifact_id, entry_id=entries[text].entry_id)
        empty = await contexts.records.get_tags("filtered-scope", target)
        await contexts.records.replace_tags("filtered-scope", target, labels, expected_etag=empty.etag)
    other = (await memory.entries(decoy))[0]
    target = MemoryEntryTagTarget(artifact_id=decoy.artifact_id, entry_id=other.entry_id)
    empty = await contexts.records.get_tags("filtered-scope", target)
    await contexts.records.replace_tags("filtered-scope", target, ("red", "blue"), expected_etag=empty.etag)
    index = SeekDBMemoryIndex(_PROFILE)
    for match, limit in (("all", 1), ("any", 2)):
        request = MemorySearchRequest(
            query="alpha",
            analyzed_query="alpha",
            memories=(head.as_ref(),),
            candidate_limit=limit,
            mode="hybrid",
            query_vector=basis,
            embedding_profile=_PROFILE,
            tag_filter=TagFilter(tags=("red", "blue"), match=match),
        )
        async with contexts.database.transaction() as connection:
            with _candidates() as calls:
                channels = await index.search(connection, "filtered-scope", request)
            assert len(calls) == 1
            assert 0 < len(channels.fts) <= limit and 0 < len(channels.vector) <= limit
            allowed = {entries[texts[2]].entry_id} if match == "all" else {entry.entry_id for entry in entries.values()}
            for hit in (*channels.fts, *channels.vector):
                assert hit.memory_ref == head.as_ref() and hit.entry_id in allowed
            plan = "\n".join(
                str(row[0]) for row in (await connection.exec_driver_sql("EXPLAIN " + calls[0][0], calls[0][1])).all()
            )
            assert "TEXT RETRIEVAL SCAN" in plan and "VECTOR INDEX" in plan


@pytest.mark.asyncio
async def test_native_hybrid_paired_quality_and_cost(tmp_path, monkeypatch):
    fixture = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    documents, queries = fixture["documents"], fixture["queries"]
    assert len(documents) == 173 and len(queries) == 20
    payload = zlib.decompress(base64.b64decode(fixture["vectors"]["data"], validate=True))
    assert hashlib.sha256(payload).hexdigest() == fixture["vectors"]["sha256"]
    vectors = tuple(struct.iter_unpack("<384d", payload))
    assert len(vectors) == 193
    assert all(abs(sum(value * value for value in vector) - 1) < 1e-8 for vector in vectors)
    texts = tuple(doc["text"] for doc in documents) + tuple(normalize_query(query["text"]) for query in queries)
    model = _FixedEmbedding(texts, vectors)
    by_text = {document["text"]: document["id"] for document in documents}
    results = []
    plans = []
    config = BuiltinConfig(database=SeekDBConfig(path=tmp_path / "native-db"))
    async with open_builtin_contexts(config, embedding_model=model) as contexts:
        scopes = []
        inputs = tuple(MemoryEntryInput(kind="document", text=document["text"]) for document in documents)
        for scope_index in range(3):
            memory = (await contexts.get(f"quality-{scope_index}")).artifacts.memory
            initial = await memory.remember(memory=None, entries=inputs[:-1], mode="append")
            head = await memory.remember(memory=initial, entries=inputs[-1:], mode="append")
            assert initial is not None and head is not None and head.revision > initial.revision
            entries = {entry.entry_id: entry for entry in await memory.entries(head)}
            assert len(entries) == 173
            scopes.append((memory, head, entries))
        for scope_index, (memory, head, entries) in enumerate(scopes):
            first = MemoryQueryEmbedding(embedding_profile=_PROFILE, query_vector=vectors[173])
            for frozen in (True, False):
                await _search(memory, head, queries[0]["text"], first, monkeypatch, frozen=frozen)
            for query_index, query in enumerate(queries):
                embedding = MemoryQueryEmbedding(embedding_profile=_PROFILE, query_vector=vectors[173 + query_index])
                pair = {}
                calls = None
                for frozen in (True, False) if query_index % 2 == 0 else (False, True):
                    result, elapsed, statements = await _search(
                        memory, head, query["text"], embedding, monkeypatch, frozen=frozen
                    )
                    recall, reciprocal, signature = _metrics(result, head, entries, by_text, set(query["relevant"]))
                    pair["baseline" if frozen else "candidate"] = (recall, reciprocal, signature, elapsed)
                    if not frozen:
                        calls = statements
                baseline, candidate = pair["baseline"], pair["candidate"]
                assert candidate[0] >= baseline[0] and candidate[1] >= baseline[1], (scope_index, query["id"], pair)
                repeat, _, _ = await _search(memory, head, query["text"], embedding, monkeypatch, frozen=False)
                assert _metrics(repeat, head, entries, by_text, set(query["relevant"]))[2] == candidate[2]
                if query_index == 0:
                    assert calls is not None
                    async with contexts.database.transaction() as connection:
                        plan = "\n".join(
                            str(row[0])
                            for row in (await connection.exec_driver_sql("EXPLAIN " + calls[0][0], calls[0][1])).all()
                        )
                    assert "TEXT RETRIEVAL SCAN" in plan and "VECTOR INDEX" in plan
                    plans.append(plan)
                results.append({
                    "scope": scope_index,
                    "query": query["id"],
                    "recall_at_10": candidate[0],
                    "mrr_at_10": candidate[1],
                    "baseline_recall_at_10": baseline[0],
                    "baseline_mrr_at_10": baseline[1],
                    "latency_ms": candidate[3],
                    "baseline_latency_ms": baseline[3],
                    "identical_ranking": candidate[2] == baseline[2],
                })
        await _filtered_candidates(contexts, model)
    assert len(results) == 60 and len(plans) == 3
    report = {
        "binding_version": version("pylibseekdb"),
        "documents": 173,
        "queries": 20,
        "scopes": 3,
        "timed_searches": len(results),
        "candidate_statements_per_hybrid_search": 1,
        "evidence_recall_at_10": sum(row["recall_at_10"] for row in results) / len(results),
        "mean_mrr_at_10": sum(row["mrr_at_10"] for row in results) / len(results),
        "p50_latency_ms": median(row["latency_ms"] for row in results),
        "baseline_p50_latency_ms": median(row["baseline_latency_ms"] for row in results),
        "identical_ranking_fraction": sum(row["identical_ranking"] for row in results) / len(results),
        "per_query": results,
    }
    print("HYBRID_NATIVE_REPORT=" + json.dumps(report, sort_keys=True))
    assert report["evidence_recall_at_10"] >= 0.95
    assert report["p50_latency_ms"] <= 500
