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

"""Persist complete row projections and read exact citations from a new process."""

import asyncio
import csv
import io
import json
import os
import subprocess
import sys
from pathlib import Path

from powercontext import CSV_ROW_SOURCE_DEFINITION, CSVDocumentInput, csv_row_source_id, parse_csv_document
from powercontext.artifacts import MemoryCitation
from powercontext.builtin.artifacts.memory import MemoryEntryInput
from powercontext.builtin.persistence.sqlite import SQLiteConfig
from powercontext.builtin.runtime import (
    BuiltinConfig,
    GetMemoryEntryRequest,
    SearchMemoryRequest,
    SubmitSourceObservation,
    open_builtin_runtime,
)
from powercontext.builtin.scope import ScopeDraft
from powercontext.sources import (
    TEXT_EVIDENCE_PROJECTION_KEY,
    SourceDefinitionRegistry,
    manifest_for_definition,
    project_source_for_transport,
)

_READ_AFTER_RESTART = """
import asyncio
import json
import sys
from pathlib import Path

from powercontext.artifacts import MemoryCitation
from powercontext.builtin.persistence.sqlite import SQLiteConfig
from powercontext.builtin.runtime import (
    BuiltinConfig, GetMemoryEntryRequest, open_builtin_contexts, open_builtin_runtime,
)
from powercontext.sources import TEXT_EVIDENCE_PROJECTION_KEY

data = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
config = BuiltinConfig(database=SQLiteConfig(url=data["database_url"]))

async def verify():
    async with open_builtin_contexts(config) as contexts:
        journal = await (await contexts.get(data["scope_id"])).sources.journal.entries()
        actual = {
            item.source_ref.source_id: item.source.projection(TEXT_EVIDENCE_PROJECTION_KEY)
            for item in journal
        }
        assert actual == data["projections"]
        for source_id, evidence in actual.items():
            assert list(evidence["metadata"]["row"]) == evidence["metadata"]["columns"]
    async with open_builtin_runtime(config) as runtime:
        memory = runtime.memory.for_scope(data["scope_id"])
        for source_id, record in data["entries"].items():
            citation = MemoryCitation.model_validate(record["citation"])
            entry = (await memory.get(GetMemoryEntryRequest(citation=citation))).entry
            assert entry.text == data["projections"][source_id]["content"]
            assert any(source.source_id == source_id for source in entry.sources)

asyncio.run(verify())
"""


def test_complete_rows_and_exact_citations_survive_a_new_process(tmp_path: Path) -> None:
    columns = ("note", " id ", "名称", "amount", "empty")
    keys = (" key ", "key ", "01", "1", "a:b", "a/b", "a%b", "北京", "false", "0", "true", "null")
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\r\n")
    writer.writerow(columns)
    writer.writerows(
        (f'runwaycharlie record{index}\r\n"line"', key, "机场", "001.00", "") for index, key in enumerate(keys)
    )
    rows = parse_csv_document(
        CSVDocumentInput(
            document_id="airports:/%表",
            primary_key=" id ",
            content=output.getvalue().encode("utf-8-sig"),
        )
    )
    expected = {}
    for row in rows:
        identity = csv_row_source_id(row.document_id, row.primary_key_value)
        mapping = dict(zip(columns, row.values, strict=True))
        expected[identity] = {
            "source_type": "csv-row",
            "source_id": identity,
            "content": json.dumps(mapping, ensure_ascii=False, separators=(",", ":")),
            "metadata": {
                "document_id": row.document_id,
                "primary_key": row.primary_key,
                "primary_key_value": row.primary_key_value,
                "columns": list(columns),
                "row": mapping,
            },
        }
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'rows.db'}"
    config = BuiltinConfig(database=SQLiteConfig(url=database_url))

    class RowPipeline:
        async def extract(self, request, /) -> tuple[MemoryEntryInput, ...]:
            return tuple(
                MemoryEntryInput(
                    kind="csv-row",
                    text=source.projection(TEXT_EVIDENCE_PROJECTION_KEY)["content"],
                    sources=(source,),
                )
                for source in request.sources
            )

    async def capture_and_search() -> dict[str, object]:
        registry = SourceDefinitionRegistry((CSV_ROW_SOURCE_DEFINITION,))
        async with open_builtin_runtime(config) as runtime:
            assert runtime.scopes is not None
            scope = await runtime.scopes.create(
                ScopeDraft(title="CSV rows", summary="Original row evidence", idempotency_key="csv-rows")
            )
            await runtime.ingestion.register(manifest_for_definition(CSV_ROW_SOURCE_DEFINITION))
            # Ingestion order is independent of the row's identity and original column order.
            for row in reversed(rows):
                source = await registry.resolve(row)
                await runtime.ingestion.submit(
                    SubmitSourceObservation(
                        scope_id=scope.scope_id,
                        observation=project_source_for_transport(registry, source),
                    )
                )
        async with open_builtin_runtime(config, candidate_pipeline=RowPipeline()) as runtime:
            memory = runtime.memory.for_scope(scope.scope_id)
            for _ in range(len(rows) + 1):
                if not (await memory.flush()).processed:
                    break
            else:
                raise AssertionError("Row processing did not drain")  # noqa: TRY003
            hits = (await memory.search(SearchMemoryRequest(query="runwaycharlie", mode="fts", limit=20))).hits
            entries = {}
            for hit in hits:
                citation = MemoryCitation(
                    memory_ref=hit.memory_ref,
                    entry_id=hit.entry_id,
                    entry_version_id=hit.entry_version_id,
                )
                entry = (await memory.get(GetMemoryEntryRequest(citation=citation))).entry
                assert len(entry.sources) == 1
                source_id = entry.sources[0].source_id
                assert entry.text == expected[source_id]["content"]
                entries[source_id] = {"citation": citation.model_dump(mode="json")}
            assert set(entries) == set(expected)
        return {"database_url": database_url, "scope_id": scope.scope_id, "projections": expected, "entries": entries}

    payload = tmp_path / "restart.json"
    payload.write_text(json.dumps(asyncio.run(capture_and_search()), ensure_ascii=False), encoding="utf-8")
    environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2] / "src"))
    result = subprocess.run(
        [sys.executable, "-c", _READ_AFTER_RESTART, str(payload)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
