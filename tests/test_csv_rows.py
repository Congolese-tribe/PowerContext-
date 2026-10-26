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

"""CSV row contracts beyond the published airport workload."""

import asyncio
import json
from urllib.parse import quote

import pytest

from powercontext import CSV_ROW_SOURCE_DEFINITION, CSVDocumentInput, CSVRowInput, csv_row_source_id, parse_csv_document
from powercontext.sources import (
    TEXT_EVIDENCE_PROJECTION_KEY,
    SourceDefinitionRegistry,
    SourceMaterialization,
    TextEvidence,
)
from powercontext.sources.csv_rows import CSVRowSource


def test_original_fields_survive_csv_and_source_serialization() -> None:
    rows = parse_csv_document(
        CSVDocumentInput(
            document_id="机场:百分%/表",
            primary_key=" id ",
            content=(
                '\ufeff" id ",id,名称,empty,number,note\r\n'
                '\r\n" 001 ",other,北京,,0001.00,"first\r\nsecond ""quoted"""\r\n'
            ).encode(),
        )
    )
    assert len(rows) == 1
    row = rows[0]
    assert row.primary_key == " id "
    assert row.primary_key_value == " 001 "
    assert row.columns == (" id ", "id", "名称", "empty", "number", "note")
    assert row.values == (" 001 ", "other", "北京", "", "0001.00", 'first\r\nsecond "quoted"')
    registry = SourceDefinitionRegistry((CSV_ROW_SOURCE_DEFINITION,))
    source = asyncio.run(registry.resolve(row))
    assert isinstance(source, CSVRowSource)
    assert source.materialization == SourceMaterialization.CAPTURED
    restored = CSVRowSource.model_validate_json(source.model_dump_json())
    assert asyncio.run(registry.read(restored)) == row
    evidence = TextEvidence.model_validate(registry.project(restored, TEXT_EVIDENCE_PROJECTION_KEY))
    expected_row = dict(zip(row.columns, row.values, strict=True))
    assert evidence.content == json.dumps(expected_row, ensure_ascii=False, separators=(",", ":"))
    assert evidence.source_id == source.name == source.source_id
    projected_row = evidence.metadata["row"]
    assert isinstance(projected_row, dict)
    assert list(projected_row) == list(row.columns)
    assert evidence.metadata == {
        "document_id": row.document_id,
        "primary_key": row.primary_key,
        "primary_key_value": row.primary_key_value,
        "columns": list(row.columns),
        "row": expected_row,
    }


@pytest.mark.parametrize(
    "content",
    [
        b"",
        b"\xef\xbb\xbf",
        b"\n",
        b'""\n',
        b"id,id\n",
        b"id, \t\n",
        b"id,\n",
        b"id,name\n1,ok\n1,duplicate\n",
        b"id,name\n1,ok\n\t ,blank-key\n",
        b"id,name\n1,ok\n2\n",
        b"id,name\n1,ok\n2,too,wide\n",
        b'id,name\n1,ok\n2,"unclosed\n',
        b'id,name\n1,ok\n2,"closed"junk\n',
        b"id,name\n1,ok\n2,\xff\n",
        b"id,name\n1,ok\n2,\xc0\xaf\n",
        b"id,name\n1,ok\n2,\xed\xa0\x80\n",
        b'id\nvalid\n""\n',
        b"id\nvalid\n \n",
        b"id\nvalid\n" + b"x" * 300 + b"\n",
    ],
)
def test_document_errors_fail_atomically(content: bytes) -> None:
    with pytest.raises(ValueError):
        parse_csv_document(CSVDocumentInput(document_id="doc", primary_key="id", content=content))


@pytest.mark.parametrize("document_id", ["", " ", " doc", "doc\t", "\u2003doc", "doc\n"])
def test_invalid_document_identity_rejected_even_without_data(document_id: str) -> None:
    with pytest.raises(ValueError):
        parse_csv_document(CSVDocumentInput(document_id=document_id, primary_key="id", content=b"id\n"))


@pytest.mark.parametrize("primary_key", ["", "id", " id", "id ", "missing"])
def test_primary_key_matches_header_exactly(primary_key: str) -> None:
    with pytest.raises(ValueError):
        parse_csv_document(CSVDocumentInput(document_id="doc", primary_key=primary_key, content=b'" id "\n'))


@pytest.mark.parametrize("ending", ["\n", "\r\n", "\r"])
def test_physical_blank_records_are_ignored_but_quoted_content_is_preserved(ending: str) -> None:
    content = ending.join(["id", "", '" first' + ending + 'second "', "", "last", ""])
    rows = parse_csv_document(CSVDocumentInput(document_id="doc", primary_key="id", content=content.encode()))
    assert [row.values for row in rows] == [(f" first{ending}second ",), ("last",)]
    assert (
        parse_csv_document(
            CSVDocumentInput(document_id="doc", primary_key="id", content=f"id{ending}{ending}".encode())
        )
        == ()
    )


def test_csv_reader_unquoted_quotes_are_literal() -> None:
    row = parse_csv_document(
        CSVDocumentInput(document_id="doc", primary_key="id", content=b'id,note\n1,unquoted"literal\n')
    )[0]
    assert row.values == ("1", 'unquoted"literal')


def test_identity_framing_and_exact_length_boundary() -> None:
    assert csv_row_source_id("a:b", "c") != csv_row_source_id("a", "b:c")
    assert csv_row_source_id("a%3Ab", "c") != csv_row_source_id("a:b", "c")
    document_id, key = "文档:/%", " 值:/% "
    assert csv_row_source_id(document_id, key) == f"csv:{quote(document_id, safe='')}:{quote(key, safe='')}"
    assert len(csv_row_source_id("d", "x" * 250)) == 256
    with pytest.raises(ValueError):
        csv_row_source_id("d", "x" * 251)
    assert len(csv_row_source_id("d", "界" * 27 + "x" * 7)) == 256
    with pytest.raises(ValueError):
        csv_row_source_id("d", "界" * 28)
    for empty_key in ("", " ", "\t\r\n", "\u2003"):
        with pytest.raises(ValueError):
            csv_row_source_id("doc", empty_key)


@pytest.mark.parametrize(
    "changes",
    [
        {"columns": ("id", "id")},
        {"values": ("1",)},
        {"primary_key": "missing"},
        {"primary_key_value": "different"},
        {"primary_key_value": " ", "values": (" ", "note")},
    ],
)
def test_direct_row_inputs_validate_before_resolution(changes: dict[str, object]) -> None:
    data = {
        "document_id": "doc",
        "primary_key": "id",
        "primary_key_value": "1",
        "columns": ("id", "note"),
        "values": ("1", "note"),
    }
    with pytest.raises(ValueError):
        CSVRowInput.model_validate(data | changes)
