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

"""Offline CSV capture with stable primary-key identities and complete row evidence."""

from __future__ import annotations

import csv
import io
import json
from typing import Self
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, model_validator

from powercontext.limits import MAX_SOURCE_ID_LENGTH
from powercontext.sources.definitions import SourceDefinition, SourceProjection
from powercontext.sources.models import Source, SourceMaterialization
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY, TextEvidence


class CSVDocumentInput(BaseModel):
    """Complete UTF-8 CSV bytes and the exact name of their primary-key column."""

    model_config = ConfigDict(frozen=True)

    document_id: str
    primary_key: str
    content: bytes


def _validate_document_id(document_id: str) -> None:
    if not document_id or document_id != document_id.strip():
        raise ValueError("document_id must be non-empty and have no surrounding whitespace")  # noqa: TRY003


def csv_row_source_id(document_id: str, primary_key_value: str) -> str:
    """Encode the two identity components separately, rejecting oversized identities."""
    _validate_document_id(document_id)
    if not primary_key_value.strip():
        raise ValueError("primary_key_value must not be blank")  # noqa: TRY003
    identity = f"csv:{quote(document_id, safe='')}:{quote(primary_key_value, safe='')}"
    if len(identity) > MAX_SOURCE_ID_LENGTH:
        raise ValueError(f"CSV row identity must not exceed {MAX_SOURCE_ID_LENGTH} characters")  # noqa: TRY003
    return identity


def _validate_columns(columns: tuple[str, ...], primary_key: str) -> int:
    if not columns or any(not column.strip() for column in columns):
        raise ValueError("CSV column names must not be blank")  # noqa: TRY003
    if len(set(columns)) != len(columns):
        raise ValueError("CSV column names must be unique")  # noqa: TRY003
    if not primary_key or primary_key not in columns:
        raise ValueError("primary_key must exactly match a CSV column name")  # noqa: TRY003
    return columns.index(primary_key)


class CSVRowInput(BaseModel):
    """One validated record, retaining original column order and string values."""

    model_config = ConfigDict(frozen=True)

    document_id: str
    primary_key: str
    primary_key_value: str
    columns: tuple[str, ...]
    values: tuple[str, ...]

    @model_validator(mode="after")
    def validate_record(self) -> Self:
        key_index = _validate_columns(self.columns, self.primary_key)
        if len(self.values) != len(self.columns):
            raise ValueError("CSV record width must match its header")  # noqa: TRY003
        if self.values[key_index] != self.primary_key_value:
            raise ValueError("primary_key_value must match the record's primary-key field")  # noqa: TRY003
        csv_row_source_id(self.document_id, self.primary_key_value)
        return self


def parse_csv_document(value: CSVDocumentInput) -> tuple[CSVRowInput, ...]:
    """Parse and validate the entire document atomically, or raise ``ValueError``.

    Only physically empty records after the header are ignored. Whitespace,
    quoted empty strings, and delimiters remain meaningful CSV field content.
    """
    _validate_document_id(value.document_id)
    try:
        # newline="" preserves CR/LF inside quoted fields rather than normalizing it.
        with io.StringIO(value.content.decode("utf-8-sig"), newline="") as content:
            reader = csv.reader(content, delimiter=",", quotechar='"', doublequote=True, strict=True)
            columns = tuple(next(reader, ()))
            key_index = _validate_columns(columns, value.primary_key)
            rows: list[CSVRowInput] = []
            seen_keys: set[str] = set()
            for record in reader:
                if not record:
                    continue
                if len(record) != len(columns):
                    raise ValueError("CSV record width must match its header")  # noqa: TRY003
                key = record[key_index]
                if key in seen_keys:
                    raise ValueError("CSV primary-key values must be unique within a document")  # noqa: TRY003
                rows.append(
                    CSVRowInput(
                        document_id=value.document_id,
                        primary_key=value.primary_key,
                        primary_key_value=key,
                        columns=columns,
                        values=tuple(record),
                    )
                )
                seen_keys.add(key)
    except (UnicodeDecodeError, csv.Error) as error:
        raise ValueError("Cannot decode or parse CSV document") from error  # noqa: TRY003
    return tuple(rows)


class CSVRowSource(Source):
    """A complete captured row, identified by document and original primary-key value."""

    source_id: str
    document_id: str
    primary_key: str
    primary_key_value: str
    columns: tuple[str, ...]
    values: tuple[str, ...]


class _CSVRowTextProjection:
    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class: type[CSVRowSource] = CSVRowSource
    output_class: type[BaseModel] = TextEvidence

    def project(self, source: CSVRowSource, /) -> TextEvidence:
        row = dict(zip(source.columns, source.values, strict=True))
        return TextEvidence(
            source_type="csv-row",
            source_id=source.source_id,
            content=json.dumps(row, ensure_ascii=False, separators=(",", ":")),
            metadata={
                "document_id": source.document_id,
                "primary_key": source.primary_key,
                "primary_key_value": source.primary_key_value,
                "columns": list(source.columns),
                "row": row,
            },
        )


class _CSVRowDefinition:
    input_class: type[CSVRowInput] = CSVRowInput
    name: str = "csv-row"
    version: str = "1"
    source_class: type[CSVRowSource] = CSVRowSource
    projections: tuple[SourceProjection[CSVRowSource], ...] = (_CSVRowTextProjection(),)

    async def resolve(self, value: CSVRowInput, /) -> CSVRowSource:
        identity = csv_row_source_id(value.document_id, value.primary_key_value)
        return CSVRowSource(
            name=identity,
            source_id=identity,
            materialization=SourceMaterialization.CAPTURED,
            **value.model_dump(),
        )

    async def read(self, source: CSVRowSource, /) -> CSVRowInput:
        return CSVRowInput(
            document_id=source.document_id,
            primary_key=source.primary_key,
            primary_key_value=source.primary_key_value,
            columns=source.columns,
            values=source.values,
        )


CSV_ROW_SOURCE_DEFINITION: SourceDefinition[CSVRowInput, CSVRowSource, CSVRowInput] = _CSVRowDefinition()

__all__ = [
    "CSV_ROW_SOURCE_DEFINITION",
    "CSVDocumentInput",
    "CSVRowInput",
    "CSVRowSource",
    "csv_row_source_id",
    "parse_csv_document",
]
