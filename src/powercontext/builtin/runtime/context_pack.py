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

"""Portable, verified context bytes and stateless prefix deltas."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from dataclasses import dataclass, field

_PACK_SCHEMA = "powercontext.context-pack.v1"
_DELTA_SCHEMA = "powercontext.context-pack-delta.v1"
_PACK_FIELDS = {"schema", "items", "digest", "byte_count"}
_DELTA_FIELDS = {"schema", "base_digest", "target_digest", "prefix_items", "appended"}


class ContextPackError(ValueError):
    """Invalid context chunk, wire payload, or delta reconstruction."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"Invalid context pack: {code}")


def _json_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ContextPackError("duplicate-field")
        result[key] = value
    return result


def _decode_payload(data: bytes, schema: str, fields: set[str]) -> dict[str, object]:
    if not isinstance(data, bytes):
        raise ContextPackError("wire-type")
    try:
        payload = json.loads(data.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeError, ValueError, RecursionError) as error:
        raise ContextPackError("invalid-json") from error
    if not isinstance(payload, dict) or set(payload) != fields or payload["schema"] != schema:
        raise ContextPackError("schema-or-fields")
    return payload


def _validate_items(items: tuple[bytes, ...]) -> None:
    if not isinstance(items, tuple) or any(not isinstance(item, bytes) for item in items):
        raise ContextPackError("chunk-types")
    try:
        for item in items:
            item.decode("utf-8")
    except UnicodeError as error:
        raise ContextPackError("chunk-utf8") from error


def _encoded_items(items: tuple[bytes, ...]) -> list[str]:
    return [base64.b64encode(item).decode("ascii") for item in items]


def _decoded_items(value: object) -> tuple[bytes, ...]:
    if not isinstance(value, list):
        raise ContextPackError("chunk-array")
    items: list[bytes] = []
    for encoded in value:
        if not isinstance(encoded, str):
            raise ContextPackError("chunk-encoding-type")
        try:
            item = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as error:
            raise ContextPackError("chunk-base64") from error
        if base64.b64encode(item).decode("ascii") != encoded:
            raise ContextPackError("chunk-base64-canonical")
        items.append(item)
    return tuple(items)


def _validate_digest(digest: object) -> None:
    if not isinstance(digest, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
        raise ContextPackError("digest-format")


@dataclass(frozen=True)
class ContextPack:
    """Immutable UTF-8 chunks with a canonical digest and complete wire byte count.

    The digest identifies chunk boundaries as well as content. ``byte_count``
    includes its own decimal representation in the serialized pack.
    """

    items: tuple[bytes, ...]
    digest: str = field(init=False)
    byte_count: int = field(init=False)

    def __post_init__(self) -> None:
        _validate_items(self.items)
        payload: dict[str, object] = {"schema": _PACK_SCHEMA, "items": _encoded_items(self.items)}
        digest = "sha256:" + hashlib.sha256(_json_bytes(payload)).hexdigest()
        object.__setattr__(self, "digest", digest)
        payload["digest"] = digest
        count = 0
        while True:
            payload["byte_count"] = count
            size = len(_json_bytes(payload))
            if count == size:
                break
            count = size
        object.__setattr__(self, "byte_count", count)

    @classmethod
    def build(cls, items: tuple[bytes, ...]) -> ContextPack:
        """Build a verified pack without changing caller-supplied boundaries."""
        return cls(items)

    @classmethod
    def from_content(cls, content: str | None, *, chunk_bytes: int = 512) -> ContextPack:
        """Greedily split text at UTF-8 boundaries within ``chunk_bytes``.

        ``None`` and empty text produce an empty pack. A character that cannot
        fit in one chunk is an error, even if the remaining text would fit.
        """
        if type(chunk_bytes) is not int or chunk_bytes <= 0:
            raise ContextPackError("chunk-limit")
        if content is not None and not isinstance(content, str):
            raise ContextPackError("content-type")
        try:
            data = (content or "").encode("utf-8")
        except UnicodeError as error:
            raise ContextPackError("content-utf8") from error
        items: list[bytes] = []
        start = 0
        while start < len(data):
            end = min(start + chunk_bytes, len(data))
            while end < len(data) and data[end] & 0xC0 == 0x80:
                end -= 1
            if end == start:
                raise ContextPackError("chunk-boundary")
            items.append(data[start:end])
            start = end
        return cls.build(tuple(items))

    def to_bytes(self) -> bytes:
        """Serialize the complete v1 pack as canonical UTF-8 JSON."""
        return _json_bytes({
            "schema": _PACK_SCHEMA,
            "items": _encoded_items(self.items),
            "digest": self.digest,
            "byte_count": self.byte_count,
        })

    @classmethod
    def from_bytes(cls, data: bytes) -> ContextPack:
        """Decode and verify fields, chunks, digest, and canonical wire size."""
        payload = _decode_payload(data, _PACK_SCHEMA, _PACK_FIELDS)
        pack = cls.build(_decoded_items(payload["items"]))
        if payload["digest"] != pack.digest:
            raise ContextPackError("pack-digest")
        if type(payload["byte_count"]) is not int or payload["byte_count"] != pack.byte_count:
            raise ContextPackError("pack-byte-count")
        return pack


@dataclass(frozen=True)
class ContextPackDelta:
    """A base digest, reusable prefix length, and complete replacement suffix."""

    base_digest: str
    target_digest: str
    prefix_items: int
    appended: tuple[bytes, ...]

    def __post_init__(self) -> None:
        _validate_digest(self.base_digest)
        _validate_digest(self.target_digest)
        if type(self.prefix_items) is not int or self.prefix_items < 0:
            raise ContextPackError("prefix-type-or-range")
        _validate_items(self.appended)

    @classmethod
    def between(cls, base: ContextPack, target: ContextPack) -> ContextPackDelta:
        """Reuse the longest equal prefix of two complete packs."""
        prefix = 0
        for left, right in zip(base.items, target.items, strict=False):
            if left != right:
                break
            prefix += 1
        return cls(base.digest, target.digest, prefix, target.items[prefix:])

    @property
    def byte_count(self) -> int:
        """Actual delta wire bytes, including every protocol field."""
        return len(self.to_bytes())

    def to_bytes(self) -> bytes:
        """Serialize v1 delta fields; byte_count is not a wire field."""
        return _json_bytes({
            "schema": _DELTA_SCHEMA,
            "base_digest": self.base_digest,
            "target_digest": self.target_digest,
            "prefix_items": self.prefix_items,
            "appended": _encoded_items(self.appended),
        })

    @classmethod
    def from_bytes(cls, data: bytes) -> ContextPackDelta:
        """Decode a delta; base bounds and target identity are checked on apply."""
        payload = _decode_payload(data, _DELTA_SCHEMA, _DELTA_FIELDS)
        base_digest = payload["base_digest"]
        target_digest = payload["target_digest"]
        prefix = payload["prefix_items"]
        if not isinstance(base_digest, str) or not isinstance(target_digest, str) or type(prefix) is not int:
            raise ContextPackError("delta-field-types")
        return cls(base_digest, target_digest, prefix, _decoded_items(payload["appended"]))


def apply_context_pack_delta(base: ContextPack, delta: ContextPackDelta) -> ContextPack:
    """Reconstruct and verify the exact target using only base and delta data."""
    if base.digest != delta.base_digest:
        raise ContextPackError("base-digest")
    if delta.prefix_items > len(base.items):
        raise ContextPackError("prefix-bounds")
    target = ContextPack.build(base.items[: delta.prefix_items] + delta.appended)
    if target.digest != delta.target_digest:
        raise ContextPackError("target-digest")
    return target
