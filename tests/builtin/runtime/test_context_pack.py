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

"""Context pack interoperability, UTF-8 boundaries, and corrupt input handling."""

from __future__ import annotations

import base64
import hashlib
import json
import subprocess
import sys

import pytest

from powercontext.builtin.runtime.context_pack import (
    ContextPack,
    ContextPackDelta,
    ContextPackError,
    apply_context_pack_delta,
)


def test_pack_matches_independent_wire_encoder() -> None:
    items = (b"alpha\n", "海洋🌊".encode())
    unsigned = {"schema": "powercontext.context-pack.v1", "items": [base64.b64encode(x).decode() for x in items]}
    encode = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    payload = {**unsigned, "digest": "sha256:" + hashlib.sha256(encode(unsigned)).hexdigest(), "byte_count": 0}
    while payload["byte_count"] != len(encode(payload)):
        payload["byte_count"] = len(encode(payload))
    pack = ContextPack.build(items)
    assert pack.to_bytes() == encode(payload)
    assert pack.byte_count == len(pack.to_bytes())
    assert ContextPack.from_bytes(pack.to_bytes()) == pack


@pytest.mark.parametrize("limit", [4, 5, 7, 512, 1000])
def test_utf8_chunks_preserve_every_byte(limit: int) -> None:
    content = "a海é🌊\r\n\x00" * 200
    pack = ContextPack.from_content(content, chunk_bytes=limit)
    assert b"".join(pack.items) == content.encode()
    assert all(0 < len(item) <= limit and item.decode().encode() == item for item in pack.items)
    assert pack == ContextPack.from_content(content, chunk_bytes=limit)


@pytest.mark.parametrize("content,limit", [("海", 2), ("🌊", 3), ("x", 0), ("x", -1), ("x", True), ("x", 1.5)])
def test_invalid_chunk_limits_fail(content, limit) -> None:
    with pytest.raises(ContextPackError):
        ContextPack.from_content(content, chunk_bytes=limit)


@pytest.mark.parametrize("content", ["", None])
def test_empty_content_is_an_empty_pack(content: str | None) -> None:
    assert ContextPack.from_content(content).items == ()


@pytest.mark.parametrize("size", [0, 1, 600, 7000, 75000])
def test_byte_count_remains_exact_across_decimal_widths(size: int) -> None:
    pack = ContextPack.build((b"x" * size,))
    assert pack.byte_count == len(pack.to_bytes())
    assert ContextPack.from_bytes(pack.to_bytes()) == pack


@pytest.mark.parametrize("target", [(b"new", b"b", b"c"), (b"a", b"new", b"c"), (b"a", b"b", b"new"), (), (b"a",)])
def test_every_valid_prefix_rebuilds_exact_target(target: tuple[bytes, ...]) -> None:
    base = ContextPack.build((b"a", b"b", b"c"))
    pack = ContextPack.build(target)
    longest = ContextPackDelta.between(base, pack)
    for prefix in range(longest.prefix_items + 1):
        delta = ContextPackDelta(base.digest, pack.digest, prefix, target[prefix:])
        assert delta.byte_count == len(delta.to_bytes())
        assert apply_context_pack_delta(base, ContextPackDelta.from_bytes(delta.to_bytes())) == pack


@pytest.mark.parametrize("kind", ["pack", "delta"])
@pytest.mark.parametrize("bad_items", [None, {}, "eA==", [1], ["!"], ["海"], ["eA==\n"], ["eB=="], ["/w=="]])
def test_decoders_reject_bad_chunks(kind: str, bad_items: object) -> None:
    pack = ContextPack.build((b"x",))
    value = pack if kind == "pack" else ContextPackDelta.between(pack, pack)
    payload = json.loads(value.to_bytes())
    payload["items" if kind == "pack" else "appended"] = bad_items
    with pytest.raises(ContextPackError):
        type(value).from_bytes(json.dumps(payload).encode())


@pytest.mark.parametrize("kind", ["pack", "delta"])
def test_decoders_reject_missing_extra_and_duplicate_fields(kind: str) -> None:
    pack = ContextPack.build((b"x",))
    value = pack if kind == "pack" else ContextPackDelta.between(pack, pack)
    payload = json.loads(value.to_bytes())
    invalid = [{key: val for key, val in payload.items() if key != missing} for missing in payload]
    invalid.extend([{**payload, "extra": 1}, {**payload, "schema": "unknown"}])
    for bad in invalid:
        with pytest.raises(ContextPackError):
            type(value).from_bytes(json.dumps(bad).encode())
    for wire in (b"null", b"[]", b"{", b"\xff", value.to_bytes()[:-1] + b',"schema":"duplicate"}'):
        with pytest.raises(ContextPackError):
            type(value).from_bytes(wire)


@pytest.mark.parametrize("count", [True, 1.5, -1, 0, "200"])
def test_pack_rejects_incorrect_byte_count(count: object) -> None:
    payload = json.loads(ContextPack.build(()).to_bytes())
    payload["byte_count"] = count
    with pytest.raises(ContextPackError):
        ContextPack.from_bytes(json.dumps(payload).encode())


@pytest.mark.parametrize("prefix", [True, False, 1.5, -1, "1", None])
def test_delta_rejects_invalid_prefix_type_or_range(prefix: object) -> None:
    pack = ContextPack.build(())
    payload = json.loads(ContextPackDelta.between(pack, pack).to_bytes())
    payload["prefix_items"] = prefix
    with pytest.raises(ContextPackError):
        ContextPackDelta.from_bytes(json.dumps(payload).encode())


def test_apply_checks_base_bounds_and_target_digest() -> None:
    base = ContextPack.build((b"x",))
    other = ContextPack.build((b"y",))
    for delta in (
        ContextPackDelta(other.digest, base.digest, 1, ()),
        ContextPackDelta(base.digest, base.digest, 2, ()),
        ContextPackDelta(base.digest, other.digest, 1, ()),
    ):
        with pytest.raises(ContextPackError):
            apply_context_pack_delta(base, delta)


def test_receiver_rebuilds_in_a_fresh_python_process() -> None:
    base = ContextPack.from_content("旧正文🌊" * 200)
    target = ContextPack.from_content("旧正文🌊" * 200 + "\n新增引用")
    delta = ContextPackDelta.between(base, target)
    script = r"""
import sys
from powercontext.builtin.runtime.context_pack import ContextPack, ContextPackDelta, apply_context_pack_delta
base = ContextPack.from_bytes(sys.stdin.buffer.readline().rstrip(b'\n'))
delta = ContextPackDelta.from_bytes(sys.stdin.buffer.read())
sys.stdout.buffer.write(apply_context_pack_delta(base, delta).to_bytes())
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        input=base.to_bytes() + b"\n" + delta.to_bytes(),
        capture_output=True,
        check=True,
    )
    assert result.stdout == target.to_bytes()
