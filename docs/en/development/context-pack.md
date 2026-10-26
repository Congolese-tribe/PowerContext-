---
title: Incremental context packs
description: Serialize prepared context and reconstruct it with verified prefix deltas.
---

# Incremental context packs

`ScopedContextApplication.prepare_incremental(request, base_pack=None)` calls ordinary
`prepare` and packages its exact content, including citations. With no base it returns a
`ContextPack`; with a complete base it returns a `ContextPackDelta`. A legitimate
`content=None` becomes an empty pack. Scope authorization and all existing preparation
rules apply. The optional `authorize_scopes` callback is forwarded to `prepare`.

```python
from powercontext.builtin.runtime import (
    ContextPack,
    ContextPackDelta,
    PrepareContextRequest,
    apply_context_pack_delta,
)

# Sender and receiver communicate only through these serialized bytes.
request = PrepareContextRequest(query="remote", max_bytes=8192)
first = await context.prepare_incremental(request)
receiver = ContextPack.from_bytes(first.to_bytes())
update = await context.prepare_incremental(request, base_pack=receiver)
receiver = apply_context_pack_delta(
    receiver, ContextPackDelta.from_bytes(update.to_bytes())
)
body = b"".join(receiver.items).decode("utf-8")
```

## Wire contract

The public types also live in `powercontext.builtin.runtime.context_pack`.
`ContextPack.build(items)` preserves the supplied tuple of UTF-8 byte chunks.
`ContextPack.from_content(content, chunk_bytes=512)` greedily fills chunks without
splitting UTF-8 characters. The limit must be a positive integer, excluding booleans;
a character wider than the limit raises `ContextPackError`. Empty text produces no chunks.

Both wire representations use UTF-8 JSON with sorted keys, compact separators, and
standard canonical Base64 strings for chunks. Full packs contain exactly `schema`,
`items`, `digest`, and `byte_count`; their schema is `powercontext.context-pack.v1`.
The `sha256:<hex>` digest hashes canonical JSON containing only `schema` and `items`.
`byte_count` includes its own field and decimal digits; it equals `len(pack.to_bytes())`.

Deltas contain exactly `schema`, `base_digest`, `target_digest`, `prefix_items`, and
`appended`; their schema is `powercontext.context-pack-delta.v1`. Their `byte_count`
property measures `len(delta.to_bytes())` and is never added to the JSON. The sender
selects the longest equal chunk prefix. Receivers accept any valid prefix, including
zero, and reconstruct the prefix plus replacement suffix. Rebuilding verifies both
base and target digests, including the exact target chunk boundaries.

Missing, extra, or duplicate fields, wrong schemas, invalid UTF-8 or Base64, invalid
prefix types or bounds, incorrect sizes, and digest mismatches raise `ContextPackError`.
Decoders accept JSON whitespace and key ordering variations but verify full-pack
`byte_count` against the canonical serialization. Chunks and deltas are immutable;
reconstruction requires no sender cache or persisted artifact. This API belongs to the
native Python runtime; the HTTP and MCP contracts expose ordinary context preparation.

Packaging and verification perform linear work in the content size; finding the prefix
compares at most the smaller pack's chunk count. Incremental delivery reduces wire
bytes for repeated content, while ordinary preparation and digest construction still
process the target content each round. Changes near the start can replace most chunks.

## Reproducible cost measurement

`tests/e2e/test_context_pack.py` uses actual native SQLite capture, Memory flushing,
FTS retrieval, and ordinary preparation. A deterministic extraction pipeline preserves
source turns verbatim instead of calling a model. The QMSum fixture and MIT license
are in `tests/fixtures/context_pack/`. This is an additional local regression sequence,
separate from the competition's public acceptance tests and hidden cost sequence.

The fixture records twelve turns in original order from ES2004a at QMSum commit
`83d7768c1f2b4dfeb091385d3dc7e239b8e5bb7e`, with speaker and complete turn text.
Eight turns initialize Memory. Before zero-based rounds 8, 18, 28, and 38, one further
turn is appended and flushed. Queries are `remote`, `control`, `mobile`, and `buttons`,
each for ten rounds, with an 8192-byte preparation budget. All forty outputs must be
nonempty; every wire message must reconstruct the ordinary content and full target pack.

Observed with Python 3.12.14 on Windows and native SQLite:

| Measure | Bytes | Saving from incremental delivery |
| --- | ---: | ---: |
| Ordinary UTF-8 bodies | 55,662 | 54.39% |
| Complete v1 packs every round | 80,876 | 68.61% |
| Independent gzip per body | 29,580 | 14.16% |
| First complete pack plus 39 deltas | 25,390 | — |

The run produced eight distinct contexts. All protocol fields, Base64, checksums, and
the first full pack count toward transmitted bytes. Gzip uses
`gzip.compress(content, compresslevel=9, mtime=0)` independently each round.
Each saving is `1 - incremental_bytes / comparison_bytes`; negative values indicate
larger incremental output. This measurement does not establish the hidden evaluator's
result or a general compression advantage over gzip.

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q -s -p pytest_asyncio.plugin \
  tests/builtin/runtime/test_context_pack.py tests/e2e/test_context_pack.py
make -C competition test-incremental
```
