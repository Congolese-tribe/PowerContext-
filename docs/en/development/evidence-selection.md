---
title: Query-driven evidence selection
description: Select literal Memory fragments with exact citations and complete UTF-8 output budgets.
---

# Query-driven evidence selection

Set `evidence_selection=True` on the native Runtime's `PrepareContextRequest` to select
original evidence inside recalled Memory entries. The flag is a strict boolean and
defaults to `False`. Ordinary preparation retains its existing output when the flag is
omitted or false.

```python
from powercontext.builtin.runtime import PrepareContextRequest

prepared = await runtime.context.for_scope(scope_id).prepare(
    PrepareContextRequest(
        query="cobalt sensor calibration temperature",
        max_bytes=1400,
        evidence_selection=True,
    )
)
```

This mode selects only Memory. It takes precedence over `assembly` and `include_code`;
the native Runtime recalls Memory without fetching Code, Topic Memory, Experience,
or Profile for this request. Scope authorization and exact Memory revision checks
still apply. The selector itself uses no model, network, external corpus, or labels.
The HTTP/MCP request contracts are unchanged; this option belongs to the native API.

## Literal fragments and budget accounting

`PreparedContextBuilder.build_scopes_result` exposes `PreparedContextBuild.fragments`.
Each `PreparedEvidenceFragment` contains:

- `address`: a `MemoryEntryAddress` identifying scope, Memory revision, entry ID,
  and entry version ID.
- `start`, `end`: half-open Python character offsets in the original entry text.
- `text`: exactly `entry.text[start:end]`, including original whitespace.

`origins` and `fragments` correspond item by item. Duplicate candidate identities are
removed using scope and exact revision/version; identical text in another scope retains
its own identity. A single entry may contribute several separate fragments. Adjacent
or overlapping selected intervals are merged before rendering.

Nonempty output retains the existing trust policy and
`BEGIN_POWERCONTEXT_PREPARED_CONTEXT_V1` / `END_POWERCONTEXT_PREPARED_CONTEXT_V1`
envelope. The middle JSON has `trust="untrusted_history"` and `items`. Each item has
`citation` (`scope_id`, `memory_ref`, `entry_id`, `entry_version_id`), `content`, `start`,
and `end`. Literal text is JSON-escaped for transport and recovered unchanged by decoding.

Every admission attempt measures the complete rendered output: trust text, envelope,
JSON, scope and citation identifiers, offsets, and UTF-8 content. `content_bytes` equals
the final UTF-8 byte length and does not exceed `max_bytes`. If no relevant fragment
fits, the result is the ordinary empty context: `content=None`, `content_bytes=0`, and
empty origins/fragments. Disabled selection always returns an empty fragment tuple.

The native incremental pack API also accepts an evidence-selection request: it packages
the same final evidence body and retains the existing verified reconstruction contract.

## Selection policy and work

The selector scores original sentences using query terms, light English inflection
normalization, local sentence/document inverse frequencies, and length-normalized term
frequency. Identifier-shaped query terms receive extra weight. A small neighbouring
sentence credit helps retain explanatory continuations. These transformations affect
ranking only; delivered evidence is never rewritten.

Sentences that cannot fit by themselves are partitioned near word boundaries so that
interior evidence in a long paragraph remains selectable. The greedy budget search
considers the highest-scoring 96 units and prefers relevance relative to marginal wire
cost. Merging adjacent units lets them share citation overhead. Stable candidate and
character ordering make results deterministic.

Ranking scans all candidate text. The existing Memory candidate limit remains in force;
budget exploration is bounded to 96 scored units and the existing output item limit.
Each proposal measures its actual serialization, so extra CPU work compared with prefix
truncation is intentional. This lexical policy does not guarantee semantic entailment
or evidence coverage for an arbitrary query.

## Independent public-data quality check

`tests/fixtures/evidence_selection/scifact-dev.json` contains 24 original public SciFact
development claims, each with three original abstracts. The fixture README records
provenance, licensing, deterministic distractor selection, and the independent gold
budget feasibility check. Gold intervals stay in tests and are never passed to the
selector or stored in product source.

The supplemental regression makes 48 requests at 1400 and 1800 bytes. It merges selected
intervals per original document, counts duplicate characters once, and compares lowercase
`[a-z]+|\d+` token multisets with public gold evidence. Full coverage requires every gold
character interval, including original whitespace, to be covered.

Observed with Python 3.12.14 on Windows:

| Measure | Result | Required threshold |
| --- | ---: | ---: |
| Mean evidence token recall | 68.45% | 65% |
| Complete evidence requests | 26 / 48 (54.17%) | 40% |
| Queries and budgets | 24 × 2 | 48 requests |

These are supplemental public-development measurements, separate from the competition's
three public acceptance tests and hidden 48-request quality sequence. They do not
establish the hidden evaluator's result.

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q -s \
  -p pytest_asyncio.plugin tests/builtin/runtime/test_evidence_selection.py \
  tests/e2e/test_evidence_selection.py
make -C competition test-evidence
```
