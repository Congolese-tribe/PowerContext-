---
title: Single-statement seekdb hybrid retrieval
description: Native full-text and vector candidates with shared filtering and current Memory identities.
---

# Single-statement seekdb hybrid retrieval

The native Runtime uses `SeekDBMemoryIndex` when configured with `SeekDBConfig` and
an embedding model. A `Memory.search(..., mode="hybrid")` candidate read executes
one parameterized SQL statement through the product's existing `AsyncConnection`.
No extra database or query service is started.

The statement combines two independently ordered, limited subqueries with `UNION ALL`.
The full-text branch uses `MATCH ... AGAINST`; the vector branch uses native L2
`ORDER BY ... APPROXIMATE`. Each branch applies scope, requested Memory artifact IDs,
optional all/any tag predicates, and the candidate limit inside the database. Returned
rows carry their channel and distance. An outer ordering preserves channel ranks and
breaks equal scores by Memory artifact, entry, and entry-version identity.

Shared Memory admission, reciprocal-rank fusion, identity deduplication, final limiting,
and exact body reads remain in the existing product flow. Head validation and vector
completeness checks can execute additional statements; they are not candidate queries.
FTS-only and vector-only calls delegate to the existing indexes. SQLite, OceanBase,
and seekdb configurations without an embedding model retain their existing composition.

## Current identity and incremental projection

Projection writes, updates, deletion, restoration, schema, and index initialization reuse
the existing implementations. An unchanged active entry may retain an older projection
`head_revision` after another entry is appended. Candidate SQL therefore does not filter
projection rows by the requested head revision. The existing backend validates the
requested Memory head, and `memory_channel_hits` attaches that validated current head
to returned identities. Each row joins its exact immutable entry version for text.
Historical heads remain unsupported under the existing capability contract.

## Validation

The `Competition seekdb hybrid` workflow uses the published competition container,
Python 3.12, and `pylibseekdb==1.3.0.post4`, with the published one-CPU, 2 GiB resource
limits and network disabled during native execution. It first runs the original public
`retrieval-02-hybrid-search` acceptance without changing evaluation assets. That test
checks native EXPLAIN plans, state changes, current references, filtering, scope isolation,
historical-head rejection, and single-channel behavior.

The additional `tests/e2e/test_seekdb_hybrid.py` uses 173 original SciFact documents,
20 public development claims, fixed 384-dimensional unit L2 vectors, and three scopes.
It compares 60 full search calls with the unchanged two-query composition against the
same database, current heads, and embeddings. Both paths are warmed, execution order
alternates, every query checks Recall@10 and MRR@10 for regression, and candidate SQL
counts are captured at the actual SQLAlchemy execution boundary. Repeated searches
check stable identities and ordering. Separate tagged plans check both native indexes,
all/any tags, candidate limits, and exclusion of another artifact in the same scope.

The supplemental fixture uses lexical TF-IDF/SVD vectors and public relevance labels
only for test scoring. Production code reads no fixtures or relevance labels. Its
measurements do not substitute for the competition's unavailable formal corpus and
embeddings. Logs include per-query measurements, median complete-search latency,
baseline latency, and full-ranking equality; one candidate statement alone does not
establish an end-to-end speedup. Dataset attribution and licenses are recorded in
`tests/fixtures/seekdb_hybrid/README.md`.

Run the public check in the fixed Linux environment with:

```bash
POWERCONTEXT_CANDIDATE="$PWD" docker compose -f competition/compose.yaml run --rm selftest \
  python -m pytest -q -s -c /selftest/pytest.ini --confcutdir=/selftest \
  /selftest/challenges/retrieval-02-hybrid-search/acceptance
```
