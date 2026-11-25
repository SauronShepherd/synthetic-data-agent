# SDA 11 relational generator contract

This document records the executable contract implemented for SDA 11. It is
deliberately narrower than a promise to reproduce an entire source graph.

## Inputs

- An approved `GenerationPlan`.
- A frozen relationship/graph artifact whose plan fingerprint matches the
  plan and whose requested edges are accepted, validated, and not stale.
- Explicit key-domain, bridge, self-reference, and cycle-strategy declarations
  where inference is not sufficient.

`build_relational_plan` freezes these inputs into a `RelationalPlan`. Rejected,
unresolved, stale, superseded, or fingerprint-incompatible artifacts fail
closed before generation.

## Generation contract

- Parents are generated before children using a deterministic topological order;
  ties are sorted by table name.
- Key domains are synthetic and deterministic. Foreign keys are assigned from
  generated parent rows; production identifiers are not copied or hashed.
- Single-column and ordered composite key domains are supported.
- Fan-out supports explicit `distribution-first` and `exact-total` modes,
  deterministic reconciliation, zero-child parents, caps, and recorded
  fallback metrics. The compatibility default for the local API remains
  `strict`.
- Primary-key generation and foreign-key assignment are separate operations.
- Optional relationships produce valid `NULL` values; a populated orphan is an
  error.
- One-to-one relationships allocate parent keys without replacement.
- Bridge tables allocate unique parent pairs subject to finite capacity.
- Self-references point only to an earlier generated level, preserving roots
  as `NULL`.
- Cycles execute only when an approved staging, deferred, or bootstrap
  strategy names executable edges. Unresolved cycles fail closed.

The local implementation uses Python row tuples. The Spark adapter performs
the same assignment and validation with DataFrames and does not collect key
domains to the driver. The Databricks bundle exposes `relational_generator`
and `relational_error_smoke` entry points.

## Validation and lifecycle

Validation covers key uniqueness, null key domains, semi-join matches,
anti-join orphans, optional-null counts, one-to-one duplicates, composite
tuples, bridge pair uniqueness, self-reference rules, row counts, and fan-out
bounds. A dataset moves through `GENERATED_AWAITING_VALIDATION`, `VALIDATED`,
and `APPROVED`; approval is explicit and does not publish data.

The generated tables and manifest are scoped by run ID. A retry with the same
plan, counts, fan-out, and run ID reuses the same dataset fingerprint. A
different fingerprint is rejected instead of mixing generations. Validation
reports are idempotent per run ID.

## Verified limits

This contract does not claim global graph-topology preservation, privacy,
complete statistical quality, or automatic publication. Those are separate
concerns. The Databricks smoke fixture proves the relational features listed
above; it is not evidence that every possible schema, distribution, or Spark
data type is supported.
