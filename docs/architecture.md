# Architecture — Article 05

Article 05 adds the second deterministic tool, `table_profiler`, after the Article 04
`uc_metadata_reader` contract.

The core rule remains unchanged:

> The agent orchestrates. Deterministic tools calculate facts and emit traceable evidence.

Here, the evidence is metadata. The tool reads the governed shape of the estate before any source values are sampled or profiled.

## What `uc_metadata_reader` owns

`uc_metadata_reader` owns metadata discovery and normalization:

- allowed catalogs and schemas;
- tables and views;
- object type, owner, and comments;
- column names, data types, nullability, ordinal positions, comments, and tags;
- declared primary-key, foreign-key, and unique claims; column nullability;
- explicit limitation warnings when CHECK expressions are not available from the inspected views;
- sensitivity signals from tags, names, comments, and table context;
- relationship hints from declared constraints;
- warnings when metadata is incomplete or unvalidated;
- compact summaries for agent reasoning.

The tool does not read table values, calculate distributions, validate relationships, approve privacy behavior, or generate rows.

`table_profiler` reads one approved relation and calculates bounded value-level evidence.
It reuses the Article 04 inventory, labels metric methods, avoids raw-value retention,
and persists queryable governed profile artifacts.

## Metadata source

The reader uses Unity Catalog information schema views. For Databricks Free/serverless compatibility, it queries workspace-wide views under:

```sql
system.information_schema
```

Examples include:

```sql
system.information_schema.tables
system.information_schema.columns
system.information_schema.table_tags
system.information_schema.column_tags
system.information_schema.table_constraints
```

The reader filters these views by `table_catalog`, `table_schema`, and configured table patterns. It does not assume that every catalog exposes a catalog-local `<catalog>.information_schema` schema.

## Metadata is evidence, not prophecy

Unity Catalog metadata is a governed starting point, not a final verdict. Comments can be stale. Tags can be missing. Owners can be outdated. Databricks primary-key, foreign-key, and unique constraints are useful metadata declarations, but they are informational rather than enforced.

The reader therefore preserves both the claim and its status. Relationship hints are marked as unvalidated so the next tool can test actual behavior.

## Designed Article 04 flow

```text
Configured allowlist
  -> Read Unity Catalog metadata through system.information_schema
  -> Discover visible catalogs and schemas, then select tables and views
  -> Normalize object, column, tag, owner, and constraint metadata
  -> Detect sensitivity signals
  -> Add warnings and relationship hints
  -> Emit structured inventory and compact summary
  -> Next: source data profiling
```

## Execution modes

### Local demo

`metadata-demo` uses deterministic in-repository sample metadata. It is for tests, documentation, and contract review.

### Local SQL Warehouse

`metadata-read-sql` uses the Databricks SQL Connector and a SQL Warehouse. This path is useful from a laptop or CI system when connection settings are provided through environment variables.

### Databricks Bundle serverless

The `uc_metadata_reader` bundle job runs the Spark entrypoint in Databricks serverless compute. The job uses an `environment_key` instead of a cluster configuration, making it suitable for Databricks Free/serverless workspaces.

## Core contracts

- `MetadataReadConfig`: explicit metadata scope and safe limits.
- `ColumnMetadata`: column-level metadata without values.
- `ConstraintMetadata`: declared constraint metadata as unvalidated claims.
- `TableMetadata`: normalized table or view context for the agent.
- `MetadataInventory`: collection of accepted objects, skipped objects, and warnings.
- `UcMetadataReader`: deterministic tool implementation integrated with the orchestrator.

## Safety properties added

- Discovery starts from a catalog allowlist.
- Broad visibility does not automatically become broad scanning.
- Skipped objects are recorded.
- Metadata warnings stay attached to the result.
- Sensitivity detection is reason-based, not a binary magic flag.
- Declared relationships are kept as hints, not proof.
- The reader never profiles source values.
- A missing tag or sensitivity signal is not a safety verdict.
- Optional metadata query failures are reported as unavailable rather than empty results.
- Documentation uses placeholders instead of personal workspace values.

## Boundary with Article 06

Article 04 answers:

```text
What does the governed metadata say exists?
```

Article 05 answers:

```text
How does one approved table actually behave?
```

Article 06 validates relationships using structural and statistical evidence. The
current implementation supports declared constraints, bounded Spark-native inferred
single/composite candidates, cycle-aware graph summaries, and durable development
artifacts. It now provides bounded deterministic generation, validation/privacy/publication
contracts, and local durable-state references. Production-scale generation, external human
review, managed Lakebase integration, and governed Unity Catalog integration-test proof are
still outstanding. Local privacy checks cover direct identifiers, quasi-identifier
rarity, duplicate-row risk, and raw-value-free report serialization; they do not
replace governed external privacy approval.
