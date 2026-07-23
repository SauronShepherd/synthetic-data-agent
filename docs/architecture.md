# Architecture — Article 02

Article 02 turns the mission from Article 01 into explicit software boundaries. The central rule is:

> The agent orchestrates. Deterministic tools calculate facts and execute work.

The implementation in this branch is intentionally a **design executable**, not a data generator. It introduces typed contracts, tool protocols, an auditable state object, and a guarded orchestration sequence. It does not connect to Databricks or inspect real data yet.

## Responsibility map

| Component | Owns | Does not own |
|---|---|---|
| Agent/orchestrator | Request normalization, routing, state transitions, explanations | Statistics, Spark execution, governance enforcement |
| Deterministic tools | Metadata reads, profiling, relationship checks, planning, generation, validation, publishing | User-facing reasoning or silent policy decisions |
| Unity Catalog | Governed source and destination boundaries | Operational conversation memory |
| Lakebase / metadata stores | Requests, run state, checkpoints, approvals, artifact references | Large analytical profiles or generated tables |
| Validator | Measurable quality gates before publication | Optimistic approval because a job completed |

## Designed workflow

```text
Request
  -> Metadata discovery
  -> Profiling
  -> Relationship mapping
  -> Generation plan
  -> Generation
  -> Validation
  -> Publication
  -> Explanation
```

This branch executes only through `PLAN_DRAFTED`. Later article branches replace each design stub with its actual deterministic implementation.

## Core contracts

- `GenerationRequest`: normalized intent, source scope, scale, privacy mode, and destination.
- `AgentState`: current stage, artifact references, warnings, and completed tools.
- `AgentTool`: protocol for deterministic tools.
- `ToolResult`: auditable output with artifacts, metrics, and warnings.
- `SyntheticDataAgent`: validates transitions and coordinates tools.

## Safety properties already enforced

- A request requires an explicit catalog, schema, and table list.
- Scale factors must be positive.
- Target catalog and schema are configured together.
- Workflow stages cannot be skipped.
- Only the expected tool may produce a protected stage.
- Intermediate artifacts and warnings remain attached to state.
- Generation does not occur in Article 02.

## Article reference

This implementation follows **SDA 02: Designing the Synthetic Data Agent**, especially its separation of agent reasoning, deterministic tools, persistent state, validation, and governed publication.
