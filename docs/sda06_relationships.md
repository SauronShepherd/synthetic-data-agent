# SDA-06 relationship detector

`sda.relationships` discovers and validates relational structure without
rewriting Unity Catalog metadata. The detector consumes normalized table
metadata and cached row/profile evidence, then emits a versioned JSON-safe
artifact.

The artifact contains:

- evaluated relationships with ordered child and parent columns;
- declared/inferred provenance and validation evidence;
- row coverage, distinct-value coverage, orphan rate, null rate, and parent
  reference rate;
- observed cardinality and fan-out summaries;
- explainable confidence score, band, decision, warnings, and policy version;
- accepted dependency edges, deterministic generation order, cycles, and
  bridge-table candidates.

Accepted edges are the only relationships added to the generation graph.
Rejected, review-only, and untestable candidates remain in the artifact for
auditability but cannot control generation automatically.

The current implementation provides exact in-memory metrics. A Spark adapter
can preserve the same contract using left-semi and left-anti joins without
collecting complete key sets to the driver.
