# ADR 0003: SDA 07 pattern detector and distributed evidence

The pattern detector is deterministic and evidence-preserving. It emits one
content-addressable aggregate `PATTERN_REGISTRY` artifact per analysis and keeps
large metric cells in a separate distributed evidence table. Observed statistical
patterns remain probabilistic/review-required; support alone cannot promote them
to approved hard rules. Reuse requires compatible environment, source snapshots,
upstream IDs, configuration, and detector/scoring/precedence versions.

## Rule-evaluation boundary

Both the local coordinator and the Spark entrypoint evaluate structured
`BusinessRule` objects. The Spark job accepts a JSON list of rules, parses it
into the same typed model, and compiles only a closed predicate vocabulary;
it never executes arbitrary SQL from the JSON. Effective dates and conflicts
are checked before aggregate evaluation. Spark-emitted rule findings remain
`review_required`, even when a supplied rule says `approved`: job parameters
are not authority to activate a hard generation constraint. A governed rule
artifact with owner/reviewer approval is still needed for that promotion.
