# ADR 0003: SDA 07 pattern detector and distributed evidence

The pattern detector is deterministic and evidence-preserving. It emits one
content-addressable aggregate `PATTERN_REGISTRY` artifact per analysis and keeps
large metric cells in a separate distributed evidence table. Observed statistical
patterns remain probabilistic/review-required; support alone cannot promote them
to approved hard rules. Reuse requires compatible environment, source snapshots,
upstream IDs, configuration, and detector/scoring/precedence versions.

## Rule-evaluation boundary

Structured business rules are evaluated by the local coordinator, where the
governed `BusinessRule` object and precedence policy are available. The Spark
entrypoint intentionally does not accept ad-hoc rule expressions: it evaluates
distributed statistical families and persists their evidence, while rule
evaluation is performed in the coordinator after approved rules are supplied.
This prevents ungoverned job parameters from becoming executable hard
constraints; a future Spark rule task must consume a governed rule artifact.
