# ADR 0003: SDA 07 pattern detector and distributed evidence

The pattern detector is deterministic and evidence-preserving. It emits one
content-addressable aggregate `PATTERN_REGISTRY` artifact per analysis and keeps
large metric cells in a separate distributed evidence table. Observed statistical
patterns remain probabilistic/review-required; support alone cannot promote them
to approved hard rules. Reuse requires compatible environment, source snapshots,
upstream IDs, configuration, and detector/scoring/precedence versions.
