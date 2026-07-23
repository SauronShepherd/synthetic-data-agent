# ADR 0002: Preserve intermediate artifacts

- Status: Accepted
- Milestone: Article 02

## Decision

Metadata inventories, profiles, relationship graphs, generation plans, validation results, warnings, and output references are first-class artifacts.

## Why

They make the workflow explainable, reusable, recoverable, and suitable for future approvals and cost analysis.

## Current implementation

Article 02 records lightweight `ArtifactRef` objects in memory. Article 08 will add durable operational state, while governed analytical evidence will live in Unity Catalog and Delta.
