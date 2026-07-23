# ADR 0001: Agent as orchestrator, not execution engine

- Status: Accepted
- Milestone: Article 02

## Decision

The agent interprets user intent, chooses tools, controls workflow transitions, and explains results. Deterministic components calculate profiles, discover relationships, generate rows, validate outputs, and publish assets.

## Why

A single prompt would hide intermediate evidence, make failures difficult to isolate, and allow non-deterministic reasoning to replace measurable calculations.

## Consequences

Every tool must have a typed input/output contract and emit an auditable artifact. The agent cannot bypass validation or publish directly.
