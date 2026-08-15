<!--
PR agent: your brief is .github/AGENT_WORKFLOW.md. Read it before reviewing.
Review and post findings BEFORE fixing anything.
-->

## Phase

<!-- e.g. Phase 3a — enrichment-service. Link the TEST_PLAN section this phase owns. -->

## What this does

<!-- One paragraph. What exists after this that did not before. -->

## Exit criteria

<!-- The named tests from docs/TEST_PLAN.md this phase must pass.
     Paste actual output. "Should pass" is not evidence. -->

| Test | Status | Evidence |
|---|---|---|
| | | |

## Spec §9 requirements touched

<!-- Tick only what this PR actually implements, and say where. -->

- [ ] 9.1 Idempotent ingestion — Redis fast path **and** `insert()` as the authority
- [ ] 9.2 Outbox — transaction + outbox row in ONE Couchbase ACID transaction
- [ ] 9.3 Atomic Redis — every check-then-act in a single Lua `EVAL`
- [ ] 9.4 Scoring and decision are separate deployables
- [ ] 9.5 Explicit `CooperativeStickyAssignor` on every consumer group
- [ ] 9.6 Manual offset commit, strictly after durable work
- [ ] 9.7 Append-only ledger — no mutating methods on the interface
- [ ] 9.8 Timeout default is REVIEW, never ALLOW
- [ ] None — infrastructure or scaffolding only

## Self-check

- [ ] No assertion weakened, deleted, or `@Disabled` to get green
- [ ] **Every new test would fail if the behaviour it names were removed** (see AGENT_WORKFLOW.md — a green vacuous test is worse than no test)
- [ ] Message shapes match `docs/CONTRACTS.md` exactly, or the doc was updated
- [ ] Docs updated if behaviour changed
- [ ] Integration tests actually run locally, not just CI

## Deviations from spec or ADRs

<!-- Anything built differently from the documented design, and why.
     If it changes a decision, it needs an ADR — not a PR comment. -->

## Notes for the reviewer

<!-- Where to look hardest. Anything you are unsure about — say so here rather
     than hoping it goes unnoticed. -->
