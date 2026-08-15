# Architecture Decision Records

Each ADR follows the idiom the build spec itself uses in §9: state the requirement, state the
**naive alternative** that a reasonable engineer would reach for first, and name the **specific
failure mode** that alternative produces. A decision recorded without its rejected alternative is
not a decision, it is a preference.

| # | Decision | Drives |
|---|---|---|
| [0001](0001-spring-boot-4.md) | Build on Spring Boot 4.1, not the spec's EOL 3.3.x | Every `pom.xml` |
| [0002](0002-no-shared-dto-jar.md) | No shared DTO jar; JSON Schema is the contract | Service independence |
| [0003](0003-pubsub-not-polling.md) | Push-based Redis Pub/Sub wait, subscribe-before-ingest | The 150ms budget |
| [0004](0004-review-not-allow-on-timeout.md) | Timeout default is REVIEW, never ALLOW | Safety posture |
| [0005](0005-transactional-outbox.md) | Transactional outbox, not dual-write | No silent loss |
| [0006](0006-idempotent-ingestion.md) | Two-layer idempotency; `insert()` is authoritative | Duplicate retries |
| [0007](0007-lua-atomic-counters.md) | All Redis check-then-act in one Lua `EVAL` | Counter correctness |
| [0008](0008-scoring-decision-split.md) | Scoring and decision are separate deployables | Policy vs model auditability |
| [0009](0009-cooperative-sticky-assignor.md) | Explicit `CooperativeStickyAssignor` everywhere | Rebalance blast radius |
| [0010](0010-manual-offset-commits.md) | Manual commits, strictly after durable work | Crash safety |
| [0011](0011-append-only-ledger.md) | Ledger immutability enforced by the type system | Regulatory integrity |
| [0012](0012-customer-id-partition-key.md) | `customerId` as partition key | Velocity correctness |
| [0013](0013-couchbase-ce-single-node-kraft.md) | Couchbase CE + single-node KRaft locally | Honest local topology |
| [0014](0014-redis-fail-open.md) | Redis outage fails open, signals omitted not zeroed | Availability trade |
