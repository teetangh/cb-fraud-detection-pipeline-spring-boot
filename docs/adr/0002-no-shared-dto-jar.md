# ADR-0002 — No shared DTO jar; JSON Schema is the contract

**Status:** Accepted · Spec §3

## Context

Seven services exchange five message shapes over Kafka plus three HTTP payloads. Something has to
keep them agreeing about field names and types.

## Decision

**No shared compiled library between services.** Each service has its own `pom.xml`, is
independently buildable and deployable, and implements its own DTOs — as Java records, since Java
21 makes them nearly free to declare.

The contract lives in [`docs/CONTRACTS.md`](../CONTRACTS.md) as prose plus JSON Schema. Each
service's integration test validates its own serialized output against the schema for the shapes
it produces.

Every consumer is a **tolerant reader**: unknown fields are ignored, never fatal.

## Naive alternative

A `fraud-contracts` jar containing shared record definitions, depended on by all seven services.
It is the obvious move, and it buys real compile-time safety.

## Failure mode

It converts seven independently deployable services into a **distributed monolith**. Adding one
field to the enriched message means rebuilding and redeploying every producer and consumer of it
in lockstep — because a consumer running the old jar against a new producer gets
`NoSuchMethodError` or a silently missing field at runtime, not a compile error. The version
matrix grows as the product of services and contract versions, and the "independently deployable"
property in §2 becomes false in practice while remaining true on paper.

The failure is insidious because the jar works fine right up until the first time two services
need to deploy on different schedules — which is exactly the situation microservices exist to
permit.

## Consequences

**Accepted cost:** a typo in a field name is a runtime bug, not a compile error. This is a real
loss and it is paid for deliberately.

**Mitigations, in order of how much they actually help:**

1. **Tolerant readers** — the reason additive change is safe one service at a time. This is the
   load-bearing mitigation; the others are supporting.
2. Schema validation in each service's own test suite, so a shape drift fails that service's
   build.
3. End-to-end tests ([T1](../TEST_PLAN.md#t1), [T2](../TEST_PLAN.md#t2)) that traverse all seven
   services against the real stack, which catch cross-service drift that per-service tests cannot.
4. The compatibility rules table in CONTRACTS.md, which marks renames and type changes as
   forbidden rather than merely discouraged.

The `signalKey` registry deserves specific mention: a rule referencing a signal key that
enrichment does not produce **fails silently** — the rule simply never fires. Scoring-service
therefore logs a `WARN` for any enabled rule whose signal key is absent from the registry, on
every evaluation. Turning a silent misconfiguration into a loud one is the only defence available
once the compiler is out of the picture.

## Verified by

Schema-validation tests per service; [T1](../TEST_PLAN.md#t1) and [T2](../TEST_PLAN.md#t2)
end-to-end.
