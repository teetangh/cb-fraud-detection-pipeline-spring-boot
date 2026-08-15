# ADR-0001 — Build on Spring Boot 4.1, not the spec's pinned 3.3.x

**Status:** Accepted · **Deviates from spec §3** (approved)

## Context

The build spec pins Spring Boot 3.3.x and instructs (instruction #6) that pinned versions be
treated as deliberate design decisions rather than placeholders. Checked against reality at build
time (August 2026):

- **Every Spring Boot 3.x branch is end-of-life.** 3.5.16 (June 2026) was the final OSS 3.x
  release; 3.3.x lost support well before that.
- Only 4.0.x and 4.1.x still receive security patches. 4.1.0 is current.

Building a system whose entire purpose is *fraud prevention in the payment path* on a runtime
that receives no security patches is a contradiction worth resolving rather than inheriting.

## Decision

Build on **Spring Boot 4.1.0**, with Java 21.

This was validated before any application code was written, by resolving the full dependency set
and asserting that every load-bearing class exists on the classpath:

| Concern | Resolution |
|---|---|
| Spring Framework | 7.0.8 |
| `spring-kafka` / `kafka-clients` | 4.1.0 / **4.2.1** — satisfies the spec's "Kafka 4.x" requirement |
| Lettuce (reactive Redis) | 7.5.2 |
| `ReactiveRedisMessageListenerContainer` | present — the sync facade is viable |
| Couchbase `java-client` | 3.11.3, **version-managed by the Boot BOM** |
| `Cluster.transactions()` | present — §9.2's mandated API is available |
| Testcontainers | 2.0.5 |
| Micrometer | 1.17.0, `micrometer-registry-prometheus` unchanged |
| Jackson | 3.1.4 (`tools.jackson`) |

## Naive alternative

Build on 3.3.x exactly as written, because the spec says so.

## Failure mode

An unpatched runtime in the payment path, and a codebase that is born needing a migration. The
migration would not be free either: 3.x → 4.x moves `com.fasterxml.jackson` → `tools.jackson`,
renames `spring-boot-starter-web` → `spring-boot-starter-webmvc`, and swaps `JsonSerializer` for
`JacksonJsonSerializer` in Spring Kafka. Paying that cost once, now, on an empty repository, is
strictly cheaper than paying it later across seven services.

## Consequences

Things that had to change versus a 3.x build, all confirmed rather than assumed:

- `spring-boot-starter-web` → **`spring-boot-starter-webmvc`**.
- Jackson 3: `com.fasterxml.jackson.*` → `tools.jackson.*` (the `jackson-annotations` module
  keeps its old coordinates).
- Spring Kafka: `JsonSerializer`/`JsonDeserializer` are deprecated → use
  **`JacksonJsonSerializer`/`JacksonJsonDeserializer`**.
- Testcontainers 2.x renamed its artifacts: `org.testcontainers:couchbase` →
  **`testcontainers-couchbase`**, `kafka` → **`testcontainers-kafka`**, `junit-jupiter` →
  **`testcontainers-junit-jupiter`**. The old IDs 404 at 2.0.5. Container classes also moved to
  per-module packages and lost their no-arg constructors.

One benefit falls out for free: Spring Boot 3.4+ ships **native structured JSON logging**
(`logging.structured.format.console=ecs`), so the spec §11 requirement for correlation-ID-carrying
JSON logs needs no custom Logback encoder at all.

## Fallback

If Boot 4.1 had failed to resolve, the fallback was **3.5.16** — the terminal, most-patched 3.x.
It was not needed.

## Verified by

The A0 dependency probe (`javap` against the resolved classpath), re-run as a build-time check.
