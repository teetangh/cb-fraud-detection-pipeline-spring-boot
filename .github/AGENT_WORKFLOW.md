# Agent workflow contract

This repository is built by agents working in parallel. This file is the contract they follow.
**If you are a review/merge agent assigned to a PR, this document is your brief — read it fully
before touching anything.**

---

## Roles

| Role | Does | Never does |
|---|---|---|
| **Implementer** (main session) | Implements one phase per branch, opens the PR, moves immediately to the next phase | Never merges its own PR. Never waits for review. |
| **PR agent** (one background agent per PR) | Reviews the diff, posts findings as PR comments, triages, fixes the legitimate ones, updates docs, verifies, merges | Never expands scope beyond the PR's phase. Never weakens a test. |

The implementer does not block on review. The PR agent owns the PR from open to merge.

---

## Branching — sequential PRs, non-blocking work

PRs are **sequential, not stacked**: every PR's base is `main`, and every PR's diff contains only
its own phase.

The implementer still needs the previous phase's code to keep working, so:

```
1. Cut pN from main, implement phase N, push, open PR N.
2. Immediately cut p(N+1) from pN locally and start phase N+1. Do NOT push it.
3. When PR N merges, on p(N+1):  git fetch origin && git rebase origin/main
   Already-merged commits are dropped by patch-id matching; only phase N+1 commits replay.
4. Push p(N+1), open PR N+1. Its diff is phase N+1 only.
```

**Merge method is a merge commit** (`gh pr merge --merge`) — not squash. Squashing rewrites SHAs,
which makes step 3's rebase replay the whole previous phase and conflict with itself. This is not
a style preference; squash breaks the workflow.

Branch naming: `p1-infra`, `p2-scaffolding`, `p3-ingestion`, … Conflicts are rare by construction
— each service is its own directory. The genuinely shared files are `docker-compose.yml`,
`README.md`, and `scripts/`.

---

## PR agent procedure

Work in this order. The order matters: **articulate findings before you are in a position to
rationalise them away.**

### 1. Review first, fix second

Read the diff against three things, in this order of authority:

1. **`FRAUD_PIPELINE_BUILD_SPEC.txt` §9** — the non-negotiable correctness requirements.
2. **`docs/adr/`** — every decision, with the failure mode it prevents.
3. **`docs/CONTRACTS.md`** — message shapes. There is no shared DTO jar, so nothing but this
   document stops two services disagreeing.

Post findings as PR comments **before** fixing anything. A finding you fix silently is a finding
nobody can audit.

### 2. What is legitimate, and what is not

**Fix these:**
- Any §9 violation. These have named failure modes; they are not style.
- A contract mismatch — a field name, type, or enum that differs from `docs/CONTRACTS.md`.
- A test that passes without proving its claim (see "Assertions" below).
- Docs that now contradict the code. Either is allowed to be wrong; they are not allowed to
  disagree.
- Real bugs: races, resource leaks, swallowed exceptions, missing teardown.

**Do not act on these:**
- Style, naming, or formatting preferences.
- "You could also…" suggestions that expand scope. File an issue instead.
- Anything belonging to a later phase. `docs/TEST_PLAN.md` says which phase owns which test.
- Speculative hardening with no named failure mode.

**Escalate — comment on the PR, label `needs-human`, and do NOT merge:**
- A spec requirement that cannot be met as written (say why, propose the alternative).
- A finding requiring a change to a *merged* ADR's decision.
- Any change to the sync-facade ordering, the outbox transaction, or the append-only ledger
  interface. These three carry the system's core guarantees; a plausible-looking "improvement" to
  any of them is exactly the change that must not land unreviewed.
- Test infrastructure that cannot pass locally for environmental reasons (out of disk, out of
  RAM). Say so plainly rather than skipping the test.

### 3. Hard gates before merge

All of these, no exceptions:

- [ ] CI green.
- [ ] The phase's named exit criteria from `docs/TEST_PLAN.md` actually pass — **run them**, do
      not infer from the diff.
- [ ] No test assertion was weakened, deleted, or `@Disabled` to get green.
- [ ] Docs updated if behaviour changed.
- [ ] Every posted finding is either fixed, or has a reply explaining why not.

### 4. Merge

```bash
gh pr merge <N> --merge --delete-branch
```

Then comment a two-line summary: what was found, what was fixed.

---

## Assertions: the rule that matters most

Spec §10: *"do not weaken an assertion to make a test pass."*

The subtler failure is a test that is **green but vacuous** — it passes whether or not the thing
it names is true. Two live examples in this repo:

- **T5** must assert the no-gap check *fails* under `RangeAssignor`. A rebalance test that only
  checks eventual consistency passes under eager rebalancing too, and proves nothing about the
  setting it claims to verify.
- **T6** must assert degraded signal keys are **absent** from the signal map, not that they equal
  zero. Asserting `velocity_1m == 0` passes on the zero-defaulting implementation that ADR-0014
  explicitly rejects.

Before approving any test, ask: **would this still pass if the behaviour it names were removed?**
If yes, it is not a test. Say so.

---

## Test naming (load-bearing for CI)

- `*Test` — unit tests. No containers. Run on every push, in CI.
- `*IT` — integration tests. Real Kafka/Redis/Couchbase via Testcontainers. Run locally and by
  the PR agent; **skipped in CI** (`-DskipITs`), because a Couchbase container per test class on a
  hosted runner is slow and flaky.

CI is a fast gate, not proof. **The PR agent is responsible for actually running the integration
tests** before merging. CI being green is necessary, not sufficient.

---

## Resource reality

The build machine is constrained: ~8 GB free disk, and RAM that has been observed as low as
2.2 GB free. The full stack needs ~4.6 GB.

- Run `./scripts/preflight.sh` before anything that starts containers.
- Prefer `docker compose --profile infra` / `--profile core` over `full` where the phase allows.
- `apache/kafka-native` in tests for faster startup and lower memory.
- Reuse containers across test classes (`testcontainers.reuse.enable=true`) where safe.
- If a test cannot run because the machine is out of resources, **say so and escalate**. Do not
  mark the phase complete on untested code, and do not report a skipped test as a passing one.

---

## When uncertain

Web-search it rather than guessing — the stack is current (Spring Boot 4.1, Jackson 3,
Testcontainers 2.x, Kafka 4.2) and much of the material online still describes the previous major
version of each. Things already verified against the real classpath and recorded in
[ADR-0001](../docs/adr/0001-spring-boot-4.md):

- `spring-boot-starter-web` → **`spring-boot-starter-webmvc`**
- Jackson 3 is **`tools.jackson`**, not `com.fasterxml.jackson`
- Spring Kafka: **`JacksonJsonSerializer`**, not `JsonSerializer` (deprecated)
- Testcontainers 2.x artifacts: **`testcontainers-kafka`**, **`testcontainers-couchbase`**,
  **`testcontainers-junit-jupiter`** — the old `kafka` / `couchbase` IDs 404
- `CouchbaseContainer` **throws** on a CE image if `ANALYTICS` or `EVENTING` are enabled — pin
  `withEnabledServices(KV, QUERY, INDEX)`
- **N1QL defaults to `NOT_BOUNDED` scan consistency.** A query right after a KV write returns
  stale results — usually zero rows. Any read-after-write needs
  `QueryOptions.scanConsistency(QueryScanConsistency.REQUEST_PLUS)`. This already broke the Phase 1
  probe once; if a query-based test is intermittently green, check this first. See
  [LLD §7](../docs/LLD.md#7-couchbase-query-consistency--read-after-write-is-not-free).
- **Do not assert `plan.contains("IndexScan")`.** Couchbase picks among `IndexScan3`,
  `IndexCountScan2`, `DistinctScan`, `IntersectScan` by query shape — a covering `COUNT` plans to
  `IndexCountScan2`, which is a *better* plan and does not contain that substring. This also broke
  the Phase 1 probe. Assert `doesNotContain("PrimaryScan")` plus the index name and `"using":"gsi"`.
