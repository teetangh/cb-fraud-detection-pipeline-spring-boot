# Agent workflow contract

This repository is built by agents working in parallel. This file is the contract they follow.
**If you are a review/merge agent assigned to a PR, this document is your brief — read it fully
before touching anything.**

---

## Isolation — structural, not by instruction

**Every PR agent runs in its own git worktree.** Not as a nicety: the implementer is building the
*next* phase in the main working tree at the same time, on a different branch.

Telling an agent "touch nothing outside `pN`" does not achieve this. Both processes share one
checkout, so the agent's edits land on whatever branch the implementer currently has checked out.
That happened on PR #11: the agent's review fixes were written into the tree while the implementer
was on `p3-gateway`, so they sat uncommitted on the wrong branch and never reached the PR at all —
one `git checkout` from being lost silently.

Spawn with worktree isolation, and treat any instruction-based separation as advisory on top of
it, never as the mechanism.

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

### 0. Triage CodeRabbit first

**CodeRabbit reviews every PR on this repo.** Read its comments before forming your own view:

```bash
gh pr view <N> --json comments,reviews
gh api repos/{owner}/{repo}/pulls/<N>/comments --jq '.[] | "\(.path):\(.line) \(.body)"'
```

CodeRabbit is a **generic** reviewer. It has not read `FRAUD_PIPELINE_BUILD_SPEC.txt`, and does not
know that `insert()` must never become `upsert()`, that the rebalance test must *fail* under
`RangeAssignor`, or that a degraded signal must be absent rather than zero. So a real share of its
comments will be style, taste, or scope expansion — **deciding which is which is the job**, and it
is why this is a triage step and not an auto-apply.

Judge each comment against the authority order in step 1, then:

- **Legitimate** → fix it, and reply on the thread saying what changed.
- **Rejected** → reply with the reason, citing the ADR or spec section that makes the current
  behaviour deliberate. Never silently ignore one: an unanswered review comment is
  indistinguishable from a missed one.
- **Real but out of scope** → open an issue, link it in the reply, move on.

A CodeRabbit comment is evidence, not an instruction. Where it contradicts a merged ADR, the ADR
wins — and you say so on the thread rather than just declining.

#### The PR agent does NOT wait for CodeRabbit. The implementer resolves it first.

**This is the rule. The rest of this section is background.**

Four PR agents in a row stalled waiting on CodeRabbit — through three successive
tightenings of the *instructions*, including an explicit "hard bound: 20 minutes". Every one of
them sat in a monitor loop instead. The conclusion is not that the instruction needed better
wording: **an agent given a thing to wait for will wait, and no amount of prose fixes that.** So
the wait is removed from the agent's job entirely.

**Implementer**, after opening the PR and before spawning the agent, resolve CodeRabbit to one of
three verdicts and pass it in the agent's prompt:

| Verdict | How you know | What the agent is told |
|---|---|---|
| **REVIEWED — findings** | check `SUCCESS`, inline comments > 0 | the comment IDs, to triage and reply on-thread |
| **REVIEWED — clean** | check `SUCCESS`, `"Actionable comments posted: 0"` | "CodeRabbit reviewed and found nothing" |
| **DECLINED** | check `SUCCESS` **but** body says `rate limited` / `review available in` | "CodeRabbit declined; you are the only review; say so on the PR" |

```bash
gh pr view <N> --json statusCheckRollup --jq '.statusCheckRollup[]|select(.name=="CodeRabbit")|.conclusion'
gh pr view <N> --json comments --jq '.comments[]|select(.author.login=="coderabbitai")|.body' \
  | grep -iE "rate limit|review available in|Actionable comments posted"
gh api repos/{owner}/{repo}/pulls/<N>/comments --jq 'length'
```

If CodeRabbit is still `PENDING` when the implementer needs to move on, that is a **DECLINED**
verdict for this PR. Record it, do not block the pipeline on a third-party free tier.

The agent's brief must say, in as many words: **do not poll CodeRabbit, do not start a monitor for
it, the verdict below is final for this PR.**

#### Background: why the check alone is not enough

**Poll the check, not the comment list.** An empty comment list means "not posted yet" and
"nothing to say" *identically* — you cannot tell them apart, and treating the second as the first
is how an agent waits forever on a review that already finished:

```bash
gh pr view <N> --json statusCheckRollup \
  --jq '.statusCheckRollup[] | select(.name == "CodeRabbit") | .conclusion'
```

`PENDING` = keep waiting. But **`SUCCESS` is not sufficient on its own** — see below.

#### `SUCCESS` does not mean "reviewed"

CodeRabbit's check goes green **even when it declined to review**. On PR #13 it had hit its
open-source rate limit, posted zero findings, said *"Next review available in: 51 minutes"* — and
reported `SUCCESS`. The conclusion conflates two completely different outcomes:

- *reviewed, nothing to say* → the gate is genuinely satisfied
- *declined to review* → **nothing was reviewed at all**

Only the comment body tells them apart. So after the check goes green, confirm which happened:

```bash
gh pr view <N> --json comments \
  --jq '.comments[] | select(.author.login=="coderabbitai") | .body' | grep -i "rate limit\|review available in"
```

A hit means it declined. Treat that as a **timeout, not a clean review**: proceed on your own
review and say on the PR that CodeRabbit declined and why you merged anyway. Do not record a
declined review as "CodeRabbit found nothing" — that is a false clean bill of health, and it is
worse than no review because it looks like coverage that never happened.

If its stated retry window fits inside the 45-minute bound, waiting for it is the better call.

**Bound the wait to ~45 minutes.** CodeRabbit's free tier rate-limits, and a rate-limited review
can take that long or never arrive. This is not hypothetical: on PR #11 an agent waited 69 minutes
on a review that had *already completed*, because it watched for comments instead of the check.

On expiry, proceed on your own review — but **say so on the PR**: that CodeRabbit did not respond
in the window, that you reviewed independently, and that all other gates passed. Never merge
silently past a CodeRabbit timeout; the record must show it was skipped and why.

A **re-review of a fix commit** gets a shorter leash — ~10 minutes. The substantive round has
already happened and every finding has been answered.

#### Replies go on the thread

```bash
gh api repos/{owner}/{repo}/pulls/<N>/comments/<comment_id>/replies -f body="..."
```

A summary comment on the PR does **not** discharge this. Thread replies are what a future reader
follows to find out why a specific line is the way it is.

### 1. Then review it yourself, and fix only after

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
- [ ] **Every CodeRabbit comment has been replied to** — fixed, rejected with a reason, or
      converted to an issue.

If CI is unavailable for an infrastructure reason (billing, outage) rather than failing on this
PR's code, that is a `needs-human` escalation, not a judgement call to make alone. Say so on the
PR and do not merge. Note that CI here only compiles and runs `*Test`; it skips every `*IT`, so
running the integration suite locally is required regardless of what CI says.

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
- **`CollectionManager.createCollection(CollectionSpec)` does not work on Couchbase CE.**
  `CollectionSpec` carries a `maxTTL` that the SDK always sends, and CE rejects it:
  `{"errors":{"maxTTL":"Supported in enterprise edition only"}}`. Use the two-arg
  `createCollection(scopeName, collectionName)` instead. Every service's integration test
  provisions collections this way.
- **Do not assert `plan.contains("IndexScan")`.** Couchbase picks among `IndexScan3`,
  `IndexCountScan2`, `DistinctScan`, `IntersectScan` by query shape — a covering `COUNT` plans to
  `IndexCountScan2`, which is a *better* plan and does not contain that substring. This also broke
  the Phase 1 probe. Assert `doesNotContain("PrimaryScan")` plus the index name and `"using":"gsi"`.
