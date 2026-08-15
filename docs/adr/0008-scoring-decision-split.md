# ADR-0008 — Scoring and decision are separate deployable services

**Status:** Accepted · Spec §9.4

## Context

Two questions get asked about every transaction, and they look like one question:

1. *How risky is this?* → a number.
2. *Given that number, what do we do?* → an action.

## Decision

They are **genuinely separate services**: separate deployables, separate Kafka consumer groups,
separate Couchbase documents, with `fraud.transactions.scored` between them.

- **scoring-service** loads the **ruleset** and produces an objective `riskScore` plus the
  `triggeredRules` breakdown. It stamps `rulesetVersion`. It has no opinion about what the score
  means.
- **decision-service** loads the **policy** (`policy::default`, a document it owns) and maps score
  → ALLOW / REVIEW / BLOCK. It stamps `policyVersion`. It has no opinion about how the score was
  computed.

## Naive alternative

One service that scores and decides. It is simpler, it is one fewer network hop (~14ms of the
latency budget), one fewer consumer group, one fewer deployable.

## Failure mode

**Two failures — one operational, one that only bites during an audit, months later.**

**Operational:** raising the BLOCK cutoff from 70 to 80 for a promotional weekend is a *business*
decision, made by a fraud ops lead, on a business timescale. Fused, it requires redeploying the
artifact containing the scoring logic — so a threshold tweak inherits the full engineering release
cycle: code review, CI, staged rollout. In practice this means one of two bad outcomes: the
business change does not happen when it is needed, or someone adds a hot-config backdoor that
bypasses review entirely.

**Auditability:** this is the one that actually matters. A regulator asks *"why was transaction X
blocked in March but an identical transaction allowed in May?"* With the concerns fused, there is
one version number covering both, and the honest answer is **"something changed, and we cannot
tell you what."** Split, the decision record carries `rulesetVersion` and `policyVersion` as
independent fields, and the answer is precise: *the model was identical; the policy threshold
moved from 70 to 80 on April 2nd.*

You cannot reconstruct that distinction after the fact. It has to be designed in before the
decisions are written, because the decisions are immutable once written.

## Consequences

- **Cost paid:** one extra Kafka hop, ~14ms of the 150ms budget, one more service to run. At a
  typical 69ms end-to-end there is room for it. If the budget ever became binding, collapsing
  these two is the first optimisation to consider — and it should be recognised as trading away
  audit granularity, not as a free win.
- Ruleset and policy have independent change cadences and independent blast radii. A bad rule
  edit affects scores; a bad policy edit affects decisions; the two are never confused.
- `riskScore` is meaningful on its own and can be consumed by anything else that wants a risk
  number without inheriting this system's policy.
- Policy lives in its own document rather than in application config, so changing it needs no
  deployment of any kind — matching the "fraud ops can change things without engineering"
  requirement for policy as well as for rules.

## Verified by

[T8](../TEST_PLAN.md#t8) — a rule edit changes scoring behaviour with no restart. Policy changes
are exercised independently by decision-service's own tests, demonstrating that each can move
without the other.
