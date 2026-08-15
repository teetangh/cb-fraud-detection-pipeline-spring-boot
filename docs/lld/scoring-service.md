# LLD — scoring-service

**Port 8084 · `fraud.transactions.enriched` → `fraud.transactions.scored`**

Answers one objective question: *given these signals and this ruleset, what is the risk score?* It
has no opinion about what the score means — that is decision-service's job
([ADR-0008](../adr/0008-scoring-decision-split.md)).

Consumer group: `fraud-scoring-group`.

## Class design

```
api/
  EnrichedTransactionListener    @KafkaListener
  RuleAdminController            POST /admin/rules/refresh · GET /admin/rules
domain/
  FraudRule                      record
  RuleOperator                   sealed interface — exhaustive switch
  RuleEvaluator                  pure logic, no framework imports
  TriggeredRule                  record
  ScoreResult                    record
infra/
  RuleRepository                 N1QL against intelligence.fraud-rules
  RuleCache                      in-process, 60s refresh
config/
  KafkaConsumerConfig
```

## Rule evaluation

```java
public ScoreResult score(SignalSet signals, List<FraudRule> enabledRules) {
    var triggered = new ArrayList<TriggeredRule>();
    int total = 0;

    for (FraudRule rule : enabledRules) {
        Object value = signals.get(rule.signalKey());

        if (value == null) {                     // degraded or unknown key
            if (!SignalKey.REGISTRY.contains(rule.signalKey())) {
                log.warn("Rule {} references unknown signalKey '{}' — it can never fire",
                         rule.ruleId(), rule.signalKey());
            }
            continue;                            // absent ⇒ does not fire, contributes nothing
        }

        if (matches(rule.operator(), value, rule.threshold())) {
            triggered.add(new TriggeredRule(rule.ruleId(), rule.weight(), value,
                                            rule.threshold(), rule.operator(),
                                            rule.category(), rule.version()));
            total += rule.weight();
        }
    }
    return new ScoreResult(Math.min(total, 100), triggered, rulesetVersion());
}
```

- **Score is the sum of triggered weights, capped at 100.** Deterministic and explainable — every
  contributing rule lands in `triggeredRules` with its actual value and the threshold in force at
  that moment.
- The cap matters: the eight seed rules sum to 175, so without it a heavily-flagged transaction
  would produce a score outside the documented 0–100 range and break the policy comparison.
- **An absent signal never fires a rule.** This is the mechanical basis of fail-open degradation.

### The unknown-signal-key warning

A rule whose `signalKey` is not in the registry **fails silently** — it simply never fires. With
no shared DTO jar ([ADR-0002](../adr/0002-no-shared-dto-jar.md)) there is no compiler to catch a
typo like `velocity_1min`. The fraud analyst who created the rule believes it is live. It is not,
and nothing says so.

Logging a `WARN` on every evaluation converts a silent misconfiguration into a loud one. It is the
only defence available once the compiler is out of the picture. `GET /admin/rules` also reports
each rule's `signalKeyValid` flag so it is visible without reading logs.

### Sealed operators

```java
public sealed interface RuleOperator
        permits GreaterThan, LessThan, Equals, BooleanTrue {}
```

Exhaustive pattern-matching switch, no `default` branch — adding a ninth operator without handling
it everywhere is a **compile error**, not a runtime surprise on a transaction at 3am.

## Hot reload

```java
@Scheduled(fixedDelayString = "${rules.refresh-interval-ms:60000}")
public void refresh() {
    List<FraudRule> loaded = ruleRepository.findEnabled();
    cache.set(loaded);
    log.info("Ruleset refreshed count={} version={}", loaded.size(), rulesetVersion());
}
```

- Rules are cached in-process, so **the hot path does zero I/O** — rule evaluation is ~2ms of the
  latency budget precisely because it never touches Couchbase.
- 60s timer refresh: a fraud analyst edits the document, the change is live within a minute, no
  restart, no deploy.
- `POST /admin/rules/refresh` forces it immediately — required both for operational urgency and so
  [T8](../TEST_PLAN.md#t8) can assert hot reload without a 60-second sleep.
- The cache reference is swapped atomically (`volatile` field, immutable list). A transaction
  scored mid-refresh sees either the whole old ruleset or the whole new one, never a mixture — a
  half-applied ruleset would produce a score that corresponds to no ruleset that ever existed,
  which is unexplainable after the fact.

### `rulesetVersion`

`{enabledRuleCount}:{sha256 of sorted (ruleId, version, weight, threshold, enabled) tuples}`.

Stamped on every scored message so an auditor can tell exactly which ruleset produced a historical
decision — and, paired with `policyVersion` from decision-service, can distinguish *"the model
changed"* from *"the policy changed"*. That distinction cannot be reconstructed after the fact, so
it has to be recorded at decision time.

## The N1QL query and its index

```sql
SELECT r.* FROM `fraud-detection`.`intelligence`.`fraud-rules` r
WHERE r.docType = "FRAUD_RULE" AND r.enabled = true
```

Backed by `idx_rules_enabled (enabled, category)`. The integration suite runs `EXPLAIN` and
**asserts the plan uses `IndexScan`, not `PrimaryScan`** — an index that exists but which the
planner ignores is indistinguishable from no index at p99, and the failure only appears under
data volume, long after the code is written.

## Explainability logging

Spec §11 requires the full `triggeredRules` breakdown at INFO for every non-ALLOW-bound score:

```
INFO  Scored transactionId=txn-8f2a… score=30 rules=[VELOCITY_1M(+30 actual=6 threshold=5)]
      rulesetVersion=8:sha256-3f9a… correlationId=5b8e0c1a…
```

This is what makes explainability demonstrable in the logs, not only in the stored document — it
works at 3am with `docker compose logs | grep`, without a query console.

## Metrics

| Metric | Type |
|---|---|
| `fraud.scoring.processed` | Counter |
| `fraud.scoring.rule.triggered` | Counter, tag `ruleId` |
| `fraud.scoring.score` | DistributionSummary |
| `fraud.scoring.rules.refresh` | Counter + Timer |
| `fraud.scoring.rules.unknown_signal_key` | Gauge |

`fraud.scoring.rule.triggered` by `ruleId` is the most useful operational metric here: a rule that
suddenly fires on 40% of traffic is either an attack or a bad edit, and both need attention.

## Failure modes

| Failure | Behaviour |
|---|---|
| Couchbase down at refresh | Keep serving the **last known good** ruleset, log ERROR, increment staleness gauge. Scoring with slightly stale rules beats not scoring. |
| Couchbase down at startup | Fail readiness — starting with no ruleset would score everything 0 (= ALLOW everything), which is the fail-wide-open case this system exists to prevent. |
| Rule references unknown signal | Rule never fires; WARN per evaluation; surfaced on `/admin/rules` |
| Malformed message | 3 attempts → `fraud.transactions.enriched.dlq` |

The startup/steady-state asymmetry is deliberate: **degrading with stale rules is safe, starting
with no rules is not.**
