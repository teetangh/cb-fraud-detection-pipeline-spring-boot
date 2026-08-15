# Message Contracts

> **This file is load-bearing.** Spec §3 forbids a shared compiled DTO jar between services —
> each service implements its own DTOs. That means nothing at compile time stops two services
> from disagreeing about a field name. **This document is the only contract.** If you change a
> shape here, you must change it in every producer and consumer of that shape, and bump the
> version note at the bottom.

## Why no shared jar

A shared `fraud-contracts.jar` would make these shapes compile-time-safe, and it is the obvious
thing to reach for. It is rejected because it converts seven independently deployable services
into a distributed monolith: every contract change forces a coordinated rebuild-and-redeploy of
all consumers, in lockstep, or you get `NoSuchMethodError` at runtime. At this scale the
coupling costs more than the convenience buys. See [ADR-0002](adr/0002-no-shared-dto-jar.md).

The mitigation for the lost type-safety is:
1. This document, kept exact.
2. A JSON Schema per shape (below), which each service's integration test validates its own
   serialized output against.
3. **Tolerant reader** on every consumer — unknown fields are ignored, never fatal. This is what
   makes additive changes safe to roll out one service at a time.

---

## Field conventions

| Convention | Rule |
|---|---|
| Timestamps | ISO-8601 UTC with millisecond precision, e.g. `2026-08-15T09:41:22.317Z` |
| Money | JSON number, major units (`149.50` = ₹149.50). Never a float in Java — `BigDecimal`. |
| `correlationId` | UUID v4, minted once by gateway-service, propagated through every hop and every log line. Never regenerated downstream. |
| `transactionId` | Client-supplied. Doubles as the idempotency key. |
| Enums | Uppercase `SCREAMING_SNAKE`. Consumers must tolerate unknown enum values by routing to DLQ, not crashing. |
| Unknown fields | Always ignored by consumers (tolerant reader). |

---

## 1. `fraud.transactions.raw`

Produced by **ingestion-service** (via the outbox publisher). Consumed by **enrichment-service**.
Key: `customerId` · 6 partitions · 7 day retention.

```json
{
  "transactionId": "txn-8f2a1c04-9b31-4e77-a1d2-5c0e9a7b3f61",
  "customerId": "cust-00417",
  "merchantId": "merch-2231",
  "merchantCategoryCode": "5967",
  "amount": 14990.00,
  "currency": "INR",
  "countryCode": "IN",
  "deviceId": "dev-a91f77c2",
  "ipAddress": "203.0.113.44",
  "paymentMethod": "CARD",
  "correlationId": "5b8e0c1a-7d44-4a90-9c3e-2f61b8d05a77",
  "createdAt": "2026-08-15T09:41:22.317Z"
}
```

<details>
<summary>JSON Schema</summary>

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "fraud.transactions.raw",
  "type": "object",
  "required": ["transactionId","customerId","merchantId","amount","currency",
               "deviceId","paymentMethod","correlationId","createdAt"],
  "properties": {
    "transactionId":        { "type": "string", "minLength": 1 },
    "customerId":           { "type": "string", "minLength": 1 },
    "merchantId":           { "type": "string", "minLength": 1 },
    "merchantCategoryCode": { "type": "string", "pattern": "^[0-9]{4}$" },
    "amount":               { "type": "number", "exclusiveMinimum": 0 },
    "currency":             { "type": "string", "pattern": "^[A-Z]{3}$" },
    "countryCode":          { "type": "string", "pattern": "^[A-Z]{2}$" },
    "deviceId":             { "type": "string", "minLength": 1 },
    "ipAddress":            { "type": "string" },
    "paymentMethod":        { "enum": ["CARD","BANK_TRANSFER","WALLET"] },
    "correlationId":        { "type": "string", "format": "uuid" },
    "createdAt":            { "type": "string", "format": "date-time" }
  }
}
```
</details>

---

## 2. `fraud.transactions.enriched`

Produced by **enrichment-service**. Consumed by **scoring-service**.
Key: `customerId` · 6 partitions · 7 day retention.

All of the raw envelope, plus:

```json
{
  "…all fields from fraud.transactions.raw…": "…",

  "signals": {
    "velocity_1m": 6,
    "velocity_1h": 12,
    "distinct_countries_24h": 2,
    "customers_per_device": 1,
    "is_new_device_high_amt": false,
    "is_off_hours_large": false,
    "merchant_risk_score": 45,
    "amount_vs_p90_ratio": 1.24
  },
  "signalsDegraded": false,
  "degradedSignalKeys": [],
  "enrichedAt": "2026-08-15T09:41:22.359Z"
}
```

### The signal key registry

**These keys are a hard contract with the `signalKey` field of every fraud rule document** (spec
§8). A rule whose `signalKey` is not in this table can never fire, and scoring-service logs a
`WARN` on every evaluation if it finds one — a silently dead rule is worse than a loud one.

| Signal key | Type | Source | Meaning | Absent when |
|---|---|---|---|---|
| `velocity_1m` | integer | Redis Lua | Transactions by this customer in the last 60s, *including this one* | Redis down |
| `velocity_1h` | integer | Redis Lua | Transactions by this customer in the last 3600s, including this one | Redis down |
| `distinct_countries_24h` | integer | Redis Lua | Distinct `countryCode` values seen for this customer in 24h | Redis down |
| `customers_per_device` | integer | Redis Lua | Distinct `customerId` values seen on this `deviceId` in 30d | Redis down |
| `is_new_device_high_amt` | boolean | Redis + txn | `deviceId` not in customer's known-device set **and** `amount` > 10 000 | Redis down |
| `is_off_hours_large` | boolean | txn only | `createdAt` hour ∈ [00:00, 05:00) UTC **and** `amount` > 50 000 | never — computed from the message itself |
| `merchant_risk_score` | integer 0–100 | Couchbase | Risk score for the MCC, from `intelligence.customer-profiles` MCC table | Couchbase down |
| `amount_vs_p90_ratio` | number | Couchbase | `amount ÷ customer's p90 historical amount`. `1.0` if no history. | Couchbase down |

**`signalsDegraded` semantics.** `true` means at least one signal could not be computed. The
missing keys are listed in `degradedSignalKeys` and are **omitted from the `signals` map
entirely** — they are never defaulted to `0`, because `velocity_1m: 0` is a *positive assertion
that this customer has been quiet*, which is the opposite of "we don't know". A rule whose
signal key is absent does not fire and contributes nothing. This is what makes
[T6](TEST_PLAN.md#t6) meaningful and is the mechanical basis of the fail-open behaviour in
[ADR-0014](adr/0014-redis-fail-open.md).

---

## 3. `fraud.transactions.scored`

Produced by **scoring-service**. Consumed by **decision-service**.
Key: `customerId` · 6 partitions · 7 day retention.

All of the enriched envelope, plus:

```json
{
  "…all fields from fraud.transactions.enriched…": "…",

  "riskScore": 30,
  "triggeredRules": [
    {
      "ruleId": "VELOCITY_1M",
      "contribution": 30,
      "actualValue": 6,
      "threshold": 5,
      "operator": "GREATER_THAN",
      "category": "VELOCITY",
      "ruleVersion": 1
    }
  ],
  "evaluatedRuleCount": 8,
  "rulesetVersion": "8:sha256-3f9a…",
  "scoredAt": "2026-08-15T09:41:22.371Z"
}
```

`rulesetVersion` is `{enabledRuleCount}:{sha256 of the sorted (ruleId,version,weight,threshold,enabled) tuples}`.
It exists so an auditor reading a historical decision can tell **exactly which ruleset produced
it** — and, critically, can distinguish "the ruleset changed" from "the policy changed", because
the two live in different fields written by different services. See
[ADR-0008](adr/0008-scoring-decision-split.md).

`actualValue` is `number | boolean`; `threshold` is `number | null` (null for `BOOLEAN_TRUE` operators).

---

## 4. `fraud.transactions.decisioned`

Produced by **decision-service**. Consumed by **action-audit-service**.
Key: `customerId` · 6 partitions · **30 day** retention (longer — this is the audit-relevant topic).

```json
{
  "transactionId": "txn-8f2a1c04-…",
  "customerId": "cust-00417",
  "merchantId": "merch-2231",
  "amount": 14990.00,
  "currency": "INR",
  "correlationId": "5b8e0c1a-…",
  "riskScore": 30,
  "decision": "REVIEW",
  "triggeredRules": [ { "ruleId": "VELOCITY_1M", "contribution": 30,
                        "actualValue": 6, "threshold": 5 } ],
  "rulesetVersion": "8:sha256-3f9a…",
  "policyVersion": "v1",
  "signalsDegraded": false,
  "decisionAt": "2026-08-15T09:41:22.383Z"
}
```

---

## 5. `fraud.transactions.actioned`

Produced by **action-audit-service**. Terminal topic — no service consumes it in the core build;
it exists so downstream consumers (case management, analytics) can be added without touching
this pipeline. Key: `customerId` · 6 partitions · 30 day retention.

```json
{
  "transactionId": "txn-8f2a1c04-…",
  "customerId": "cust-00417",
  "correlationId": "5b8e0c1a-…",
  "decision": "REVIEW",
  "riskScore": 30,
  "actions": [
    { "type": "AUDIT_WRITTEN",     "status": "SUCCESS", "detail": "audit::txn-8f2a…::DECISION_RECORDED" },
    { "type": "WEBHOOK_NOTIFIED",  "status": "SUCCESS", "detail": "200 from mock-payment-api" },
    { "type": "CASE_CREATED",      "status": "SKIPPED", "detail": "decision != BLOCK" }
  ],
  "actionedAt": "2026-08-15T09:41:22.402Z"
}
```

`type` ∈ `AUDIT_WRITTEN | WEBHOOK_NOTIFIED | CASE_CREATED | ALERT_RAISED`
`status` ∈ `SUCCESS | FAILED | SKIPPED`

---

## 6. `fraud.alerts.realtime`

Produced by **action-audit-service** for every non-ALLOW decision.
**Key: `merchantId`** — deliberately *not* `customerId`. Alert consumers ask "is this merchant
under attack right now", which is a per-merchant aggregation; keying by merchant puts all of one
merchant's alerts on one partition and in order. 3 partitions · 1 day retention.

```json
{
  "alertId": "alert-7c1e…",
  "merchantId": "merch-2231",
  "customerId": "cust-00417",
  "transactionId": "txn-8f2a1c04-…",
  "correlationId": "5b8e0c1a-…",
  "decision": "REVIEW",
  "riskScore": 30,
  "triggeredRuleIds": ["VELOCITY_1M"],
  "raisedAt": "2026-08-15T09:41:22.404Z"
}
```

---

## 7. DLQ envelope

Used by all three `.dlq` topics (`fraud.transactions.raw.dlq`, `.enriched.dlq`, `.scored.dlq`).
3 partitions · 30 day retention. The original payload is carried as an **opaque string**, never
re-parsed — the whole reason a message is here may be that it does not parse.

```json
{
  "originalTopic": "fraud.transactions.enriched",
  "originalPartition": 3,
  "originalOffset": 91827,
  "originalKey": "cust-00417",
  "payload": "{\"transactionId\":\"txn-…\",\"amount\":\"not-a-number\"}",
  "errorClass": "tools.jackson.databind.exc.MismatchedInputException",
  "errorMessage": "Cannot deserialize value of type `java.math.BigDecimal` from String \"not-a-number\"",
  "attempts": 3,
  "correlationId": "5b8e0c1a-…",
  "failedAt": "2026-08-15T09:41:22.500Z"
}
```

---

## 8. Redis Pub/Sub — `decision:{correlationId}`

Not Kafka, but every bit as much a contract: this is the payload that wakes up the waiting
gateway request. Published by **decision-service**, consumed by **gateway-service**.

```json
{
  "transactionId": "txn-8f2a1c04-…",
  "correlationId": "5b8e0c1a-…",
  "decision": "REVIEW",
  "riskScore": 30,
  "triggeredRules": [ { "ruleId": "VELOCITY_1M", "contribution": 30,
                        "actualValue": 6, "threshold": 5 } ],
  "policyVersion": "v1",
  "decisionAt": "2026-08-15T09:41:22.383Z"
}
```

**Fire-and-forget.** Redis Pub/Sub has no persistence and no delivery guarantee — if no
subscriber is listening, the message evaporates. That is *acceptable and by design* here,
because this channel is only an optimisation: the authoritative decision is already durably in
Couchbase before this publish happens, and the timeout path plus webhook reconciliation covers
every case where the message is missed. Never treat a missed publish as a lost decision.

---

## 9. Synchronous HTTP contracts

### `POST /payments/initiate` → mock-payment-api

```json
{ "customerId": "cust-00417", "merchantId": "merch-2231",
  "merchantCategoryCode": "5967", "amount": 14990.00, "currency": "INR",
  "countryCode": "IN", "deviceId": "dev-a91f77c2", "ipAddress": "203.0.113.44",
  "paymentMethod": "CARD", "transactionId": "txn-8f2a1c04-…" }
```

Response `200`:
```json
{ "paymentId": "pay-3d81…", "transactionId": "txn-8f2a1c04-…",
  "correlationId": "5b8e0c1a-…", "status": "HELD",
  "fraudDecision": "REVIEW", "riskScore": 30, "resolvedBy": "PIPELINE" }
```

`status` maps from the fraud decision: `ALLOW → COMPLETED`, `REVIEW → HELD`, `BLOCK → DECLINED`.

### `POST /fraud/v1/evaluate` → gateway-service

Requires `Authorization: Bearer <HMAC-SHA256 JWT>`. Body is the raw transaction shape (§1),
minus `correlationId` — the gateway mints that.

Response `200`:
```json
{ "transactionId": "txn-8f2a1c04-…", "correlationId": "5b8e0c1a-…",
  "decision": "REVIEW", "riskScore": 30,
  "triggeredRules": [ … ], "policyVersion": "v1",
  "resolvedBy": "PIPELINE", "latencyMs": 47 }
```

**`resolvedBy` ∈ `PIPELINE | TIMEOUT_DEFAULT`** is the most operationally important field in the
whole API. `PIPELINE` means the real decision arrived within the 150ms budget. `TIMEOUT_DEFAULT`
means the budget was blown and this is the safe REVIEW default, with the real decision still
inbound asynchronously. It is what makes [T7](TEST_PLAN.md#t7) assertable, and in production it
is the numerator of the single most important SLO on the system: *what fraction of decisions are
real decisions?*

### `POST /webhooks/fraud-decision` → mock-payment-api

Called by action-audit-service **only** when the true decision differs from what the caller was
already told.

```json
{ "transactionId": "txn-8f2a1c04-…", "correlationId": "5b8e0c1a-…",
  "previousDecision": "REVIEW", "finalDecision": "BLOCK", "riskScore": 85,
  "triggeredRules": [ … ], "policyVersion": "v1",
  "reason": "RECONCILIATION", "decisionAt": "2026-08-15T09:41:24.900Z" }
```

Receivers must treat this endpoint as **at-least-once** and key their handling on
`transactionId` — a webhook retry must not double-refund.

---

## Compatibility rules

| Change | Safe? | Why |
|---|---|---|
| Add an optional field | ✅ | Tolerant readers ignore it |
| Add a new signal key | ✅ | Rules referencing it simply start firing once it appears |
| Add a new enum value | ⚠️ | Consumers route unknown values to DLQ. Deploy consumers first. |
| Rename or remove a field | ❌ | Breaks every consumer at once. New topic + dual-write instead. |
| Change a field's type | ❌ | Same. |
| Change `signalKey` naming | ❌ | Silently kills rules — they stop firing with no error. |

---

**Contract version: 1.0** — initial. Bump on any change and note it here.
