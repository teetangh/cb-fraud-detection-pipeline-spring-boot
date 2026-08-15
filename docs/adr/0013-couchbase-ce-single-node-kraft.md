# ADR-0013 — Couchbase Community Edition and a single-node KRaft broker

**Status:** Accepted · Spec §3, §7, §13

## Context

Everything runs on one developer machine via Docker Compose. Two choices follow from that: which
Couchbase edition, and how many Kafka brokers.

## Decision

- **Couchbase Server 7.6.x Community Edition**, single node.
- **Apache Kafka 4.x, one KRaft node** in combined `process.roles=broker,controller` mode, with
  `replication.factor=1` on every topic. No ZooKeeper — Kafka 4.x removed it entirely, so KRaft is
  not a preference, it is the only mode that exists.

### The Community Edition question, resolved

Spec §3 asserts that multi-document ACID transactions, N1QL, GSI indexes and the Binary Collection
counter API are all available in CE. This is worth checking rather than assuming, because
Couchbase's own product-comparison page lists "Distributed ACID Transactions" in the Enterprise
column — which, taken at face value, would invalidate the outbox pattern in
[ADR-0005](0005-transactional-outbox.md), i.e. the single most important durability guarantee in
the system.

The spec is correct and the marketing page is misleading. SDK transactions are implemented
**client-side**: the SDK stages mutations and coordinates them through Active Transaction Record
(ATR) documents using ordinary KV sub-document operations. There is no server-side transaction
service to license. Couchbase's own SDK engineering blog states plainly that transactions are
available for Community Edition, with nothing to configure on the cluster.

Because the cost of being wrong here is high and discovered late, **this is not taken on faith**:
Phase 1 includes a ~30-line test that runs a real `cluster.transactions().run(...)` against the CE
container and asserts the commit. It is a gate — the outbox is not built until it is green.

Known CE limits, none of which bind locally: max 4 cores per node, max 5-node cluster, no XDCR, no
index replicas or partitioned indexes, no Analytics or Eventing services.

## Naive alternative

A three-broker Kafka cluster in Compose with `RF=3`, `min.insync.replicas=2`, `acks=all`, to
"look production-like".

## Failure mode

**It tests nothing that a single broker does not, while claiming to test something it does not.**

Three brokers on one host share one kernel, one disk, one page cache, and one failure domain. The
property RF=3 exists to provide — surviving the loss of a machine — cannot be exercised, because
there is only one machine. Killing one container proves that Kafka can lose a broker it never
depended on. Meanwhile the setup costs roughly 3× the RAM and disk on a machine that is already
constrained, and every local test gets slower.

The deeper harm is that it makes the local environment **misrepresent** what is verified. Someone
reading `RF=3` in `docker-compose.yml` reasonably concludes replication is tested. It is not. An
honest single node with `RF=1`, plus a written note about the real target topology, is more
truthful and more useful than a convincing-looking cluster that verifies nothing.

Spec §7 says this directly — *"do not create a fake 3-broker Docker Compose setup purely for
show"* — and it is right.

## Consequences

- `replication.factor=1`, and `offsets.topic.replication.factor=1` /
  `transaction.state.log.replication.factor=1` must be set explicitly, or the broker fails to
  create its internal topics on a single node.
- Losing the broker loses the local cluster. Correct: durability locally comes from the Couchbase
  outbox, which is where it comes from in production too. Kafka is the transport, not the record.
- The real multi-broker topology (RF=3, `min.insync.replicas=2`, `acks=all`, rack awareness) is
  described in `future-work/DESIGN_ONLY_aws-topology.md` and tracked as a GitHub issue —
  documented, explicitly unexercised, and labelled as such.
- CE's 4-core cap is invisible here and would matter under real load. Noted so it is not
  discovered by surprise.
- Testcontainers' `CouchbaseContainer` **throws** `ContainerLaunchException` if `ANALYTICS` or
  `EVENTING` are requested against a CE image, so integration tests pin
  `withEnabledServices(KV, QUERY, INDEX)` explicitly.

## Verified by

The Phase 1 CE transaction probe. Topic creation asserted by `kafka-topics.sh --describe` in the
init container and in [T10](../TEST_PLAN.md#t10).
