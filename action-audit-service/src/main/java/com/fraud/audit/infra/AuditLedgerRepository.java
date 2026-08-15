package com.fraud.audit.infra;

import com.fraud.audit.domain.AuditLedgerEntry;

import java.util.List;
import java.util.Optional;

/**
 * The regulatory record of what this system decided and why (spec §8, §9.7,
 * ADR-0011).
 *
 * <p><b>There is no update. No delete. No upsert. No save. No replace. No remove.</b>
 * Not by convention, not by code review — <em>the methods do not exist</em>, so
 * mutating a historical audit record is a compile error.
 *
 * <p>Couchbase Community Edition has no insert-only RBAC role (that is an
 * Enterprise feature), so the guarantee has to come from somewhere. This
 * interface is where.
 *
 * <p>The threat is not malice. It is a Tuesday, a bug is found where some entries
 * carry a wrong {@code serviceVersion}, and someone writes a one-off backfill
 * with {@code ledgerRepo.save(...)}. It passes review — a small, well-intentioned
 * data fix using a method that was right there. The audit trail is now something
 * that has been edited, and there is no way to prove what changed, because the
 * change overwrote the evidence.
 *
 * <p>The same applies with more force to a future AI agent working from a task
 * description and reaching for the obvious available method. <b>Conventions are
 * not enforceable against a contributor who has not read the convention. A
 * missing method is.</b>
 *
 * <p>Corrections are made by <b>appending a correcting entry</b>, never by
 * editing. The ledger grows monotonically; history is reconstructed by replay —
 * which is how ledgers have worked since double-entry bookkeeping.
 *
 * @see com.fraud.audit.AuditLedgerImmutabilityTest T9 asserts this reflectively,
 *      so the guarantee cannot regress.
 */
public interface AuditLedgerRepository {

    /**
     * Appends an entry. Uses Couchbase {@code insert()}, so re-appending the same
     * {@code audit::{transactionId}::{eventType}} key throws rather than
     * overwriting.
     *
     * @return true if appended, false if this exact entry already existed
     *         (normal under at-least-once redelivery — ADR-0010)
     */
    boolean append(AuditLedgerEntry entry);

    Optional<AuditLedgerEntry> findByKey(String key);

    List<AuditLedgerEntry> findByTransactionId(String transactionId);
}
