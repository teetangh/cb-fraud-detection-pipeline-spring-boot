package com.fraud.audit;

import com.fraud.audit.infra.AuditLedgerRepository;
import com.fraud.audit.infra.CouchbaseAuditLedgerRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T9 — the append-only ledger cannot be mutated (spec §10, §9.7, ADR-0011).
 *
 * <p>A plain unit test, no containers. It is a compile-time/code-review-level
 * guarantee, asserted reflectively so it cannot regress silently.
 *
 * <p>The audience for a failure here is a future contributor — human or agent —
 * who added a mutating method for a perfectly reasonable one-off data fix and has
 * no idea it was forbidden. The failure message is written for them.
 */
@DisplayName("T9 — append-only audit ledger")
class AuditLedgerImmutabilityTest {

    private static final Set<String> FORBIDDEN_PREFIXES =
            Set.of("update", "delete", "remove", "save", "upsert", "replace", "merge",
                   "set", "put", "modify", "edit", "patch", "truncate", "drop");

    private static final String WHY = """

            The audit ledger is APPEND-ONLY (spec §9.7, ADR-0011).

            Couchbase Community Edition has no insert-only RBAC role, so this
            interface IS the enforcement. A mutating method here makes editing a
            historical audit record possible — and in a regulated system that means
            the trail is no longer a trail, with no way to prove what changed,
            because the change overwrote the evidence.

            Corrections are made by APPENDING a correcting entry, never by editing.
            The ledger grows monotonically; history is reconstructed by replay.

            If you came here to backfill or fix some entries: append corrections.
            """;

    @Test
    @DisplayName("the interface exposes no mutating method")
    void interfaceHasNoMutatingMethods() {
        List<String> violations = mutatingMethodsOf(AuditLedgerRepository.class);

        assertThat(violations)
                .withFailMessage("AuditLedgerRepository declares mutating method(s): %s%s",
                                 violations, WHY)
                .isEmpty();
    }

    @Test
    @DisplayName("the implementation does not widen the interface either")
    void implementationHasNoMutatingMethods() {
        // Constraining only the interface would be theatre: a caller holding the
        // concrete type could reach a public update() on the implementation.
        List<String> violations = mutatingMethodsOf(CouchbaseAuditLedgerRepository.class);

        assertThat(violations)
                .withFailMessage("CouchbaseAuditLedgerRepository declares mutating method(s): %s%s",
                                 violations, WHY)
                .isEmpty();
    }

    @Test
    @DisplayName("append() is the only way in")
    void appendIsTheOnlyWriteMethod() {
        List<String> declared = Arrays.stream(AuditLedgerRepository.class.getDeclaredMethods())
                .map(Method::getName).sorted().toList();

        assertThat(declared)
                .as("the write surface must be exactly one method")
                .containsExactly("append", "findByKey", "findByTransactionId");
    }

    private static List<String> mutatingMethodsOf(Class<?> type) {
        return Arrays.stream(type.getMethods())
                // Only methods this type actually declares — Object's wait/notify
                // and the like are not ours to constrain.
                .filter(m -> m.getDeclaringClass() == type)
                .map(Method::getName)
                .filter(name -> {
                    String lower = name.toLowerCase(Locale.ROOT);
                    return FORBIDDEN_PREFIXES.stream().anyMatch(lower::startsWith);
                })
                .sorted()
                .toList();
    }
}
