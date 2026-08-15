package com.fraud.enrichment.domain;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The computed signals for one transaction.
 *
 * <p><b>A signal that could not be computed is ABSENT from {@code signals}, never
 * defaulted to zero.</b> This is the single most important property of this
 * type, and it is easy to get wrong in a way that looks harmless.
 *
 * <p>{@code velocity_1m: 0} is not "unknown" — it is a positive assertion that
 * this customer has been quiet, which is the strongest possible *exonerating*
 * claim about them. During a Redis outage a zero-defaulting implementation would
 * manufacture exculpatory evidence for every transaction it saw, and because the
 * values look like real measurements it would be indistinguishable from a
 * genuine quiet period in the stored decision record.
 *
 * <p>An auditor reviewing a transaction that got through would see zero recent
 * velocity, zero country diversity and zero shared devices — all false, all
 * recorded as fact, with nothing marking them as fabricated. That corrupts the
 * audit trail's truthfulness, not merely its completeness. Omission is honest:
 * the rule cannot be evaluated, so it does not fire. See ADR-0014.
 */
public record SignalSet(
        Map<String, Object> signals,
        boolean signalsDegraded,
        List<String> degradedSignalKeys
) {
    public SignalSet {
        signals = Map.copyOf(signals);
        degradedSignalKeys = List.copyOf(degradedSignalKeys);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final Map<String, Object> signals = new LinkedHashMap<>();
        private final List<String> degraded = new java.util.ArrayList<>();

        public Builder put(String key, Object value) {
            signals.put(key, value);
            return this;
        }

        /** Records a signal as unavailable. Deliberately does NOT write a zero. */
        public Builder degrade(String key) {
            degraded.add(key);
            return this;
        }

        public SignalSet build() {
            return new SignalSet(signals, !degraded.isEmpty(), degraded);
        }
    }
}
