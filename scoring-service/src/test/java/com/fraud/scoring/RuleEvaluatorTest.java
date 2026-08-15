package com.fraud.scoring;

import com.fraud.scoring.domain.FraudRule;
import com.fraud.scoring.domain.RuleEvaluator;
import com.fraud.scoring.domain.RuleOperator;
import com.fraud.scoring.domain.ScoreResult;
import com.fraud.scoring.domain.SignalKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure unit test — no containers, milliseconds. The evaluator has no framework
 * or driver imports precisely so this can exist.
 */
@DisplayName("Rule evaluation")
class RuleEvaluatorTest {

    private final RuleEvaluator evaluator = new RuleEvaluator(SignalKey.REGISTRY);

    private static FraudRule rule(String id, String signalKey, String op, String threshold, int weight) {
        return new FraudRule(id, id, signalKey,
                RuleOperator.parse(op, threshold == null ? null : new BigDecimal(threshold)),
                weight, true, "TEST", 1);
    }

    @Test
    @DisplayName("velocity_1m = 6 trips GREATER_THAN 5 — the 6th transaction, not the 7th")
    void velocityFiresOnSixth() {
        FraudRule velocity = rule("VELOCITY_1M", SignalKey.VELOCITY_1M, "GREATER_THAN", "5", 30);

        // 5 must NOT fire; 6 must. An off-by-one here silently shifts every
        // velocity rule by one transaction.
        assertThat(evaluator.score(Map.of(SignalKey.VELOCITY_1M, 5), List.of(velocity)).triggeredRules())
                .as("the 5th transaction is exactly at the threshold and must not fire")
                .isEmpty();

        ScoreResult sixth = evaluator.score(Map.of(SignalKey.VELOCITY_1M, 6), List.of(velocity));
        assertThat(sixth.riskScore()).isEqualTo(30);
        assertThat(sixth.triggeredRules()).singleElement()
                .satisfies(t -> {
                    assertThat(t.ruleId()).isEqualTo("VELOCITY_1M");
                    assertThat(t.contribution()).isEqualTo(30);
                    assertThat(t.threshold()).isEqualByComparingTo("5");
                    // The actual value must be recorded — this is what makes the
                    // decision explainable a year later (spec §1).
                    assertThat(t.actualValue()).hasToString("6");
                });
    }

    @Test
    @DisplayName("an ABSENT signal never fires — the basis of fail-open degradation")
    void absentSignalDoesNotFire() {
        FraudRule velocity = rule("VELOCITY_1M", SignalKey.VELOCITY_1M, "GREATER_THAN", "5", 30);

        // Redis was down, so the key is absent (ADR-0014). The rule cannot be
        // evaluated and must contribute nothing — rather than defaulting the
        // signal to 0, which would be a positive claim of innocence.
        ScoreResult result = evaluator.score(Map.of(), List.of(velocity));
        assertThat(result.riskScore()).isZero();
        assertThat(result.triggeredRules()).isEmpty();
    }

    @Test
    @DisplayName("score is CAPPED at 100 — the 8 seed rules sum to 175")
    void scoreIsCapped() {
        List<FraudRule> everything = List.of(
                rule("A", SignalKey.VELOCITY_1M, "GREATER_THAN", "0", 30),
                rule("B", SignalKey.VELOCITY_1H, "GREATER_THAN", "0", 20),
                rule("C", SignalKey.DISTINCT_COUNTRIES_24H, "GREATER_THAN", "0", 25),
                rule("D", SignalKey.CUSTOMERS_PER_DEVICE, "GREATER_THAN", "0", 20),
                rule("E", SignalKey.IS_NEW_DEVICE_HIGH_AMT, "BOOLEAN_TRUE", null, 25),
                rule("F", SignalKey.IS_OFF_HOURS_LARGE, "BOOLEAN_TRUE", null, 15),
                rule("G", SignalKey.MERCHANT_RISK_SCORE, "GREATER_THAN", "0", 20),
                rule("H", SignalKey.AMOUNT_VS_P90_RATIO, "GREATER_THAN", "0", 20));

        Map<String, Object> allFiring = Map.of(
                SignalKey.VELOCITY_1M, 9, SignalKey.VELOCITY_1H, 9,
                SignalKey.DISTINCT_COUNTRIES_24H, 9, SignalKey.CUSTOMERS_PER_DEVICE, 9,
                SignalKey.IS_NEW_DEVICE_HIGH_AMT, true, SignalKey.IS_OFF_HOURS_LARGE, true,
                SignalKey.MERCHANT_RISK_SCORE, 99, SignalKey.AMOUNT_VS_P90_RATIO, new BigDecimal("9"));

        ScoreResult result = evaluator.score(allFiring, everything);
        assertThat(result.triggeredRules()).hasSize(8);
        assertThat(result.riskScore())
                .as("uncapped this would be 175, outside the documented 0-100 range, and the "
                    + "policy comparison in decision-service would be meaningless")
                .isEqualTo(100);
    }

    @Test
    @DisplayName("BOOLEAN_TRUE fires only on a real boolean true")
    void booleanTrueSemantics() {
        FraudRule r = rule("NEW_DEV", SignalKey.IS_NEW_DEVICE_HIGH_AMT, "BOOLEAN_TRUE", null, 25);

        assertThat(evaluator.score(Map.of(SignalKey.IS_NEW_DEVICE_HIGH_AMT, true), List.of(r)).riskScore())
                .isEqualTo(25);
        assertThat(evaluator.score(Map.of(SignalKey.IS_NEW_DEVICE_HIGH_AMT, false), List.of(r)).riskScore())
                .isZero();
        // A boolean signal arriving as a number must not fire — that would mean a
        // serialization change silently turned a rule on.
        assertThat(evaluator.score(Map.of(SignalKey.IS_NEW_DEVICE_HIGH_AMT, 1), List.of(r)).riskScore())
                .isZero();
        // threshold is null for BOOLEAN_TRUE (docs/CONTRACTS.md §3)
        assertThat(evaluator.score(Map.of(SignalKey.IS_NEW_DEVICE_HIGH_AMT, true), List.of(r))
                .triggeredRules().getFirst().threshold()).isNull();
    }

    @Test
    @DisplayName("a rule referencing an unknown signalKey can never fire")
    void unknownSignalKeyNeverFires() {
        // The typo case: velocity_1min instead of velocity_1m. Looks completely
        // live in the database; there is no compiler to catch it (ADR-0002).
        FraudRule typo = rule("TYPO", "velocity_1min", "GREATER_THAN", "5", 30);

        ScoreResult result = evaluator.score(Map.of(SignalKey.VELOCITY_1M, 99), List.of(typo));
        assertThat(result.riskScore())
                .as("the signal is present under its CORRECT name, but the rule names a key "
                    + "that does not exist — it must not fire")
                .isZero();
    }

    @Test
    @DisplayName("numeric comparison is exact across JSON types")
    void numericTypesCompareExactly() {
        FraudRule ratio = rule("AMT", SignalKey.AMOUNT_VS_P90_RATIO, "GREATER_THAN", "3.0", 20);

        // Signals arrive from JSON as Integer/Long/Double/BigDecimal depending on
        // the value. All must compare identically, and 3.0 itself must NOT fire.
        assertThat(evaluator.score(Map.of(SignalKey.AMOUNT_VS_P90_RATIO, new BigDecimal("3.0")), List.of(ratio)).riskScore()).isZero();
        assertThat(evaluator.score(Map.of(SignalKey.AMOUNT_VS_P90_RATIO, 3.0001d), List.of(ratio)).riskScore()).isEqualTo(20);
        assertThat(evaluator.score(Map.of(SignalKey.AMOUNT_VS_P90_RATIO, 4), List.of(ratio)).riskScore()).isEqualTo(20);
        assertThat(evaluator.score(Map.of(SignalKey.AMOUNT_VS_P90_RATIO, 4L), List.of(ratio)).riskScore()).isEqualTo(20);
    }

    @Test
    @DisplayName("an unknown operator is rejected at parse time, not silently ignored")
    void unknownOperatorRejected() {
        assertThat(org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> RuleOperator.parse("NOT_AN_OPERATOR", BigDecimal.ONE)).getMessage())
                .contains("Unknown rule operator");
    }
}
