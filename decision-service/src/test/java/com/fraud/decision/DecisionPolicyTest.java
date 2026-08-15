package com.fraud.decision;

import com.fraud.decision.domain.Decision;
import com.fraud.decision.domain.DecisionPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Policy evaluation and validation. Pure unit test, no containers.
 *
 * <p>Added because decision-service shipped in Phase 4 with zero tests — the
 * review flagged that `BUILD SUCCESS` there meant "compiles", not "verified".
 * The policy is the only place in the system that turns a number into a business
 * outcome, so it is the last thing that should be untested.
 */
@DisplayName("Decision policy")
class DecisionPolicyTest {

    private static final DecisionPolicy SEED = new DecisionPolicy("v1", 30, 70);

    @ParameterizedTest(name = "score {0} -> {1}")
    @CsvSource({
            "0,   ALLOW",
            "29,  ALLOW",
            "30,  REVIEW",   // boundary: allowBelow is EXCLUSIVE
            "31,  REVIEW",
            "69,  REVIEW",
            "70,  BLOCK",    // boundary: blockAtOrAbove is INCLUSIVE
            "100, BLOCK"
    })
    @DisplayName("seed policy maps score to decision at the exact boundaries")
    void mapsScoreToDecision(int score, Decision expected) {
        // The boundaries are the whole contract. 30 must be REVIEW, not ALLOW —
        // T2's velocity-only case scores exactly 30, so an off-by-one here would
        // silently let a velocity-flagged transaction through.
        assertThat(SEED.apply(score)).isEqualTo(expected);
    }

    @Test
    @DisplayName("REJECTS an inverted policy — it would make BLOCK unreachable")
    void rejectsInvertedPolicy() {
        // allowBelow > blockAtOrAbove means every score is either ALLOW or
        // REVIEW and BLOCK can never be reached. That is the fail-wide-open case
        // the system exists to prevent, arriving through a typo in a JSON
        // document rather than a decision.
        assertThatThrownBy(() -> new DecisionPolicy("bad", 80, 20))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BLOCK unreachable");
    }

    @Test
    @DisplayName("REJECTS out-of-range thresholds")
    void rejectsOutOfRange() {
        assertThatThrownBy(() -> new DecisionPolicy("bad", -1, 70))
                .isInstanceOf(IllegalArgumentException.class);
        // Score is capped at 100 by scoring-service, so a block threshold above
        // 100 is also unreachable.
        assertThatThrownBy(() -> new DecisionPolicy("bad", 30, 101))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("a degenerate but VALID policy is accepted — validation must not overreach")
    void acceptsDegenerateButValidPolicy() {
        // allowBelow == blockAtOrAbove is legal: it removes REVIEW entirely,
        // making every transaction ALLOW or BLOCK. Unusual, but a legitimate
        // business choice — validation exists to catch the unreachable-BLOCK
        // case, not to impose taste.
        DecisionPolicy binary = new DecisionPolicy("binary", 50, 50);
        assertThat(binary.apply(49)).isEqualTo(Decision.ALLOW);
        assertThat(binary.apply(50)).isEqualTo(Decision.BLOCK);

        // Block everything.
        assertThat(new DecisionPolicy("strict", 0, 0).apply(0)).isEqualTo(Decision.BLOCK);
    }

    @Test
    @DisplayName("policyVersion is carried, since it is what an auditor reads")
    void carriesPolicyVersion() {
        // Paired with rulesetVersion on the decision record, this is what lets an
        // auditor distinguish "the model changed" from "the policy changed" for a
        // historical decision (ADR-0008).
        assertThat(SEED.policyVersion()).isEqualTo("v1");
    }
}
