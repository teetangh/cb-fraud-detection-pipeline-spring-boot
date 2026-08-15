package com.fraud.scoring.infra;

import com.couchbase.client.java.json.JsonObject;
import com.fraud.scoring.domain.FraudRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pure unit test of row-mapping — no cluster required, since {@link JsonObject}
 * is an in-memory model. Exercises {@link RuleRepository#toRule} directly to
 * pin down how a malformed hand-edited rule document is rejected, without
 * paying for a Couchbase container per case.
 */
@DisplayName("RuleRepository row mapping")
class RuleRepositoryTest {

    private static JsonObject validRule() {
        return JsonObject.create()
                .put("ruleId", "VELOCITY_1M")
                .put("description", "velocity 1m")
                .put("signalKey", "velocity_1m")
                .put("operator", "GREATER_THAN")
                .put("threshold", 5)
                .put("weight", 30)
                .put("enabled", true)
                .put("category", "VELOCITY")
                .put("version", 1);
    }

    @Test
    @DisplayName("a well-formed document maps cleanly")
    void mapsAWellFormedDocument() {
        FraudRule rule = RuleRepository.toRule(validRule());
        assertThat(rule.ruleId()).isEqualTo("VELOCITY_1M");
        assertThat(rule.weight()).isEqualTo(30);
        assertThat(rule.version()).isEqualTo(1);
    }

    @Test
    @DisplayName("a missing 'weight' field is rejected with the ruleId, not an unboxing NPE")
    void missingWeightIsRejectedLoudly() {
        JsonObject withoutWeight = validRule();
        withoutWeight.removeKey("weight");

        assertThatThrownBy(() -> RuleRepository.toRule(withoutWeight))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("VELOCITY_1M")
                .hasMessageContaining("weight");
    }

    @Test
    @DisplayName("a missing 'version' field is rejected with the ruleId, not an unboxing NPE")
    void missingVersionIsRejectedLoudly() {
        JsonObject withoutVersion = validRule();
        withoutVersion.removeKey("version");

        assertThatThrownBy(() -> RuleRepository.toRule(withoutVersion))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("VELOCITY_1M")
                .hasMessageContaining("version");
    }
}
