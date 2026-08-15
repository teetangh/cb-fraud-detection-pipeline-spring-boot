package com.fraud.scoring.config;

import com.fraud.scoring.domain.RuleEvaluator;
import com.fraud.scoring.domain.SignalKey;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EvaluatorConfig {

    /** The registry is injected so the evaluator stays framework-free and unit-testable. */
    @Bean
    public RuleEvaluator ruleEvaluator() {
        return new RuleEvaluator(SignalKey.REGISTRY);
    }
}
