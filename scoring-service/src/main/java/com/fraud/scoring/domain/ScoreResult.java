package com.fraud.scoring.domain;

import java.util.List;

/** The objective score plus its full derivation. No opinion about what it means. */
public record ScoreResult(int riskScore, List<TriggeredRule> triggeredRules) {}
