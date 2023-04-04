package io.debezium.connector.yugabytedb.annotations.conditions;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class RunOnStableOnly implements ExecutionCondition {
	private final String STABLE_VERSION = "2.16";
	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		String imageName = System.getenv("YB_DOCKER_IMAGE");
		if (imageName.contains(STABLE_VERSION)) {
			return ConditionEvaluationResult.enabled("Test enabled");
		} else {
			return ConditionEvaluationResult.disabled("Test disabled on preview builds");
		}
	}
}
