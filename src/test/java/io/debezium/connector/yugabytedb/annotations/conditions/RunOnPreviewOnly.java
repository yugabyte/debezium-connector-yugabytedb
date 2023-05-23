package io.debezium.connector.yugabytedb.annotations.conditions;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class RunOnPreviewOnly implements ExecutionCondition {
	private final String PREVIEW_VERSION = "2.17";
	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		String imageName = System.getenv("YB_DOCKER_IMAGE");
		if (imageName.contains(PREVIEW_VERSION)) {
			return ConditionEvaluationResult.enabled("Test enabled");
		} else {
			return ConditionEvaluationResult.disabled("Test disabled on preview builds");
		}
	}
}
