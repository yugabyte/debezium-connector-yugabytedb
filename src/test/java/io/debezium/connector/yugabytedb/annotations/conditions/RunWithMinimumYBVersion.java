package io.debezium.connector.yugabytedb.annotations.conditions;

import io.debezium.connector.yugabytedb.annotations.MinimumYBVersion;
import io.debezium.connector.yugabytedb.connection.YBVersion;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.AnnotatedElement;
import java.util.Optional;

/**
 * This class is to define the condition with which certain test will be executed if the
 * annotation {@code @MinimumYBVersion} is present.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class RunWithMinimumYBVersion implements ExecutionCondition {
  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    final Optional<AnnotatedElement> element = context.getElement();
    if (element.isPresent()) {
      YBVersion testYBVersion = new YBVersion(element.get().getAnnotation(MinimumYBVersion.class).value());

      if (YBVersion.getCurrentYBVersionEnv().compareTo(testYBVersion) >= 0) {
        return ConditionEvaluationResult.enabled("Test enabled");
      } else {
        return ConditionEvaluationResult.disabled("Test disabled as minimum YB version required to run this test is " + testYBVersion);
      }
    }

    // The possibility of hitting this code block is not even there. If this is encountered, you've
    // unleashed the devil now - run as fast as you can.
    return ConditionEvaluationResult.disabled("No test element found");
  }

}
