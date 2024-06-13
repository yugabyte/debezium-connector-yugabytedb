package io.debezium.connector.yugabytedb.annotations;

import io.debezium.connector.yugabytedb.annotations.conditions.RunWithMinimumYBVersion;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to specify what is the minimum YB version a test should run against. <br><br>
 *
 * <strong>Usage:</strong><br>
 * {@code @MinimumYBVersion("2.21")}
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(RunWithMinimumYBVersion.class)
public @interface MinimumYBVersion {
  String value() default "2.18.2.0";
  String reason() default "";
}
