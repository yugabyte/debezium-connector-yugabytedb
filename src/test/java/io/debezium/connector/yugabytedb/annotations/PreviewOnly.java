package io.debezium.connector.yugabytedb.annotations;

import io.debezium.connector.yugabytedb.annotations.conditions.RunOnPreviewOnly;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@code @PreviewOnly} is used to signify that the annotated test class or method is only supposed
 * to run against preview YugabyteDB builds.
 *
 * <p>{@code @PreviewOnly} can optionally be declared with a {@link #reason reason} to explain
 * what was the need for the test method or the class to be run against preview builds only.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(RunOnPreviewOnly.class)
public @interface PreviewOnly {
	String reason() default "";
}
