package io.debezium.connector.yugabytedb.rules;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test annotation to make sure that we are logging the proper test name before and after each test.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBLogTestName implements BeforeEachCallback, AfterEachCallback {
    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        if (extensionContext.getTestClass().isPresent()
                && extensionContext.getTestMethod().isPresent()) {
            LOGGER.info("Starting test {}#{}",
                        extensionContext.getTestClass().get().getSimpleName(),
                        extensionContext.getTestMethod().get().getName());
        }
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        if (extensionContext.getTestClass().isPresent()
                && extensionContext.getTestMethod().isPresent()) {
            LOGGER.info("Finished test {}#{}",
                        extensionContext.getTestClass().get().getSimpleName(),
                        extensionContext.getTestMethod().get().getName());
        }
    }
}
