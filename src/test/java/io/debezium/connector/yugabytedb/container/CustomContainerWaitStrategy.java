package io.debezium.connector.yugabytedb.container;

import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;

/**
 * Custom wait strategy for the YSQL container to not wait for anything.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public final class CustomContainerWaitStrategy extends AbstractWaitStrategy {
    @Override
    public void waitUntilReady(WaitStrategyTarget waitStrategyTarget) {
        // Do nothing.
    }

    @Override
    protected void waitUntilReady() {
        // Do nothing.
    }
}
