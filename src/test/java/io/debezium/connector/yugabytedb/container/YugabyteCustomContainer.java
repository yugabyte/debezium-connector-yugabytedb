package io.debezium.connector.yugabytedb.container;

import org.testcontainers.containers.YugabyteYSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Custom container class to let us skip the container startup JDBC condition check.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteCustomContainer extends YugabyteYSQLContainer {
    public YugabyteCustomContainer(final String imageName) {
		this(DockerImageName.parse(imageName));
	}
    
    public YugabyteCustomContainer(final DockerImageName imageName) {
        super(imageName);
    }

    @Override
    protected void waitUntilContainerStarted() {
        // The super method for this initiates a JDBC connection to the database
        // and only when the connection is successful, it marks the container to be up,
        // otherwise the container startup fails.

        // The default behaviour will only suffice when we are starting the yugabyted
        // process using the startup command for the docker container. But for custom
        // behaviour, we start a infinite loop as the custom startup command and then
        // start the yugabyted process - in this case, it is mandatory for us to skip
        // this check.

        // TODO: This method and class are only here for the time an upstream
        // contribution is made which can let us skip the check this container provides.
        
        logger().debug("Returning from the waitUntilConnectorStarted method");
    }
}
