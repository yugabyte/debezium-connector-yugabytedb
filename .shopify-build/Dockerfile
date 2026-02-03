FROM us-docker.pkg.dev/shopify-docker-images/containers/cloud/maven-gradle-sbt-jdk@sha256:cb3a7e87993d1865d6169142b0fb6033445ea7bfea80aaebb401408e28cf43b4


WORKDIR /app/src
COPY . /app/src/

RUN mvn clean package -DskipTests -Ddocker.skip=true

# Add the required jar files to be packaged with the base connector
RUN curl -sLo kafka-connect-jdbc-10.6.5.jar https://github.com/yugabyte/kafka-connect-jdbc/releases/download/10.6.5-CUSTOM/kafka-connect-jdbc-10.6.5.jar
RUN curl -sLo jdbc-yugabytedb-42.3.5-yb-1.jar https://repo1.maven.org/maven2/com/yugabyte/jdbc-yugabytedb/42.3.5-yb-1/jdbc-yugabytedb-42.3.5-yb-1.jar
RUN curl -sLo mysql-connector-java-8.0.30.jar https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar
RUN curl -sLo postgresql-42.5.1.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.1/postgresql-42.5.1.jar
