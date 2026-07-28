FROM us-docker.pkg.dev/shopify-docker-images/containers/cloud/maven-gradle-sbt-jdk@sha256:cb3a7e87993d1865d6169142b0fb6033445ea7bfea80aaebb401408e28cf43b4


WORKDIR /app/src
COPY . /app/src/

RUN mvn clean package -DskipTests -Ddocker.skip=true

# Add the required jar files to be packaged with the base connector
RUN curl -sLo postgresql-42.7.7.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar
