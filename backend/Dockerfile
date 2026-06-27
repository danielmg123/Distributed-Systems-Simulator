# --- Build stage: compile and package the Spring Boot jar with Maven (JDK 17) ---
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
# Copy the POM first so dependency resolution can be cached across source-only changes.
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests clean package

# --- Run stage: small JRE image that runs the packaged jar ---
FROM eclipse-temurin:17-jre
WORKDIR /app
# The Spring Boot plugin produces backend-<version>.jar; copy it under a stable name.
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
