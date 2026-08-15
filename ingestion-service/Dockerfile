# Multi-stage build. This file is the template every service copies.
#
# pom.xml is copied and resolved BEFORE src, so a source-only change reuses the
# cached dependency layer instead of re-downloading the world.
#
# The BuildKit cache mount on ~/.m2 is shared across all service builds, so
# Maven downloads once for the whole repo rather than once per service — which
# matters on a disk-constrained machine.

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    mvn -B -ntp dependency:go-offline
COPY src ./src
# Tests are skipped here deliberately: the integration tests need a Docker
# socket, and building images inside an image build is not a road worth going
# down. They run via ./mvnw verify. See .github/AGENT_WORKFLOW.md.
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    mvn -B -ntp clean package -DskipTests

# All seven services share this base layer, so the marginal cost per service is
# the ~40MB jar rather than a fresh JRE.
FROM eclipse-temurin:21-jre-alpine AS runtime
RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=build /build/target/*.jar app.jar
USER app
EXPOSE 8082
# JAVA_OPTS is set per-service in docker-compose.yml so the memory tuning can
# be changed without a rebuild.
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
