FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:17-jre
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 1001 spring
WORKDIR /app
COPY --from=build /app/target/ms-recomendaciones-0.0.1-SNAPSHOT.jar app.jar
USER spring
EXPOSE 8088
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
