FROM eclipse-temurin:25-jdk-noble AS build
WORKDIR /workspace
COPY gradlew build.gradle settings.gradle ./
COPY gradle/ gradle/
COPY src/ src/
RUN sed -i 's/\r$//' gradlew \
    && sh ./gradlew --no-daemon bootJar \
    && sh ./gradlew --no-daemon bootJar agentJar

FROM eclipse-temurin:25-jre-noble
WORKDIR /app
COPY --from=build /workspace/build/libs/seshat-0.0.1-SNAPSHOT.jar /app/seshat.jar
COPY --from=build /workspace/build/libs/agent.jar /app/build/libs/agent.jar
ENTRYPOINT ["java", "-jar", "/app/seshat.jar"]