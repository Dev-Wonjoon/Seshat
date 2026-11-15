FROM eclipse-temurin:25-jdk-noble AS build
WORKDIR /workspace
COPY gradlew build.gradle settings.gradle ./
COPY gradle/ gradle/
COPY src/ src/
RUN sed -i 's/\r$//' gradlew \
    && sh ./gradlew --no-daemon bootJar

FROM eclipse-temurin:25-jre-noble
WORKDIR /app
COPY --from=build /workspace/build/libs/*.jar /app/seshat.jar
ENTRYPOINT ["java", "-jar", "/app/seshat.jar"]