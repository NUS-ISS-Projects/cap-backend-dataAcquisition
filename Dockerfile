FROM openjdk:17-jdk
WORKDIR /app
COPY target/data-acquisition-service-0.0.1-SNAPSHOT.jar /app/data-acquisition-service.jar
ENTRYPOINT ["java", "-jar", "data-acquisition-service.jar"]
