FROM eclipse-temurin:21-jdk-jammy
COPY target/*-jar-with-dependencies.jar /app/worker.jar
# 预留端口参数
ENTRYPOINT ["java", "-jar", "/app/worker.jar"]