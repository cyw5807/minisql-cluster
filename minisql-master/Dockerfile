FROM eclipse-temurin:21-jdk-jammy
# 把刚刚打好的大包复制到虚拟机里
COPY target/*-jar-with-dependencies.jar /app/master.jar
# 运行它
ENTRYPOINT ["java", "-jar", "/app/master.jar"]