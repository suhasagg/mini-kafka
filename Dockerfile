FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY . .
RUN ./scripts/build.sh

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/build/mini-kafka.jar /app/mini-kafka.jar
EXPOSE 9092
ENV PORT=9092
ENV DATA_DIR=/data
ENV BROKERS=3
ENV SEGMENT_MAX_RECORDS=50
ENV MIN_IN_SYNC_REPLICAS=2
CMD ["java", "-jar", "/app/mini-kafka.jar"]
