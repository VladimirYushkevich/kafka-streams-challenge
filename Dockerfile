FROM adoptopenjdk/openjdk11:latest
WORKDIR ./metric-producer
ADD /build/libs/kafka-streams-challenge-0.0.1-SNAPSHOT.jar kafka-streams-challenge.jar
ENV JAVA_OPTS ""
CMD [ "bash", "-c", "java ${JAVA_OPTS} -jar kafka-streams-challenge.jar"]