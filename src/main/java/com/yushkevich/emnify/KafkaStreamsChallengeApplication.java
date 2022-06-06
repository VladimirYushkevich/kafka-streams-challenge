package com.yushkevich.emnify;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

public class KafkaStreamsChallengeApplication {

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";
  private static final Duration INACTIVITY_GAP = Duration.ofMinutes(1);
  private static final Duration INACTIVITY_GRACE_PERIOD = Duration.ofMinutes(1);

  public static void main(final String[] args) {
    final String bootstrapServers =
        Optional.ofNullable(System.getenv("KAFKA_BOOTSTRAP_SERVERS")).orElse("localhost:29092");
    final KafkaStreams streams =
        new KafkaStreams(
            buildTopology(INPUT_TOPIC, OUTPUT_TOPIC),
            streamsConfig(
                bootstrapServers,
                "/tmp/kafka-streams",
                "kafka-streams-challenge",
                "kafka-streams-challenge-client"));

    streams.cleanUp();

    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    runPunctuator(bootstrapServers);
  }

  /**
   * Generates an event after window end + grace period to trigger flush everything through
   * suppression
   *
   * @see org.apache.kafka.streams.kstream.Suppressed#untilWindowCloses
   * @see <a
   *     href="https://stackoverflow.com/questions/54222594/kafka-stream-suppress-session-windowed-aggregation">Same
   *     issue</a>
   */
  static void runPunctuator(final String bootstrapServers) {
    final Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    Producer<String, String> producer =
        new KafkaProducer<>(
            producerProperties, Serdes.String().serializer(), Serdes.String().serializer());

    final ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);

    Runnable punctuatorTask =
        () -> {
          producer.send(
              new ProducerRecord<>(INPUT_TOPIC, null, StringUtils.buildMessage(-1, "PUNCTUATOR")));
        };
    ses.scheduleAtFixedRate(punctuatorTask, 60, 20, TimeUnit.SECONDS);
  }

  static Properties streamsConfig(
      final String bootstrapServers,
      final String stateDir,
      final String applicationIdConfig,
      final String clientIdConfig) {
    final Properties config = new Properties();
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.put(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationIdConfig);
    config.put(StreamsConfig.CLIENT_ID_CONFIG, clientIdConfig);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
    // Set to the earliest so we don't miss any data that arrived in the topics before the proces
    // started
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // disable caching to see window merging
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    config.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.TRACE.name);
    return config;
  }

  static Topology buildTopology(String inputTopic, String outputTopic) {
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
        // create key
        .map(((key, value) -> StringUtils.getKeyValue(value)))
        // group by key so we can use session window
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        // window by session
        .windowedBy(SessionWindows.ofInactivityGapAndGrace(INACTIVITY_GAP, INACTIVITY_GRACE_PERIOD))
        // here some logic to handle repeated events within same window (maybe it is not needed,
        // then just last else case should be present)
        .reduce(
            (prev, next) -> {
              if (prev.equals(next)) {
                return prev;
              } else if (prev.endsWith("END") && next.equals("END")) {
                return prev;
              } else {
                return prev.concat("-").concat(next);
              }
            },
            Materialized.with(Serdes.String(), Serdes.String()))
        // We only care about final result
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .filter(((key, value) -> "START-END".equals(value)))
        .map((key, value) -> KeyValue.pair(key.key(), key.key()))
        .to(outputTopic);

    return builder.build();
  }
}
