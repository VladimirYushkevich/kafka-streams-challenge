package com.yushkevich.emnify;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaStreamsChallengeApplicationTests {

  @TempDir File tempDir;

  private static final String inputTopic = "inputTopic";
  private static final String outputTopic = "outputTopic";
  private static final ZoneId zone = ZoneOffset.UTC;

  @Test
  @DisplayName(
      "Should produce one message from following event order START->END->START. Last START after 1 minute.")
  void shouldProduceOneMessage() {
    final List<TestRecord<String, String>> inputValues =
        Arrays.asList(
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 0, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "END"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 20, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 30, 21, 0, zone).toInstant()),
            dummyEventToForceSuppression());
    final List<KeyValue<String, String>> expectedValues = List.of(KeyValue.pair("1", "1"));
    verify(inputValues, expectedValues);
  }

  @Test
  @DisplayName(
      "Should produce one message from following event order START->START->END->START. Last START after 1 minute.")
  void shouldProduceOneMessageWithMultipleStartEvents() {
    final List<TestRecord<String, String>> inputValues =
        Arrays.asList(
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 28, 0, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 10, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "END"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 20, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 30, 21, 0, zone).toInstant()),
            dummyEventToForceSuppression());
    final List<KeyValue<String, String>> expectedValues = List.of(KeyValue.pair("1", "1"));
    verify(inputValues, expectedValues);
  }

  @Test
  @DisplayName(
      "Should produce one message from following event order START->START->END->START. Multiple START within 1 minute.")
  void shouldNotProduceOneMessageWithMultipleStartEvents() {
    final List<TestRecord<String, String>> inputValues =
        Arrays.asList(
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 0, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 10, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "END"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 20, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 30, 21, 0, zone).toInstant()),
            dummyEventToForceSuppression());
    final List<KeyValue<String, String>> expectedValues = List.of(KeyValue.pair("1", "1"));
    verify(inputValues, expectedValues);
  }

  @Test
  @DisplayName(
      "Should produce one message from following event order START->END->END->START. Multiple END within 1 minute.")
  void shouldNotProduceOneMessageWithMultipleEndEvents() {
    final List<TestRecord<String, String>> inputValues =
        Arrays.asList(
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 0, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 10, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "END"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 20, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "END"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 21, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 30, 22, 0, zone).toInstant()),
            dummyEventToForceSuppression());
    final List<KeyValue<String, String>> expectedValues = List.of(KeyValue.pair("1", "1"));
    verify(inputValues, expectedValues);
  }

  @Test
  @DisplayName(
      "Should produce 2 messages for 2 ids from following event order START->END->START. Last START after 1 minute.")
  void shouldProduceMultipleMessagesForDifferentIds() {
    final List<TestRecord<String, String>> inputValues =
        Arrays.asList(
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 0, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "END"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 20, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 30, 21, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(2, "START"),
                ZonedDateTime.of(2022, 1, 1, 18, 29, 0, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(2, "END"),
                ZonedDateTime.of(2022, 1, 1, 18, 29, 20, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(2, "START"),
                ZonedDateTime.of(2022, 1, 1, 18, 30, 21, 0, zone).toInstant()),
            dummyEventToForceSuppression());
    final List<KeyValue<String, String>> expectedValues =
        Arrays.asList(KeyValue.pair("1", "1"), KeyValue.pair("2", "2"));
    verify(inputValues, expectedValues);
  }

  @Test
  @DisplayName(
      "Should produce 2 messages for 1 ids from following event order START->END->START. Last START after 1 minute.")
  void shouldProduceMultipleMessagesForSameId() {
    final List<TestRecord<String, String>> inputValues =
        Arrays.asList(
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 0, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "END"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 20, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 30, 21, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 18, 29, 0, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "END"),
                ZonedDateTime.of(2022, 1, 1, 18, 29, 20, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 18, 30, 21, 0, zone).toInstant()),
            dummyEventToForceSuppression());
    final List<KeyValue<String, String>> expectedValues =
        Arrays.asList(KeyValue.pair("1", "1"), KeyValue.pair("1", "1"));
    verify(inputValues, expectedValues);
  }

  @Test
  @DisplayName(
      "Should produce one message from following event order START->END->START. Last START before 1 minute.")
  void shouldNotProduceOneMessage() {
    final List<TestRecord<String, String>> inputValues =
        Arrays.asList(
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 0, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "END"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 20, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 30, 19, 0, zone).toInstant()),
            dummyEventToForceSuppression());
    verify(inputValues, Collections.emptyList());
  }

  @Test
  @DisplayName(
      "Should not produce messages for different ids from following event order START->END->START. Last START before 1 minute.")
  void shouldNotProduceMultipleMessagesForDifferentIds() {
    final List<TestRecord<String, String>> inputValues =
        Arrays.asList(
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 0, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "END"),
                ZonedDateTime.of(2022, 1, 1, 16, 29, 20, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(1, "START"),
                ZonedDateTime.of(2022, 1, 1, 16, 30, 19, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(2, "START"),
                ZonedDateTime.of(2022, 1, 1, 18, 29, 0, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(2, "END"),
                ZonedDateTime.of(2022, 1, 1, 18, 29, 20, 0, zone).toInstant()),
            new TestRecord<>(
                null,
                StringUtils.buildMessage(2, "START"),
                ZonedDateTime.of(2022, 1, 1, 18, 30, 19, 0, zone).toInstant()),
            dummyEventToForceSuppression());
    verify(inputValues, Collections.emptyList());
  }

  private void verify(
      final List<TestRecord<String, String>> inputValues,
      final List<KeyValue<String, String>> expectedValues) {
    final Properties streamsConfiguration =
        KafkaStreamsChallengeApplication.streamsConfig(
            "dummy:1234",
            tempDir.getAbsolutePath(),
            "kafka-streams-challenge-test",
            "kafka-streams-challenge-client-test");

    final Topology topology =
        KafkaStreamsChallengeApplication.buildTopology(inputTopic, outputTopic);

    try (final TopologyTestDriver testDriver =
        new TopologyTestDriver(topology, streamsConfiguration)) {
      final TestInputTopic<String, String> input =
          testDriver.createInputTopic(inputTopic, new StringSerializer(), new StringSerializer());
      final TestOutputTopic<String, String> output =
          testDriver.createOutputTopic(
              outputTopic, new StringDeserializer(), new StringDeserializer());
      input.pipeRecordList(inputValues);
      assertEquals(expectedValues, output.readKeyValuesToList());
    }
  }

  private TestRecord<String, String> dummyEventToForceSuppression() {
    return new TestRecord<>(
        null, StringUtils.buildMessage(-1, "PUNCTUATOR"), ZonedDateTime.now().toInstant());
  }
}
