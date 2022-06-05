package com.yushkevich.emnify;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class StringUtilsTests {

  @Test
  @DisplayName("Should parse valid message.")
  void shouldParseValidMessage() {
    assertEquals(
        KeyValue.pair("1", "ANY_EVENT"),
        StringUtils.getKeyValue(StringUtils.buildMessage(1, "ANY_EVENT")));
  }

  @Test
  @DisplayName("Should create message body.")
  void shouldCreateMessageBody() {
    assertEquals(
        "<Id>     ::= 1\n" + "<Action> ::= ANY_EVENT\n" + "<Event>  ::= 1,ANY_EVENT",
        StringUtils.buildMessage(1, "ANY_EVENT"));
  }
}
