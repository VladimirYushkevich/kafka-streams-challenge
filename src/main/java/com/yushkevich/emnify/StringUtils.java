package com.yushkevich.emnify;

import org.apache.kafka.streams.KeyValue;

/**
 * Utility class to parse message in following format: <Id> ::= <number> <Action> ::= START | END
 * <Event> ::= <Id>,<Action>
 */
public final class StringUtils {

  private static final String FIELD_SEPARATOR = "::=";
  private static final String FIELD_NAME = "<Event>";
  private static final String MESSAGE_FORMAT =
      "<Id>     ::= %d\n" + "<Action> ::= %s\n" + "<Event>  ::= %d,%s";

  private StringUtils() {}

  /**
   * Extracts {@link KeyValue} from message
   *
   * @param message String message
   * @return Id
   */
  public static KeyValue<String, String> getKeyValue(String message) {
    String[] event = message.split(FIELD_NAME)[1].split(FIELD_SEPARATOR)[1].split(",");

    return KeyValue.pair(event[0].trim(), event[1].trim());
  }

  /**
   * Creates message body by using {@link com.yushkevich.emnify.StringUtils#MESSAGE_FORMAT}
   *
   * @param id Message id
   * @param action Message action
   * @return Message
   */
  public static String buildMessage(int id, String action) {
    return String.format(MESSAGE_FORMAT, id, action, id, action);
  }
}
