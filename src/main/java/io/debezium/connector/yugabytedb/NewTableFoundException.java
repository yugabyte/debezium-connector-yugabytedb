package io.debezium.connector.yugabytedb;

/**
 * Exception thrown when a new table is added to an existing stream ID on the server side.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class NewTableFoundException extends RuntimeException {
  public NewTableFoundException() {
  }

  public NewTableFoundException(String message) {
    super(message);
  }

  public NewTableFoundException(Throwable cause) {
    super(cause);
  }

  public NewTableFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public NewTableFoundException(String message, Throwable cause,
                                boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
