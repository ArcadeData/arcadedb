package com.arcadedb.polyglot;

import com.arcadedb.exception.ArcadeDBException;

/**
 * Exception on user code in functions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class UserCodeException extends ArcadeDBException {
  public UserCodeException() {
  }

  public UserCodeException(String message) {
    super(message);
  }

  public UserCodeException(String message, Throwable cause) {
    super(message, cause);
  }
}
