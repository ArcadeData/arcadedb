/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.exception;

import com.arcadedb.serializer.json.JSONObject;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Base exception class for all ArcadeDB exceptions.
 * <p>
 * This enhanced exception class provides:
 * <ul>
 *   <li>Standardized error codes for programmatic handling</li>
 *   <li>Diagnostic context map for troubleshooting</li>
 *   <li>JSON serialization for API responses</li>
 *   <li>HTTP status code mapping</li>
 *   <li>Timestamp tracking</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>{@code
 * throw new DatabaseException(ErrorCode.DB_NOT_FOUND, "Database 'mydb' not found")
 *     .withContext("databaseName", "mydb")
 *     .withContext("user", currentUser);
 * }</pre>
 *
 * @since 25.12
 * @see ErrorCode
 * @see ExceptionBuilder
 */
public abstract class ArcadeDBException extends RuntimeException {

  private final ErrorCode errorCode;
  private final Map<String, Object> context = new LinkedHashMap<>();
  private final long timestamp = System.currentTimeMillis();

  /**
   * Constructs a new exception with the specified error code and message.
   * <p>
   * This is the preferred constructor for new code.
   *
   * @param errorCode the standardized error code
   * @param message   the detail message
   */
  protected ArcadeDBException(final ErrorCode errorCode, final String message) {
    super(message);
    this.errorCode = Objects.requireNonNull(errorCode, "Error code cannot be null");
  }

  /**
   * Constructs a new exception with the specified error code, message, and cause.
   * <p>
   * This is the preferred constructor when wrapping another exception.
   *
   * @param errorCode the standardized error code
   * @param message   the detail message
   * @param cause     the underlying cause
   */
  protected ArcadeDBException(final ErrorCode errorCode, final String message, final Throwable cause) {
    super(message, cause);
    this.errorCode = Objects.requireNonNull(errorCode, "Error code cannot be null");
  }

  /**
   * Legacy constructor for backward compatibility.
   * Uses a default error code based on the exception type.
   *
   * @param message the detail message
   * @deprecated Use constructor with explicit ErrorCode parameter
   */
  @Deprecated(since = "25.12", forRemoval = false)
  protected ArcadeDBException(final String message) {
    super(message);
    this.errorCode = getDefaultErrorCode();
  }

  /**
   * Legacy constructor for backward compatibility.
   * Uses a default error code based on the exception type.
   *
   * @param message the detail message
   * @param cause   the underlying cause
   * @deprecated Use constructor with explicit ErrorCode parameter
   */
  @Deprecated(since = "25.12", forRemoval = false)
  protected ArcadeDBException(final String message, final Throwable cause) {
    super(message, cause);
    this.errorCode = getDefaultErrorCode();
  }

  /**
   * Legacy constructor for backward compatibility.
   * Uses a default error code based on the exception type.
   *
   * @param cause the underlying cause
   * @deprecated Use constructor with explicit ErrorCode parameter
   */
  @Deprecated(since = "25.12", forRemoval = false)
  protected ArcadeDBException(final Throwable cause) {
    super(cause);
    this.errorCode = getDefaultErrorCode();
  }

  /**
   * Returns the default error code for this exception type when using legacy constructors.
   * Subclasses should override this method to provide appropriate default codes.
   *
   * @return the default error code for this exception type
   */
  protected ErrorCode getDefaultErrorCode() {
    return ErrorCode.INTERNAL_ERROR;
  }

  /**
   * Adds diagnostic context to this exception.
   * Context is included in JSON serialization and can be used for debugging.
   * <p>
   * This method supports method chaining:
   * <pre>{@code
   * throw exception
   *     .withContext("key1", value1)
   *     .withContext("key2", value2);
   * }</pre>
   *
   * @param key   the context key (ignored if null)
   * @param value the context value (ignored if null)
   * @return this exception for method chaining
   */
  public ArcadeDBException withContext(final String key, final Object value) {
    if (key != null && value != null) {
      context.put(key, value);
    }
    return this;
  }

  /**
   * Returns the error code associated with this exception.
   *
   * @return the error code
   */
  public ErrorCode getErrorCode() {
    return errorCode;
  }

  /**
   * Returns the error code name (enum name).
   *
   * @return the error code name (e.g., "DB_NOT_FOUND")
   */
  public String getErrorCodeName() {
    return errorCode.name();
  }

  /**
   * Returns the error category.
   *
   * @return the error category
   */
  public ErrorCategory getErrorCategory() {
    return errorCode.getCategory();
  }

  /**
   * Returns the error category name as a string.
   *
   * @return the category name (e.g., "Database")
   */
  public String getErrorCategoryName() {
    return errorCode.getCategoryName();
  }

  /**
   * Returns an unmodifiable view of the diagnostic context map.
   *
   * @return the context map
   */
  public Map<String, Object> getContext() {
    return Collections.unmodifiableMap(context);
  }

  /**
   * Returns the timestamp when this exception was created (milliseconds since epoch).
   *
   * @return the creation timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }


  /**
   * Serializes this exception to a JSON string.
   * <p>
   * The JSON includes:
   * <ul>
   *   <li>error: the error code name</li>
   *   <li>category: the error category</li>
   *   <li>message: the error message</li>
   *   <li>timestamp: creation timestamp</li>
   *   <li>context: diagnostic context (if any)</li>
   *   <li>cause: cause exception info (if any)</li>
   * </ul>
   * <p>
   * Example output:
   * <pre>{@code
   * {
   *   "error": "DB_NOT_FOUND",
   *   "category": "Database",
   *   "message": "Database 'mydb' not found",
   *   "timestamp": 1702834567890,
   *   "context": {
   *     "databaseName": "mydb",
   *     "user": "admin"
   *   }
   * }
   * }</pre>
   *
   * @return JSON representation of this exception
   */
  public String toJSON() {
    final JSONObject json = new JSONObject();
    json.put("error", errorCode.name());
    json.put("category", errorCode.getCategoryName());
    json.put("message", getMessage());
    json.put("timestamp", timestamp);

    if (!context.isEmpty()) {
      json.put("context", new JSONObject(context));
    }

    if (getCause() != null) {
      final Throwable cause = getCause();
      json.put("cause", cause.getClass().getSimpleName() + ": " + cause.getMessage());
    }

    return json.toString();
  }

  /**
   * Returns a detailed string representation including error code and context.
   *
   * @return detailed string representation
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName())
      .append("[").append(errorCode.name()).append("]: ")
      .append(getMessage());

    if (!context.isEmpty()) {
      sb.append(" {")
        .append(context.entrySet().stream()
          .map(entry -> entry.getKey() + "=" + entry.getValue())
          .collect(Collectors.joining(", ")))
        .append("}");
    }

    return sb.toString();
  }
}
