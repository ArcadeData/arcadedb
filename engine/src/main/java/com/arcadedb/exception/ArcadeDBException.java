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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Base exception class for all ArcadeDB exceptions.
 * Provides standardized error codes and diagnostic context.
 */
public class ArcadeDBException extends RuntimeException {
  private final ErrorCode errorCode;
  private final Map<String, Object> context;

  public ArcadeDBException(final String message) {
    super(message);
    this.errorCode = ErrorCode.UNKNOWN_ERROR;
    this.context = new HashMap<>();
  }

  public ArcadeDBException(final String message, final Throwable cause) {
    super(message, cause);
    this.errorCode = ErrorCode.UNKNOWN_ERROR;
    this.context = new HashMap<>();
  }

  public ArcadeDBException(final Throwable cause) {
    super(cause);
    this.errorCode = ErrorCode.UNKNOWN_ERROR;
    this.context = new HashMap<>();
  }

  public ArcadeDBException(final ErrorCode errorCode, final String message) {
    super(message);
    this.errorCode = errorCode != null ? errorCode : ErrorCode.UNKNOWN_ERROR;
    this.context = new HashMap<>();
  }

  public ArcadeDBException(final ErrorCode errorCode, final String message, final Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode != null ? errorCode : ErrorCode.UNKNOWN_ERROR;
    this.context = new HashMap<>();
  }

  public ArcadeDBException(final ErrorCode errorCode, final String message, final Map<String, Object> context) {
    super(message);
    this.errorCode = errorCode != null ? errorCode : ErrorCode.UNKNOWN_ERROR;
    this.context = context != null ? new HashMap<>(context) : new HashMap<>();
  }

  public ArcadeDBException(final ErrorCode errorCode, final String message, final Throwable cause, final Map<String, Object> context) {
    super(message, cause);
    this.errorCode = errorCode != null ? errorCode : ErrorCode.UNKNOWN_ERROR;
    this.context = context != null ? new HashMap<>(context) : new HashMap<>();
  }

  /**
   * Gets the error code associated with this exception.
   *
   * @return the error code
   */
  public ErrorCode getErrorCode() {
    return errorCode;
  }

  /**
   * Gets the diagnostic context for this exception.
   * The context contains additional information about the error.
   *
   * @return unmodifiable map of context information
   */
  public Map<String, Object> getContext() {
    return Collections.unmodifiableMap(context);
  }

  /**
   * Adds a context entry to this exception.
   *
   * @param key   the context key
   * @param value the context value
   * @return this exception for method chaining
   */
  public ArcadeDBException addContext(final String key, final Object value) {
    if (key != null) {
      this.context.put(key, value);
    }
    return this;
  }

  /**
   * Converts this exception to a JSON string.
   *
   * @return JSON representation of this exception
   */
  public String toJSON() {
    final StringBuilder json = new StringBuilder();
    json.append("{");
    json.append("\"errorCode\":").append(errorCode.getCode()).append(",");
    json.append("\"errorName\":\"").append(errorCode.name()).append("\",");
    json.append("\"category\":\"").append(errorCode.getCategory()).append("\",");
    json.append("\"message\":\"").append(escapeJson(getMessage())).append("\"");

    if (!context.isEmpty()) {
      json.append(",\"context\":{");
      boolean first = true;
      for (final Map.Entry<String, Object> entry : context.entrySet()) {
        if (!first) {
          json.append(",");
        }
        json.append("\"").append(escapeJson(entry.getKey())).append("\":");
        appendJsonValue(json, entry.getValue());
        first = false;
      }
      json.append("}");
    }

    if (getCause() != null) {
      json.append(",\"cause\":\"").append(escapeJson(getCause().getMessage())).append("\"");
    }

    json.append("}");
    return json.toString();
  }

  private void appendJsonValue(final StringBuilder json, final Object value) {
    if (value == null) {
      json.append("null");
    } else if (value instanceof String) {
      json.append("\"").append(escapeJson(value.toString())).append("\"");
    } else if (value instanceof Number || value instanceof Boolean) {
      json.append(value);
    } else {
      json.append("\"").append(escapeJson(value.toString())).append("\"");
    }
  }

  /**
   * Escapes a string for JSON output.
   * Note: This is a simplified implementation that handles common escape sequences.
   * For production use with complex strings, consider using a JSON library.
   *
   * @param str the string to escape
   * @return the escaped string
   */
  private String escapeJson(final String str) {
    if (str == null) {
      return "";
    }
    return str.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
        .replace("\b", "\\b")
        .replace("\f", "\\f");
  }

  @Override
  public String toString() {
    return String.format("%s [%s-%d]: %s",
        getClass().getSimpleName(),
        errorCode.getCategory(),
        errorCode.getCode(),
        getMessage());
  }
}
