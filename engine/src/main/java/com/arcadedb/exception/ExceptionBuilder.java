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

import java.util.HashMap;
import java.util.Map;

/**
 * Builder for creating ArcadeDBException instances with fluent API.
 *
 * Example usage:
 * <pre>
 * throw ExceptionBuilder.create()
 *     .errorCode(ErrorCode.DATABASE_NOT_FOUND)
 *     .message("Database 'mydb' not found")
 *     .addContext("databaseName", "mydb")
 *     .addContext("path", "/data/databases")
 *     .build();
 * </pre>
 */
public class ExceptionBuilder {
  private ErrorCode errorCode = ErrorCode.UNKNOWN_ERROR;
  private String message;
  private Throwable cause;
  private final Map<String, Object> context = new HashMap<>();

  private ExceptionBuilder() {
  }

  /**
   * Creates a new ExceptionBuilder instance.
   *
   * @return a new builder
   */
  public static ExceptionBuilder create() {
    return new ExceptionBuilder();
  }

  /**
   * Creates a new ExceptionBuilder with the specified error code.
   *
   * @param errorCode the error code
   * @return a new builder
   */
  public static ExceptionBuilder create(final ErrorCode errorCode) {
    return new ExceptionBuilder().errorCode(errorCode);
  }

  /**
   * Sets the error code.
   *
   * @param errorCode the error code
   * @return this builder
   */
  public ExceptionBuilder errorCode(final ErrorCode errorCode) {
    this.errorCode = errorCode != null ? errorCode : ErrorCode.UNKNOWN_ERROR;
    return this;
  }

  /**
   * Sets the exception message.
   *
   * @param message the message
   * @return this builder
   */
  public ExceptionBuilder message(final String message) {
    this.message = message;
    return this;
  }

  /**
   * Sets the exception message using a format string.
   *
   * @param format the format string
   * @param args   the format arguments
   * @return this builder
   */
  public ExceptionBuilder message(final String format, final Object... args) {
    this.message = String.format(format, args);
    return this;
  }

  /**
   * Sets the cause of the exception.
   *
   * @param cause the cause
   * @return this builder
   */
  public ExceptionBuilder cause(final Throwable cause) {
    this.cause = cause;
    return this;
  }

  /**
   * Adds a context entry.
   *
   * @param key   the context key
   * @param value the context value
   * @return this builder
   */
  public ExceptionBuilder addContext(final String key, final Object value) {
    if (key != null) {
      this.context.put(key, value);
    }
    return this;
  }

  /**
   * Adds multiple context entries.
   *
   * @param context the context map
   * @return this builder
   */
  public ExceptionBuilder addContext(final Map<String, Object> context) {
    if (context != null) {
      this.context.putAll(context);
    }
    return this;
  }

  /**
   * Builds and returns the exception.
   *
   * @return the built exception
   */
  public ArcadeDBException build() {
    // Ensure errorCode is never null
    final ErrorCode finalErrorCode = errorCode != null ? errorCode : ErrorCode.UNKNOWN_ERROR;
    final String finalMessage = message != null ? message : finalErrorCode.getDescription();

    if (cause != null) {
      return new ArcadeDBException(finalErrorCode, finalMessage, cause, context);
    } else {
      return new ArcadeDBException(finalErrorCode, finalMessage, context);
    }
  }

  /**
   * Builds and throws the exception.
   *
   * @throws ArcadeDBException always
   */
  public void buildAndThrow() throws ArcadeDBException {
    throw build();
  }
}
