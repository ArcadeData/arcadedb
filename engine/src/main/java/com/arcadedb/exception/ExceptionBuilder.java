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

import com.arcadedb.index.IndexException;

import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Fluent builder for constructing ArcadeDB exceptions with error codes and diagnostic context.
 * <p>
 * The builder pattern provides an ergonomic way to create exceptions with rich diagnostic information:
 * <pre>{@code
 * throw ExceptionBuilder.database()
 *     .code(ErrorCode.DATABASE_NOT_FOUND)
 *     .message("Database '%s' not found", dbName)
 *     .context("databaseName", dbName)
 *     .context("user", currentUser)
 *     .context("requestId", requestId)
 *     .build();
 * }</pre>
 * <p>
 * When wrapping another exception:
 * <pre>{@code
 * try {
 *     fileChannel.write(buffer);
 * } catch (IOException e) {
 *     throw ExceptionBuilder.storage()
 *         .code(ErrorCode.IO_ERROR)
 *         .message("Failed to write to file: %s", file.getName())
 *         .cause(e)
 *         .context("filePath", file.getAbsolutePath())
 *         .context("bufferSize", buffer.remaining())
 *         .build();
 * }
 * }</pre>
 *
 * @since 25.12
 * @see ArcadeDBException
 * @see ErrorCode
 */
public class ExceptionBuilder {

  private ErrorCode errorCode;
  private String message;
  private Throwable cause;
  private final Map<String, Object> context = new LinkedHashMap<>();
  private final Class<? extends ArcadeDBException> exceptionClass;

  private ExceptionBuilder(final Class<? extends ArcadeDBException> exceptionClass) {
    this.exceptionClass = exceptionClass;
  }

  /**
   * Creates a builder for database-related exceptions.
   *
   * @return a new builder for DatabaseException
   */
  public static ExceptionBuilder database() {
    return new ExceptionBuilder(DatabaseException.class);
  }

  /**
   * Creates a builder for transaction-related exceptions.
   *
   * @return a new builder for TransactionException
   */
  public static ExceptionBuilder transaction() {
    return new ExceptionBuilder(TransactionException.class);
  }

  /**
   * Creates a builder for query-related exceptions.
   *
   * @return a new builder for QueryException
   */
  public static ExceptionBuilder query() {
    return new ExceptionBuilder(QueryException.class);
  }

  /**
   * Creates a builder for security-related exceptions.
   *
   * @return a new builder for SecurityException
   */
  public static ExceptionBuilder security() {
    return new ExceptionBuilder(SecurityException.class);
  }

  /**
   * Creates a builder for storage-related exceptions.
   *
   * @return a new builder for StorageException
   */
  public static ExceptionBuilder storage() {
    return new ExceptionBuilder(StorageException.class);
  }

  /**
   * Creates a builder for network-related exceptions.
   *
   * @return a new builder for NetworkException
   */
  public static ExceptionBuilder network() {
    return new ExceptionBuilder(NetworkException.class);
  }

  /**
   * Creates a builder for schema-related exceptions.
   *
   * @return a new builder for SchemaException
   */
  public static ExceptionBuilder schema() {
    return new ExceptionBuilder(SchemaException.class);
  }

  /**
   * Creates a builder for index-related exceptions.
   *
   * @return a new builder for IndexException
   */
  public static ExceptionBuilder index() {
    return new ExceptionBuilder(IndexException.class);
  }

  /**
   * Sets the error code for the exception.
   *
   * @param errorCode the error code (required)
   * @return this builder for method chaining
   */
  public ExceptionBuilder code(final ErrorCode errorCode) {
    this.errorCode = errorCode;
    return this;
  }

  /**
   * Sets the error message for the exception.
   *
   * @param message the error message
   * @return this builder for method chaining
   */
  public ExceptionBuilder message(final String message) {
    this.message = message;
    return this;
  }

  /**
   * Sets the error message using String.format() syntax.
   *
   * @param format the format string
   * @param args   the format arguments
   * @return this builder for method chaining
   */
  public ExceptionBuilder message(final String format, final Object... args) {
    this.message = String.format(format, args);
    return this;
  }

  /**
   * Sets the underlying cause of the exception.
   *
   * @param cause the underlying cause
   * @return this builder for method chaining
   */
  public ExceptionBuilder cause(final Throwable cause) {
    this.cause = cause;
    return this;
  }

  /**
   * Adds a diagnostic context entry.
   * Multiple context entries can be added by calling this method multiple times.
   *
   * @param key   the context key
   * @param value the context value
   * @return this builder for method chaining
   */
  public ExceptionBuilder context(final String key, final Object value) {
    if (key != null && value != null) {
      this.context.put(key, value);
    }
    return this;
  }

  /**
   * Builds and returns the configured exception.
   * <p>
   * The exception is constructed using reflection to instantiate the appropriate
   * exception type with the provided error code, message, and optional cause.
   * Context entries are added after construction.
   *
   * @return the configured exception
   * @throws IllegalStateException if error code is not specified
   * @throws RuntimeException      if exception construction fails (should not occur normally)
   */
  public ArcadeDBException build() {
    if (errorCode == null) {
      throw new IllegalStateException("Error code must be specified");
    }

    // Use default message from error code if no custom message provided
    if (message == null || message.isEmpty()) {
      message = errorCode.getDefaultMessage();
    }

    try {
      final ArcadeDBException exception;

      if (cause != null) {
        // Constructor with cause: (ErrorCode, String, Throwable)
        final Constructor<? extends ArcadeDBException> constructor =
            exceptionClass.getConstructor(ErrorCode.class, String.class, Throwable.class);
        exception = constructor.newInstance(errorCode, message, cause);
      } else {
        // Constructor without cause: (ErrorCode, String)
        final Constructor<? extends ArcadeDBException> constructor =
            exceptionClass.getConstructor(ErrorCode.class, String.class);
        exception = constructor.newInstance(errorCode, message);
      }

      // Add context entries
      context.forEach(exception::withContext);

      return exception;

    } catch (final Exception e) {
      // This should not occur if exception classes follow the standard constructor pattern
      throw new InternalException("Failed to build exception: " + exceptionClass.getName(), e);
    }
  }

  /**
   * Convenience method that builds and throws the exception.
   * <p>
   * Example usage:
   * <pre>{@code
   * ExceptionBuilder.database()
   *     .code(ErrorCode.DATABASE_NOT_FOUND)
   *     .message("Database not found")
   *     .throwException();
   * }</pre>
   * <p>
   * This method always throws; it never returns normally.
   *
   * @return never returns (always throws)
   * @throws ArcadeDBException the built exception
   */
  public ArcadeDBException throwException() {
    throw build();
  }
}
