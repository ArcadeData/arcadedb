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
package com.arcadedb.network.exception;

import com.arcadedb.exception.ArcadeDBException;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.Objects;

/**
 * Utility class for translating exceptions at the network module boundary.
 * <p>
 * This class converts lower-level exceptions (I/O errors, engine exceptions) into
 * {@link NetworkException} to maintain proper exception semantics at the network layer.
 * <p>
 * Translation rules:
 * <ul>
 *   <li>If already {@link NetworkException}, pass through unchanged</li>
 *   <li>If {@link ArcadeDBException}, wrap with {@link NetworkErrorCode#REMOTE_ERROR}</li>
 *   <li>If {@link SocketTimeoutException}, map to {@link NetworkErrorCode#CONNECTION_TIMEOUT}</li>
 *   <li>If {@link ClosedChannelException}, map to {@link NetworkErrorCode#CHANNEL_CLOSED}</li>
 *   <li>If {@link IOException}, map to {@link NetworkErrorCode#CONNECTION_ERROR}</li>
 *   <li>If unknown type, wrap as {@link NetworkErrorCode#CONNECTION_ERROR}</li>
 * </ul>
 * <p>
 * The translator preserves the original exception as the cause chain, allowing
 * detailed debugging while providing network-appropriate error codes.
 * <p>
 * Example usage:
 * <pre>{@code
 * try {
 *     // Network operation that might throw IOException or ArcadeDBException
 *     performNetworkOperation();
 * } catch (Exception e) {
 *     throw NetworkExceptionTranslator.translate(e);
 * }
 * }</pre>
 * <p>
 * For checked exceptions that are expected to occur:
 * <pre>{@code
 * String data = NetworkExceptionTranslator.translateChecked(() -> {
 *     return readFromSocket();  // May throw IOException
 * });
 * }</pre>
 *
 * @since 26.1
 * @see NetworkException
 * @see NetworkErrorCode
 * @see CheckedSupplier
 */
public class NetworkExceptionTranslator {

  /**
   * Translates an exception to {@link NetworkException} if needed.
   * <p>
   * This method is used at the network module boundary to convert lower-level
   * exceptions (I/O, engine, or unknown types) into network-specific exceptions.
   * <p>
   * Translation logic:
   * <ol>
   *   <li>If the exception is already a {@link NetworkException}, it is returned as-is</li>
   *   <li>If the exception is an {@link ArcadeDBException}, it is wrapped with context
   *       about the original error code</li>
   *   <li>If the exception is a {@link SocketTimeoutException}, it indicates a connection timeout</li>
   *   <li>If the exception is a {@link ClosedChannelException}, the network channel was closed</li>
   *   <li>If the exception is an {@link IOException}, it indicates a general connection error</li>
   *   <li>For all other exceptions, a generic {@link NetworkErrorCode#CONNECTION_ERROR} is used</li>
   * </ol>
   * <p>
   * The original exception is always preserved in the cause chain for debugging purposes.
   *
   * @param e the exception to translate (must not be null)
   * @return a {@link NetworkException} (or passes through if already one)
   * @throws NullPointerException if the exception is null
   *
   * @see NetworkErrorCode
   * @see #translateChecked(CheckedSupplier)
   */
  public static RuntimeException translate(final Throwable e) {
    Objects.requireNonNull(e, "Exception to translate cannot be null");

    // Already a network exception - pass through unchanged
    if (e instanceof NetworkException) {
      return (NetworkException) e;
    }

    // Engine exceptions - wrap with network context and preserve original error info
    if (e instanceof ArcadeDBException arcadeEx) {
      return new NetworkException(
          NetworkErrorCode.REMOTE_ERROR,
          "Remote database operation failed: " + arcadeEx.getMessage(),
          arcadeEx
      ).withContext("originalError", arcadeEx.getErrorCodeName())
       .withContext("category", arcadeEx.getErrorCategoryName());
    }

    // Socket timeout - connection timeout specific error
    if (e instanceof SocketTimeoutException) {
      return new NetworkException(
          NetworkErrorCode.CONNECTION_TIMEOUT,
          "Network operation timed out: " + e.getMessage(),
          e
      ).withContext("exceptionType", "SocketTimeoutException");
    }

    // Closed channel - channel already closed
    if (e instanceof ClosedChannelException) {
      return new NetworkException(
          NetworkErrorCode.CHANNEL_CLOSED,
          "Network channel closed: " + e.getMessage(),
          e
      ).withContext("exceptionType", "ClosedChannelException");
    }

    // Generic I/O exception - general connection error
    if (e instanceof IOException) {
      return new NetworkException(
          NetworkErrorCode.CONNECTION_ERROR,
          "Network I/O error: " + e.getMessage(),
          e
      ).withContext("exceptionType", "IOException");
    }

    // Unknown exception - wrap as generic network error
    return new NetworkException(
        NetworkErrorCode.CONNECTION_ERROR,
        "Unexpected network error: " + e.getMessage(),
        e
    ).withContext("exceptionType", e.getClass().getSimpleName());
  }

  /**
   * Wraps code that may throw checked exceptions and automatically translates them
   * to {@link NetworkException}.
   * <p>
   * This method simplifies exception handling for operations that may throw checked
   * exceptions by automatically converting them to network exceptions. This is useful
   * for wrapping third-party library calls or low-level I/O operations.
   * <p>
   * If the operation throws an exception:
   * <ul>
   *   <li>It is passed through {@link #translate(Throwable)}</li>
   *   <li>The resulting {@link NetworkException} (or equivalent) is thrown as a runtime exception</li>
   * </ul>
   * <p>
   * If the operation completes successfully, the result is returned.
   * <p>
   * Example usage with socket operations:
   * <pre>{@code
   * byte[] data = NetworkExceptionTranslator.translateChecked(() -> {
   *     byte[] buffer = new byte[1024];
   *     int bytesRead = socket.getInputStream().read(buffer);
   *     return buffer;
   * });
   * }</pre>
   * <p>
   * Example usage with database operations:
   * <pre>{@code
   * DatabaseResult result = NetworkExceptionTranslator.translateChecked(() -> {
   *     return database.query("SELECT * FROM users");
   * });
   * }</pre>
   *
   * @param <T> the return type of the operation
   * @param operation the operation to execute (must not be null)
   * @return the result of the operation if it completes successfully
   * @throws RuntimeException (specifically {@link NetworkException} or its equivalent)
   *         if any exception occurs during operation execution
   * @throws NullPointerException if the operation is null
   *
   * @see CheckedSupplier
   * @see #translate(Throwable)
   */
  public static <T> T translateChecked(final CheckedSupplier<T> operation) {
    Objects.requireNonNull(operation, "Operation cannot be null");

    try {
      return operation.get();
    } catch (final Exception e) {
      throw translate(e);
    }
  }

  /**
   * Functional interface for operations that may throw checked exceptions.
   * <p>
   * This interface represents a task that may throw any kind of exception (checked or
   * unchecked) and produces a result of type {@code T}. It is similar to
   * {@code java.util.function.Supplier}, but allows checked exceptions.
   * <p>
   * Used with {@link NetworkExceptionTranslator#translateChecked(CheckedSupplier)} to
   * wrap exception-throwing operations in a clean, functional way.
   * <p>
   * Example implementation:
   * <pre>{@code
   * CheckedSupplier<String> readFile = () -> {
   *     return Files.readString(Paths.get("file.txt"));
   * };
   *
   * String content = NetworkExceptionTranslator.translateChecked(readFile);
   * }</pre>
   *
   * @param <T> the type of the result
   *
   * @since 26.1
   * @see NetworkExceptionTranslator#translateChecked(CheckedSupplier)
   */
  @FunctionalInterface
  public interface CheckedSupplier<T> {

    /**
     * Executes the operation and returns the result.
     * <p>
     * This method may throw any type of exception, which will be caught and
     * translated by {@link NetworkExceptionTranslator#translateChecked(CheckedSupplier)}.
     *
     * @return the result of the operation
     * @throws Exception if any error occurs during execution
     */
    T get() throws Exception;
  }

  /**
   * Private constructor to prevent instantiation of this utility class.
   */
  private NetworkExceptionTranslator() {
    // Utility class - no instantiation
  }
}
