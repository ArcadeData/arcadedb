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
import com.arcadedb.exception.DatabaseException;
import com.arcadedb.exception.ErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * Unit tests for {@link NetworkExceptionTranslator}.
 * <p>
 * Tests cover:
 * <ul>
 *   <li>Pass-through of existing NetworkExceptions</li>
 *   <li>Translation of ArcadeDBException to NetworkException with REMOTE_ERROR</li>
 *   <li>Translation of SocketTimeoutException to CONNECTION_TIMEOUT</li>
 *   <li>Translation of ClosedChannelException to CHANNEL_CLOSED</li>
 *   <li>Translation of IOException to CONNECTION_ERROR</li>
 *   <li>Translation of unknown exceptions to CONNECTION_ERROR</li>
 *   <li>Context preservation and cause chain</li>
 *   <li>Checked supplier functional interface behavior</li>
 * </ul>
 *
 * @since 26.1
 * @see NetworkExceptionTranslator
 * @see NetworkException
 * @see NetworkErrorCode
 */
@DisplayName("NetworkExceptionTranslator Unit Tests")
class NetworkExceptionTranslatorTest {

  // ========== Pass-through Tests ==========

  @Test
  @DisplayName("NetworkException passed through unchanged")
  void testTranslateNetworkException() {
    final NetworkException original = new NetworkException(
        NetworkErrorCode.CONNECTION_LOST,
        "Connection lost to server"
    ).withContext("server", "192.168.1.100:2424");

    final RuntimeException result = NetworkExceptionTranslator.translate(original);

    assertThat(result).isSameAs(original);
    assertThat(result).isInstanceOf(NetworkException.class);
    final NetworkException netEx = (NetworkException) result;
    assertThat(netEx.getNetworkErrorCode())
        .isEqualTo(NetworkErrorCode.CONNECTION_LOST);
  }

  // ========== ArcadeDBException Translation Tests ==========

  @Test
  @DisplayName("ArcadeDBException wrapped with REMOTE_ERROR")
  void testTranslateArcadeDBException() {
    final ArcadeDBException cause = new DatabaseException(
        ErrorCode.DB_NOT_FOUND,
        "Database 'mydb' not found"
    );

    final RuntimeException result = NetworkExceptionTranslator.translate(cause);

    assertThat(result).isInstanceOf(NetworkException.class);
    final NetworkException netEx = (NetworkException) result;
    assertThat(netEx.getNetworkErrorCode()).isEqualTo(NetworkErrorCode.REMOTE_ERROR);
    assertThat(netEx.getMessage()).contains("Remote database operation failed");
    assertThat(netEx.getCause()).isSameAs(cause);
  }

  @Test
  @DisplayName("ArcadeDBException context includes original error code")
  void testTranslateArcadeDBExceptionPreservesContext() {
    final DatabaseException cause = new DatabaseException(
        ErrorCode.DB_NOT_FOUND,
        "Database not found"
    );

    final RuntimeException result = NetworkExceptionTranslator.translate(cause);
    final NetworkException netEx = (NetworkException) result;

    assertThat(netEx.getContext())
        .containsEntry("originalError", "DB_NOT_FOUND")
        .containsEntry("category", "Database");
  }

  // ========== SocketTimeoutException Translation Tests ==========

  @Test
  @DisplayName("SocketTimeoutException mapped to CONNECTION_TIMEOUT")
  void testTranslateSocketTimeoutException() {
    final SocketTimeoutException cause = new SocketTimeoutException("Read timeout after 5000ms");

    final RuntimeException result = NetworkExceptionTranslator.translate(cause);

    assertThat(result).isInstanceOf(NetworkException.class);
    final NetworkException netEx = (NetworkException) result;
    assertThat(netEx.getNetworkErrorCode()).isEqualTo(NetworkErrorCode.CONNECTION_TIMEOUT);
    assertThat(netEx.getMessage()).contains("Network operation timed out");
    assertThat(netEx.getCause()).isSameAs(cause);
  }

  @Test
  @DisplayName("SocketTimeoutException context includes exception type")
  void testTranslateSocketTimeoutExceptionContext() {
    final SocketTimeoutException cause = new SocketTimeoutException("Timeout");

    final RuntimeException result = NetworkExceptionTranslator.translate(cause);
    final NetworkException netEx = (NetworkException) result;

    assertThat(netEx.getContext())
        .containsEntry("exceptionType", "SocketTimeoutException");
  }

  // ========== ClosedChannelException Translation Tests ==========

  @Test
  @DisplayName("ClosedChannelException mapped to CHANNEL_CLOSED")
  void testTranslateClosedChannelException() {
    final ClosedChannelException cause = new ClosedChannelException();

    final RuntimeException result = NetworkExceptionTranslator.translate(cause);

    assertThat(result).isInstanceOf(NetworkException.class);
    final NetworkException netEx = (NetworkException) result;
    assertThat(netEx.getNetworkErrorCode()).isEqualTo(NetworkErrorCode.CHANNEL_CLOSED);
    assertThat(netEx.getMessage()).contains("Network channel closed");
    assertThat(netEx.getCause()).isSameAs(cause);
  }

  @Test
  @DisplayName("ClosedChannelException context includes exception type")
  void testTranslateClosedChannelExceptionContext() {
    final ClosedChannelException cause = new ClosedChannelException();

    final RuntimeException result = NetworkExceptionTranslator.translate(cause);
    final NetworkException netEx = (NetworkException) result;

    assertThat(netEx.getContext())
        .containsEntry("exceptionType", "ClosedChannelException");
  }

  // ========== IOException Translation Tests ==========

  @Test
  @DisplayName("IOException mapped to CONNECTION_ERROR")
  void testTranslateIOException() {
    final IOException cause = new IOException("Connection refused");

    final RuntimeException result = NetworkExceptionTranslator.translate(cause);

    assertThat(result).isInstanceOf(NetworkException.class);
    final NetworkException netEx = (NetworkException) result;
    assertThat(netEx.getNetworkErrorCode()).isEqualTo(NetworkErrorCode.CONNECTION_ERROR);
    assertThat(netEx.getMessage()).contains("Network I/O error");
    assertThat(netEx.getCause()).isSameAs(cause);
  }

  @Test
  @DisplayName("IOException context includes exception type")
  void testTranslateIOExceptionContext() {
    final IOException cause = new IOException("IO error message");

    final RuntimeException result = NetworkExceptionTranslator.translate(cause);
    final NetworkException netEx = (NetworkException) result;

    assertThat(netEx.getContext())
        .containsEntry("exceptionType", "IOException");
  }

  // ========== Unknown Exception Translation Tests ==========

  @Test
  @DisplayName("Unknown exception mapped to CONNECTION_ERROR")
  void testTranslateUnknownException() {
    final RuntimeException cause = new RuntimeException("Some unknown error");

    final RuntimeException result = NetworkExceptionTranslator.translate(cause);

    assertThat(result).isInstanceOf(NetworkException.class);
    final NetworkException netEx = (NetworkException) result;
    assertThat(netEx.getNetworkErrorCode()).isEqualTo(NetworkErrorCode.CONNECTION_ERROR);
    assertThat(netEx.getMessage()).contains("Unexpected network error");
    assertThat(netEx.getCause()).isSameAs(cause);
  }

  @Test
  @DisplayName("Unknown exception context includes exception type")
  void testTranslateUnknownExceptionContext() {
    final CustomException cause = new CustomException("Custom error");

    final RuntimeException result = NetworkExceptionTranslator.translate(cause);
    final NetworkException netEx = (NetworkException) result;

    assertThat(netEx.getContext())
        .containsEntry("exceptionType", "CustomException");
  }

  // ========== Null Input Tests ==========

  @Test
  @DisplayName("translate() throws NullPointerException for null exception")
  void testTranslateNullThrowsNPE() {
    assertThatNullPointerException()
        .isThrownBy(() -> NetworkExceptionTranslator.translate(null))
        .withMessage("Exception to translate cannot be null");
  }

  @Test
  @DisplayName("translateChecked() throws NullPointerException for null operation")
  void testTranslateCheckedNullOperationThrowsNPE() {
    assertThatNullPointerException()
        .isThrownBy(() -> NetworkExceptionTranslator.translateChecked(null))
        .withMessage("Operation cannot be null");
  }

  // ========== CheckedSupplier Tests ==========

  @Test
  @DisplayName("translateChecked() returns successful result")
  void testTranslateCheckedSuccess() {
    final String expected = "test data";
    final String result = NetworkExceptionTranslator.translateChecked(() -> expected);

    assertThat(result).isEqualTo(expected);
  }

  @Test
  @DisplayName("translateChecked() with checked exception translates to NetworkException")
  void testTranslateCheckedIOException() {
    final IOException cause = new IOException("Connection error");

    assertThatThrownBy(() ->
        NetworkExceptionTranslator.translateChecked(() -> {
          throw cause;
        })
    )
        .isInstanceOf(NetworkException.class)
        .hasFieldOrPropertyWithValue("networkErrorCode", NetworkErrorCode.CONNECTION_ERROR)
        .hasCause(cause);
  }

  @Test
  @DisplayName("translateChecked() with ArcadeDBException translates with REMOTE_ERROR")
  void testTranslateCheckedArcadeDBException() {
    final DatabaseException cause = new DatabaseException(
        ErrorCode.DB_IS_READONLY,
        "Database is read-only"
    );

    assertThatThrownBy(() ->
        NetworkExceptionTranslator.translateChecked(() -> {
          throw cause;
        })
    )
        .isInstanceOf(NetworkException.class)
        .hasFieldOrPropertyWithValue("networkErrorCode", NetworkErrorCode.REMOTE_ERROR)
        .hasCause(cause);
  }

  @Test
  @DisplayName("translateChecked() with SocketTimeoutException")
  void testTranslateCheckedSocketTimeout() {
    final SocketTimeoutException cause = new SocketTimeoutException("Timeout");

    assertThatThrownBy(() ->
        NetworkExceptionTranslator.translateChecked(() -> {
          throw cause;
        })
    )
        .isInstanceOf(NetworkException.class)
        .hasFieldOrPropertyWithValue("networkErrorCode", NetworkErrorCode.CONNECTION_TIMEOUT)
        .hasCause(cause);
  }

  @Test
  @DisplayName("translateChecked() preserves null result")
  void testTranslateCheckedNullResult() {
    final String result = NetworkExceptionTranslator.translateChecked(() -> null);

    assertThat(result).isNull();
  }

  @Test
  @DisplayName("translateChecked() with complex object return type")
  void testTranslateCheckedComplexType() {
    final TestData data = new TestData("test", 42);

    final TestData result = NetworkExceptionTranslator.translateChecked(() -> data);

    assertThat(result).isSameAs(data);
    assertThat(result.name).isEqualTo("test");
    assertThat(result.value).isEqualTo(42);
  }

  // ========== Cause Chain Tests ==========

  @Test
  @DisplayName("Cause chain preserved for nested exceptions")
  void testTranslateCauseChain() {
    final IOException ioEx = new IOException("IO error");
    final SocketTimeoutException timeoutEx = new SocketTimeoutException("Timeout");
    timeoutEx.initCause(ioEx);

    final RuntimeException result = NetworkExceptionTranslator.translate(timeoutEx);

    assertThat(result)
        .isInstanceOf(NetworkException.class)
        .hasCause(timeoutEx)
        .hasRootCauseInstanceOf(IOException.class);
  }

  @Test
  @DisplayName("Message from original exception included in NetworkException")
  void testTranslateMessagePreservation() {
    final String originalMessage = "Specific error details";
    final IOException cause = new IOException(originalMessage);

    final RuntimeException result = NetworkExceptionTranslator.translate(cause);
    final NetworkException netEx = (NetworkException) result;

    assertThat(netEx.getMessage()).contains(originalMessage);
  }

  // ========== Internal Error Code Mapping ==========

  @Test
  @DisplayName("NetworkException uses INTERNAL_ERROR as engine error code")
  void testNetworkExceptionInternalErrorCode() {
    final NetworkException netEx = new NetworkException(
        NetworkErrorCode.CONNECTION_LOST,
        "Connection lost"
    );

    assertThat(netEx.getErrorCode()).isEqualTo(ErrorCode.INTERNAL_ERROR);
    assertThat(netEx.getNetworkErrorCode()).isEqualTo(NetworkErrorCode.CONNECTION_LOST);
  }

  // ========== Helper Classes ==========

  /**
   * Custom exception for testing unknown exception handling.
   */
  static class CustomException extends Exception {
    CustomException(final String message) {
      super(message);
    }
  }

  /**
   * Test data class for testing complex return types.
   */
  static class TestData {
    final String name;
    final int value;

    TestData(final String name, final int value) {
      this.name = name;
      this.value = value;
    }
  }
}
