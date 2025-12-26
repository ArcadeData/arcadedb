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

/**
 * Exception thrown when network and communication errors occur.
 * <p>
 * This exception category covers:
 * <ul>
 *   <li>Connection failures (cannot establish connection)</li>
 *   <li>Connection lost (network interruption)</li>
 *   <li>Network protocol errors</li>
 *   <li>Remote operation failures</li>
 *   <li>Replication errors</li>
 *   <li>Cluster quorum failures</li>
 *   <li>Leader election issues</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>{@code
 * throw new NetworkException(ErrorCode.CONNECTION_ERROR, "Failed to connect to server")
 *     .withContext("server", serverAddress)
 *     .withContext("port", port)
 *     .withContext("timeout", connectionTimeout);
 * }</pre>
 *
 * @since 25.12
 * @see ErrorCode
 * @see ArcadeDBException
 */
public class NetworkException extends ArcadeDBException {

  /**
   * Constructs a new network exception with the specified error code and message.
   *
   * @param errorCode the error code
   * @param message   the detail message
   */
  public NetworkException(final ErrorCode errorCode, final String message) {
    super(errorCode, message);
  }

  /**
   * Constructs a new network exception with the specified error code, message, and cause.
   *
   * @param errorCode the error code
   * @param message   the detail message
   * @param cause     the underlying cause
   */
  public NetworkException(final ErrorCode errorCode, final String message, final Throwable cause) {
    super(errorCode, message, cause);
  }

  /**
   * Returns the default error code for network exceptions.
   *
   * @return CONNECTION_ERROR
   */
  @Override
  protected ErrorCode getDefaultErrorCode() {
    return ErrorCode.CONNECTION_ERROR;
  }

  /**
   * Returns the HTTP status code for this exception.
   * Network exceptions typically map to 503 (Service Unavailable) or 502 (Bad Gateway).
   *
   * @return the HTTP status code
   */
  @Override
  public int getHttpStatus() {
    return switch (getErrorCode()) {
      case CONNECTION_ERROR, CONNECTION_LOST -> 503; // Service Unavailable
      case NETWORK_PROTOCOL_ERROR -> 502; // Bad Gateway
      case REMOTE_ERROR -> 502; // Bad Gateway
      case REPLICATION_ERROR, QUORUM_NOT_REACHED -> 503; // Service Unavailable
      case SERVER_NOT_LEADER -> 307; // Temporary Redirect (to leader)
      default -> 503;
    };
  }
}
