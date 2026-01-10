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
import com.arcadedb.exception.ErrorCode;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Exception thrown for network communication errors.
 * <p>
 * This exception extends {@link ArcadeDBException} but uses network-specific error codes
 * via {@link NetworkErrorCode}. When wrapped at the engine level (where the engine doesn't
 * understand network concepts), the underlying engine error code is always set to
 * {@link ErrorCode#INTERNAL_ERROR}, while the network-specific error code is preserved
 * separately in the {@code networkErrorCode} field.
 * <p>
 * This design maintains proper architectural layering:
 * <ul>
 *   <li>Engine layer: Only sees INTERNAL_ERROR (doesn't know about network concepts)</li>
 *   <li>Network layer: Preserves specific NetworkErrorCode for detailed error handling</li>
 *   <li>Server/API layer: Can translate NetworkException to HTTP status codes</li>
 * </ul>
 * <p>
 * When serialized to JSON or converted to string, the network-specific error code is used
 * instead of the engine error code, providing clear context about the nature of the network error.
 * <p>
 * Example usage:
 * <pre>{@code
 * throw new NetworkException(
 *     NetworkErrorCode.CONNECTION_LOST,
 *     "Connection to server lost"
 * ).withContext("server", "192.168.1.100:2424")
 *  .withContext("reconnectAttempts", 3);
 * }</pre>
 *
 * @since 26.1
 * @see NetworkErrorCode
 * @see ArcadeDBException
 */
public class NetworkException extends ArcadeDBException {

  private final NetworkErrorCode networkErrorCode;

  /**
   * Constructs a new network exception with network-specific error code.
   * <p>
   * The underlying engine error code is set to {@link ErrorCode#INTERNAL_ERROR}
   * since the engine layer doesn't understand network-specific concepts.
   *
   * @param networkErrorCode the network-specific error code
   * @param message the detail message
   * @throws NullPointerException if networkErrorCode is null
   */
  public NetworkException(final NetworkErrorCode networkErrorCode, final String message) {
    super(ErrorCode.INTERNAL_ERROR, message);
    this.networkErrorCode = Objects.requireNonNull(networkErrorCode, "Network error code cannot be null");
  }

  /**
   * Constructs a new network exception with network-specific error code and cause.
   * <p>
   * The underlying engine error code is set to {@link ErrorCode#INTERNAL_ERROR}
   * since the engine layer doesn't understand network-specific concepts.
   *
   * @param networkErrorCode the network-specific error code
   * @param message the detail message
   * @param cause the underlying cause
   * @throws NullPointerException if networkErrorCode is null
   */
  public NetworkException(final NetworkErrorCode networkErrorCode, final String message, final Throwable cause) {
    super(ErrorCode.INTERNAL_ERROR, message, cause);
    this.networkErrorCode = Objects.requireNonNull(networkErrorCode, "Network error code cannot be null");
  }

  /**
   * Returns the network-specific error code.
   * <p>
   * This is the primary error code for network exceptions, providing detailed
   * context about what went wrong at the network layer.
   *
   * @return the network error code (never null)
   */
  public NetworkErrorCode getNetworkErrorCode() {
    return networkErrorCode;
  }

  /**
   * Returns the network error code name as a string.
   * <p>
   * This is equivalent to {@code getNetworkErrorCode().name()}, providing
   * the enum name of the network error code (e.g., "CONNECTION_LOST").
   *
   * @return the error code name (e.g., "CONNECTION_LOST", "PROTOCOL_ERROR")
   */
  public String getNetworkErrorCodeName() {
    return networkErrorCode.name();
  }

  /**
   * Returns the default error code for this exception type.
   * <p>
   * For network exceptions, this always returns {@link ErrorCode#INTERNAL_ERROR}
   * because network layer errors are opaque to the engine layer and are
   * handled as internal failures from the engine's perspective.
   *
   * @return {@link ErrorCode#INTERNAL_ERROR}
   */
  @Override
  protected ErrorCode getDefaultErrorCode() {
    return ErrorCode.INTERNAL_ERROR;
  }

  /**
   * Adds context information for debugging and returns this exception.
   * <p>
   * Overrides the parent method to return NetworkException for method chaining.
   *
   * @param key   the context key (ignored if null)
   * @param value the context value (ignored if null)
   * @return this exception for method chaining
   */
  @Override
  public NetworkException withContext(final String key, final Object value) {
    super.withContext(key, value);
    return this;
  }

  /**
   * Serializes this exception to JSON, including network-specific information.
   * <p>
   * The JSON representation uses the network error code name instead of the
   * underlying engine error code, providing clear context about the network error.
   * <p>
   * Example output:
   * <pre>{@code
   * {
   *   "error": "CONNECTION_LOST",
   *   "category": "Network",
   *   "message": "Connection to server lost",
   *   "timestamp": 1704834567890,
   *   "context": {
   *     "server": "192.168.1.100:2424",
   *     "reconnectAttempts": 3
   *   }
   * }
   * }</pre>
   *
   * @return JSON representation of this exception with network error code
   */
  @Override
  public String toJSON() {
    final JSONObject json = new JSONObject();
    json.put("error", networkErrorCode.name());
    json.put("category", networkErrorCode.getCategory());
    json.put("message", getMessage());
    json.put("timestamp", getTimestamp());

    if (!getContext().isEmpty()) {
      json.put("context", new JSONObject(getContext()));
    }

    if (getCause() != null) {
      final Throwable cause = getCause();
      json.put("cause", cause.getClass().getSimpleName() + ": " + cause.getMessage());
    }

    return json.toString();
  }

  /**
   * Returns a detailed string representation including network error code and context.
   * <p>
   * The format includes the network error code name instead of the underlying
   * engine error code, providing clear visibility into the network issue.
   * <p>
   * Example: "NetworkException[CONNECTION_LOST]: Connection to server lost {server=192.168.1.100:2424}"
   *
   * @return detailed string representation with network error code
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName())
      .append("[").append(networkErrorCode.name()).append("]: ")
      .append(getMessage());

    if (!getContext().isEmpty()) {
      sb.append(" {")
        .append(getContext().entrySet().stream()
          .map(entry -> entry.getKey() + "=" + entry.getValue())
          .collect(Collectors.joining(", ")))
        .append("}");
    }

    return sb.toString();
  }
}
