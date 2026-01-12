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

/**
 * Network-specific error codes for ArcadeDB network layer.
 * <p>
 * This enum defines all network-layer error codes, separate from the core engine error codes
 * ({@code com.arcadedb.exception.ErrorCode}). This separation maintains proper architectural
 * layering: the engine module has no knowledge of network concepts, while the network module
 * provides its own error taxonomy.
 * <p>
 * <strong>Design Principles:</strong>
 * <ul>
 *   <li><strong>String-based:</strong> Error codes are enum names (e.g., "CONNECTION_LOST"), not numeric values</li>
 *   <li><strong>Layer-specific:</strong> Only network-level errors; engine errors are separate</li>
 *   <li><strong>Self-documenting:</strong> Names clearly indicate the network condition</li>
 *   <li><strong>Categorized:</strong> Organized by networking concepts (connection, protocol, replication, etc.)</li>
 * </ul>
 * <p>
 * <strong>Error Code Categories:</strong>
 * <ul>
 *   <li><strong>CONNECTION_*</strong> - Network connection establishment, management, and lifecycle</li>
 *   <li><strong>PROTOCOL_*</strong> - Network protocol violations, incompatibilities, and message format errors</li>
 *   <li><strong>REPLICATION_*</strong> - Cluster replication, synchronization, quorum, and leader election</li>
 *   <li><strong>REMOTE_*</strong> - Remote server operations and responses</li>
 *   <li><strong>CHANNEL_*</strong> - Network channel I/O operations</li>
 * </ul>
 * <p>
 * <strong>Architectural Context:</strong>
 * Network errors occur when:
 * <ul>
 *   <li>TCP/IP connections fail or are interrupted</li>
 *   <li>Binary protocol messages are malformed or incompatible</li>
 *   <li>Cluster replication operations encounter issues</li>
 *   <li>Remote database operations fail at the transport level</li>
 *   <li>Network channels experience I/O errors</li>
 * </ul>
 * <p>
 * <strong>Usage Examples:</strong>
 * <pre>{@code
 * // Throwing a network exception
 * throw new NetworkException(
 *     NetworkErrorCode.CONNECTION_LOST,
 *     "Connection to server lost"
 * ).withContext("server", "192.168.1.100:2424")
 *  .withContext("reconnectAttempts", 3);
 *
 * // Accessing error code information
 * NetworkErrorCode code = NetworkErrorCode.CONNECTION_LOST;
 * String name = code.name();                    // "CONNECTION_LOST"
 * String category = code.getCategory();         // "Network"
 * String message = code.getDefaultMessage();    // "Connection lost"
 *
 * // In exception handlers
 * try {
 *     remoteDatabase.query("SELECT * FROM users");
 * } catch (NetworkException e) {
 *     if (e.getNetworkErrorCode() == NetworkErrorCode.CONNECTION_TIMEOUT) {
 *         // Handle timeout - maybe retry with backoff
 *     }
 * }
 * }</pre>
 * <p>
 * <strong>Translation to HTTP:</strong>
 * Network errors are translated to HTTP status codes in the server module via
 * {@code HttpExceptionTranslator}. This maintains separation of concerns:
 * <ul>
 *   <li>Network module: Knows about network errors, not HTTP</li>
 *   <li>Server module: Translates network errors to HTTP status codes</li>
 * </ul>
 *
 * @since 26.1
 * @see NetworkException
 * @see com.arcadedb.exception.ErrorCode
 * @see com.arcadedb.server.http.HttpExceptionTranslator
 */
public enum NetworkErrorCode {

  // ========== Connection Errors ==========
  /**
   * Failed to establish network connection to remote server.
   * This is a general connection failure, used when more specific codes don't apply.
   */
  CONNECTION_ERROR("Connection error"),

  /**
   * Network connection was lost or unexpectedly closed.
   * This occurs when an established connection terminates unexpectedly.
   */
  CONNECTION_LOST("Connection lost"),

  /**
   * Connection closed by remote peer.
   * The remote server closed the connection gracefully.
   */
  CONNECTION_CLOSED("Connection closed"),

  /**
   * Network connection operation timed out.
   * A connection attempt or operation exceeded the configured time limit.
   */
  CONNECTION_TIMEOUT("Connection timeout"),

  // ========== Protocol Errors ==========
  /**
   * Network protocol violation or general incompatibility.
   * Used for protocol-level errors that don't fit other categories.
   */
  PROTOCOL_ERROR("Network protocol error"),

  /**
   * Invalid or malformed network message received.
   * The message format doesn't conform to protocol specifications.
   */
  PROTOCOL_INVALID_MESSAGE("Invalid message format"),

  /**
   * Protocol version mismatch between client and server.
   * The client and server are using incompatible protocol versions.
   */
  PROTOCOL_VERSION_MISMATCH("Protocol version mismatch"),

  // ========== Replication Errors ==========
  /**
   * General replication operation failure.
   * Used for replication errors that don't fit more specific categories.
   */
  REPLICATION_ERROR("Replication error"),

  /**
   * Cluster quorum not reached for the operation.
   * Insufficient cluster nodes are available to perform the operation.
   */
  REPLICATION_QUORUM_NOT_REACHED("Quorum not reached"),

  /**
   * Current server is not the cluster leader.
   * Write operations require routing to the leader node.
   */
  REPLICATION_NOT_LEADER("Server is not the leader"),

  /**
   * Replication synchronization failed.
   * The node failed to synchronize with other cluster members.
   */
  REPLICATION_SYNC_ERROR("Replication sync error"),

  // ========== Remote Errors ==========
  /**
   * Remote operation failed on the server side.
   * A general error from a remote server operation.
   */
  REMOTE_ERROR("Remote operation error"),

  /**
   * Remote server returned an error response.
   * The remote server explicitly reported an error.
   */
  REMOTE_SERVER_ERROR("Remote server error"),

  // ========== Channel Errors ==========
  /**
   * Network channel closed unexpectedly.
   * The underlying network channel was closed without proper cleanup.
   */
  CHANNEL_CLOSED("Channel closed"),

  /**
   * Network channel operation failed.
   * A general error occurred during channel I/O operations.
   */
  CHANNEL_ERROR("Channel error");

  private final String defaultMessage;

  /**
   * Constructs a network error code with a default message.
   *
   * @param defaultMessage the default human-readable error message
   */
  NetworkErrorCode(final String defaultMessage) {
    this.defaultMessage = defaultMessage;
  }

  /**
   * Returns the default human-readable error message.
   * <p>
   * This message can be used when no custom message is provided,
   * or as a template for more detailed error descriptions.
   *
   * @return the default error message
   */
  public String getDefaultMessage() {
    return defaultMessage;
  }

  /**
   * Returns the error category.
   * <p>
   * Network errors always belong to the "Network" category to distinguish them
   * from engine-level errors.
   *
   * @return "Network"
   */
  public String getCategory() {
    return "Network";
  }

  /**
   * Returns a string representation of this error code.
   * <p>
   * The format is: "CODE_NAME [Network]: default message"
   * Example: "CONNECTION_LOST [Network]: Connection lost"
   *
   * @return a formatted string representation
   */
  @Override
  public String toString() {
    return String.format("%s [Network]: %s", name(), defaultMessage);
  }
}
