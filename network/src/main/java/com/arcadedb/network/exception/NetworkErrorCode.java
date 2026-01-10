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
 * These codes are separate from core engine codes to maintain proper layering and
 * architectural separation. Network errors use string-based codes organized by
 * networking concept categories.
 * <p>
 * Error codes are organized by category:
 * <ul>
 *   <li>CONNECTION_* - Network connection establishment and management</li>
 *   <li>PROTOCOL_* - Network protocol violations and incompatibilities</li>
 *   <li>REPLICATION_* - Cluster replication and synchronization</li>
 *   <li>REMOTE_* - Remote server operations</li>
 *   <li>CHANNEL_* - Network channel operations</li>
 * </ul>
 *
 * @since 26.1
 * @see com.arcadedb.network.exception.NetworkException
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
