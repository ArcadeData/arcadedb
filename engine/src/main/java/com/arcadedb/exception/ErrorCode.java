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

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Standardized error codes for ArcadeDB exceptions.
 * Error codes are organized in categories based on the first digit(s):
 * <ul>
 *   <li>1xxx - Database errors (lifecycle, operations, metadata)</li>
 *   <li>2xxx - Transaction errors (concurrency, locking, timeouts)</li>
 *   <li>3xxx - Query errors (parsing, execution, functions)</li>
 *   <li>4xxx - Security errors (authentication, authorization)</li>
 *   <li>5xxx - Storage errors (I/O, serialization, encryption)</li>
 *   <li>6xxx - Network errors (connections, replication, clustering)</li>
 *   <li>7xxx - Schema errors (types, properties, validation)</li>
 *   <li>8xxx - Index errors (operations, constraints)</li>
 *   <li>9xxx - Graph errors (algorithms, traversals)</li>
 *   <li>10xxx - Import/Export errors</li>
 *   <li>99xxx - Internal errors</li>
 * </ul>
 *
 * @since 25.12
 * @see ArcadeDBException
 */
public enum ErrorCode {

  // ========== Static cache for O(1) lookup ==========
  // Populated at class loading time to detect duplicate codes early

  // ========== Database Errors (1xxx) ==========
  /** Database not found in the file system or registry */
  DATABASE_NOT_FOUND(1001, "Database not found"),

  /** Attempt to create a database that already exists */
  DATABASE_ALREADY_EXISTS(1002, "Database already exists"),

  /** Operation attempted on a closed database */
  DATABASE_IS_CLOSED(1003, "Database is closed"),

  /** Write operation attempted on a read-only database */
  DATABASE_IS_READONLY(1004, "Database is read-only"),

  /** Database metadata corruption or inconsistency */
  DATABASE_METADATA_ERROR(1005, "Database metadata error"),

  /** General database operation failure */
  DATABASE_OPERATION_ERROR(1006, "Database operation error"),

  /** Invalid or stale database instance reference */
  INVALID_DATABASE_INSTANCE(1007, "Invalid database instance"),

  /** Database configuration error */
  CONFIGURATION_ERROR(1008, "Configuration error"),

  // ========== Transaction Errors (2xxx) ==========
  /** Transaction exceeded time limit */
  TRANSACTION_TIMEOUT(2001, "Transaction timeout"),

  /** Optimistic locking conflict detected */
  TRANSACTION_CONFLICT(2002, "Transaction conflict detected"),

  /** Operation requires retry (optimistic concurrency) */
  TRANSACTION_RETRY_NEEDED(2003, "Transaction needs retry"),

  /** Concurrent modification of the same record */
  CONCURRENT_MODIFICATION(2004, "Concurrent modification detected"),

  /** Failed to acquire lock within timeout period */
  LOCK_TIMEOUT(2005, "Lock acquisition timeout"),

  /** General transaction management error */
  TRANSACTION_ERROR(2006, "Transaction error"),

  // ========== Query Errors (3xxx) ==========
  /** Query syntax is invalid or malformed */
  QUERY_SYNTAX_ERROR(3001, "Query syntax error"),

  /** Query execution failed during runtime */
  QUERY_EXECUTION_ERROR(3002, "Query execution error"),

  /** Command parsing error (generic) */
  COMMAND_PARSING_ERROR(3003, "Command parsing error"),

  /** Command execution error */
  COMMAND_EXECUTION_ERROR(3004, "Command execution error"),

  /** User-defined function execution failed */
  FUNCTION_EXECUTION_ERROR(3005, "Function execution error"),

  // ========== Security Errors (4xxx) ==========
  /** User is not authenticated */
  UNAUTHORIZED(4001, "Unauthorized access"),

  /** User lacks required permissions */
  FORBIDDEN(4002, "Access forbidden"),

  /** Authentication credentials are invalid */
  AUTHENTICATION_FAILED(4003, "Authentication failed"),

  /** User is not authorized for the requested operation */
  AUTHORIZATION_FAILED(4004, "Authorization failed"),

  // ========== Storage Errors (5xxx) ==========
  /** File system I/O operation failed */
  IO_ERROR(5001, "I/O error"),

  /** Data corruption detected in storage files */
  CORRUPTION_DETECTED(5002, "Data corruption detected"),

  /** Write-ahead log operation failed */
  WAL_ERROR(5003, "Write-ahead log error"),

  /** Binary serialization/deserialization failed */
  SERIALIZATION_ERROR(5004, "Serialization error"),

  /** Encryption or decryption operation failed */
  ENCRYPTION_ERROR(5005, "Encryption error"),

  /** Database backup operation failed */
  BACKUP_ERROR(5006, "Backup operation error"),

  /** Database restore operation failed */
  RESTORE_ERROR(5007, "Restore operation error"),

  // ========== Network Errors (6xxx) ==========
  /** Failed to establish or maintain connection */
  CONNECTION_ERROR(6001, "Connection error"),

  /** Network connection was lost */
  CONNECTION_LOST(6002, "Connection lost"),

  /** Network protocol violation or incompatibility */
  NETWORK_PROTOCOL_ERROR(6003, "Network protocol error"),

  /** Remote operation failed on the server side */
  REMOTE_ERROR(6004, "Remote operation error"),

  /** Replication operation failed */
  REPLICATION_ERROR(6005, "Replication error"),

  /** Cluster quorum not reached for operation */
  QUORUM_NOT_REACHED(6006, "Quorum not reached"),

  /** Current server is not the cluster leader */
  SERVER_NOT_LEADER(6007, "Server is not the leader"),

  // ========== Schema Errors (7xxx) ==========
  /** General schema definition error */
  SCHEMA_ERROR(7001, "Schema error"),

  /** Referenced type does not exist */
  TYPE_NOT_FOUND(7002, "Type not found"),

  /** Referenced property does not exist */
  PROPERTY_NOT_FOUND(7003, "Property not found"),

  /** Data validation failed against schema constraints */
  VALIDATION_ERROR(7004, "Validation error"),

  // ========== Index Errors (8xxx) ==========
  /** General index operation error */
  INDEX_ERROR(8001, "Index error"),

  /** Referenced index does not exist */
  INDEX_NOT_FOUND(8002, "Index not found"),

  /** Unique constraint violation */
  DUPLICATE_KEY(8003, "Duplicate key violation"),

  // ========== Graph Errors (9xxx) ==========
  /** Graph algorithm execution failed */
  GRAPH_ALGORITHM_ERROR(9001, "Graph algorithm error"),

  // ========== Import/Export Errors (10xxx) ==========
  /** Data import operation failed */
  IMPORT_ERROR(10001, "Import error"),

  /** Data export operation failed */
  EXPORT_ERROR(10002, "Export error"),

  // ========== General Errors (99xxx) ==========
  /** Unexpected internal error (should not normally occur) */
  INTERNAL_ERROR(99999, "Internal error");

  // Static map for efficient O(1) error code lookup
  private static final Map<Integer, ErrorCode> CODE_MAP =
      java.util.stream.Stream.of(values())
          .collect(Collectors.toUnmodifiableMap(ErrorCode::getCode, e -> e));

  private final int code;
  private final String defaultMessage;

  ErrorCode(final int code, final String defaultMessage) {
    this.code = code;
    this.defaultMessage = defaultMessage;
  }

  /**
   * Returns the numeric error code.
   *
   * @return the error code (e.g., 1001, 2001, etc.)
   */
  public int getCode() {
    return code;
  }

  /**
   * Returns the default human-readable error message.
   * This can be overridden when throwing exceptions with custom messages.
   *
   * @return the default error message
   */
  public String getDefaultMessage() {
    return defaultMessage;
  }

  /**
   * Returns the error category based on the error code range.
   *
   * @return the error category name
   */
  public String getCategory() {
    final int category = code / 1000;
    return switch (category) {
      case 1 -> "Database";
      case 2 -> "Transaction";
      case 3 -> "Query";
      case 4 -> "Security";
      case 5 -> "Storage";
      case 6 -> "Network";
      case 7 -> "Schema";
      case 8 -> "Index";
      case 9 -> "Graph";
      case 10 -> "Import/Export";
      case 99 -> "Internal";
      default -> "Unknown";
    };
  }

  /**
   * Finds an ErrorCode by its numeric code.
   * <p>
   * Uses a static map for O(1) lookup efficiency. This also allows
   * detection of duplicate error codes at class-loading time.
   *
   * @param code the numeric error code to look up
   * @return the matching ErrorCode, or INTERNAL_ERROR if not found
   */
  public static ErrorCode fromCode(final int code) {
    return CODE_MAP.getOrDefault(code, INTERNAL_ERROR);
  }

  @Override
  public String toString() {
    return String.format("%s(%d): %s", name(), code, defaultMessage);
  }
}
