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
 * Standardized error codes for ArcadeDB exceptions.
 * Error codes are organized by category:
 * - 1xxx: Database errors
 * - 2xxx: Transaction errors
 * - 3xxx: Query errors
 * - 4xxx: Security errors
 * - 5xxx: I/O errors
 * - 6xxx: Network errors
 */
public enum ErrorCode {
  // Database errors (1xxx)
  DATABASE_NOT_FOUND(1001, "Database not found"),
  DATABASE_ALREADY_EXISTS(1002, "Database already exists"),
  DATABASE_IS_CLOSED(1003, "Database is closed"),
  DATABASE_IS_READONLY(1004, "Database is read-only"),
  DATABASE_OPERATION_FAILED(1005, "Database operation failed"),
  INVALID_DATABASE_INSTANCE(1006, "Invalid database instance"),
  DATABASE_METADATA_ERROR(1007, "Database metadata error"),
  DUPLICATED_KEY(1008, "Duplicated key"),
  RECORD_NOT_FOUND(1009, "Record not found"),
  CONFIGURATION_ERROR(1010, "Configuration error"),
  SCHEMA_ERROR(1011, "Schema error"),
  VALIDATION_ERROR(1012, "Validation error"),

  // Transaction errors (2xxx)
  TRANSACTION_FAILED(2001, "Transaction failed"),
  TRANSACTION_TIMEOUT(2002, "Transaction timeout"),
  CONCURRENT_MODIFICATION(2003, "Concurrent modification detected"),
  NEED_RETRY(2004, "Operation needs retry"),
  LOCK_TIMEOUT(2005, "Lock acquisition timeout"),

  // Query errors (3xxx)
  QUERY_PARSING_ERROR(3001, "Query parsing error"),
  SQL_PARSING_ERROR(3002, "SQL parsing error"),
  COMMAND_PARSING_ERROR(3003, "Command parsing error"),
  COMMAND_EXECUTION_ERROR(3004, "Command execution error"),
  FUNCTION_EXECUTION_ERROR(3005, "Function execution error"),
  GRAPH_ALGORITHM_ERROR(3006, "Graph algorithm error"),
  INDEX_ERROR(3007, "Index error"),

  // Security errors (4xxx)
  AUTHENTICATION_FAILED(4001, "Authentication failed"),
  AUTHORIZATION_FAILED(4002, "Authorization failed"),
  SECURITY_ERROR(4003, "Security error"),
  ENCRYPTION_ERROR(4004, "Encryption error"),

  // I/O errors (5xxx)
  IO_ERROR(5001, "I/O error"),
  SERIALIZATION_ERROR(5002, "Serialization error"),
  JSON_ERROR(5003, "JSON error"),
  WAL_ERROR(5004, "Write-Ahead Log error"),
  BACKUP_ERROR(5005, "Backup error"),
  RESTORE_ERROR(5006, "Restore error"),
  IMPORT_ERROR(5007, "Import error"),
  EXPORT_ERROR(5008, "Export error"),

  // Network errors (6xxx)
  NETWORK_ERROR(6001, "Network error"),
  NETWORK_PROTOCOL_ERROR(6002, "Network protocol error"),
  CONNECTION_ERROR(6003, "Connection error"),
  REMOTE_ERROR(6004, "Remote error"),
  REPLICATION_ERROR(6005, "Replication error"),
  REPLICATION_LOG_ERROR(6006, "Replication log error"),
  QUORUM_NOT_REACHED(6007, "Quorum not reached"),
  SERVER_NOT_LEADER(6008, "Server is not the leader"),

  // Generic/Unknown errors (9xxx)
  UNKNOWN_ERROR(9999, "Unknown error");

  private final int code;
  private final String description;

  ErrorCode(final int code, final String description) {
    this.code = code;
    this.description = description;
  }

  public int getCode() {
    return code;
  }

  public String getDescription() {
    return description;
  }

  public String getCategory() {
    final int category = code / 1000;
    return switch (category) {
      case 1 -> "Database";
      case 2 -> "Transaction";
      case 3 -> "Query";
      case 4 -> "Security";
      case 5 -> "I/O";
      case 6 -> "Network";
      default -> "Unknown";
    };
  }

  @Override
  public String toString() {
    return String.format("%s(%d): %s", name(), code, description);
  }
}
