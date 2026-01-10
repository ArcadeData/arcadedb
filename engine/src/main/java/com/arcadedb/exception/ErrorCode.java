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
 * Core error codes for ArcadeDB engine exceptions.
 * <p>
 * Error codes are organized by category using enum name prefixes:
 * <ul>
 *   <li>DB_* - Database lifecycle and operations</li>
 *   <li>TX_* - Transaction management</li>
 *   <li>QUERY_* - Query parsing and execution</li>
 *   <li>SEC_* - Security (authentication, authorization)</li>
 *   <li>STORAGE_* - I/O and persistence</li>
 *   <li>SCHEMA_* - Schema and type system</li>
 *   <li>INDEX_* - Index operations</li>
 *   <li>GRAPH_* - Graph algorithms and traversal</li>
 *   <li>IMPORT_* / EXPORT_* - Data import/export</li>
 *   <li>INTERNAL_* - Internal system errors</li>
 * </ul>
 * <p>
 * Note: Network errors (CONNECTION_*, REPLICATION_*, etc.) are NOT in this enum.
 * They are defined in the network module to maintain proper layering.
 *
 * @since 26.1
 * @see ArcadeDBException
 */
public enum ErrorCode {

  // ========== Database Errors ==========
  /** Database not found in the file system or registry */
  DB_NOT_FOUND(ErrorCategory.DATABASE, "Database not found"),

  /** Attempt to create a database that already exists */
  DB_ALREADY_EXISTS(ErrorCategory.DATABASE, "Database already exists"),

  /** Operation attempted on a closed database */
  DB_IS_CLOSED(ErrorCategory.DATABASE, "Database is closed"),

  /** Write operation attempted on a read-only database */
  DB_IS_READONLY(ErrorCategory.DATABASE, "Database is read-only"),

  /** Database metadata corruption or inconsistency */
  DB_METADATA_ERROR(ErrorCategory.DATABASE, "Database metadata error"),

  /** General database operation failure */
  DB_OPERATION_ERROR(ErrorCategory.DATABASE, "Database operation error"),

  /** Invalid or stale database instance reference */
  DB_INVALID_INSTANCE(ErrorCategory.DATABASE, "Invalid database instance"),

  /** Database configuration error */
  DB_CONFIG_ERROR(ErrorCategory.DATABASE, "Configuration error"),

  // ========== Transaction Errors ==========
  /** Transaction exceeded time limit */
  TX_TIMEOUT(ErrorCategory.TRANSACTION, "Transaction timeout"),

  /** Optimistic locking conflict detected */
  TX_CONFLICT(ErrorCategory.TRANSACTION, "Transaction conflict detected"),

  /** Operation requires retry (optimistic concurrency) */
  TX_RETRY_NEEDED(ErrorCategory.TRANSACTION, "Transaction needs retry"),

  /** Concurrent modification of the same record */
  TX_CONCURRENT_MODIFICATION(ErrorCategory.TRANSACTION, "Concurrent modification detected"),

  /** Failed to acquire lock within timeout period */
  TX_LOCK_TIMEOUT(ErrorCategory.TRANSACTION, "Lock acquisition timeout"),

  /** General transaction management error */
  TX_ERROR(ErrorCategory.TRANSACTION, "Transaction error"),

  // ========== Query Errors ==========
  /** Query syntax is invalid or malformed */
  QUERY_SYNTAX_ERROR(ErrorCategory.QUERY, "Query syntax error"),

  /** Query execution failed during runtime */
  QUERY_EXECUTION_ERROR(ErrorCategory.QUERY, "Query execution error"),

  /** Command parsing error (generic) */
  QUERY_PARSING_ERROR(ErrorCategory.QUERY, "Command parsing error"),

  /** Command execution error */
  QUERY_COMMAND_ERROR(ErrorCategory.QUERY, "Command execution error"),

  /** User-defined function execution failed */
  QUERY_FUNCTION_ERROR(ErrorCategory.QUERY, "Function execution error"),

  // ========== Security Errors ==========
  /** User is not authenticated */
  SEC_UNAUTHORIZED(ErrorCategory.SECURITY, "Unauthorized access"),

  /** User lacks required permissions */
  SEC_FORBIDDEN(ErrorCategory.SECURITY, "Access forbidden"),

  /** Authentication credentials are invalid */
  SEC_AUTHENTICATION_FAILED(ErrorCategory.SECURITY, "Authentication failed"),

  /** User is not authorized for the requested operation */
  SEC_AUTHORIZATION_FAILED(ErrorCategory.SECURITY, "Authorization failed"),

  // ========== Storage Errors ==========
  /** File system I/O operation failed */
  STORAGE_IO_ERROR(ErrorCategory.STORAGE, "I/O error"),

  /** Data corruption detected in storage files */
  STORAGE_CORRUPTION(ErrorCategory.STORAGE, "Data corruption detected"),

  /** Write-ahead log operation failed */
  STORAGE_WAL_ERROR(ErrorCategory.STORAGE, "Write-ahead log error"),

  /** Binary serialization/deserialization failed */
  STORAGE_SERIALIZATION_ERROR(ErrorCategory.STORAGE, "Serialization error"),

  /** Encryption or decryption operation failed */
  STORAGE_ENCRYPTION_ERROR(ErrorCategory.STORAGE, "Encryption error"),

  /** Database backup operation failed */
  STORAGE_BACKUP_ERROR(ErrorCategory.STORAGE, "Backup operation error"),

  /** Database restore operation failed */
  STORAGE_RESTORE_ERROR(ErrorCategory.STORAGE, "Restore operation error"),

  // ========== Schema Errors ==========
  /** General schema definition error */
  SCHEMA_ERROR(ErrorCategory.SCHEMA, "Schema error"),

  /** Referenced type does not exist */
  SCHEMA_TYPE_NOT_FOUND(ErrorCategory.SCHEMA, "Type not found"),

  /** Referenced property does not exist */
  SCHEMA_PROPERTY_NOT_FOUND(ErrorCategory.SCHEMA, "Property not found"),

  /** Data validation failed against schema constraints */
  SCHEMA_VALIDATION_ERROR(ErrorCategory.SCHEMA, "Validation error"),

  // ========== Index Errors ==========
  /** General index operation error */
  INDEX_ERROR(ErrorCategory.INDEX, "Index error"),

  /** Referenced index does not exist */
  INDEX_NOT_FOUND(ErrorCategory.INDEX, "Index not found"),

  /** Unique constraint violation */
  INDEX_DUPLICATE_KEY(ErrorCategory.INDEX, "Duplicate key violation"),

  // ========== Graph Errors ==========
  /** Graph algorithm execution failed */
  GRAPH_ALGORITHM_ERROR(ErrorCategory.GRAPH, "Graph algorithm error"),

  // ========== Import/Export Errors ==========
  /** Data import operation failed */
  IMPORT_ERROR(ErrorCategory.IMPORT_EXPORT, "Import error"),

  /** Data export operation failed */
  EXPORT_ERROR(ErrorCategory.IMPORT_EXPORT, "Export error"),

  // ========== Internal Errors ==========
  /** Unexpected internal error (should not normally occur) */
  INTERNAL_ERROR(ErrorCategory.INTERNAL, "Internal error");

  private final ErrorCategory category;
  private final String defaultMessage;

  ErrorCode(final ErrorCategory category, final String defaultMessage) {
    this.category = category;
    this.defaultMessage = defaultMessage;
  }

  /**
   * Returns the error category.
   *
   * @return the error category
   */
  public ErrorCategory getCategory() {
    return category;
  }

  /**
   * Returns the category name as a string.
   *
   * @return the category name
   */
  public String getCategoryName() {
    return category.getDisplayName();
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

  @Override
  public String toString() {
    return String.format("%s [%s]: %s", name(), category.getDisplayName(), defaultMessage);
  }
}
