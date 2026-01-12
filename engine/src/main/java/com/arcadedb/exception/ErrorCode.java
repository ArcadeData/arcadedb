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
 * This enum defines all engine-level error codes, organized by category using systematic
 * enum name prefixes. Each error code is associated with an {@link ErrorCategory} and
 * includes a default human-readable message.
 * <p>
 * <strong>Design Principles:</strong>
 * <ul>
 *   <li><strong>String-based:</strong> Error codes are enum names (e.g., "DB_NOT_FOUND"), not numeric values</li>
 *   <li><strong>Self-documenting:</strong> Names clearly indicate the error condition</li>
 *   <li><strong>Categorized:</strong> Each code belongs to a category for organization</li>
 *   <li><strong>Layer-specific:</strong> Only engine-level errors; network errors are separate</li>
 * </ul>
 * <p>
 * <strong>Error Code Prefixes by Category:</strong>
 * <ul>
 *   <li><strong>DB_*</strong> - Database lifecycle and operations ({@link ErrorCategory#DATABASE})</li>
 *   <li><strong>TX_*</strong> - Transaction management ({@link ErrorCategory#TRANSACTION})</li>
 *   <li><strong>QUERY_*</strong> - Query parsing and execution ({@link ErrorCategory#QUERY})</li>
 *   <li><strong>SEC_*</strong> - Security, authentication, authorization ({@link ErrorCategory#SECURITY})</li>
 *   <li><strong>STORAGE_*</strong> - I/O, persistence, serialization ({@link ErrorCategory#STORAGE})</li>
 *   <li><strong>SCHEMA_*</strong> - Schema and type system ({@link ErrorCategory#SCHEMA})</li>
 *   <li><strong>INDEX_*</strong> - Index operations ({@link ErrorCategory#INDEX})</li>
 *   <li><strong>GRAPH_*</strong> - Graph algorithms and traversal ({@link ErrorCategory#GRAPH})</li>
 *   <li><strong>IMPORT_* / EXPORT_*</strong> - Data import/export ({@link ErrorCategory#IMPORT_EXPORT})</li>
 *   <li><strong>INTERNAL_*</strong> - Internal system errors ({@link ErrorCategory#INTERNAL})</li>
 * </ul>
 * <p>
 * <strong>Important Note:</strong> Network errors (CONNECTION_*, REPLICATION_*, etc.) are NOT in this enum.
 * They are defined in {@code com.arcadedb.network.exception.NetworkErrorCode} to maintain proper
 * architectural layering. The engine module has no knowledge of network concepts.
 * <p>
 * <strong>Usage Examples:</strong>
 * <pre>{@code
 * // Throwing an exception with error code
 * throw new DatabaseException(ErrorCode.DB_NOT_FOUND, "Database 'mydb' not found")
 *     .withContext("databaseName", "mydb");
 *
 * // Accessing error code information
 * ErrorCode code = ErrorCode.DB_NOT_FOUND;
 * String name = code.name();                      // "DB_NOT_FOUND"
 * ErrorCategory category = code.getCategory();    // ErrorCategory.DATABASE
 * String message = code.getDefaultMessage();      // "Database not found"
 *
 * // In exception handlers
 * try {
 *     database.open();
 * } catch (ArcadeDBException e) {
 *     if (e.getErrorCode() == ErrorCode.DB_NOT_FOUND) {
 *         // Handle database not found
 *     }
 * }
 * }</pre>
 *
 * @since 26.1
 * @see ErrorCategory
 * @see ArcadeDBException
 * @see com.arcadedb.network.exception.NetworkErrorCode
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

  /**
   * Constructs an error code with category and default message.
   *
   * @param category the error category this code belongs to
   * @param defaultMessage the default human-readable error message
   */
  ErrorCode(final ErrorCategory category, final String defaultMessage) {
    this.category = category;
    this.defaultMessage = defaultMessage;
  }

  /**
   * Returns the error category this code belongs to.
   * <p>
   * Error categories provide high-level classification of errors (e.g., DATABASE,
   * TRANSACTION, QUERY). This allows filtering, grouping, and analyzing errors
   * by category.
   *
   * @return the error category (never null)
   * @see ErrorCategory
   */
  public ErrorCategory getCategory() {
    return category;
  }

  /**
   * Returns the category display name as a string.
   * <p>
   * This is a convenience method equivalent to {@code getCategory().getDisplayName()}.
   * It returns the human-readable category name (e.g., "Database", "Transaction").
   *
   * @return the category display name (never null)
   */
  public String getCategoryName() {
    return category.getDisplayName();
  }

  /**
   * Returns the default human-readable error message for this code.
   * <p>
   * This message provides a generic description of the error condition. It can be
   * overridden when throwing exceptions to provide more specific context:
   * <pre>{@code
   * // Using default message
   * throw new DatabaseException(ErrorCode.DB_NOT_FOUND);
   *
   * // Overriding with custom message
   * throw new DatabaseException(ErrorCode.DB_NOT_FOUND, "Database 'mydb' not found in /data");
   * }</pre>
   *
   * @return the default error message (never null)
   */
  public String getDefaultMessage() {
    return defaultMessage;
  }

  /**
   * Returns a formatted string representation of this error code.
   * <p>
   * The format is: "ERROR_CODE_NAME [Category]: default message"
   * <p>
   * Example: "DB_NOT_FOUND [Database]: Database not found"
   *
   * @return formatted string representation (never null)
   */
  @Override
  public String toString() {
    return String.format("%s [%s]: %s", name(), category.getDisplayName(), defaultMessage);
  }
}
