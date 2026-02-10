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
 * Categories for organizing error codes in the ArcadeDB exception hierarchy.
 * <p>
 * Error categories provide a high-level classification of error types, allowing
 * developers and operations teams to quickly understand the nature of an error.
 * Each {@link ErrorCode} belongs to exactly one category.
 * <p>
 * <strong>Available Categories:</strong>
 * <ul>
 *   <li>{@link #DATABASE} - Database lifecycle, operations, and metadata</li>
 *   <li>{@link #TRANSACTION} - Transaction management, locking, and concurrency</li>
 *   <li>{@link #QUERY} - Query parsing, execution, and command processing</li>
 *   <li>{@link #SECURITY} - Authentication, authorization, and access control</li>
 *   <li>{@link #STORAGE} - I/O operations, persistence, serialization, and encryption</li>
 *   <li>{@link #SCHEMA} - Schema definitions, types, properties, and validation</li>
 *   <li>{@link #INDEX} - Index operations and constraints</li>
 *   <li>{@link #GRAPH} - Graph algorithms and traversal operations</li>
 *   <li>{@link #IMPORT_EXPORT} - Data import and export operations</li>
 *   <li>{@link #INTERNAL} - Internal system errors and unexpected conditions</li>
 * </ul>
 * <p>
 * <strong>Usage Example:</strong>
 * <pre>{@code
 * ErrorCode errorCode = ErrorCode.DB_NOT_FOUND;
 * ErrorCategory category = errorCode.getCategory();
 * String displayName = category.getDisplayName();  // "Database"
 * }</pre>
 *
 * @since 26.1
 * @see ErrorCode
 * @see ArcadeDBException
 */
public enum ErrorCategory {
  DATABASE("Database"),
  TRANSACTION("Transaction"),
  QUERY("Query"),
  SECURITY("Security"),
  STORAGE("Storage"),
  SCHEMA("Schema"),
  INDEX("Index"),
  GRAPH("Graph"),
  IMPORT_EXPORT("Import/Export"),
  INTERNAL("Internal");

  private final String displayName;

  /**
   * Constructs an error category with a display name.
   *
   * @param displayName the human-readable name for this category
   */
  ErrorCategory(final String displayName) {
    this.displayName = displayName;
  }

  /**
   * Returns the human-readable display name for this category.
   * <p>
   * This is used in error messages, logs, and JSON responses to provide
   * clear context about the type of error that occurred.
   *
   * @return the display name (e.g., "Database", "Transaction")
   */
  public String getDisplayName() {
    return displayName;
  }

  /**
   * Returns the display name as the string representation.
   *
   * @return the display name
   */
  @Override
  public String toString() {
    return displayName;
  }
}
