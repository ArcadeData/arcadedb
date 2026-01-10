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
 * Exception thrown when database lifecycle or operation errors occur.
 * <p>
 * This exception category covers:
 * <ul>
 *   <li>Database not found or already exists</li>
 *   <li>Database state issues (closed, read-only)</li>
 *   <li>Metadata corruption or inconsistency</li>
 *   <li>Configuration errors</li>
 *   <li>General database operation failures</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>{@code
 * throw new DatabaseException(ErrorCode.DB_NOT_FOUND, "Database 'mydb' not found")
 *     .withContext("databaseName", "mydb")
 *     .withContext("searchPath", "/data/databases");
 * }</pre>
 *
 * @since 25.12
 * @see ErrorCode
 * @see ArcadeDBException
 */
public class DatabaseException extends ArcadeDBException {

  /**
   * Constructs a new database exception with the specified error code and message.
   *
   * @param errorCode the error code
   * @param message   the detail message
   */
  public DatabaseException(final ErrorCode errorCode, final String message) {
    super(errorCode, message);
  }

  /**
   * Constructs a new database exception with the specified error code, message, and cause.
   *
   * @param errorCode the error code
   * @param message   the detail message
   * @param cause     the underlying cause
   */
  public DatabaseException(final ErrorCode errorCode, final String message, final Throwable cause) {
    super(errorCode, message, cause);
  }

  /**
   * Returns the default error code for database exceptions.
   *
   * @return DB_OPERATION_ERROR
   */
  @Override
  protected ErrorCode getDefaultErrorCode() {
    return ErrorCode.DB_OPERATION_ERROR;
  }
}
