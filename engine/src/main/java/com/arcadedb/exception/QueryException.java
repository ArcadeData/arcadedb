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
 * Exception thrown when query parsing or execution errors occur.
 * <p>
 * This exception category covers:
 * <ul>
 *   <li>Query syntax errors (SQL, Cypher, Gremlin, etc.)</li>
 *   <li>Semantic errors (undefined types, properties, functions)</li>
 *   <li>Query execution failures</li>
 *   <li>Function evaluation errors</li>
 *   <li>Command parsing errors</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>{@code
 * throw new QueryException(ErrorCode.QUERY_SYNTAX_ERROR, "Unexpected token 'FORM' at line 1, column 8")
 *     .withContext("query", "SELECT FROM User")
 *     .withContext("language", "SQL")
 *     .withContext("position", 8);
 * }</pre>
 *
 * @since 25.12
 * @see ErrorCode
 * @see ArcadeDBException
 */
public class QueryException extends ArcadeDBException {

  /**
   * Constructs a new query exception with the specified error code and message.
   *
   * @param errorCode the error code
   * @param message   the detail message
   */
  public QueryException(final ErrorCode errorCode, final String message) {
    super(errorCode, message);
  }

  /**
   * Constructs a new query exception with the specified error code, message, and cause.
   *
   * @param errorCode the error code
   * @param message   the detail message
   * @param cause     the underlying cause
   */
  public QueryException(final ErrorCode errorCode, final String message, final Throwable cause) {
    super(errorCode, message, cause);
  }

  /**
   * Returns the default error code for query exceptions.
   *
   * @return QUERY_EXECUTION_ERROR
   */
  @Override
  protected ErrorCode getDefaultErrorCode() {
    return ErrorCode.QUERY_EXECUTION_ERROR;
  }

  /**
   * Returns the HTTP status code for this exception.
   * Query exceptions typically map to 400 (Bad Request) for syntax errors,
   * or 500 (Internal Server Error) for execution errors.
   *
   * @return the HTTP status code
   */
  @Override
  public int getHttpStatus() {
    return switch (getErrorCode()) {
      case QUERY_SYNTAX_ERROR, COMMAND_PARSING_ERROR -> 400; // Bad Request
      case QUERY_EXECUTION_ERROR, COMMAND_EXECUTION_ERROR, FUNCTION_EXECUTION_ERROR -> 500; // Internal Server Error
      default -> 500;
    };
  }
}
