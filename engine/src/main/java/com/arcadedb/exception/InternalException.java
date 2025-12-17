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
 * Internal exception for unexpected errors that don't fit into specific categories.
 * <p>
 * This exception should be used sparingly, primarily for:
 * <ul>
 *   <li>Wrapping unexpected checked exceptions</li>
 *   <li>Internal assertion failures</li>
 *   <li>Errors that should never occur in normal operation</li>
 * </ul>
 * <p>
 * For known error categories, use specific exception types instead:
 * <ul>
 *   <li>{@link DatabaseException} for database errors</li>
 *   <li>{@link TransactionException} for transaction errors</li>
 *   <li>{@link QueryException} for query errors</li>
 *   <li>{@link StorageException} for I/O errors</li>
 *   <li>etc.</li>
 * </ul>
 *
 * @since 25.12
 * @see ArcadeDBException
 * @see ErrorCode#INTERNAL_ERROR
 */
public class InternalException extends ArcadeDBException {

  /**
   * Constructs a new internal exception with the specified message.
   * Uses INTERNAL_ERROR as the default error code.
   *
   * @param message the detail message
   */
  public InternalException(final String message) {
    super(ErrorCode.INTERNAL_ERROR, message);
  }

  /**
   * Constructs a new internal exception with the specified message and cause.
   * Uses INTERNAL_ERROR as the default error code.
   *
   * @param message the detail message
   * @param cause   the underlying cause
   */
  public InternalException(final String message, final Throwable cause) {
    super(ErrorCode.INTERNAL_ERROR, message, cause);
  }

  /**
   * Constructs a new internal exception with the specified cause.
   * Uses the cause's message as the detail message.
   *
   * @param cause the underlying cause
   */
  public InternalException(final Throwable cause) {
    super(ErrorCode.INTERNAL_ERROR, cause.getMessage(), cause);
  }

  /**
   * Constructs a new internal exception with a specific error code and message.
   * <p>
   * This constructor is protected to prevent misuse of InternalException with
   * non-internal error codes. Use specific exception types (DatabaseException,
   * QueryException, etc.) for their respective error categories.
   *
   * @param errorCode the error code (should only be INTERNAL_ERROR)
   * @param message   the detail message
   */
  protected InternalException(final ErrorCode errorCode, final String message) {
    super(errorCode, message);
  }

  /**
   * Constructs a new internal exception with a specific error code, message, and cause.
   * <p>
   * This constructor is protected to prevent misuse of InternalException with
   * non-internal error codes. Use specific exception types (DatabaseException,
   * QueryException, etc.) for their respective error categories.
   *
   * @param errorCode the error code (should only be INTERNAL_ERROR)
   * @param message   the detail message
   * @param cause     the underlying cause
   */
  protected InternalException(final ErrorCode errorCode, final String message, final Throwable cause) {
    super(errorCode, message, cause);
  }
}
