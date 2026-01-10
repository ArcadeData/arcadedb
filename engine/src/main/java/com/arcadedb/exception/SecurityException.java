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
 * Exception thrown when security violations occur.
 * <p>
 * This exception category covers:
 * <ul>
 *   <li>Authentication failures (invalid credentials)</li>
 *   <li>Authorization violations (insufficient permissions)</li>
 *   <li>Access control violations</li>
 *   <li>Token or session validation errors</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>{@code
 * throw new SecurityException(ErrorCode.SEC_UNAUTHORIZED, "Invalid credentials")
 *     .withContext("user", username)
 *     .withContext("ipAddress", clientIP);
 * }</pre>
 * <p>
 * <b>Security Note:</b> Be careful not to leak sensitive information in error messages
 * or context. Avoid including passwords, tokens, or detailed security information.
 *
 * @since 25.12
 * @see ErrorCode
 * @see ArcadeDBException
 */
public class SecurityException extends ArcadeDBException {

  /**
   * Constructs a new security exception with the specified error code and message.
   *
   * @param errorCode the error code
   * @param message   the detail message
   */
  public SecurityException(final ErrorCode errorCode, final String message) {
    super(errorCode, message);
  }

  /**
   * Constructs a new security exception with the specified error code, message, and cause.
   *
   * @param errorCode the error code
   * @param message   the detail message
   * @param cause     the underlying cause
   */
  public SecurityException(final ErrorCode errorCode, final String message, final Throwable cause) {
    super(errorCode, message, cause);
  }

  /**
   * Returns the default error code for security exceptions.
   *
   * @return SEC_AUTHORIZATION_FAILED
   */
  @Override
  protected ErrorCode getDefaultErrorCode() {
    return ErrorCode.SEC_AUTHORIZATION_FAILED;
  }
}
