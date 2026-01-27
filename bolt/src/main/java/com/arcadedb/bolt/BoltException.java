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
package com.arcadedb.bolt;

import com.arcadedb.exception.ArcadeDBException;

/**
 * Exception for BOLT protocol errors.
 * Error codes are defined in {@link BoltErrorCodes}.
 */
public class BoltException extends ArcadeDBException {
  private final String errorCode;

  public BoltException(final String message) {
    super(message);
    this.errorCode = BoltErrorCodes.DATABASE_ERROR;
  }

  public BoltException(final String errorCode, final String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public BoltException(final String message, final Throwable cause) {
    super(message, cause);
    this.errorCode = BoltErrorCodes.DATABASE_ERROR;
  }

  public BoltException(final String errorCode, final String message, final Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public String getErrorCode() {
    return errorCode;
  }

  // Deprecated: Use BoltErrorCodes constants instead
  /** @deprecated Use {@link BoltErrorCodes#AUTHENTICATION_ERROR} */
  @Deprecated
  public static final String AUTHENTICATION_ERROR = BoltErrorCodes.AUTHENTICATION_ERROR;
  /** @deprecated Use {@link BoltErrorCodes#SYNTAX_ERROR} */
  @Deprecated
  public static final String SYNTAX_ERROR = BoltErrorCodes.SYNTAX_ERROR;
  /** @deprecated Use {@link BoltErrorCodes#SEMANTIC_ERROR} */
  @Deprecated
  public static final String SEMANTIC_ERROR = BoltErrorCodes.SEMANTIC_ERROR;
  /** @deprecated Use {@link BoltErrorCodes#DATABASE_ERROR} */
  @Deprecated
  public static final String DATABASE_ERROR = BoltErrorCodes.DATABASE_ERROR;
  /** @deprecated Use {@link BoltErrorCodes#TRANSACTION_ERROR} */
  @Deprecated
  public static final String TRANSACTION_ERROR = BoltErrorCodes.TRANSACTION_ERROR;
  /** @deprecated Use {@link BoltErrorCodes#FORBIDDEN_ERROR} */
  @Deprecated
  public static final String FORBIDDEN_ERROR = BoltErrorCodes.FORBIDDEN_ERROR;
  /** @deprecated Use {@link BoltErrorCodes#PROTOCOL_ERROR} */
  @Deprecated
  public static final String PROTOCOL_ERROR = BoltErrorCodes.PROTOCOL_ERROR;
}
