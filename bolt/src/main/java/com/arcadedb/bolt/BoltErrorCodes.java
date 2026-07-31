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

/**
 * Centralized constants for Neo4j-compatible BOLT error codes.
 * These error codes are used across BOLT protocol messages and exceptions.
 */
public final class BoltErrorCodes {
  private BoltErrorCodes() {
    // Utility class - prevent instantiation
  }

  // Security errors
  public static final String AUTHENTICATION_ERROR = "Neo.ClientError.Security.Unauthorized";
  public static final String FORBIDDEN_ERROR      = "Neo.ClientError.Security.Forbidden";

  // Statement errors
  public static final String SYNTAX_ERROR   = "Neo.ClientError.Statement.SyntaxError";
  public static final String SEMANTIC_ERROR = "Neo.ClientError.Statement.SemanticError";
  // A statement that parses and is semantically valid but references a $parameter the client never bound.
  // Neo4j gives this its own title, and drivers/tools key off it to tell "your query is wrong" apart from
  // "you forgot to send a value", so it must not collapse into SyntaxError.
  public static final String PARAMETER_MISSING_ERROR = "Neo.ClientError.Statement.ParameterMissing";
  // A 64-bit integer overflow or a division by zero. The statement is fine and so is the server; the values the
  // caller supplied have no representable answer, which Neo4j reports as a client error - not the generic
  // DatabaseError a driver would surface as "the server broke". See issue #5602.
  public static final String ARITHMETIC_ERROR      = "Neo.ClientError.Statement.ArithmeticError";

  // Transaction errors
  public static final String TRANSACTION_ERROR = "Neo.ClientError.Transaction.TransactionNotFound";

  // Transient (retryable) errors. ArcadeDB's optimistic-concurrency conflicts (NeedRetryException:
  // ConcurrentModificationException / LockTimeoutException) map here so Neo4j drivers auto-retry a
  // managed transaction. The code is a TransientError classification that the drivers retry on; the
  // two excluded titles (Transaction.Terminated / Transaction.LockClientStopped) are deliberately avoided.
  public static final String TRANSIENT_CONFLICT_ERROR = "Neo.TransientError.Transaction.DeadlockDetected";

  // Request errors
  public static final String PROTOCOL_ERROR = "Neo.ClientError.Request.Invalid";

  // Database errors
  public static final String DATABASE_ERROR = "Neo.DatabaseError.General.UnknownError";
}
