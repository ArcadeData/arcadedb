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
