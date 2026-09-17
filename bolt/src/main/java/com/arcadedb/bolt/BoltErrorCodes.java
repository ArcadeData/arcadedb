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
  // Security errors
  public static final String AUTHENTICATION_ERROR = "Neo.ClientError.Security.Unauthorized";
  public static final String FORBIDDEN_ERROR      = "Neo.ClientError.Security.Forbidden";

  // Statement errors
  public static final String SYNTAX_ERROR   = "Neo.ClientError.Statement.SyntaxError";
  public static final String SEMANTIC_ERROR = "Neo.ClientError.Statement.SemanticError";
  // A unique-index violation (DuplicatedKeyException). It is a permanent failure - retrying the
  // identical write can never succeed - so it must not fall into the generic DatabaseError a driver
  // would treat as a transient server fault (issue #7123).
  public static final String CONSTRAINT_VIOLATION_ERROR = "Neo.ClientError.Schema.ConstraintValidationFailed";
  // A statement that parses and is semantically valid but references a $parameter the client never bound.
  // Neo4j gives this its own title, and drivers/tools key off it to tell "your query is wrong" apart from
  // "you forgot to send a value", so it must not collapse into SyntaxError.
  public static final String PARAMETER_MISSING_ERROR = "Neo.ClientError.Statement.ParameterMissing";
  // A 64-bit integer overflow or a division by zero. The statement is fine and so is the server; the values the
  // caller supplied have no representable answer, which Neo4j reports as a client error - not the generic
  // DatabaseError a driver would surface as "the server broke". See issue #5602.
  public static final String ARITHMETIC_ERROR       = "Neo.ClientError.Statement.ArithmeticError";
  // A value a property cannot hold - openCypher refuses a map, or a list containing one, exactly as Neo4j does with
  // "Property values can only be of primitive types or arrays thereof". The statement is fine and so is the server;
  // the value the caller asked to store is not storable, which Neo4j reports under this title. Without it the
  // refusal reached a driver as the generic DatabaseError it reads as an unexplained server fault, and the reporter
  // of issue #7629 saw exactly that. See issues #7629 and #7729.
  public static final String TYPE_ERROR             = "Neo.ClientError.Statement.TypeError";
  // The record the caller addressed does not exist (ErrorCategory.NOT_FOUND, a RecordNotFoundException). Neo4j's
  // own title for "you named an entity that is not there"; it is a permanent client error, so it must not fall
  // into the generic DatabaseError a driver logs as an internal server fault and, on a managed transaction,
  // cannot distinguish from a broken database (issue #7624).
  public static final String ENTITY_NOT_FOUND_ERROR = "Neo.ClientError.Statement.EntityNotFound";
  // The request is well formed but asks for something invalid - a constraint violation, a bad parameter value, a
  // write on an idempotent-only path (ErrorCategory.VALIDATION). Neo4j's own title for a statement performing
  // operations with invalid arguments. Same reasoning as ENTITY_NOT_FOUND_ERROR: permanent and the caller's, not
  // the server's (issue #7624).
  public static final String ARGUMENT_ERROR         = "Neo.ClientError.Statement.ArgumentError";

  // Transaction errors
  public static final String TRANSACTION_ERROR = "Neo.ClientError.Transaction.TransactionNotFound";

  // Transient (retryable) errors. ArcadeDB's optimistic-concurrency conflicts (NeedRetryException:
  // ConcurrentModificationException / LockTimeoutException) map here so Neo4j drivers auto-retry a
  // managed transaction. The code is a TransientError classification that the drivers retry on; the
  // two excluded titles (Transaction.Terminated / Transaction.LockClientStopped) are deliberately avoided.
  public static final String TRANSIENT_CONFLICT_ERROR = "Neo.TransientError.Transaction.DeadlockDetected";

  // A deadline/budget timeout (com.arcadedb.exception.TimeoutException - a query or the SQL TIMEOUT
  // clause ran out of time), as opposed to the LockTimeoutException contention above. NOT
  // Neo.TransientError.Transaction.Terminated: that title means "explicitly terminated by the user"
  // (e.g. dbms.killTransaction()) and both the Neo4j driver and Spring Data Neo4j explicitly EXCLUDE
  // it from their retry predicates for exactly that reason - retrying a transaction the user killed on
  // purpose is never correct (code review on issue #7123; see the two-title exclusion documented on
  // TRANSIENT_CONFLICT_ERROR above, which already knew this). TransactionTimedOut is Neo4j's own code
  // for this case and its own documentation says what to do with it: "You may want to retry with a
  // longer timeout" - a caller decision, not an automatic driver retry, but still not the
  // generic DatabaseError a driver reads as an unexplained server fault.
  public static final String TRANSACTION_TIMED_OUT_ERROR = "Neo.ClientError.Transaction.TransactionTimedOut";

  // Request errors
  public static final String PROTOCOL_ERROR = "Neo.ClientError.Request.Invalid";

  // Database errors
  public static final String DATABASE_ERROR = "Neo.DatabaseError.General.UnknownError";

  private BoltErrorCodes() {
    // Utility class - prevent instantiation
  }
}
