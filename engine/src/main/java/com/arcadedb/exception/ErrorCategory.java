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
 * The kind of failure a wire protocol has to report, decided once over ArcadeDB's exception hierarchy so every
 * protocol answers the question the same way. A module keeps only the table that turns a category into its own
 * vocabulary - a SQLSTATE for Postgres, a RESP prefix for Redis, an error code for MongoDB.
 * <p>
 * Classification walks the whole cause chain rather than the outermost throwable, because a failure is wrapped
 * differently depending on how the request arrived: directly, inside the auto-commit {@code TransactionException},
 * or doubly wrapped on the {@code CALL} path. Inspecting only {@code getCause()} is what made a client error
 * report as a server fault before {@link CauseChain} existed.
 * <p>
 * The HTTP handler and the Bolt executor predate this enum and keep their own ladders; they encode
 * protocol-specific ordering this enum does not model, and their behaviour is pinned by the regression suites of
 * several shipped issues.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public enum ErrorCategory {
  /**
   * An optimistic-concurrency conflict the caller can simply retry.
   */
  RETRY,

  /**
   * A 64-bit overflow, or a division or modulo by zero - decided by the values the caller supplied.
   */
  ARITHMETIC,

  /**
   * A unique index already holds the key. Never retryable: a retry would duplicate writes.
   */
  DUPLICATED_KEY,

  /**
   * The record the caller addressed does not exist.
   */
  NOT_FOUND,

  /**
   * The caller named a type, bucket or property the schema does not define.
   */
  SCHEMA,

  /**
   * The caller is not allowed to do this.
   */
  SECURITY,

  /**
   * The request is well formed but asks for something invalid - a bad parameter, a constraint violation, a write
   * on an idempotent-only path.
   */
  VALIDATION,

  /**
   * The statement could not be parsed, or failed semantic validation.
   */
  PARSING,

  /**
   * The operation ran out of time.
   */
  TIMEOUT,

  /**
   * Anything else: the server, not the caller, is at fault.
   */
  SERVER;

  /**
   * The category of {@code error}, or {@link #SERVER} when nothing in its cause chain is recognised.
   * <p>
   * The order of the tests is behaviour, not style. {@link #RETRY} is decided first so a chain carrying both a
   * conflict and an arithmetic error keeps the transient classification a driver acts on - the same precedence
   * {@code BoltNetworkExecutor.classifyExecutionError} documents. {@link #ARITHMETIC} is decided before
   * {@link #PARSING} so the {@code CommandParsingException} that GraphQL and the query engines wrap execution
   * failures in cannot relabel an arithmetic error as invalid syntax, and before any test on
   * {@link CommandExecutionException} would be, since {@link ArithmeticErrorException} extends it.
   * <p>
   * {@link IllegalArgumentException} is the one entry that is not self-evidently the caller's fault: the engine
   * raises it both for bad input and for internal invariant violations, so classifying it as {@link #VALIDATION}
   * can label a server bug a client error. It is mapped anyway because the HTTP handler has answered it with 400
   * since long before this enum, and having the two disagree would be worse than either verdict.
   */
  public static ErrorCategory of(final Throwable error) {
    if (CauseChain.contains(error, NeedRetryException.class))
      return RETRY;
    if (CauseChain.contains(error, ArithmeticErrorException.class))
      return ARITHMETIC;
    if (CauseChain.contains(error, DuplicatedKeyException.class))
      return DUPLICATED_KEY;
    if (CauseChain.contains(error, RecordNotFoundException.class))
      return NOT_FOUND;
    if (CauseChain.contains(error, SchemaException.class))
      return SCHEMA;
    if (CauseChain.contains(error, SecurityException.class))
      return SECURITY;
    if (CauseChain.contains(error, ValidationException.class) //
        || CauseChain.contains(error, QueryNotIdempotentException.class) //
        || CauseChain.contains(error, IllegalArgumentException.class))
      return VALIDATION;
    if (CauseChain.contains(error, CommandParsingException.class))
      return PARSING;
    if (CauseChain.contains(error, TimeoutException.class))
      return TIMEOUT;
    return SERVER;
  }

  /**
   * Whether the caller, rather than the server, has to change something for the request to succeed. {@link #RETRY}
   * and {@link #TIMEOUT} are excluded: the same request repeated can succeed, so they are neither the caller's
   * fault nor a permanent server fault.
   */
  public boolean isClientError() {
    return this != SERVER && this != RETRY && this != TIMEOUT;
  }
}
