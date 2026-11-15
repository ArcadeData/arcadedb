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
 * Thrown when a function or a procedure is called with an argument it is not defined for: a null where it needs a value, text that
 * does not parse as what it must name (a date, a duration unit), a number outside the range it accepts. The argument COUNT is
 * right (a wrong count is a {@link CommandSemanticException}) and so is its KIND (a value of the wrong kind is a
 * {@link TypeMismatchException}); its value is not one the function accepts.
 * <p>
 * The statement is well-formed and the engine is healthy, so this is the caller's error and not an internal failure: HTTP 400,
 * {@link ErrorCategory#VALIDATION} on every other protocol - Neo4j's {@code Neo.ClientError.Statement.ArgumentError} over Bolt,
 * {@code 22023} invalid_parameter_value over Postgres (issue #11589, where such calls answered 500, some of them with a raw
 * {@code NullPointerException} or {@code DateTimeParseException}).
 * <p>
 * It extends {@link CommandExecutionException} for the reason {@link TypeMismatchException} documents: the openCypher engine
 * rethrows a {@link CommandExecutionException} unchanged and wraps anything else in one whose message is the query text, so this
 * keeps the diagnosis on the outermost throwable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class InvalidArgumentException extends CommandExecutionException {
  public InvalidArgumentException(final String message) {
    super(message);
  }

  public InvalidArgumentException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
