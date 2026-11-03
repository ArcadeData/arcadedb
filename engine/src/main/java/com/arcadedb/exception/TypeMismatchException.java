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
 * Thrown when a query applies an operation to a value of a type the operation is not defined for, and the mismatch is
 * known only once the query runs - reading a property off a list ({@code UNWIND [[1]] AS l RETURN l.x}), indexing into a
 * number, storing a map in a property.
 * <p>
 * The query is well-formed and the engine is healthy; what is wrong is the value the caller made the operation meet, so
 * this is the caller's error and not an internal failure. Neo4j - the OpenCypher reference implementation - agrees and
 * reports the whole category as {@code Neo.ClientError.Statement.TypeError}; ArcadeDB reports it as HTTP 400 and as that
 * same status code over Bolt (issue #11211, where a property read on a variable-length relationship list answered 500).
 * <p>
 * It extends {@link CommandExecutionException}, following {@link ArithmeticErrorException}, for the reason
 * {@link InvalidPropertyTypeException} - its subtype - documents: the openCypher engine rethrows a
 * {@link CommandExecutionException} unchanged and wraps anything else in one whose message is the query text, so this
 * keeps the diagnosis on the outermost throwable, and embedded code catching {@link CommandExecutionException} keeps
 * catching it. {@link ErrorCategory#VALIDATION} and the HTTP handler name this type, so every subtype is a client error
 * without an arm of its own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TypeMismatchException extends CommandExecutionException {
  public TypeMismatchException(final String message) {
    super(message);
  }

  public TypeMismatchException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
