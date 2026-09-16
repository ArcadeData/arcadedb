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
 * Thrown when a query asks a property to hold a value of a type that property type system does not allow - today,
 * an openCypher write clause asked to store a map, or a list containing one.
 * <p>
 * The query is well-formed and the engine is healthy; what is wrong is the value the caller asked to store, so this
 * is the caller's error and not an internal failure. Neo4j - the OpenCypher reference implementation - agrees and
 * reports the whole category as {@code Neo.ClientError.Statement.TypeError} with "Property values can only be of
 * primitive types or arrays thereof"; ArcadeDB reports it as HTTP 400 and as that same status code over Bolt.
 * <p>
 * It extends {@link CommandExecutionException}, following {@link ArithmeticErrorException}, and that choice carries
 * behaviour rather than taste. The openCypher engine rethrows a {@link CommandExecutionException} unchanged but
 * wraps anything else in one whose message is the query text, so the {@link IllegalArgumentException} this used to
 * be reached a Bolt client as an unexplained {@code Neo.DatabaseError.General.UnknownError} naming only the
 * statement that failed - the diagnosis existed but was reachable only in the server log (issues #7629, #7729).
 * Subclassing keeps the diagnosis on the outermost throwable, where every wire layer reports from. The
 * {@link ErrorCategory#VALIDATION} arm and the {@code IllegalArgumentException} the HTTP handler answers 400 for
 * both name this type explicitly so the classification it used to get by being an
 * {@link IllegalArgumentException} is preserved rather than silently downgraded to a server fault.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class InvalidPropertyTypeException extends CommandExecutionException {
  public InvalidPropertyTypeException(final String message) {
    super(message);
  }

  public InvalidPropertyTypeException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
