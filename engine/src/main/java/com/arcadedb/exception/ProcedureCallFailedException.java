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
 * An {@code apoc.util.validate} / {@code apoc.util.validatePredicate} guard fired: the caller's own message, raised by a procedure or
 * a function. Still the {@link IllegalArgumentException} it always was, so it is {@link ErrorCategory#VALIDATION} on every protocol and
 * its {@link #getMessage()} is the caller's text; its own type so that Bolt can answer it as Neo4j with APOC does, where every
 * exception of an APOC procedure or function reaches the client as
 * <pre>Neo.ClientError.Procedure.ProcedureCallFailed: Failed to invoke procedure `apoc.util.validate`: Caused by: java.lang.RuntimeException: &lt;message&gt;</pre>
 * Clients decode the guard from that text: {@code @neo4j/graphql} recognises its authentication and authorization failures by
 * {@code Caused by: java.lang.RuntimeException: @neo4j/graphql/FORBIDDEN}.
 */
public class ProcedureCallFailedException extends IllegalArgumentException {
  private final boolean function;
  private final String  name;

  /**
   * @param function whether the guard is called as a function ({@code apoc.util.validatePredicate}) or as a procedure
   * @param name     the name as APOC registers it, e.g. {@code apoc.util.validate}
   * @param message  the caller's message, already formatted
   */
  public ProcedureCallFailedException(final boolean function, final String name, final String message) {
    super(message);
    this.function = function;
    this.name = name;
  }

  /** What Neo4j with APOC answers for the same failure. */
  public String getNeo4jMessage() {
    return "Failed to invoke " + (function ? "function" : "procedure") + " `" + name + "`: Caused by: java.lang.RuntimeException: "
        + getMessage();
  }
}
