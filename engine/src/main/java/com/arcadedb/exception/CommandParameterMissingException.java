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
 * Thrown when a query references a parameter the caller never bound. The statement parses and is
 * semantically valid; what is missing is a value, which is why this extends
 * {@link CommandSemanticException} rather than signalling a syntax error.
 * <p>
 * The message is Neo4j's ({@code Expected parameter(s): a, b}) so that a driver or a user moving from
 * Neo4j reads the same text; the names are also carried structurally so a caller that wants to react -
 * prompt for the values, name them in a log - never has to parse the message back apart. Over Bolt this
 * maps to {@code Neo.ClientError.Statement.ParameterMissing}, over HTTP to a 400.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CommandParameterMissingException extends CommandSemanticException {
  private final String[] missingParameters;

  /**
   * @param missingParameters the names the query references but the caller did not bind, in the order the
   *                          query mentions them. Retained by reference: the array is built for this
   *                          exception and never handed out to anyone else.
   */
  public CommandParameterMissingException(final String... missingParameters) {
    super("Expected parameter(s): " + String.join(", ", missingParameters));
    this.missingParameters = missingParameters;
  }

  /**
   * The unbound parameter names, in the order the query mentions them.
   */
  public String[] getMissingParameters() {
    return missingParameters;
  }
}
