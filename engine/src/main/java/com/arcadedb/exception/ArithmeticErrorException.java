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
 * Thrown when an arithmetic operation has no representable answer: a 64-bit integer overflow
 * ({@code 9223372036854775807 + 1}, {@code abs(-9223372036854775808)}, {@code -9223372036854775808 / -1}) or a
 * division / modulo by zero.
 * <p>
 * The query is well-formed and the engine is healthy; what is wrong is the pair of values the caller supplied, so
 * this is the caller's error and not an internal failure. Neo4j - the OpenCypher reference implementation - agrees
 * and reports the whole category as {@code Neo.ClientError.Statement.ArithmeticError}; ArcadeDB reports it as
 * HTTP 400 and as that same status code over Bolt.
 * <p>
 * It extends {@link CommandExecutionException} rather than {@link CommandSemanticException} on purpose. Issue #5164
 * settled that overflow surfaces as a {@code CommandExecutionException}, and embedded code written against that
 * still catches this; and unlike a semantic error it cannot be decided while parsing, since it depends on values
 * known only when the query runs. Subclassing keeps both properties: existing catch blocks are unaffected, while
 * the wire layers can single this out from the runtime failures that genuinely are the server's fault.
 * <p>
 * Issues #5164, #5494 and #5602.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArithmeticErrorException extends CommandExecutionException {
  public ArithmeticErrorException(final String message) {
    super(message);
  }

  public ArithmeticErrorException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
