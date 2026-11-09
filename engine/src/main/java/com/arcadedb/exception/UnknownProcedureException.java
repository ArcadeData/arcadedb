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
 * A {@code CALL} names a procedure that no registered procedure, function, library function or SQL function answers (issue
 * #11384). An {@link UnknownFunctionException}, because what a {@code CALL} resolves is looked up among the functions too and
 * because every protocol already answers that one as the caller's mistake ({@code 42883} undefined_function on the Postgres wire,
 * what PostgreSQL answers a {@code CALL} of a procedure it does not have with; HTTP 400). Its own type so that Bolt can answer it as
 * Neo4j does, {@code Neo.ClientError.Procedure.ProcedureNotFound}, where a function nothing answers is a SyntaxError. It used to be a
 * plain {@link CommandExecutionException}, a server fault on every protocol (Bolt {@code UnknownError}, Postgres {@code XX000}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class UnknownProcedureException extends UnknownFunctionException {
  public UnknownProcedureException(final String procedureName, final String message) {
    super(procedureName, message);
  }

  /** The name of the procedure as the statement wrote it. */
  public String getProcedureName() {
    return getFunctionName();
  }
}
