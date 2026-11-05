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
 * A statement calls a function that no built-in, library or user-defined function answers. The caller's mistake, classified
 * {@link ErrorCategory#PARSING} with the statements that cannot be read, which is what openCypher reports it as at parse time;
 * SQL resolves a function only when it runs, which is why this is a {@link CommandExecutionException}: every caller that catches
 * the failure of a lookup keeps catching it. It used to be a plain {@link CommandExecutionException}, which no rule classifies,
 * so every protocol reported a misspelled function as a server fault ({@code XX000} on the Postgres wire, which answers
 * {@code 42883} undefined_function for it now, issue #11314).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class UnknownFunctionException extends CommandExecutionException {
  private final String functionName;

  public UnknownFunctionException(final String functionName, final String message) {
    super(message);
    this.functionName = functionName;
  }

  /** The name of the function as the statement wrote it. */
  public String getFunctionName() {
    return functionName;
  }
}
