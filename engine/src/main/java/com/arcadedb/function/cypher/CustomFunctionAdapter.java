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
package com.arcadedb.function.cypher;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.opencypher.parser.FunctionValidator;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.schema.Schema;

/**
 * Adapter for custom user-defined functions (SQL, JavaScript, Cypher).
 * Bridges FunctionDefinition interface to StatelessFunction interface.
 * Performs case-insensitive function lookup to match SQL behavior.
 */
public class CustomFunctionAdapter implements StatelessFunction {
  private final String libraryName;
  private final String functionName;
  private final String fullName;  // "library.function"

  public CustomFunctionAdapter(final String libraryName, final String functionName) {
    this.libraryName = libraryName;
    this.functionName = functionName;
    this.fullName = libraryName + "." + functionName;
  }

  @Override
  public String getName() {
    return fullName;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    // Lazy lookup from schema each time (handles function redefinition)
    final FunctionDefinition function = resolve(context.getDatabase().getSchema(), libraryName, functionName);
    // A function dropped while a query calling it is still planned or running: the same client error the check before the
    // execution gives (issue #11115), not a server fault
    if (function == null)
      throw FunctionValidator.unknownFunction(fullName);
    return function.execute(args);
  }

  /**
   * Whether {@code dottedName} ({@code library.function}, the library possibly dotted itself) names a user-defined function
   * of the database, in any letter case.
   */
  public static boolean exists(final Schema schema, final String dottedName) {
    final int dot = dottedName.lastIndexOf('.');
    if (dot <= 0 || dot >= dottedName.length() - 1)
      return false;
    return resolve(schema, dottedName.substring(0, dot), dottedName.substring(dot + 1)) != null;
  }

  /**
   * The user-defined function {@code functionName} of the library {@code libraryName}, or null when there is none. Both names
   * match in any letter case, as Cypher function names do: an exact match is tried first, so the common case costs one lookup
   * each. Cypher folds the name it is called by to lower case, so a library or function defined with an upper-case letter was
   * only reachable through the case-insensitive pass.
   */
  public static FunctionDefinition resolve(final Schema schema, final String libraryName, final String functionName) {
    final FunctionLibraryDefinition<?> library = library(schema, libraryName);
    if (library == null)
      return null;

    if (library.hasFunction(functionName)) {
      final FunctionDefinition function = library.getFunction(functionName);
      // FunctionLibraryDefinition is a public extension point: a third-party implementation that violates the
      // "getFunction() never returns null" contract gets a clear error here instead of a raw NPE.
      if (function == null)
        throw new CommandExecutionException("Function library '" + library.getName() + "' returned null for '" + functionName
            + "', violating the FunctionLibraryDefinition.getFunction() contract");
      return function;
    }

    for (final FunctionDefinition function : library.getFunctions())
      if (function.getName().equalsIgnoreCase(functionName))
        return function;
    return null;
  }

  private static FunctionLibraryDefinition<?> library(final Schema schema, final String libraryName) {
    if (schema.hasFunctionLibrary(libraryName))
      return schema.getFunctionLibrary(libraryName);
    for (final FunctionLibraryDefinition<?> library : schema.getFunctionLibraries())
      if (library.getName().equalsIgnoreCase(libraryName))
        return library;
    return null;
  }
}
