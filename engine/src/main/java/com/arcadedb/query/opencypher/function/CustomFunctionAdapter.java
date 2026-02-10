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
package com.arcadedb.query.opencypher.function;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

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
    final Database database = context.getDatabase();

    if (!database.getSchema().hasFunctionLibrary(libraryName))
      throw new CommandExecutionException("Unknown function: " + fullName);

    // Try exact match first
    try {
      final FunctionDefinition function = database.getSchema().getFunction(libraryName, functionName);
      if (function != null)
        return function.execute(args);
    } catch (final IllegalArgumentException e) {
      // Fall through to case-insensitive search
    }

    // Case-insensitive search - iterate through all functions in library
    final var library = database.getSchema().getFunctionLibrary(libraryName);
    for (final Object funcObj : library.getFunctions()) {
      if (funcObj instanceof FunctionDefinition) {
        final FunctionDefinition func = (FunctionDefinition) funcObj;
        if (func.getName().equalsIgnoreCase(functionName))
          return func.execute(args);
      }
    }

    throw new CommandExecutionException("Unknown function: " + fullName);
  }
}
