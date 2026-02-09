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
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;

/**
 * Function defined using Cypher query language.
 * Executes a Cypher query with named parameters and returns the first result.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherFunctionDefinition implements FunctionDefinition {
  private final Database database;
  private final String   functionName;
  private final String   implementation;
  private final String[] parameterNames;

  /**
   * Creates the function with its name, implementation in form of Cypher query and optional parameters.
   *
   * @param database       Database instance
   * @param functionName   Name of the function
   * @param implementation Implementation code as Cypher query
   * @param parameterNames Names of the parameters
   */
  public CypherFunctionDefinition(final Database database, final String functionName, final String implementation,
                                   final String... parameterNames) {
    this.database = database;
    this.functionName = functionName;
    this.implementation = implementation;
    this.parameterNames = parameterNames;
  }

  @Override
  public String getName() {
    return functionName;
  }

  @Override
  public Object execute(final Object... args) {
    // Build parameter map for Cypher query
    final Map<String, Object> paramMap = new HashMap<>();
    for (int i = 0; i < parameterNames.length && i < args.length; i++) {
      paramMap.put(parameterNames[i], args[i]);
    }

    // Execute Cypher query with parameters
    try (final ResultSet rs = database.query("opencypher", implementation, paramMap)) {
      if (rs.hasNext()) {
        final Result result = rs.next();

        // Return first property value or the result itself
        final Set<String> propertyNames = result.getPropertyNames();
        if (propertyNames.size() == 1) {
          return result.getProperty(propertyNames.iterator().next());
        }
        // Multiple properties → return as map
        return result.toMap();
      }
      return null;  // No results
    }
  }

  public String getImplementation() {
    return implementation;
  }

  public String[] getParameters() {
    return parameterNames;
  }
}
