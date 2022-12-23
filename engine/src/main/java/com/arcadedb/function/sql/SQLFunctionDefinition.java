package com.arcadedb.function.sql;/*
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
 */

import com.arcadedb.database.Database;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;

/**
 * Javascript implementation of a function. To define the function, pass the function name, code and optional parameters in the constructor.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionDefinition implements FunctionDefinition {
  private final Database database;
  private final String   functionName;
  private final String   implementation;
  private final String[] parameters;

  /**
   * Creates the function with its name, implementation in form of text and optional parameters.
   *
   * @param functionName   Name of the function
   * @param implementation Implementation code as string
   * @param parameters     optional positional parameter names
   */
  public SQLFunctionDefinition(final Database database, final String functionName, final String implementation, final String... parameters) {
    this.database = database;
    this.functionName = functionName;
    this.implementation = implementation;
    this.parameters = parameters;
  }

  @Override
  public String getName() {
    return functionName;
  }

  @Override
  public Object execute(final Object... parameters) {
    final ResultSet result = database.execute("sql", implementation, parameters);
    Object first = null;
    if (result.hasNext()) {
      first = result.next();
      first = extractResult(first);

      if (result.hasNext()) {
        List list = new ArrayList<>();
        list.add(first);
        while (result.hasNext())
          list.add(extractResult(result.next()));

        return list;
      }
    }
    return first;
  }

  private static Object extractResult(Object first) {
    if (first instanceof Result) {
      if (((Result) first).isElement())
        first = ((Result) first).toElement();
      else if (((Result) first).isProjection()) {
        final Set<String> properties = ((Result) first).getPropertyNames();
        if (properties.size() == 1)
          first = ((Result) first).getProperty(properties.iterator().next());
      }
    }
    return first;
  }
}
