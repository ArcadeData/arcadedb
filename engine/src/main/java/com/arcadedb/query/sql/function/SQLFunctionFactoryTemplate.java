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
package com.arcadedb.query.sql.function;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.FunctionRegistry;
import com.arcadedb.query.sql.executor.SQLFunction;

import java.util.*;

/**
 * Template for SQL function factories.
 * <p>
 * Functions registered here are also registered in the unified {@link FunctionRegistry}
 * when registered as instances (not as classes, since class-based registration creates
 * new instances per call for stateful functions).
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public abstract class SQLFunctionFactoryTemplate implements SQLFunctionFactory {

  private final Map<String, Object> functions;

  public SQLFunctionFactoryTemplate() {
    functions = new HashMap<>();
  }

  /**
   * Registers a function instance.
   * <p>
   * Also registers in the unified {@link FunctionRegistry} for cross-engine access.
   * </p>
   */
  public void register(final SQLFunction function) {
    functions.put(function.getName().toLowerCase(Locale.ENGLISH), function);
    // Also register in the unified FunctionRegistry for cross-engine access
    FunctionRegistry.register(function);
  }

  public void unregister(final String name) {
    functions.remove(name);
    // Note: Not unregistering from FunctionRegistry to maintain cross-engine availability
  }

  /**
   * Registers a function by name.
   * <p>
   * If function is an instance, also registers in the unified {@link FunctionRegistry}.
   * Class-based registrations are not registered in FunctionRegistry since they
   * create new instances per call (for stateful functions).
   * </p>
   */
  public void register(final String name, final Object function) {
    functions.put(name.toLowerCase(Locale.ENGLISH), function);
    // If it's an instance (not a class), also register in unified registry
    if (function instanceof SQLFunction sqlFunction) {
      FunctionRegistry.register(sqlFunction);
    }
  }

  @Override
  public boolean hasFunction(final String name) {
    return functions.containsKey(name);
  }

  @Override
  public Set<String> getFunctionNames() {
    return functions.keySet();
  }

  @Override
  public SQLFunction getFunctionInstance(final String name) throws CommandExecutionException {
    final Object obj = functions.get(name.toLowerCase(Locale.ENGLISH));
    if (obj == null)
      return null;

    if (obj instanceof SQLFunction sqlFunction)
      return sqlFunction;
    else {
      // it's a class
      final Class<?> typez = (Class<?>) obj;
      try {
        return (SQLFunction) typez.getConstructor().newInstance();
      } catch (final Exception e) {
        throw new CommandExecutionException(
            "Error in creation of function " + name + "(). Probably there is not an empty constructor or the constructor generates errors", e);
      }
    }
  }

  public Map<String, Object> getFunctions() {
    return functions;
  }
}
