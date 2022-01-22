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
import com.arcadedb.query.sql.executor.SQLFunction;

import java.util.*;

/**
 * Created by frank on 25/05/2017.
 */
public abstract class SQLFunctionFactoryTemplate implements SQLFunctionFactory {

  private final Map<String, Object> functions;

  public SQLFunctionFactoryTemplate() {
    functions = new HashMap<>();
  }

  public void register(final SQLFunction function) {
    functions.put(function.getName().toLowerCase(Locale.ENGLISH), function);
  }

  public void unregister(final String name) {
    functions.remove(name);
  }

  public void register(String name, Object function) {
    functions.put(name.toLowerCase(Locale.ENGLISH), function);
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
  public SQLFunction createFunction(final String name) throws CommandExecutionException {
    Object obj = functions.get(name.toLowerCase(Locale.ENGLISH));

    if (obj == null)
      return null;

    if (obj instanceof SQLFunction)
      return (SQLFunction) obj;
    else {
      // it's a class
      final Class<?> typez = (Class<?>) obj;
      try {
        return (SQLFunction) typez.getConstructor().newInstance();
      } catch (Exception e) {
        throw new CommandExecutionException(
            "Error in creation of function " + name + "(). Probably there is not an empty constructor or the constructor generates errors", e);

      }
    }

  }

  public Map<String, Object> getFunctions() {
    return functions;
  }

}
