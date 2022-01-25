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

import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.function.misc.SQLStaticReflectiveFunction;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Factory for custom SQL functions.
 *
 * @author Fabrizio Fortino
 */
public class SQLFunctionReflectionFactory {
  private final DefaultSQLFunctionFactory factory;

  public SQLFunctionReflectionFactory(final DefaultSQLFunctionFactory factory) {
    this.factory = factory;
    register("math_", Math.class);
  }

  public void register(final String prefix, final Class<?> clazz) {
    final Map<String, List<Method>> methodsMap = Arrays.stream(clazz.getMethods()).filter(m -> Modifier.isStatic(m.getModifiers()))
        .collect(Collectors.groupingBy(Method::getName));

    for (Map.Entry<String, List<Method>> entry : methodsMap.entrySet()) {
      final String name = prefix + entry.getKey();
      if (factory.getFunctionNames().contains(name)) {
        LogManager.instance().log(this, Level.WARNING, "Unable to register reflective function with name '%s'", name);
      } else {
        List<Method> methodsList = methodsMap.get(entry.getKey());
        Method[] methods = new Method[methodsList.size()];
        int i = 0;
        int minParams = 0;
        int maxParams = 0;
        for (Method m : methodsList) {
          methods[i++] = m;
          minParams = minParams < m.getParameterTypes().length ? minParams : m.getParameterTypes().length;
          maxParams = maxParams > m.getParameterTypes().length ? maxParams : m.getParameterTypes().length;
        }
        factory.register(name.toLowerCase(Locale.ENGLISH), new SQLStaticReflectiveFunction(name, minParams, maxParams, methods));
      }
    }
  }
}
