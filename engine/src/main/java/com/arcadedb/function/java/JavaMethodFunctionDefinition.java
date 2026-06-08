package com.arcadedb.function.java;/*
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

import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionExecutionException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Maps a Java method execution to a callable function.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JavaMethodFunctionDefinition implements FunctionDefinition {
  private final Method method;
  private final Object instance;

  /**
   * Creates a function bound to a Java method.
   *
   * @param instance Java object against where to invoke the method
   * @param method   Java Method object to invoke
   *
   * @throws NoSuchMethodException
   * @throws InvocationTargetException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public JavaMethodFunctionDefinition(final Object instance, final Method method)
      throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
    this.instance = Modifier.isStatic(method.getModifiers()) ? null : instance != null ? instance : method.getDeclaringClass().getConstructor().newInstance();
    this.method = method;
  }

  /**
   * Creates a function bound to a static Java method.
   *
   * @param method static method to execute
   *
   * @throws NoSuchMethodException
   * @throws InvocationTargetException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public JavaMethodFunctionDefinition(final Method method)
      throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
    this(null, method);
  }

  @Override
  public String getName() {
    return method.getDeclaringClass() + "::" + method.getName();
  }

  @Override
  public Object execute(final Object... parameters) {
    final int received = parameters != null ? parameters.length : 0;
    if (!method.isVarArgs() && received != method.getParameterCount())
      throw new FunctionExecutionException(
          "Error on executing function '" + method + "': expected " + method.getParameterCount() + " parameter(s) but received " + received);

    try {
      return method.invoke(instance, parameters);
    } catch (final InvocationTargetException e) {
      // PRESERVE THE ORIGINAL EXCEPTION THROWN BY THE TARGET METHOD INSTEAD OF THE REFLECTION WRAPPER
      final Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new FunctionExecutionException("Error on executing function '" + method + "'", cause);
    } catch (final IllegalAccessException | IllegalArgumentException e) {
      throw new FunctionExecutionException("Error on executing function '" + method + "'", e);
    }
  }

  /**
   * Returns the current java object instance to use for method calling. If the instance is null, then the method is static.
   */
  public Object getInstance() {
    return instance;
  }
}
