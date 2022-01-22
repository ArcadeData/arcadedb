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
 */

import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionExecutionException;

import java.lang.reflect.*;

public class JavaMethodFunctionDefinition implements FunctionDefinition {
  private final Method method;
  private final Object instance;

  public JavaMethodFunctionDefinition(final Object instance, final Method method)
      throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
    this.instance = Modifier.isStatic(method.getModifiers()) ? null : instance != null ? instance : method.getDeclaringClass().getConstructor().newInstance();
    this.method = method;
  }

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
    try {
      return method.invoke(instance, parameters);
    } catch (Exception e) {
      throw new FunctionExecutionException("Error on executing function '" + method + "'", e);
    }
  }

  public Object getInstance() {
    return instance;
  }
}
