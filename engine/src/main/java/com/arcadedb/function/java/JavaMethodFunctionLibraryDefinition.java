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

import com.arcadedb.function.FunctionLibraryDefinition;

import java.lang.reflect.*;
import java.util.*;

public class JavaMethodFunctionLibraryDefinition implements FunctionLibraryDefinition<JavaMethodFunctionDefinition> {
  private final String                       libraryName;
  private final Method                       method;
  private final JavaMethodFunctionDefinition function;

  public JavaMethodFunctionLibraryDefinition(final Method method)
      throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this(method.getDeclaringClass() + "::" + method.getName(), method);
  }

  public JavaMethodFunctionLibraryDefinition(final String libraryName, final Method method)
      throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this.method = method;
    this.libraryName = libraryName;
    this.function = new JavaMethodFunctionDefinition(method);
  }

  @Override
  public String getName() {
    return libraryName;
  }

  @Override
  public Iterable<JavaMethodFunctionDefinition> getFunctions() {
    return Collections.singleton(function);
  }

  @Override
  public JavaMethodFunctionDefinition getFunction(final String functionName) {
    return method.getName().equals(functionName) ? function : null;
  }

  @Override
  public JavaMethodFunctionLibraryDefinition registerFunction(JavaMethodFunctionDefinition registerFunction) {
    throw new UnsupportedOperationException("Cannot register additional methods to a class");
  }

  @Override
  public JavaMethodFunctionLibraryDefinition unregisterFunction(String functionName) {
    throw new UnsupportedOperationException("Cannot unregister additional methods to a class");
  }
}
