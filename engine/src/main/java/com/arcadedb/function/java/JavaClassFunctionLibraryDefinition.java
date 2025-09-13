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

import com.arcadedb.function.FunctionLibraryDefinition;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Binds a Java class into a function library, where each method of the class are invokable functions. At construction time, the class is inspected to
 * find the methods by using reflection This library definition implementation does not allow dynamic function registration.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JavaClassFunctionLibraryDefinition implements FunctionLibraryDefinition<JavaMethodFunctionDefinition> {
  private final String                                              libraryName;
  private final ConcurrentMap<String, JavaMethodFunctionDefinition> functions = new ConcurrentHashMap<>();

  public JavaClassFunctionLibraryDefinition(final String javaFullClassName)
      throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this(javaFullClassName, Class.forName(javaFullClassName));
  }

  public JavaClassFunctionLibraryDefinition(final String libraryName, final String javaFullClassName)
      throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this(libraryName, Class.forName(javaFullClassName));
  }

  public JavaClassFunctionLibraryDefinition(final String libraryName, final Class<?> impl)
      throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this.libraryName = libraryName;

    Object instance = null;
    for (final Method m : impl.getDeclaredMethods()) {
      if (!Modifier.isPublic(m.getModifiers()))
        continue;

      final JavaMethodFunctionDefinition f = new JavaMethodFunctionDefinition(instance, m);
      if (f.getInstance() != null)
        instance = f.getInstance();

      functions.put(m.getName(), f);
    }
  }

  public String getName() {
    return libraryName;
  }

  public Iterable<JavaMethodFunctionDefinition> getFunctions() {
    return Collections.unmodifiableCollection(functions.values());
  }

  @Override
  public boolean hasFunction(final String functionName) {
    return functions.containsKey(functionName);
  }

  @Override
  public JavaMethodFunctionDefinition getFunction(final String functionName) throws IllegalArgumentException {
    final JavaMethodFunctionDefinition f = functions.get(functionName);
    if (f == null)
      throw new IllegalArgumentException("Function '" + functionName + "' not defined");
    return f;
  }

  @Override
  public JavaClassFunctionLibraryDefinition registerFunction(final JavaMethodFunctionDefinition registerFunction) {
    throw new UnsupportedOperationException("Cannot register additional methods to a class");
  }

  @Override
  public JavaClassFunctionLibraryDefinition unregisterFunction(final String functionName) {
    throw new UnsupportedOperationException("Cannot unregister additional methods to a class");
  }
}
