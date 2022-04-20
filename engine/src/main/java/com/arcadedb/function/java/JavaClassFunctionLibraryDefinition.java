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
import java.util.concurrent.*;

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
    for (Method m : impl.getDeclaredMethods()) {
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
  public JavaMethodFunctionDefinition getFunction(final String functionName) {
    return functions.get(functionName);
  }

  @Override
  public JavaClassFunctionLibraryDefinition registerFunction(JavaMethodFunctionDefinition registerFunction) {
    throw new UnsupportedOperationException("Cannot register additional methods to a class");
  }

  @Override
  public JavaClassFunctionLibraryDefinition unregisterFunction(String functionName) {
    throw new UnsupportedOperationException("Cannot unregister additional methods to a class");
  }
}
