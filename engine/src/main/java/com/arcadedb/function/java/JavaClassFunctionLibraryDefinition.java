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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Binds a Java class into a function library, where each method of the class are invokable functions. At construction time, the class is inspected to
 * find the methods by using reflection This library definition implementation does not allow dynamic function registration.
 * <p>
 * Overloaded public methods (same name, different signature) are all bound under their shared name: the matching
 * overload is picked at call time by {@link JavaMethodFunctionDefinition} based on the arguments actually passed,
 * rather than only one overload surviving registration depending on the JVM's unspecified method enumeration order.
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

    final Map<String, List<Method>> methodsByName = new LinkedHashMap<>();
    for (final Method m : impl.getDeclaredMethods()) {
      // Bridge/synthetic methods (generated for generic or covariant-return overrides) are public too, and would
      // otherwise be grouped alongside the real method under the same name - making an unambiguous call reject as
      // ambiguous between the real method and its own compiler-generated bridge.
      if (!Modifier.isPublic(m.getModifiers()) || m.isBridge() || m.isSynthetic())
        continue;
      methodsByName.computeIfAbsent(m.getName(), k -> new ArrayList<>()).add(m);
    }

    // A SINGLE INSTANCE IS SHARED BY ALL THE NON-STATIC METHODS OF THE CLASS
    final boolean hasInstanceMethod = methodsByName.values().stream().flatMap(List::stream).anyMatch(m -> !Modifier.isStatic(m.getModifiers()));
    final Object instance = hasInstanceMethod ? impl.getConstructor().newInstance() : null;

    for (final Map.Entry<String, List<Method>> entry : methodsByName.entrySet())
      functions.put(entry.getKey(), new JavaMethodFunctionDefinition(instance, entry.getValue()));
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
