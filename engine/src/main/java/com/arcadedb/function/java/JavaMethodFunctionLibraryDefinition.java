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
import java.util.Set;

/**
 * Function library that allows invocation of functions written in Java language: a single method, exposed under the
 * library name.
 * <p>
 * A method that is not static is invoked on an instance. The instance-less constructors create one HERE, at the
 * registration site, through the public no-arg constructor of the declaring class - the same choice
 * {@link JavaClassFunctionLibraryDefinition} makes for a whole class - and that is the only place it happens:
 * {@link JavaMethodFunctionDefinition} itself never instantiates anything (issue #7046). To invoke the method on an
 * object of the caller's choosing, one that carries state or dependencies, register it through
 * {@link #JavaMethodFunctionLibraryDefinition(String, Object, Method)}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JavaMethodFunctionLibraryDefinition implements FunctionLibraryDefinition<JavaMethodFunctionDefinition> {
  private final String                       libraryName;
  private final Method                       method;
  private final JavaMethodFunctionDefinition function;

  /**
   * Exposes a method under a library named after its declaring class and its name. A non-static method is invoked on
   * a new instance of its declaring class, created through the public no-arg constructor.
   */
  public JavaMethodFunctionLibraryDefinition(final Method method)
      throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this(method.getDeclaringClass() + "::" + method.getName(), method);
  }

  /**
   * Exposes a method under {@code libraryName}. A non-static method is invoked on a new instance of its declaring
   * class, created through the public no-arg constructor.
   *
   * @throws NoSuchMethodException     when the method is not static and its class has no public no-arg constructor
   * @throws InstantiationException    when the method is not static and its class cannot be instantiated
   * @throws IllegalAccessException    when the method is not static and its constructor is not accessible
   * @throws InvocationTargetException when the method is not static and its constructor throws
   */
  public JavaMethodFunctionLibraryDefinition(final String libraryName, final Method method)
      throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this(libraryName, Modifier.isStatic(method.getModifiers()) ? null : method.getDeclaringClass().getConstructor().newInstance(),
        method);
  }

  /**
   * Exposes a method under {@code libraryName}, invoked on {@code instance} when it is not static.
   *
   * @param libraryName the library name the function is registered under
   * @param instance    the object to invoke the method on; required when the method is not static, ignored (may be
   *                    {@code null}) when it is
   * @param method      the method to expose
   *
   * @throws IllegalArgumentException when {@code method} is not static and {@code instance} is {@code null}
   */
  public JavaMethodFunctionLibraryDefinition(final String libraryName, final Object instance, final Method method) {
    this.method = method;
    this.libraryName = libraryName;
    this.function = new JavaMethodFunctionDefinition(instance, method);
  }

  @Override
  public String getName() {
    return libraryName;
  }

  @Override
  public Iterable<JavaMethodFunctionDefinition> getFunctions() {
    return Set.of(function);
  }

  @Override
  public boolean hasFunction(final String functionName) {
    return method.getName().equals(functionName);
  }

  @Override
  public JavaMethodFunctionDefinition getFunction(final String functionName) {
    if (!method.getName().equals(functionName))
      throw new IllegalArgumentException("Function '" + functionName + "' not defined");
    return function;
  }

  @Override
  public JavaMethodFunctionLibraryDefinition registerFunction(final JavaMethodFunctionDefinition registerFunction) {
    throw new UnsupportedOperationException("Cannot register additional methods to a class");
  }

  @Override
  public JavaMethodFunctionLibraryDefinition unregisterFunction(final String functionName) {
    throw new UnsupportedOperationException("Cannot unregister additional methods to a class");
  }
}
