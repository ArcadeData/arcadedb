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
import java.util.ArrayList;
import java.util.List;

/**
 * Maps one or more overloaded Java methods sharing the same name to a callable function. When more than one public
 * method of a class shares a name (Java method overloading), all the overloads are bound here and the one matching
 * the number of arguments (and, if that is not enough to disambiguate, their runtime type) actually passed is
 * selected on every {@link #execute(Object...)} call, instead of registering only one of them depending on the
 * JVM's unspecified {@link Class#getDeclaredMethods()} order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JavaMethodFunctionDefinition implements FunctionDefinition {
  private final List<Method> methods;
  private final Object       instance;

  /**
   * Creates a function bound to a single Java method.
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
    this.methods = List.of(method);
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

  /**
   * Creates a function bound to a group of overloaded Java methods that share the same name. All the methods must
   * belong to the same declaring class; {@code instance} is used for the ones that are not static and is ignored
   * (may be {@code null}) if every overload is static.
   *
   * @param instance Java object against where to invoke the non-static overloads, or {@code null} if all of them are static
   * @param methods  the overloads, all sharing the same method name
   */
  public JavaMethodFunctionDefinition(final Object instance, final List<Method> methods) {
    if (methods.isEmpty())
      throw new IllegalArgumentException("At least one method is required");
    this.instance = instance;
    this.methods = List.copyOf(methods);
  }

  @Override
  public String getName() {
    final Method first = methods.get(0);
    return first.getDeclaringClass() + "::" + first.getName();
  }

  @Override
  public Object execute(final Object... parameters) {
    final Object[] args = parameters != null ? parameters : new Object[0];
    final int received = args.length;

    final List<Method> candidates = candidatesByParameterCount(received);

    if (candidates.isEmpty()) {
      if (methods.size() == 1) {
        final Method only = methods.get(0);
        throw new FunctionExecutionException(
            "Error on executing function '" + only + "': expected " + only.getParameterCount() + " parameter(s) but received " + received);
      }
      throw new FunctionExecutionException(
          "Error on executing function '" + getName() + "': none of the " + methods.size() + " overloads accepts " + received + " parameter(s)");
    }

    final Method method = candidates.size() == 1 ? candidates.get(0) : disambiguateByArgumentType(candidates, args);

    try {
      return method.invoke(instance, args);
    } catch (final InvocationTargetException e) {
      // PRESERVE THE ORIGINAL EXCEPTION THROWN BY THE TARGET METHOD INSTEAD OF THE REFLECTION WRAPPER
      final Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new FunctionExecutionException("Error on executing function '" + method + "'", cause);
    } catch (final IllegalAccessException | IllegalArgumentException e) {
      throw new FunctionExecutionException("Error on executing function '" + method + "'", e);
    }
  }

  private List<Method> candidatesByParameterCount(final int received) {
    final List<Method> exact = new ArrayList<>();
    final List<Method> varargs = new ArrayList<>();
    for (final Method m : methods) {
      if (m.isVarArgs()) {
        if (received >= m.getParameterCount() - 1)
          varargs.add(m);
      } else if (m.getParameterCount() == received)
        exact.add(m);
    }
    return !exact.isEmpty() ? exact : varargs;
  }

  private Method disambiguateByArgumentType(final List<Method> candidates, final Object[] args) {
    Method match = null;
    for (final Method m : candidates) {
      if (!m.isVarArgs() && acceptsArgumentTypes(m, args)) {
        if (match != null)
          throw ambiguousOverloadException(candidates, args);
        match = m;
      }
    }
    if (match == null)
      throw ambiguousOverloadException(candidates, args);
    return match;
  }

  private static boolean acceptsArgumentTypes(final Method method, final Object[] args) {
    final Class<?>[] paramTypes = method.getParameterTypes();
    for (int i = 0; i < paramTypes.length; i++) {
      final Object arg = args[i];
      if (arg == null) {
        if (paramTypes[i].isPrimitive())
          return false;
        continue;
      }
      if (!wrap(paramTypes[i]).isInstance(arg))
        return false;
    }
    return true;
  }

  private static Class<?> wrap(final Class<?> type) {
    if (!type.isPrimitive())
      return type;
    if (type == int.class)
      return Integer.class;
    if (type == long.class)
      return Long.class;
    if (type == double.class)
      return Double.class;
    if (type == float.class)
      return Float.class;
    if (type == boolean.class)
      return Boolean.class;
    if (type == byte.class)
      return Byte.class;
    if (type == short.class)
      return Short.class;
    if (type == char.class)
      return Character.class;
    return type;
  }

  private FunctionExecutionException ambiguousOverloadException(final List<Method> candidates, final Object[] args) {
    final StringBuilder argTypes = new StringBuilder();
    for (int i = 0; i < args.length; i++) {
      if (i > 0)
        argTypes.append(", ");
      argTypes.append(args[i] != null ? args[i].getClass().getName() : "null");
    }
    return new FunctionExecutionException(
        "Error on executing function '" + getName() + "': cannot resolve which overload to call among " + candidates
            + " for argument type(s) [" + argTypes + "]");
  }

  /**
   * Returns the current java object instance to use for method calling. If the instance is null, then the method is static.
   */
  public Object getInstance() {
    return instance;
  }
}
