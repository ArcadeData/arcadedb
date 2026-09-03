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

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Maps one or more overloaded Java methods sharing the same name to a callable function. When more than one public
 * method of a class shares a name (Java method overloading), all the overloads are bound here and the one matching
 * the number of arguments (and, if that is not enough to disambiguate, their runtime type) actually passed is
 * selected on every {@link #execute(Object...)} call, instead of registering only one of them depending on the
 * JVM's unspecified {@link Class#getDeclaredMethods()} order.
 * <p>
 * Unlike the Java compiler, this does not rank overloads by specificity: if more than one overload's parameter
 * types accept the arguments (e.g. {@code foo(Object)} and {@code foo(String)} both called with a {@code String}),
 * the call is rejected as ambiguous rather than silently picking the most specific match.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JavaMethodFunctionDefinition implements FunctionDefinition {
  // JLS 5.1.2 widening primitive conversions that method.invoke() itself accepts (after unboxing the argument):
  // e.g. an Integer argument is a valid call-site match for a `long` parameter.
  private static final Map<Class<?>, Set<Class<?>>> WIDENING_CONVERSIONS = Map.of(//
      byte.class, Set.of(short.class, int.class, long.class, float.class, double.class), //
      short.class, Set.of(int.class, long.class, float.class, double.class), //
      char.class, Set.of(int.class, long.class, float.class, double.class), //
      int.class, Set.of(long.class, float.class, double.class), //
      long.class, Set.of(float.class, double.class), //
      float.class, Set.of(double.class));

  private final List<Method> methods;
  private final Object       instance;

  /**
   * Creates a function bound to a single Java method.
   *
   * @param instance Java object against where to invoke the method; required when the method is not static, ignored
   *                 (may be {@code null}) when it is
   * @param method   Java Method object to invoke
   *
   * @throws IllegalArgumentException when {@code method} is not static and no instance is supplied. The declaring
   *                                  class is never instantiated on the caller's behalf (issue #7046): whether a
   *                                  class gets constructed, and how, is the registration site's decision, as
   *                                  {@link JavaClassFunctionLibraryDefinition} makes it for a whole class
   */
  public JavaMethodFunctionDefinition(final Object instance, final Method method) {
    this.methods = List.of(method);
    this.instance = instanceFor(instance, this.methods);
  }

  /**
   * Creates a function bound to a static Java method.
   *
   * @param method static method to execute
   *
   * @throws IllegalArgumentException when {@code method} is not static: see
   *                                  {@link #JavaMethodFunctionDefinition(Object, Method)} for the form that takes
   *                                  the instance to invoke it on
   */
  public JavaMethodFunctionDefinition(final Method method) {
    this(null, method);
  }

  /**
   * Creates a function bound to a group of overloaded Java methods that share the same name. All the methods must
   * belong to the same declaring class; {@code instance} is used for the ones that are not static and is ignored
   * (may be {@code null}) if every overload is static.
   *
   * @param instance Java object against where to invoke the non-static overloads, or {@code null} if all of them are static
   * @param methods  the overloads, all sharing the same method name
   *
   * @throws IllegalArgumentException when an overload is not static and no instance is supplied
   */
  public JavaMethodFunctionDefinition(final Object instance, final List<Method> methods) {
    if (methods.isEmpty())
      throw new IllegalArgumentException("At least one method is required");
    this.instance = instanceFor(instance, methods);
    // Sorted so which overload names an error message (getName(), or the "expected/received" and ambiguity
    // messages) is deterministic, rather than depending on the JVM's unspecified getDeclaredMethods() order.
    this.methods = methods.stream()
        .sorted(Comparator.<Method>comparingInt(Method::getParameterCount).thenComparing(m -> Arrays.toString(m.getParameterTypes())))
        .toList();
  }

  /**
   * The receiver the methods are invoked on: {@code instance} when any of them needs one, {@code null} when all of
   * them are static (a receiver is ignored by {@link Method#invoke} for a static method, so there is no point in
   * holding on to one). A non-static method with no instance is refused here rather than served by instantiating
   * its declaring class: constructing an arbitrary class as a side effect of registering a function is not a
   * contract a registration site can be expected to know about (issue #7046).
   */
  private static Object instanceFor(final Object instance, final List<Method> methods) {
    boolean needsInstance = false;
    for (final Method method : methods)
      if (!Modifier.isStatic(method.getModifiers())) {
        if (instance == null)
          throw new IllegalArgumentException(
              "Method '" + method + "' is not static: an instance to invoke it on is required");
        needsInstance = true;
      }
    return needsInstance ? instance : null;
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

    if (methods.size() == 1) {
      // Fast path for the overwhelmingly common non-overloaded case: skips the List allocation
      // candidatesByParameterCount()/disambiguateByArgumentType() would otherwise do on every single call.
      final Method only = methods.get(0);
      final boolean varArgs = only.isVarArgs();
      final int minReceived = varArgs ? only.getParameterCount() - 1 : only.getParameterCount();
      if (varArgs ? received < minReceived : received != minReceived)
        throw new FunctionExecutionException(
            "Error on executing function '" + only + "': expected " + (varArgs ? "at least " + minReceived : minReceived)
                + " parameter(s) but received " + received);
      return invoke(only, args);
    }

    final List<Method> candidates = candidatesByParameterCount(received);
    if (candidates.isEmpty())
      throw new FunctionExecutionException(
          "Error on executing function '" + getName() + "': none of the " + methods.size() + " overloads accepts " + received + " parameter(s)");

    final Method method = candidates.size() == 1 ? candidates.get(0) : disambiguateByArgumentType(candidates, args);
    return invoke(method, args);
  }

  private Object invoke(final Method method, final Object[] args) {
    try {
      return method.invoke(instance, toInvokeArgs(method, args));
    } catch (final InvocationTargetException e) {
      // PRESERVE THE ORIGINAL EXCEPTION THROWN BY THE TARGET METHOD INSTEAD OF THE REFLECTION WRAPPER
      final Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new FunctionExecutionException("Error on executing function '" + method + "'", cause);
    } catch (final IllegalAccessException | IllegalArgumentException e) {
      throw new FunctionExecutionException("Error on executing function '" + method + "'", e);
    }
  }

  /**
   * Unlike a normal (compiled) varargs call, {@link Method#invoke} does not pack trailing arguments into an array
   * itself: it requires the args array to have exactly as many elements as the method's formal parameters, with the
   * last one already being an array of the vararg component type. This packs the flat, positionally-passed
   * arguments this class receives into that shape for a varargs method; non-varargs methods are passed through
   * unchanged.
   */
  private static Object[] toInvokeArgs(final Method method, final Object[] args) {
    if (!method.isVarArgs() || isPrePacked(method, args))
      return args;

    final Class<?>[] paramTypes = method.getParameterTypes();
    final int fixedCount = paramTypes.length - 1;
    final Class<?> varargsType = paramTypes[fixedCount];

    final Object varargsArray = Array.newInstance(varargsType.getComponentType(), args.length - fixedCount);
    for (int i = fixedCount; i < args.length; i++)
      Array.set(varargsArray, i - fixedCount, args[i]);

    final Object[] invokeArgs = new Object[fixedCount + 1];
    System.arraycopy(args, 0, invokeArgs, 0, fixedCount);
    invokeArgs[fixedCount] = varargsArray;
    return invokeArgs;
  }

  /**
   * True when the caller already passed the vararg part pre-packed as a single array matching the vararg component
   * type - i.e. {@code args} is already in the exact shape {@link Method#invoke} requires - rather than as flat,
   * positionally-passed elements.
   */
  private static boolean isPrePacked(final Method method, final Object[] args) {
    final Class<?>[] paramTypes = method.getParameterTypes();
    final int fixedCount = paramTypes.length - 1;
    return args.length == paramTypes.length && (args[fixedCount] == null || paramTypes[fixedCount].isInstance(args[fixedCount]));
  }

  /**
   * All overloads whose parameter count can possibly accept {@code received} arguments - both an exact-arity
   * non-varargs match and a varargs match that could still absorb them. Both kinds are kept together (rather than
   * a fixed-arity match with a matching count unconditionally winning regardless of its parameter types) so that
   * {@link #disambiguateByArgumentType} gets the chance to fall back to a type-compatible varargs overload when the
   * fixed-arity one does not actually accept the arguments' runtime types.
   */
  private List<Method> candidatesByParameterCount(final int received) {
    final List<Method> candidates = new ArrayList<>();
    for (final Method m : methods) {
      if (m.isVarArgs()) {
        if (received >= m.getParameterCount() - 1)
          candidates.add(m);
      } else if (m.getParameterCount() == received)
        candidates.add(m);
    }
    return candidates;
  }

  /**
   * Picks the overload matching the arguments' runtime types, preferring a fixed-arity match over a varargs one -
   * mirroring how {@code javac} only falls back to varargs (its resolution phase 3) once no fixed-arity applicable
   * method exists - rather than ranking by parameter-type specificity in general (see the class Javadoc).
   */
  private Method disambiguateByArgumentType(final List<Method> candidates, final Object[] args) {
    final List<Method> fixedArity = new ArrayList<>();
    final List<Method> varArgs = new ArrayList<>();
    for (final Method m : candidates)
      (m.isVarArgs() ? varArgs : fixedArity).add(m);

    Method match = matchByType(candidates, fixedArity, args);
    if (match == null)
      match = matchByType(candidates, varArgs, args);
    if (match == null)
      throw noMatchingOverloadException(candidates, args);
    return match;
  }

  private Method matchByType(final List<Method> allCandidates, final List<Method> pool, final Object[] args) {
    Method match = null;
    for (final Method m : pool) {
      if (acceptsArgumentTypes(m, args)) {
        if (match != null)
          throw ambiguousOverloadException(allCandidates, args);
        match = m;
      }
    }
    return match;
  }

  private static boolean acceptsArgumentTypes(final Method method, final Object[] args) {
    final Class<?>[] paramTypes = method.getParameterTypes();
    final boolean varArgs = method.isVarArgs();
    final int fixedCount = varArgs ? paramTypes.length - 1 : paramTypes.length;

    for (int i = 0; i < fixedCount; i++)
      if (!typeMatches(paramTypes[i], args[i]))
        return false;

    if (varArgs) {
      if (isPrePacked(method, args))
        // the vararg part was passed as a single, already-built array: match it against the array type itself
        // rather than element-by-element, otherwise e.g. a pre-packed String[] would be compared to String and fail.
        return typeMatches(paramTypes[fixedCount], args[fixedCount]);

      final Class<?> componentType = paramTypes[fixedCount].getComponentType();
      for (int i = fixedCount; i < args.length; i++)
        if (!typeMatches(componentType, args[i]))
          return false;
    }
    return true;
  }

  private static boolean typeMatches(final Class<?> paramType, final Object arg) {
    if (arg == null)
      return !paramType.isPrimitive();
    if (paramType.isPrimitive()) {
      final Class<?> argPrimitiveType = unwrap(arg.getClass());
      return argPrimitiveType != null
          && (argPrimitiveType == paramType || WIDENING_CONVERSIONS.getOrDefault(argPrimitiveType, Set.of()).contains(paramType));
    }
    return paramType.isInstance(arg);
  }

  /**
   * The primitive type a wrapper class unboxes to, or {@code null} if {@code wrapperType} is not one of the eight
   * primitive wrapper classes.
   */
  private static Class<?> unwrap(final Class<?> wrapperType) {
    if (wrapperType == Integer.class)
      return int.class;
    if (wrapperType == Long.class)
      return long.class;
    if (wrapperType == Double.class)
      return double.class;
    if (wrapperType == Float.class)
      return float.class;
    if (wrapperType == Boolean.class)
      return boolean.class;
    if (wrapperType == Byte.class)
      return byte.class;
    if (wrapperType == Short.class)
      return short.class;
    if (wrapperType == Character.class)
      return char.class;
    return null;
  }

  private FunctionExecutionException noMatchingOverloadException(final List<Method> candidates, final Object[] args) {
    return new FunctionExecutionException(
        "Error on executing function '" + getName() + "': none of " + candidates + " accepts argument type(s) [" + describeArgumentTypes(args) + "]");
  }

  private FunctionExecutionException ambiguousOverloadException(final List<Method> candidates, final Object[] args) {
    return new FunctionExecutionException(
        "Error on executing function '" + getName() + "': cannot resolve which overload to call among " + candidates
            + " for argument type(s) [" + describeArgumentTypes(args) + "]");
  }

  private static String describeArgumentTypes(final Object[] args) {
    final StringBuilder argTypes = new StringBuilder();
    for (int i = 0; i < args.length; i++) {
      if (i > 0)
        argTypes.append(", ");
      argTypes.append(args[i] != null ? args[i].getClass().getName() : "null");
    }
    return argTypes.toString();
  }
}
