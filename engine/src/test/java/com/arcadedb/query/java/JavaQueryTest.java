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
package com.arcadedb.query.java;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class JavaMethods {
  public JavaMethods() {
  }

  public int sum(final int a, final int b) {
    return a + b;
  }

  public static int SUM(final int a, final int b) {
    return a + b;
  }

  public static void hello() {
    // EMPTY METHOD
  }
}

/**
 * Reference-typed parameters and overloads, the shapes {@code JavaMethods} does not cover (issue #7880).
 */
class JavaReferenceMethods {
  public JavaReferenceMethods() {
  }

  public String greet(final String name) {
    return "hello " + name;
  }

  public static String describe(final Object value) {
    return "object " + value;
  }

  public static String onlyInteger(final Integer value) {
    return "integer " + value;
  }

  public static String pick(final int value) {
    return "int " + value;
  }

  public static String pick(final String value) {
    return "string " + value;
  }

  public static String twice(final int value) {
    return "int " + (value * 2);
  }

  public static String twice(final long value) {
    return "long " + (value * 2);
  }

  public static String ambiguous(final Object value) {
    return "object";
  }

  public static String ambiguous(final String value) {
    return "string";
  }

  public static int square(final int value) {
    return value * value;
  }

  public static String join(final String... parts) {
    return String.join("+", parts);
  }

  public static String widen(final long value) {
    return "long " + value;
  }

  public static String nullable(final String value) {
    return "value " + value;
  }

  public static String log(final int value) {
    return "int";
  }

  public static String log(final Object value) {
    return "object";
  }

  public static int size(final Collection<?> values) {
    return values.size();
  }
}

/**
 * No no-argument constructor: an instance can only be created by the engine for a non-static method, so calling the
 * static overload proves the engine does not instantiate the class when the selected method does not need it.
 */
class JavaStaticAndInstanceOverloads {
  private final String prefix;

  public JavaStaticAndInstanceOverloads(final String prefix) {
    this.prefix = prefix;
  }

  public static String render(final int value) {
    return "static " + value;
  }

  public String render(final String value) {
    return prefix + value;
  }
}

interface JavaQueryTransformer<T> {
  T transform(T value);
}

/**
 * Implementing a generic interface makes javac emit a bridge {@code transform(Object)} next to {@code transform(String)}:
 * both are public and both accept a String, so the bridge must not take part in overload selection.
 */
class JavaUpperCaser implements JavaQueryTransformer<String> {
  public JavaUpperCaser() {
  }

  @Override
  public String transform(final String value) {
    return value.toUpperCase();
  }
}

class JavaQueryTest extends TestHelper {
  private static final String REF = "com.arcadedb.query.java.JavaReferenceMethods";

  @Test
  void registeredMethod() {
    assertThat(database.getQueryEngine("java").getLanguage()).isEqualTo("java");

    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods::sum");

    final ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);
  }

  @Test
  void registeredMethods() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods::sum");
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods::SUM");

    ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);

    result = database.command("java", "com.arcadedb.query.java.JavaMethods::SUM", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);

    database.getQueryEngine("java").unregisterFunctions();
  }

  @Test
  void registeredClass() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");

    ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);

    result = database.command("java", "com.arcadedb.query.java.JavaMethods::SUM", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);

    database.getQueryEngine("java").unregisterFunctions();
  }

  @Test
  void unRegisteredMethod() {
    try {
      database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
      fail("");
    } catch (final CommandExecutionException e) {
      // EXPECTED
      assertThat(e.getCause() instanceof SecurityException).isTrue();
    }
  }

  @Test
  void notExistentMethod() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    try {
      database.command("java", "com.arcadedb.query.java.JavaMethods::totallyInvented", 5, 3);
      fail("");
    } catch (final CommandExecutionException e) {
      // EXPECTED
      assertThat(e.getCause() instanceof NoSuchMethodException).isTrue();
    }
  }

  @Test
  void analyzeQuery() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("java").analyze("com.arcadedb.query.java.JavaMethods::totallyInvented");
    assertThat(analyzed.isDDL()).isFalse();
    assertThat(analyzed.isIdempotent()).isFalse();
  }

  @Test
  void unsupportedMethods() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    assertThatThrownBy(() -> database.query("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3)).isInstanceOf(UnsupportedOperationException.class);

    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    assertThatThrownBy(() -> {
      final HashMap map = new HashMap();
      map.put("name", 1);
      database.getQueryEngine("java").command("com.arcadedb.query.java.JavaMethods::hello", null, map);
    }).isInstanceOf(UnsupportedOperationException.class);

    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    assertThatThrownBy(() -> database.getQueryEngine("java").query("com.arcadedb.query.java.JavaMethods::sum", null, new HashMap<>())).isInstanceOf(UnsupportedOperationException.class);
  }

  /**
   * Issue #7880: a method whose parameter type is exactly the argument's class was rejected by the inverted
   * {@code isAssignableFrom} test and reported as not found on the classpath.
   */
  @Test
  void referenceParameterMatchingTheArgumentClassIsSelected() {
    database.getQueryEngine("java").registerFunctions(REF);

    final ResultSet result = database.command("java", REF + "::greet", "world");
    assertThat((String) result.next().getProperty("value")).isEqualTo("hello world");
  }

  @Test
  void referenceParameterOfASupertypeStillAcceptsTheArgument() {
    database.getQueryEngine("java").registerFunctions(REF);

    final ResultSet result = database.command("java", REF + "::describe", "x");
    assertThat((String) result.next().getProperty("value")).isEqualTo("object x");
  }

  /**
   * Issue #7880: a reference parameter the argument is not an instance of was accepted unchecked and failed later
   * inside {@code Method.invoke} with "argument type mismatch". It must be refused while matching instead.
   */
  @Test
  void referenceParameterNotAcceptingTheArgumentIsRefusedWhileMatching() {
    database.getQueryEngine("java").registerFunctions(REF);

    assertThatThrownBy(() -> database.command("java", REF + "::onlyInteger", "not a number"))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause()
        .isNotInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("onlyInteger")
        .hasMessageContaining("java.lang.String");
  }

  /**
   * Issue #7880: the loop stopped at the first name-and-arity match, so with overloads the pick depended on the
   * unspecified {@code Class.getMethods()} order. Both overloads must be reachable by their argument type.
   */
  @Test
  void overloadIsSelectedByArgumentTypeNotByDeclarationOrder() {
    database.getQueryEngine("java").registerFunctions(REF);

    assertThat((String) database.command("java", REF + "::pick", "x").next().getProperty("value")).isEqualTo("string x");
    assertThat((String) database.command("java", REF + "::pick", 7).next().getProperty("value")).isEqualTo("int 7");
  }

  @Test
  void narrowestPrimitiveOverloadWins() {
    database.getQueryEngine("java").registerFunctions(REF);

    assertThat((String) database.command("java", REF + "::twice", 4).next().getProperty("value")).isEqualTo("int 8");
    assertThat((String) database.command("java", REF + "::twice", 4L).next().getProperty("value")).isEqualTo("long 8");
  }

  /**
   * Same documented policy as {@code JavaMethodFunctionDefinition}: overloads are not ranked by reference-type
   * specificity, so {@code ambiguous(Object)} and {@code ambiguous(String)} for a String is an ambiguity error rather
   * than a pick by method order.
   */
  @Test
  void referenceOverloadsBothAcceptingTheArgumentAreAmbiguous() {
    database.getQueryEngine("java").registerFunctions(REF);

    assertThatThrownBy(() -> database.command("java", REF + "::ambiguous", "x"))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause()
        .hasMessageContaining("cannot resolve which overload");
  }

  /**
   * Also documented policy, and unlike javac (which picks {@code log(int)}): a primitive and a reference parameter are
   * not ranked against each other, so {@code log(int)} and {@code log(Object)} for an {@code Integer} is ambiguous.
   * Pinned so a change to that policy is a deliberate one.
   */
  @Test
  void primitiveAndObjectOverloadsBothAcceptingABoxedArgumentAreAmbiguous() {
    database.getQueryEngine("java").registerFunctions(REF);

    assertThatThrownBy(() -> database.command("java", REF + "::log", 1))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause()
        .hasMessageContaining("cannot resolve which overload");
  }

  @Test
  void nullArgumentForAPrimitiveParameterIsRefusedWhileMatching() {
    database.getQueryEngine("java").registerFunctions(REF);

    assertThatThrownBy(() -> database.command("java", REF + "::square", (Object) null))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause()
        .isNotInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("square");
  }

  @Test
  void wrongArgumentCountIsStillReportedAsNoSuchMethod() {
    database.getQueryEngine("java").registerFunctions(REF);

    assertThatThrownBy(() -> database.command("java", REF + "::greet", "a", "b"))
        .isInstanceOf(CommandExecutionException.class)
        .cause()
        .isInstanceOf(NoSuchMethodException.class);
  }

  @Test
  void varargsMethodReceivesTheArgumentsPacked() {
    database.getQueryEngine("java").registerFunctions(REF);

    assertThat((String) database.command("java", REF + "::join", "a", "b", "c").next().getProperty("value")).isEqualTo("a+b+c");
  }

  /**
   * {@code Class.getMethods()} returns the compiler-generated bridge {@code transform(Object)} next to
   * {@code transform(String)}: counting it as an overload would make every call ambiguous.
   */
  @Test
  void bridgeMethodDoesNotTakePartInOverloadSelection() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaUpperCaser");

    final ResultSet result = database.command("java", "com.arcadedb.query.java.JavaUpperCaser::transform", "abc");
    assertThat((String) result.next().getProperty("value")).isEqualTo("ABC");
  }

  @Test
  void boxedArgumentWidensIntoAWiderPrimitiveParameter() {
    database.getQueryEngine("java").registerFunctions(REF);

    assertThat((String) database.command("java", REF + "::widen", 5).next().getProperty("value")).isEqualTo("long 5");
  }

  @Test
  void nullArgumentForAReferenceParameterIsStillAccepted() {
    database.getQueryEngine("java").registerFunctions(REF);

    assertThat((String) database.command("java", REF + "::nullable", (Object) null).next().getProperty("value")).isEqualTo("value null");
  }

  @Test
  void varargsMethodAcceptsZeroArgumentsAndAPrePackedArray() {
    database.getQueryEngine("java").registerFunctions(REF);

    assertThat((String) database.command("java", REF + "::join").next().getProperty("value")).isEqualTo("");
    assertThat((String) database.command("java", REF + "::join", (Object) new String[] { "x", "y" }).next().getProperty("value"))
        .isEqualTo("x+y");
  }

  /**
   * A command without arguments reached the named-parameter overload, whose "no parameters" branch called
   * {@code command(query, null)}: that is {@code QueryEngine}'s no-parameter default, which calls the named-parameter
   * overload again with an empty map, until the stack overflowed.
   */
  @Test
  void commandWithoutArgumentsInvokesANoArgumentMethod() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");

    final ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::hello");
    assertThat(result.hasNext()).isTrue();
    assertThat((Object) result.next().getProperty("value")).isNull();
  }

  /**
   * The shape a JSON array parameter arrives in: an {@code ArrayList} argument binds to a {@code Collection}
   * parameter, a supertype of its class, which the inverted test only accepted by skipping the check. (A single
   * {@code Map} argument cannot be tested this way: {@code Database.command} binds it to its named-parameters overload.)
   */
  @Test
  void listArgumentBindsToACollectionParameter() {
    database.getQueryEngine("java").registerFunctions(REF);

    final List<Object> values = new ArrayList<>(List.of(1, 2, 3));
    assertThat((Integer) database.command("java", REF + "::size", values).next().getProperty("value")).isEqualTo(3);
  }

  /**
   * A {@code Long} (how a JSON number often arrives) does not narrow into an {@code int} parameter: refused while
   * matching, as it was by the old exact-wrapper check, and as a registered Java function refuses it.
   */
  @Test
  void longArgumentIsNotNarrowedIntoAnIntParameter() {
    database.getQueryEngine("java").registerFunctions(REF);

    assertThatThrownBy(() -> database.command("java", REF + "::square", 3L))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause()
        .hasMessageContaining("square")
        .hasMessageContaining("java.lang.Long");
  }

  @Test
  void staticOverloadIsInvokedWithoutInstantiatingTheClass() {
    final String cls = "com.arcadedb.query.java.JavaStaticAndInstanceOverloads";
    database.getQueryEngine("java").registerFunctions(cls);

    assertThat((String) database.command("java", cls + "::render", 3).next().getProperty("value")).isEqualTo("static 3");
    // the instance overload needs a no-argument constructor this class does not have: selected, then refused on creation
    assertThatThrownBy(() -> database.command("java", cls + "::render", "x"))
        .isInstanceOf(CommandExecutionException.class)
        .cause()
        .isInstanceOf(NoSuchMethodException.class);
  }
}
