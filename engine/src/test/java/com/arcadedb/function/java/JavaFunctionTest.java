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
package com.arcadedb.function.java;

import com.arcadedb.TestHelper;
import com.arcadedb.function.FunctionExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JavaFunctionTest extends TestHelper {

  public static class Sum {
    public int sum(final int a, final int b) {
      return a + b;
    }

    public static int SUM(final int a, final int b) {
      return a + b;
    }

    public static int boom() {
      throw new IllegalStateException("boom from target method");
    }
  }

  public static class Overloaded {
    public static String format(final String s) {
      return "one:" + s;
    }

    public static String format(final String s, final int repeat) {
      return "two:" + s.repeat(repeat);
    }

    public static String format(final int n) {
      return "int:" + n;
    }
  }

  public static class AmbiguousOverloaded {
    public static String describe(final Object o) {
      return "object:" + o;
    }

    public static String describe(final String s) {
      return "string:" + s;
    }
  }

  public interface Formatter<T> {
    String format(T value);
  }

  public static class StringFormatter implements Formatter<String> {
    @Override
    public String format(final String value) {
      return "fmt:" + value;
    }
  }

  public static class MixedFixedAndVarargs {
    public static String format(final Integer a, final Integer b) {
      return "ints:" + (a + b);
    }

    public static String format(final String sep, final String... parts) {
      return "strs:" + String.join(sep, parts);
    }
  }

  public static class SingleVarargs {
    public static String join(final String sep, final String... parts) {
      return String.join(sep, parts);
    }
  }

  public static class NumericVarargsOverloaded {
    public static String describe(final long... values) {
      return "longs:" + values.length;
    }

    public static String describe(final String... values) {
      return "strings:" + values.length;
    }
  }

  public static class WidthOverloaded {
    public static long twice(final long v) {
      return v * 2;
    }

    public static int twice(final int v) {
      return v * 2;
    }

    public static String scale(final long v) {
      return "long:" + v;
    }

    public static String scale(final double v) {
      return "double:" + v;
    }

    public static String pair(final long a, final double b) {
      return "long-double";
    }

    public static String pair(final double a, final long b) {
      return "double-long";
    }
  }

  public static class VarargsOverloaded {
    public static String join(final String sep, final String... parts) {
      return "strs:" + String.join(sep, parts);
    }

    public static String join(final String sep, final Integer... parts) {
      final StringBuilder sb = new StringBuilder();
      for (int i = 0; i < parts.length; i++) {
        if (i > 0)
          sb.append(sep);
        sb.append(parts[i]);
      }
      return "ints:" + sb;
    }
  }

  @Test
  void registration()
    throws Exception {
    // TEST REGISTRATION HERE
    registerClass();

    assertThatThrownBy(() -> registerClass()).isInstanceOf(IllegalArgumentException.class);

    database.getSchema().unregisterFunctionLibrary("math");
    registerClass();
  }

  @Test
  void registrationByClassInstance()
    throws Exception {
    // TEST REGISTRATION HERE
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class));

    assertThatThrownBy(() -> database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class))).isInstanceOf(IllegalArgumentException.class);

    database.getSchema().unregisterFunctionLibrary("math");
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class));
  }

  @Test
  void registrationSingleMethods()
    throws Exception {
    // TEST REGISTRATION HERE
    database.getSchema()
            .registerFunctionLibrary(new JavaMethodFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class.getMethod("sum", Integer.TYPE, Integer.TYPE)));

    assertThatThrownBy(() -> database.getSchema()
      .registerFunctionLibrary(new JavaMethodFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class.getMethod("sum", Integer.TYPE, Integer.TYPE)))).isInstanceOf(IllegalArgumentException.class);

    database.getSchema().unregisterFunctionLibrary("math");
    database.getSchema()
            .registerFunctionLibrary(new JavaMethodFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class.getMethod("sum", Integer.TYPE, Integer.TYPE)));
  }

  public static class Counter {
    private int count = 0;

    public int next() {
      return ++count;
    }
  }

  /**
   * Issue #7046: {@link JavaMethodFunctionDefinition} refuses a non-static method registered without an instance
   * rather than instantiating its declaring class behind the registration site's back.
   */
  @Test
  void nonStaticMethodWithoutAnInstanceIsRejected() throws Exception {
    final Method next = Counter.class.getMethod("next");

    assertThatThrownBy(() -> new JavaMethodFunctionDefinition(next))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not static");
    assertThatThrownBy(() -> new JavaMethodFunctionDefinition(null, next))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not static");
    assertThatThrownBy(() -> new JavaMethodFunctionDefinition(null, List.of(next)))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not static");
    assertThatThrownBy(() -> new JavaMethodFunctionLibraryDefinition("counter", null, next))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not static");
  }

  /** Issue #7046: the instance a registration supplies is the one the method is invoked on, and it keeps its state. */
  @Test
  void nonStaticMethodIsInvokedOnTheSuppliedInstance() throws Exception {
    final Counter counter = new Counter();
    database.getSchema().registerFunctionLibrary(
        new JavaMethodFunctionLibraryDefinition("counter", counter, Counter.class.getMethod("next")));

    assertThat(database.getSchema().getFunction("counter", "next").execute()).isEqualTo(1);
    assertThat(database.getSchema().getFunction("counter", "next").execute()).isEqualTo(2);
    assertThat(counter.count).isEqualTo(2);
  }

  /**
   * Issue #7046: the instance-less library form still serves a non-static method, but the instance it invokes it on
   * is created by the library - the registration site - and not by the function definition.
   */
  @Test
  void instanceLessLibraryFormInstantiatesAtTheRegistrationSite() throws Exception {
    final JavaMethodFunctionLibraryDefinition library = new JavaMethodFunctionLibraryDefinition("counter",
        Counter.class.getMethod("next"));
    assertThat(library.getFunction("next").execute()).isEqualTo(1);
    assertThat(library.getFunction("next").execute()).isEqualTo(2);

    // Each library gets an instance of its own, so two registrations of the same method never share state.
    final JavaMethodFunctionLibraryDefinition another = new JavaMethodFunctionLibraryDefinition("counter2",
        Counter.class.getMethod("next"));
    assertThat(another.getFunction("next").execute()).isEqualTo(1);
  }

  /** A static method needs no instance, and one passed anyway is neither required nor an error. */
  @Test
  void staticMethodNeedsNoInstance() throws Exception {
    final Method boom = Sum.class.getMethod("boom");
    assertThat(new JavaMethodFunctionDefinition(boom).getName()).contains("boom");
    assertThat(new JavaMethodFunctionDefinition(new Sum(), boom).getName()).contains("boom");
  }

  @Test
  void functionNotFound() {
    assertThatThrownBy(() -> database.getSchema().getFunction("math", "sum")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void methodParameterByPosition()
    throws Exception {
    // TEST REGISTRATION HERE
    registerClass();

    final Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);
  }

  @Test
  void staticMethodParameterByPosition()
    throws Exception {
    registerClass();

    final Integer result = (Integer) database.getSchema().getFunction("math", "SUM").execute(3, 5);
    assertThat(result).isEqualTo(8);
  }

  @Test
  void executeFromSQL()
    throws Exception {
    registerClass();

    database.transaction(() -> {
      final ResultSet rs = database.command("SQL", "SELECT `math.sum`(20,7) as sum");
      assertThat(rs.hasNext()).isTrue();
      final Result record = rs.next();
      assertThat(record).isNotNull();
      assertThat(record.getIdentity()).isNotPresent();
      assertThat(((Number) record.getProperty("sum")).intValue()).isEqualTo(27);
    });
  }

  @Test
  void notFound() throws Exception {
    registerClass();
    assertThatThrownBy(() -> database.getSchema().getFunction("math", "NOT_found").execute(3, 5)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void executionError()
    throws Exception {
    registerClass();
    assertThatThrownBy(() -> database.getSchema().getFunction("math", "SUM").execute("invalid", 5)).isInstanceOf(FunctionExecutionException.class);
  }

  @Test
  void wrongParameterCount()
    throws Exception {
    registerClass();
    assertThatThrownBy(() -> database.getSchema().getFunction("math", "sum").execute(3))
        .isInstanceOf(FunctionExecutionException.class)
        .hasMessageContaining("expected 2")
        .hasMessageContaining("received 1");
  }

  @Test
  void targetExceptionCausePreserved()
    throws Exception {
    registerClass();
    assertThatThrownBy(() -> database.getSchema().getFunction("math", "boom").execute())
        .isInstanceOf(FunctionExecutionException.class)
        .hasRootCauseInstanceOf(IllegalStateException.class)
        .hasRootCauseMessage("boom from target method");
  }

  @Test
  void overloadsAreAllKeptAndDispatchedByArgumentCountAndType()
    throws Exception {
    // issue #7007: overloaded public methods used to collapse to whichever one Class.getDeclaredMethods() happened
    // to return last, non-deterministically. All overloads must survive registration and be dispatched by the
    // actual arguments passed at call time.
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("fmt", JavaFunctionTest.Overloaded.class));
    try {
      final var function = database.getSchema().getFunction("fmt", "format");

      assertThat(function.execute("x")).isEqualTo("one:x");
      assertThat(function.execute("ab", 3)).isEqualTo("two:ababab");
      assertThat(function.execute(7)).isEqualTo("int:7");
    } finally {
      database.getSchema().unregisterFunctionLibrary("fmt");
    }
  }

  @Test
  void overloadWithNoMatchingParameterCountThrows()
    throws Exception {
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("fmt", JavaFunctionTest.Overloaded.class));
    try {
      final var function = database.getSchema().getFunction("fmt", "format");
      assertThatThrownBy(() -> function.execute("a", "b", "c")).isInstanceOf(FunctionExecutionException.class);
    } finally {
      database.getSchema().unregisterFunctionLibrary("fmt");
    }
  }

  @Test
  void overloadedVarargsMethodsAreDispatchedByArgumentType()
    throws Exception {
    // Regression for a gap the overload-dispatch fix (issue #7007) left open: when candidatesByParameterCount()
    // returns only varargs candidates, disambiguateByArgumentType() must still be able to pick among them by the
    // type of the trailing (vararg) arguments instead of unconditionally rejecting the call as ambiguous.
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("vjoin", JavaFunctionTest.VarargsOverloaded.class));
    try {
      final var function = database.getSchema().getFunction("vjoin", "join");

      assertThat(function.execute("-", "a", "b", "c")).isEqualTo("strs:a-b-c");
      assertThat(function.execute("-", 1, 2, 3)).isEqualTo("ints:1-2-3");
    } finally {
      database.getSchema().unregisterFunctionLibrary("vjoin");
    }
  }

  @Test
  void genuinelyAmbiguousOverloadThrows()
    throws Exception {
    // Documents the deliberate, javadoc'd tradeoff: unlike the Java compiler, dispatch here does not rank
    // overloads by specificity, so a String argument matching both describe(Object) and describe(String) is
    // rejected rather than silently resolved to the more specific overload.
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("amb", JavaFunctionTest.AmbiguousOverloaded.class));
    try {
      final var function = database.getSchema().getFunction("amb", "describe");
      assertThatThrownBy(() -> function.execute("x"))
          .isInstanceOf(FunctionExecutionException.class)
          .hasMessageContaining("cannot resolve which overload");
    } finally {
      database.getSchema().unregisterFunctionLibrary("amb");
    }
  }

  @Test
  void bridgeMethodsAreExcludedFromOverloadRegistration()
    throws Exception {
    // A generic interface override - Formatter<String>.format(String) implemented by StringFormatter - makes the
    // compiler synthesize a public bridge method format(Object) on StringFormatter. Without excluding
    // bridge/synthetic methods, that bridge would be grouped as a second "format" overload, and every call would be
    // rejected as ambiguous between the real method and its own compiler-generated bridge.
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("bridge", JavaFunctionTest.StringFormatter.class));
    try {
      final var function = database.getSchema().getFunction("bridge", "format");
      assertThat(function.execute("x")).isEqualTo("fmt:x");
    } finally {
      database.getSchema().unregisterFunctionLibrary("bridge");
    }
  }

  @Test
  void fixedArityOverloadRejectedByTypeFallsBackToVarargsOverload()
    throws Exception {
    // Regression: candidatesByParameterCount() used to let an exact-arity, non-varargs match with the right
    // parameter count win outright, even when its parameter types did not accept the arguments and a type-compatible
    // varargs overload existed. format(Integer,Integer) and format(String,String...) both accept 2 arguments; a
    // call with two Strings must fall back to the varargs overload instead of failing against the mismatched one.
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("mixed", JavaFunctionTest.MixedFixedAndVarargs.class));
    try {
      final var function = database.getSchema().getFunction("mixed", "format");

      assertThat(function.execute(3, 4)).isEqualTo("ints:7");
      assertThat(function.execute("-", "a", "b")).isEqualTo("strs:a-b");
      // Two arguments: matches format(Integer,Integer) by count but not by type, and format(String,String...) by
      // both count and type (sep="x", one vararg element "y") - must fall back to the varargs overload.
      assertThat(function.execute("x", "y")).isEqualTo("strs:y");
    } finally {
      database.getSchema().unregisterFunctionLibrary("mixed");
    }
  }

  @Test
  void singleNonOverloadedVarargsMethodIsInvokedCorrectly()
    throws Exception {
    // Pins the toInvokeArgs() vararg-packing fix independently of overload disambiguation: a single, non-overloaded
    // varargs method takes the candidates.size() == 1 short-circuit path in execute(), which must still pack the
    // flat, positionally-passed arguments into the array Method.invoke() requires.
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("sjoin", JavaFunctionTest.SingleVarargs.class));
    try {
      final var function = database.getSchema().getFunction("sjoin", "join");
      assertThat(function.execute("-", "a", "b", "c")).isEqualTo("a-b-c");
    } finally {
      database.getSchema().unregisterFunctionLibrary("sjoin");
    }
  }

  @Test
  void primitiveWideningIsAcceptedDuringOverloadSelection()
    throws Exception {
    // typeMatches() used to accept only the exact wrapper type for a primitive parameter, so an Integer argument
    // was rejected for a `long` parameter even though Method.invoke() itself accepts the unboxing+widening
    // conversion. A call that should widen into the numeric overload must not be swallowed by an unrelated one.
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("wide", JavaFunctionTest.NumericVarargsOverloaded.class));
    try {
      final var function = database.getSchema().getFunction("wide", "describe");
      assertThat(function.execute(1)).isEqualTo("longs:1");
    } finally {
      database.getSchema().unregisterFunctionLibrary("wide");
    }
  }

  @Test
  void exactPrimitiveMatchBeatsWideningOverload()
    throws Exception {
    // Regression for issue #7110: primitive widening (issue #7007) made an Integer argument applicable to both
    // twice(int) and twice(long), and the ambiguity check then refused a call the pre-widening exact-match dispatch
    // resolved without trouble. As in Java, the exact primitive match must win over the widened one.
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("width", JavaFunctionTest.WidthOverloaded.class));
    try {
      final var twice = database.getSchema().getFunction("width", "twice");
      assertThat(twice.execute(5)).isEqualTo(10);
      assertThat(twice.execute(5L)).isEqualTo(10L);

      // Both scale(long) and scale(double) only apply by widening: long is the narrower target, so it is the most
      // specific applicable overload, again as javac would resolve it.
      final var scale = database.getSchema().getFunction("width", "scale");
      assertThat(scale.execute(1)).isEqualTo("long:1");
      assertThat(scale.execute(1.5f)).isEqualTo("double:1.5");

      // Neither pair(long,double) nor pair(double,long) is more specific than the other for two ints: still ambiguous.
      final var pair = database.getSchema().getFunction("width", "pair");
      assertThatThrownBy(() -> pair.execute(1, 2))
          .isInstanceOf(FunctionExecutionException.class)
          .hasMessageContaining("cannot resolve which overload");
      assertThat(pair.execute(1L, 2.0)).isEqualTo("long-double");
    } finally {
      database.getSchema().unregisterFunctionLibrary("width");
    }
  }

  @Test
  void widthOverloadsAreCallableFromSql()
    throws Exception {
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("width", JavaFunctionTest.WidthOverloaded.class));
    try (final ResultSet rs = database.query("sql", "SELECT `width.twice`(5) AS v")) {
      assertThat(rs.next().<Number>getProperty("v").intValue()).isEqualTo(10);
    } finally {
      database.getSchema().unregisterFunctionLibrary("width");
    }
  }

  @Test
  void prePackedVarargsArrayIsDispatchedAmongMultipleVarargsOverloads()
    throws Exception {
    // Regression for a narrower gap in the varargs-dispatch fix: when the vararg part is passed pre-packed as a
    // single array (the shape Method.invoke() itself requires) rather than as flat elements, type matching against
    // more than one varargs candidate must compare the array itself to the candidate's vararg array type instead of
    // (incorrectly) treating the array object as one flat vararg element.
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("vjoin2", JavaFunctionTest.VarargsOverloaded.class));
    try {
      final var function = database.getSchema().getFunction("vjoin2", "join");
      assertThat(function.execute("-", new String[] { "a", "b" })).isEqualTo("strs:a-b");
      assertThat(function.execute("-", new Integer[] { 1, 2 })).isEqualTo("ints:1-2");
    } finally {
      database.getSchema().unregisterFunctionLibrary("vjoin2");
    }
  }

  private void registerClass() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("math", "com.arcadedb.function.java.JavaFunctionTest$Sum"));
  }
}
