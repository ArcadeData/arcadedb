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

  private void registerClass() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("math", "com.arcadedb.function.java.JavaFunctionTest$Sum"));
  }
}
