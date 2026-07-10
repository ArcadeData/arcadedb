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
package com.arcadedb.function.polyglot;

import com.arcadedb.TestHelper;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PolyglotFunctionTest extends TestHelper {
  @Test
  void embeddedFunction() {
    registerFunctions();
    final Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);
  }

  @Test
  void reuseSameQueryEngine() {
    registerFunctions();

    Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);

    result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);
  }

  @Test
  void redefineFunction() {
    registerFunctions();

    Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(100, 50);
    assertThat(result).isEqualTo(150);

    assertThatThrownBy(() -> database.getSchema().getFunctionLibrary("math")
      .registerFunction(new JavascriptFunctionDefinition("sum", "return a - b;", "a", "b"))).isInstanceOf(IllegalArgumentException.class);

    database.getSchema().getFunctionLibrary("math").unregisterFunction("sum");
    database.getSchema().getFunctionLibrary("math")
        .registerFunction(new JavascriptFunctionDefinition("sum", "return a - b;", "a", "b"));

    result = (Integer) database.getSchema().getFunction("math", "sum").execute(50, 100);
    assertThat(result).isEqualTo(-50);
  }

  @Test
  void notFound()
    throws Exception {
    registerFunctions();
    assertThatThrownBy(() -> database.getSchema().getFunction("math", "NOT_found").execute(3, 5)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void executionError() {
    assertThatThrownBy(() -> {
      database.getSchema().registerFunctionLibrary(//
        new JavascriptFunctionLibraryDefinition(database, "math")//
          .registerFunction(new JavascriptFunctionDefinition("sum", "return a ++++ b;", "a", "b")));
      database.getSchema().getFunction("math", "sum").execute("invalid", 5);
    }).isInstanceOf(FunctionExecutionException.class);
  }

  @Test
  void jsonObjectAsInput() {
    database.command("sql", """
        DEFINE FUNCTION Test.objectComparison "return a.foo == 'bar'" PARAMETERS [a] LANGUAGE js;
        """);

    FunctionDefinition function = database.getSchema().getFunction("Test", "objectComparison");

    Boolean execute = (Boolean) function.execute(Map.of("foo", "bar"));
    assertThat(execute).isTrue();
  }

  @Test
  void stringObjectAsInput() {
    database.command("sql", """
        DEFINE FUNCTION Test.lowercase "return a.toLowerCase()" PARAMETERS [a] LANGUAGE js;
        """);

    FunctionDefinition function = database.getSchema().getFunction("Test", "lowercase");

    String execute = (String) function.execute("UPPERCASE");
    assertThat(execute).isEqualTo("uppercase");

    ResultSet resultSet = database.query("sql", """
        SELECT `Test.lowercase`('UPPERCASE') as lowercase;
        """);
    assertThat(resultSet.next().<String>getProperty("lowercase")).isEqualTo("uppercase");
  }

  @Test
  void jsInjectionPrevented() {
    database.command("sql", """
        DEFINE FUNCTION Test.identity "return a" PARAMETERS [a] LANGUAGE js;
        """);

    FunctionDefinition function = database.getSchema().getFunction("Test", "identity");

    // A string that the old looksLikeJson heuristic would have passed as JS source.
    // After the fix it must arrive in JS as a plain string, not as an object literal.
    final String injectionAttempt = "{a:1,b:2}";
    final Object result = function.execute(injectionAttempt);
    assertThat(result).isInstanceOf(String.class).isEqualTo(injectionAttempt);
  }

  private void registerFunctions() {
    database.getSchema().registerFunctionLibrary(//
        new JavascriptFunctionLibraryDefinition(database, "math")//
            .registerFunction(new JavascriptFunctionDefinition("sum", "return a + b;", "a", "b")));
  }
}
