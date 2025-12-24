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
package com.arcadedb.query.sql.function.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test custom function parameter passing for both SQL and JavaScript functions.
 * This test addresses issues reported in GitHub issue about custom functions with arguments.
 */
class CustomFunctionParametersTest {

  @Test
  void testSQLFunctionWithStringParameter() throws Exception {
    TestHelper.executeInNewDatabase("testSQLFunctionWithStringParameter", (db) -> {
      // Create a type and add some test data
      db.command("sql", "CREATE DOCUMENT TYPE Beer");
      db.command("sql", "INSERT INTO Beer SET name = 'Hocus Pocus'");
      db.command("sql", "INSERT INTO Beer SET name = 'Leffe'");

      // Define a function that retrieves data using a string argument
      db.command("sql", "DEFINE FUNCTION my.getBeerId \"SELECT @rid AS result FROM `Beer` WHERE name=:a\" PARAMETERS [a] LANGUAGE sql");

      // Call the function with a string parameter
      final ResultSet result = db.query("sql", "SELECT `my.getBeerId`('Hocus Pocus') as beerRid");
      assertThat(result.hasNext()).isTrue();

      final Object beerRid = result.next().getProperty("beerRid");
      assertThat(beerRid).isNotNull();
      // The RID should be a valid RID format (e.g., #1:0)
      assertThat(beerRid.toString()).matches("#\\d+:\\d+");
    });
  }

  @Test
  void testSQLFunctionReturningInput() throws Exception {
    TestHelper.executeInNewDatabase("testSQLFunctionReturningInput", (db) -> {
      // Define a function that returns the input directly
      db.command("sql", "DEFINE FUNCTION my.returnInput \"SELECT :a AS result\" PARAMETERS [a] LANGUAGE sql");

      // Call the function with a string parameter
      final ResultSet result = db.query("sql", "SELECT `my.returnInput`('Hocus Pocus') as output");
      assertThat(result.hasNext()).isTrue();

      final String output = result.next().getProperty("output");
      assertThat(output).isEqualTo("Hocus Pocus");
    });
  }

  @Test
  void testSQLFunctionWithMultipleParameters() throws Exception {
    TestHelper.executeInNewDatabase("testSQLFunctionWithMultipleParameters", (db) -> {
      // Define a function that uses multiple parameters
      db.command("sql", "DEFINE FUNCTION my.add \"SELECT :a + :b AS result\" PARAMETERS [a, b] LANGUAGE sql");

      // Call the function with multiple numeric parameters
      final ResultSet result = db.query("sql", "SELECT `my.add`(10, 20) as output");
      assertThat(result.hasNext()).isTrue();

      final Integer output = result.next().getProperty("output");
      assertThat(output).isEqualTo(30);
    });
  }

  @Test
  void testJavaScriptFunctionReturningInput() throws Exception {
    TestHelper.executeInNewDatabase("testJavaScriptFunctionReturningInput", (db) -> {
      // Define a JavaScript function that returns the input
      db.command("sql", "DEFINE FUNCTION my.returnInputJS \"return a\" PARAMETERS [a] LANGUAGE js");

      // Call the function with a string parameter
      final ResultSet result = db.query("sql", "SELECT `my.returnInputJS`('Hocus Pocus') as output");
      assertThat(result.hasNext()).isTrue();

      final String output = result.next().getProperty("output");
      assertThat(output).isEqualTo("Hocus Pocus");
    });
  }

  @Test
  void testJavaScriptFunctionWithStringParameter() throws Exception {
    TestHelper.executeInNewDatabase("testJavaScriptFunctionWithStringParameter", (db) -> {
      // Define a JavaScript function that manipulates a string
      db.command("sql", "DEFINE FUNCTION my.uppercase \"return a.toUpperCase()\" PARAMETERS [a] LANGUAGE js");

      // Call the function with a string parameter
      final ResultSet result = db.query("sql", "SELECT `my.uppercase`('hello world') as output");
      assertThat(result.hasNext()).isTrue();

      final String output = result.next().getProperty("output");
      assertThat(output).isEqualTo("HELLO WORLD");
    });
  }

  @Test
  void testJavaScriptFunctionWithMultipleParameters() throws Exception {
    TestHelper.executeInNewDatabase("testJavaScriptFunctionWithMultipleParameters", (db) -> {
      // Define a JavaScript function with multiple parameters
      db.command("sql", "DEFINE FUNCTION my.add \"return a + b\" PARAMETERS [a, b] LANGUAGE js");

      // Call the function with numeric parameters
      final ResultSet result = db.query("sql", "SELECT `my.add`(10, 20) as output");
      assertThat(result.hasNext()).isTrue();

      final Integer output = result.next().getProperty("output");
      assertThat(output).isEqualTo(30);
    });
  }

  @Test
  void testJavaScriptFunctionWithStringContainingQuotes() throws Exception {
    TestHelper.executeInNewDatabase("testJavaScriptFunctionWithStringContainingQuotes", (db) -> {
      // Define a JavaScript function that handles strings
      db.command("sql", "DEFINE FUNCTION my.echo \"return a\" PARAMETERS [a] LANGUAGE js");

      // Call the function with a string containing special characters
      final ResultSet result = db.query("sql", "SELECT `my.echo`('It\\'s a \"test\"') as output");
      assertThat(result.hasNext()).isTrue();

      final String output = result.next().getProperty("output");
      assertThat(output).isEqualTo("It's a \"test\"");
    });
  }

  @Test
  void testSQLFunctionWithNumericParameter() throws Exception {
    TestHelper.executeInNewDatabase("testSQLFunctionWithNumericParameter", (db) -> {
      // Define a function that works with numbers
      db.command("sql", "DEFINE FUNCTION my.double \"SELECT :a * 2 AS result\" PARAMETERS [a] LANGUAGE sql");

      // Call the function with a numeric parameter
      final ResultSet result = db.query("sql", "SELECT `my.double`(21) as output");
      assertThat(result.hasNext()).isTrue();

      final Integer output = result.next().getProperty("output");
      assertThat(output).isEqualTo(42);
    });
  }

  @Test
  void testJavaScriptFunctionWithBooleanParameter() throws Exception {
    TestHelper.executeInNewDatabase("testJavaScriptFunctionWithBooleanParameter", (db) -> {
      // Define a JavaScript function that works with booleans
      db.command("sql", "DEFINE FUNCTION my.negate \"return !a\" PARAMETERS [a] LANGUAGE js");

      // Call the function with a boolean parameter
      final ResultSet result = db.query("sql", "SELECT `my.negate`(true) as output");
      assertThat(result.hasNext()).isTrue();

      final Boolean output = result.next().getProperty("output");
      assertThat(output).isFalse();
    });
  }
}
