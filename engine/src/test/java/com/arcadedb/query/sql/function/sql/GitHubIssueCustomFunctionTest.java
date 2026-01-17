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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test cases from GitHub issue to verify custom function parameter issues are fixed.
 * This reproduces the exact scenarios from the bug report.
 */
class GitHubIssueCustomFunctionTest {

  @Test
  void testCase1_SQLFunctionWithStringParameter_BeerExample() throws Exception {
    TestHelper.executeInNewDatabase("testCase1", (db) -> {
      // Create the Beer type and add Hocus Pocus
      db.command("sql", "CREATE DOCUMENT TYPE Beer");
      db.command("sql", "INSERT INTO Beer SET name = 'Hocus Pocus'");
      db.command("sql", "INSERT INTO Beer SET name = 'Leffe'");

      // Define function as in the issue
      db.command("sql", "DEFINE FUNCTION my.getBeerId \"SELECT @rid AS result FROM `Beer` WHERE name=:a\" PARAMETERS [a] LANGUAGE sql");

      // Call the function - should NOT return null
      final ResultSet result = db.query("sql", "SELECT `my.getBeerId`('Hocus Pocus') as beerRid");
      assertThat(result.hasNext()).isTrue();

      final Object beerRid = result.next().getProperty("beerRid");
      assertThat(beerRid).isNotNull();
      // System.out.println("Case 1 - Beer RID: " + beerRid);
    });
  }

  @Test
  void testCase2_SQLFunctionReturningInput() throws Exception {
    TestHelper.executeInNewDatabase("testCase2", (db) -> {
      // Define function as in the issue
      db.command("sql", "DEFINE FUNCTION my.returnInput \"SELECT :a AS result\" PARAMETERS [a] LANGUAGE sql");

      // Call the function - should NOT return null
      final ResultSet result = db.query("sql", "SELECT `my.returnInput`('Hocus Pocus') as output");
      assertThat(result.hasNext()).isTrue();

      final Object output = result.next().getProperty("output");
      assertThat(output).isNotNull();
      assertThat(output.toString()).isEqualTo("Hocus Pocus");
      // System.out.println("Case 2 - Output: " + output);
    });
  }

  @Test
  void testCase3_JavaScriptFunctionReturningInput() throws Exception {
    TestHelper.executeInNewDatabase("testCase3", (db) -> {
      // Define JS function as in the issue - should NOT cause ClassCastException
      db.command("sql", "DEFINE FUNCTION my.returnInputJS \"return a\" PARAMETERS [a] LANGUAGE js");

      // Call the function
      final ResultSet result = db.query("sql", "SELECT `my.returnInputJS`('Hocus Pocus') as output");
      assertThat(result.hasNext()).isTrue();

      final Object output = result.next().getProperty("output");
      assertThat(output).isNotNull();
      assertThat(output.toString()).isEqualTo("Hocus Pocus");
      // System.out.println("Case 3 - Output: " + output);
    });
  }

  @Test
  void testIssueObservation_NoDoubleQuotingNeeded() throws Exception {
    TestHelper.executeInNewDatabase("testObservation", (db) -> {
      // Test that strings don't need to be double quoted
      db.command("sql", "DEFINE FUNCTION my.echo \"SELECT :a AS result\" PARAMETERS [a] LANGUAGE sql");

      // Single quotes should work fine (not double quotes)
      final ResultSet result = db.query("sql", "SELECT `my.echo`('Hello World') as output");
      assertThat(result.hasNext()).isTrue();

      final String output = result.next().getProperty("output");
      assertThat(output).isEqualTo("Hello World");
      // System.out.println("Observation - No double quoting needed: " + output);
    });
  }
}
