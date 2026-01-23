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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #883: SQL field access for sub-query in projection.
 * The query `SELECT (SELECT name FROM doc).name;` should work, allowing direct
 * field access on the result of a parenthesized sub-query.
 */
class SubQueryFieldAccessTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.name STRING");
      database.command("sql", "INSERT INTO doc SET name = 'test'");
    });
  }

  /**
   * Tests direct field access on a sub-query result.
   * Issue #883: SELECT (SELECT name FROM doc).name; should return ["test"]
   */
  @Test
  void testSubQueryFieldAccess() {
    database.transaction(() -> {
      // This is the problematic query from issue #883
      // Use explicit alias for cleaner property access
      ResultSet rs = database.query("sql", "SELECT (SELECT name FROM doc).name AS extractedName");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      // The subquery returns a list of results, accessing .name should extract
      // the name field from each result
      Object value = result.getProperty("extractedName");
      assertThat(value).isNotNull();
      assertThat(value).isInstanceOf(List.class);
      @SuppressWarnings("unchecked")
      List<String> list = (List<String>) value;
      assertThat(list).containsExactly("test");
      rs.close();
    });
  }

  /**
   * Tests that the workaround with coalesce still works.
   */
  @Test
  void testSubQueryFieldAccessWithCoalesce() {
    database.transaction(() -> {
      // Workaround that currently works - use explicit alias
      ResultSet rs = database.query("sql", "SELECT coalesce((SELECT name FROM doc)).name AS extractedName");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      Object value = result.getProperty("extractedName");
      assertThat(value).isNotNull();
      rs.close();
    });
  }

  /**
   * Tests that the workaround with LET clause still works.
   */
  @Test
  void testSubQueryFieldAccessWithLet() {
    database.transaction(() -> {
      // Workaround that currently works - use explicit alias
      ResultSet rs = database.query("sql", "SELECT $temp.name AS extractedName LET $temp = (SELECT name FROM doc)");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      Object value = result.getProperty("extractedName");
      assertThat(value).isNotNull();
      rs.close();
    });
  }

  /**
   * Tests field access on nested sub-queries.
   */
  @Test
  void testNestedSubQueryFieldAccess() {
    database.transaction(() -> {
      // Add more complex test data
      database.command("sql", "INSERT INTO doc SET name = 'nested'");

      // Nested field access
      ResultSet rs = database.query("sql", "SELECT (SELECT name FROM doc WHERE name = 'test').name AS testName");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      Object value = result.getProperty("testName");
      assertThat(value).isNotNull();
      assertThat(value).isInstanceOf(List.class);
      @SuppressWarnings("unchecked")
      List<String> list = (List<String>) value;
      assertThat(list).containsExactly("test");
      rs.close();
    });
  }

  /**
   * Tests array access on sub-query results.
   */
  @Test
  void testSubQueryArrayAccess() {
    database.transaction(() -> {
      // Array access on sub-query result - use explicit alias
      ResultSet rs = database.query("sql", "SELECT (SELECT name FROM doc)[0].name AS firstName");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      Object value = result.getProperty("firstName");
      // First element's name field
      assertThat(value).isNotNull();
      assertThat(value).isEqualTo("test");
      rs.close();
    });
  }

  /**
   * Tests method call on sub-query results.
   */
  @Test
  void testSubQueryMethodCall() {
    database.transaction(() -> {
      // Method call on sub-query result (size) - use explicit alias
      ResultSet rs = database.query("sql", "SELECT (SELECT name FROM doc).size() AS resultSize");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      Object value = result.getProperty("resultSize");
      assertThat(value).isNotNull();
      assertThat(value).isEqualTo(1);
      rs.close();
    });
  }
}
