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
package com.arcadedb.query.sql.function.misc;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for SQLFunctionIfNull
 */
public class SQLFunctionIfNullTest extends TestHelper {

  @Test
  public void testFunctionInstance() {
    SQLFunctionIfNull function = new SQLFunctionIfNull();

    assertThat(function.getName()).isEqualTo("ifnull");
    assertThat(function.getSyntax()).isEqualTo(
        "Syntax error: ifnull(<field|value>, <return_value_if_null> [,<return_value_if_not_null>])");
  }

  @Test
  public void testDirectExecution() {
    SQLFunctionIfNull function = new SQLFunctionIfNull();

    // Case 1: Non-null value with 2 parameters
    Object result1 = function.execute(null, null, null, new Object[] { "value", "defaultValue" }, null);
    assertThat(result1).isEqualTo("value");

    // Case 2: Null value with 2 parameters
    Object result2 = function.execute(null, null, null, new Object[] { null, "defaultValue" }, null);
    assertThat(result2).isEqualTo("defaultValue");

    // Case 3: Non-null value with 3 parameters
    Object result3 = function.execute(null, null, null, new Object[] { "value", "defaultValue", "alternativeValue" }, null);
    assertThat(result3).isEqualTo("alternativeValue");

    // Case 4: Null value with 3 parameters
    Object result4 = function.execute(null, null, null, new Object[] { null, "defaultValue", "alternativeValue" }, null);
    assertThat(result4).isEqualTo("defaultValue");
  }

  @Test
  public void testSQLQueryWithIfNull() {
    database.transaction(() -> {
      database.command("sql", "create document type TestType");
      database.command("sql", "insert into TestType content {'name': 'Test1', 'description': 'Description 1'}");
      database.command("sql", "insert into TestType content {'name': 'Test2', 'description': null}");
      database.command("sql", "insert into TestType content {'name': 'Test3'}");

      // Basic usage with two parameters
      ResultSet result = database.query("sql",
          "select name, ifnull(description, 'No description') as description from TestType order by name");

      // First record has a description
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("description")).isEqualTo("Description 1");

      // Second record has null description
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("description")).isEqualTo("No description");

      // Third record doesn't have description field
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("description")).isEqualTo("No description");

      assertThat(result.hasNext()).isFalse();

      // Test with three parameters
      result = database.query("sql",
          "select name, ifnull(description, 'No description', 'Has description') as description from TestType order by name");

      // First record has a description
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("description")).isEqualTo("Has description");

      // Second record has null description
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("description")).isEqualTo("No description");

      // Third record doesn't have description field
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("description")).isEqualTo("No description");

      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  public void testNestedIfNullInSQL() {
    database.transaction(() -> {
      database.command("sql", "create document type TestNested");
      database.command("sql", "insert into TestNested content {'name': 'Test1', 'field1': 'Value1', 'field2': 'Value2'}");
      database.command("sql", "insert into TestNested content {'name': 'Test2', 'field1': null, 'field2': 'Value2'}");
      database.command("sql", "insert into TestNested content {'name': 'Test3', 'field1': null, 'field2': null}");

      // Test nested ifnull calls: first try field1, then field2, then default
      ResultSet result = database.query("sql",
          "select name, ifnull(field1, ifnull(field2, 'No values')) as value from TestNested order by name");

      // First record has field1
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("value")).isEqualTo("Value1");

      // Second record uses field2 as fallback
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("value")).isEqualTo("Value2");

      // Third record uses default value
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("value")).isEqualTo("No values");

      assertThat(result.hasNext()).isFalse();
    });
  }
}
