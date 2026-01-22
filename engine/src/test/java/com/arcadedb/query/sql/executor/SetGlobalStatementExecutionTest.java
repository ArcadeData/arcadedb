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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for SET GLOBAL statement.
 */
class SetGlobalStatementExecutionTest extends TestHelper {

  @Test
  void testSetGlobalString() {
    database.command("sql", "SET GLOBAL myVar = 'hello'");

    final ResultSet result = database.query("sql", "SELECT $myVar as value");
    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("value")).isEqualTo("hello");
    result.close();
  }

  @Test
  void testSetGlobalNumber() {
    database.command("sql", "SET GLOBAL counter = 42");

    final ResultSet result = database.query("sql", "SELECT $counter as value");
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(42);
    result.close();
  }

  @Test
  void testSetGlobalBoolean() {
    database.command("sql", "SET GLOBAL flag = true");

    final ResultSet result = database.query("sql", "SELECT $flag as value");
    assertThat(result.hasNext()).isTrue();
    assertThat((Boolean) result.next().getProperty("value")).isEqualTo(true);
    result.close();
  }

  @Test
  void testSetGlobalMap() {
    database.command("sql", "SET GLOBAL config = {\"key\": \"value\", \"num\": 123}");

    final ResultSet result = database.query("sql", "SELECT $config as value");
    assertThat(result.hasNext()).isTrue();
    final Object config = result.next().getProperty("value");
    assertThat(config).isInstanceOf(Map.class);
    result.close();
  }

  @Test
  void testSetGlobalNull() {
    database.command("sql", "SET GLOBAL myVar = 'test'");
    assertThat(((DatabaseInternal) database).getGlobalVariable("myVar")).isEqualTo("test");

    database.command("sql", "SET GLOBAL myVar = NULL");
    assertThat(((DatabaseInternal) database).getGlobalVariable("myVar")).isNull();
  }

  @Test
  void testLocalVariablePrecedence() {
    database.command("sql", "SET GLOBAL myVar = 'global'");

    // Local LET variable should take precedence
    final ResultSet result = database.query("sql",
        "SELECT $myVar as value LET $myVar = 'local'");
    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("value")).isEqualTo("local");
    result.close();

    // Global should still be accessible without local override
    final ResultSet result2 = database.query("sql", "SELECT $myVar as value");
    assertThat(result2.hasNext()).isTrue();
    assertThat((String) result2.next().getProperty("value")).isEqualTo("global");
    result2.close();
  }

  @Test
  void testReservedVariableNames() {
    assertThatThrownBy(() -> database.command("sql", "SET GLOBAL parent = 'test'"))
        .hasMessageContaining("reserved");

    assertThatThrownBy(() -> database.command("sql", "SET GLOBAL current = 'test'"))
        .hasMessageContaining("reserved");
  }

  @Test
  void testCrossQueryPersistence() {
    // Set in first query
    database.command("sql", "SET GLOBAL sharedCounter = 100");

    // Access in separate query
    final ResultSet result1 = database.query("sql", "SELECT $sharedCounter as value");
    assertThat(result1.hasNext()).isTrue();
    assertThat((Integer) result1.next().getProperty("value")).isEqualTo(100);
    result1.close();

    // Modify in another query
    database.command("sql", "SET GLOBAL sharedCounter = 200");

    // Verify change persisted
    final ResultSet result2 = database.query("sql", "SELECT $sharedCounter as value");
    assertThat(result2.hasNext()).isTrue();
    assertThat((Integer) result2.next().getProperty("value")).isEqualTo(200);
    result2.close();
  }

  @Test
  void testGetGlobalVariables() {
    database.command("sql", "SET GLOBAL var1 = 'value1'");
    database.command("sql", "SET GLOBAL var2 = 'value2'");

    final Map<String, Object> globals = ((DatabaseInternal) database).getGlobalVariables();
    assertThat(globals).containsEntry("var1", "value1");
    assertThat(globals).containsEntry("var2", "value2");
  }

  @Test
  void testVariableNameWithDollarPrefix() {
    // Both with and without $ prefix should work
    database.command("sql", "SET GLOBAL $withDollar = 'test1'");
    database.command("sql", "SET GLOBAL noDollar = 'test2'");

    assertThat(((DatabaseInternal) database).getGlobalVariable("withDollar")).isEqualTo("test1");
    assertThat(((DatabaseInternal) database).getGlobalVariable("$withDollar")).isEqualTo("test1");
    assertThat(((DatabaseInternal) database).getGlobalVariable("noDollar")).isEqualTo("test2");
    assertThat(((DatabaseInternal) database).getGlobalVariable("$noDollar")).isEqualTo("test2");
  }

  @Test
  void testGlobalVariableInWhereClause() {
    database.getSchema().createDocumentType("TestDoc");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO TestDoc SET name = 'Alice', age = 30");
      database.command("sql", "INSERT INTO TestDoc SET name = 'Bob', age = 25");
      database.command("sql", "INSERT INTO TestDoc SET name = 'Charlie', age = 35");
    });

    database.command("sql", "SET GLOBAL minAge = 28");

    final ResultSet result = database.query("sql", "SELECT name FROM TestDoc WHERE age > $minAge ORDER BY name");
    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("name")).isEqualTo("Charlie");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }
}
