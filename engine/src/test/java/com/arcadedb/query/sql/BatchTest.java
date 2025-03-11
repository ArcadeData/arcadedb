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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class BatchTest extends TestHelper {
  @Test
  public void testReturnArrayOnDeprecated() {
    database.transaction(() -> {
      final ResultSet rs = database.command("SQLSCRIPT", """
          let a = select 1 as result;
          let b = select 2 as result;
          return [$a,$b];""");

      assertThat(rs.hasNext()).isTrue();
      Result record = rs.next();
      assertThat(record).isNotNull();
      assertThat(record.toJSON().toString()).isEqualTo("""
          {"value":[{"result":1}]}""");

      record = rs.next();
      assertThat(record.toJSON().toString()).isEqualTo("""
          {"value":[{"result":2}]}""");
      assertThat(record).isNotNull();

      assertThat(rs.hasNext()).isFalse();
    });
  }

  @Test
  public void testReturnArray() {
    database.transaction(() -> {
      final ResultSet rs = database.command("SQLScript", """
          let a = select 1 as result;
          let b = select 2 as result;
          return [$a,$b];""");

      assertThat(rs.hasNext()).isTrue();
      Result record = rs.next();
      assertThat(record).isNotNull();
      assertThat(record.toJSON().toString()).isEqualTo("""
          {"value":[{"result":1}]}""");

      record = rs.next();
      assertThat(record.toJSON().toString()).isEqualTo("{\"value\":[{\"result\":2}]}");
      assertThat(record).isNotNull();

      assertThat(rs.hasNext()).isFalse();
    });
  }

  @Test
  public void testWhile() {
    database.command("sql", "CREATE DOCUMENT TYPE TestWhile");

    database.command("sqlscript", """
        BEGIN;
        LET $i = 0;
        WHILE ($i < 10){
          INSERT INTO TestWhile SET id = $i;
          LET $i = $i + 1;
        }
        COMMIT;""");

    final ResultSet result = database.query("sql", "select from TestWhile order by id");
    for (int i = 0; i < 10; i++) {
      final Result record = result.next();
      assertThat((int) record.getProperty("id")).isEqualTo(i);
    }
  }

  @Test
  public void testWhileWithReturn() {
    database.command("sql", "CREATE DOCUMENT TYPE TestWhileWithReturn");

    database.transaction(() -> {

      database.command("sqlscript", """
          LET $i = 0;
          WHILE ($i < 10){
            INSERT INTO TestWhileWithReturn SET id = $i;
            IF ($i = 4) {
              RETURN;
            }
            LET $i = $i + 1;
          }""");
    });

    final ResultSet result = database.query("sql", "select from TestWhileWithReturn order by id");
    for (int i = 0; i < 5; i++) {
      final Result record = result.next();
      assertThat((int) record.getProperty("id")).isEqualTo(i);
    }
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  public void testForeach() {
    database.command("sql", "CREATE DOCUMENT TYPE TestForeach");

    database.command("sqlscript", """
        BEGIN;
        FOREACH ($i IN [1, 2, 3]){
          INSERT INTO TestForeach SET id = $i;
        }
        COMMIT;""");

    final ResultSet result = database.query("sql", "select from TestForeach order by id");
    for (int i = 1; i <= 3; i++) {
      final Result record = result.next();
      assertThat((int) record.getProperty("id")).isEqualTo(i);
    }
  }

  @Test
  public void testForeachWithReturn() {
    database.command("sql", "CREATE DOCUMENT TYPE TestForeachWithReturn");

    database.transaction(() -> {
      String script = """
          FOREACH ($i IN [1, 2, 3]){
            INSERT INTO TestForeachWithReturn SET id = $i;
            IF ($i = 1) {
              RETURN;
            }
          }""";

      database.command("sqlscript", script);
    });

    final ResultSet result = database.query("sql", "select from TestForeachWithReturn order by id");
    for (int i = 1; i <= 1; i++) {
      final Result record = result.next();
      assertThat((int) record.getProperty("id")).isEqualTo(i);
    }
    assertThat(result.hasNext()).isFalse();
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/1646
   */
  @Test
  public void testLetUSeRightScope() {

    final ResultSet result = database.command("sqlscript", """
        LET $list = [];

        FOREACH ($i IN [1, 2, 3]) {
            IF ($i = 3) {
                LET $list = ['HELLO'];
            }
        }

        IF ($list.size() > 0) {
          RETURN "List element detected";
        }

        RETURN "List is empty";
        """);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("value")).isEqualTo("List element detected");
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/1647
   */
  @Test
  public void testBreakInsideForeach() {

    final ResultSet result = database.command("sqlscript", """
        LET result = "Return statement 0";
        FOREACH ($i IN [1, 2, 3]) {
        	LET result = "Return statement " + $i;
        	IF( $i = 2 ) {
        		BREAK;
        	}
        }

        RETURN $result;
        """);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("value")).isEqualTo("Return statement 2");
  }

  // Isue https://github.com/ArcadeData/arcadedb/issues/1673
  @Test
  public void testNestedBreak() {

    final ResultSet result = database.command("sqlscript", """
        LET $numbers = [1, 2, 3];
        LET $letters = ['A', 'B', 'C'];

        LET $counter = 0;

        FOREACH ($number IN $numbers) {
          FOREACH ($letter IN $letters) {
            IF ($number = 2) {
              IF ($letter = 'B') {
                BREAK;
              }
              IF ($letter = 'B') {
                CONSOLE.`error` map('ERROR', 'THIS SHOULD NEVER HAPPEN!!!');
              }
            }
            LET counter = $counter + 1;
          }
        }

        RETURN $counter;
        """);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(7);
  }

  @Test
  public void testForeachResultSet() {
    database.command("sql", "CREATE DOCUMENT TYPE DocumentType");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.command("sql", "INSERT INTO DocumentType set a = " + i);
    });

    final ResultSet result = database.command("sqlscript", """
        LET counter = 0;
        FOREACH( $row IN (select from DocumentType) ) {
          LET counter = $counter + 1;
        }

        RETURN $counter;
        """);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(100);
  }

  @Test
  public void testUsingReservedVariableNames() {
    try {
      database.command("sqlscript", """
          FOREACH ($parent IN [1, 2, 3]){
          RETURN;
          }""");
      fail("");
    } catch (CommandSQLParsingException e) {
      // EXPECTED
    }

    try {
      database.command("sqlscript", "LET parent = 33;");
      fail("");
    } catch (CommandSQLParsingException e) {
      // EXPECTED
    }
  }
}
