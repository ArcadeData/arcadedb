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
import com.arcadedb.graph.Edge;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchTest extends TestHelper {
  @Test
  void returnArrayOnDeprecated() {
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
  void returnArray() {
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
  void testWhile() {
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
  void whileWithReturn() {
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
  void foreach() {
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
  void foreachWithReturn() {
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
  void letUSeRightScope() {

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
  void breakInsideForeach() {

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
  void nestedBreak() {

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
  void foreachResultSet() {
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
  void fromSingleResultReadValueFromField() {
    database.command("sql", "CREATE DOCUMENT TYPE DocumentType");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO DocumentType set field = 'aaaa' ");
    });

    final ResultSet result = database.command("sqlscript", """
        LET row = select from DocumentType;
        LET fieldValue = $row.field;
        RETURN $fieldValue;
        """);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("value")).isEqualTo("aaaa");
  }

  @Test
  void dynamicDocumentTypeName() {
    database.command("sql", "CREATE DOCUMENT TYPE TheDoc");

    database.transaction(() -> {
      database.command("sqlscript", """
          LET docType = 'TheDoc';
          LET d =INSERT INTO $docType SET id = 1;
          """);
    });

    assertThat(database.query("sql", "SELECT count() AS value FROM TheDoc").next().<Long>getProperty("value")).isEqualTo(1);
  }

  @Test
  void dynamicGraphTypesNames() {
    database.command("sql", "CREATE VERTEX TYPE V1");
    database.command("sql", "CREATE VERTEX TYPE V2");
    database.command("sql", "CREATE EDGE TYPE HasSource");

    database.transaction(() -> {
      database.command("sqlscript", """
          LET numbers = [1, 2, 3];
          LET vTypes = ['V1', 'V2'];
          FOREACH ($i IN $numbers) {
            FOREACH ($vType IN $vTypes) {
                 CREATE VERTEX $vType SET id = $i, vType = 'V2';
            }
          }
          """);
    });

    assertThat(database.query("sql", "SELECT count() AS value FROM V1").next().<Long>getProperty("value")).isEqualTo(3);
    assertThat(database.query("sql", "SELECT count() AS value FROM V2").next().<Long>getProperty("value")).isEqualTo(3);

    final ResultSet resultSet = database.command("sqlscript", """
        BEGIN;
        LET sources = SELECT FROM V1 WHERE id = '1';
        LET source = $sources[0];
        LET type = $source.vType;
        LET target = SELECT FROM $type WHERE id = '3';
        LET edgeType = 'HasSource';
        LET e = CREATE EDGE $edgeType FROM $target TO $source IF NOT EXISTS ;
        COMMIT;
        RETURN $e;
        """);

    assertThat(resultSet.hasNext()).isTrue();
    Edge edge = resultSet.next().getEdge().get();
    assertThat(edge.getInVertex().getInteger("id")).isEqualTo(1);
    assertThat(edge.getOutVertex().getInteger("id")).isEqualTo(3);

  }

  @Test
  void usingReservedVariableNames() {
    assertThatThrownBy(() -> database.command("sqlscript", """
        FOREACH ($parent IN [1, 2, 3]){
        RETURN;
        }""")).isInstanceOf(CommandSQLParsingException.class);

    assertThatThrownBy(() -> database.command("sqlscript", "LET parent = 33;")).isInstanceOf(CommandSQLParsingException.class);
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/1723
   * Math operators should work with LET variables containing ResultSet
   * When using $max + 1 where $max is a LET variable from a SELECT,
   * the arithmetic should work on the scalar value, not return a Java object reference string.
   */
  @Test
  void mathOperatorsWithResultSetVariable() {
    database.command("sql", "CREATE VERTEX TYPE Invoice");

    database.transaction(() -> {
      // Insert some test data
      database.command("sql", "INSERT INTO Invoice SET seqid = 10");
      database.command("sql", "INSERT INTO Invoice SET seqid = 20");
      database.command("sql", "INSERT INTO Invoice SET seqid = 30");

      // First verify the max query works as expected
      final ResultSet maxResult = database.query("sql", "SELECT max(seqid) as maxSeqId FROM Invoice");
      assertThat(maxResult.hasNext()).isTrue();
      final Result maxRow = maxResult.next();
      assertThat(((Number) maxRow.getProperty("maxSeqId")).intValue()).isEqualTo(30);

      // Test the workaround syntax first: $max[0].maxSeqId + 1
      final ResultSet workaroundResult = database.command("sqlscript", """
          LET $max = SELECT max(seqid) as maxSeqId FROM Invoice;
          LET $maxValue = $max[0].maxSeqId;
          RETURN $maxValue;
          """);

      assertThat(workaroundResult.hasNext()).isTrue();
      final Result workaroundRow = workaroundResult.next();
      final Object workaroundValue = workaroundRow.getProperty("value");
      // Value should be 30, the max(seqid)
      assertThat(workaroundValue).as("Workaround $max[0].maxSeqId should return 30").isNotNull();
      assertThat(workaroundValue).isInstanceOf(Number.class);
      assertThat(((Number) workaroundValue).intValue()).isEqualTo(30);

      // Now test with arithmetic (simpler version without transaction)
      final ResultSet result = database.command("sqlscript", """
          LET $max = SELECT max(seqid) as maxSeqId FROM Invoice;
          LET $nextId = $max[0].maxSeqId + 1;
          RETURN $nextId;
          """);

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      // The nextId should be 31 (30 + 1)
      final Object nextId = row.getProperty("value");
      assertThat(nextId).as("$max[0].maxSeqId + 1 should return 31").isInstanceOf(Number.class);
      assertThat(((Number) nextId).intValue()).isEqualTo(31);
    });
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/1723
   * Direct arithmetic on ResultSet variable (without explicit field access)
   * This tests the more convenient syntax that users expect to work.
   * <p>
   * When a ResultSet contains a single row with a single column (like SELECT max(...)),
   * arithmetic operations should automatically extract the scalar value.
   */
  @Test
  void mathOperatorsWithResultSetVariableDirect() {
    database.command("sql", "CREATE VERTEX TYPE Counter");

    database.transaction(() -> {
      // Insert initial data
      database.command("sql", "INSERT INTO Counter SET value = 100");

      // Test: When ResultSet has single row with single value, arithmetic should extract it automatically
      // This is the syntax users expect: $max + 1 where $max is SELECT max(value) FROM Counter
      final ResultSet result = database.command("sqlscript", """
          LET $max = SELECT max(value) FROM Counter;
          LET $computed = $max + 1;
          RETURN $computed;
          """);

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Object value = row.getProperty("value");

      // When the bug exists, value is a String like "com.arcadedb.query.sql.executor.InternalResultSet@..."
      // When fixed, value should be a Number (101)
      assertThat(value).isNotNull();
      assertThat(value).isInstanceOf(Number.class);
      assertThat(((Number) value).intValue()).isEqualTo(101);
    });
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/1723
   * Test the exact scenario from the issue report - creating vertex with auto-increment sequence.
   */
  @Test
  void mathOperatorsWithResultSetVariableExactIssue() {
    database.command("sql", "CREATE VERTEX TYPE Invoice");

    // Insert data in a separate committed transaction so it's visible to BEGIN/COMMIT blocks
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Invoice SET seqid = 5");
      database.command("sql", "INSERT INTO Invoice SET seqid = 10");
    });

    database.transaction(() -> {
      // Test without BEGIN/COMMIT first - this should work
      final ResultSet computedResult = database.command("sqlscript", """
          let $max = SELECT max(seqid) FROM Invoice;
          let $nextId = $max + 1;
          return $nextId;
          """);

      assertThat(computedResult.hasNext()).isTrue();
      final Result computedRow = computedResult.next();
      final Object computedValue = computedRow.getProperty("value");
      assertThat(computedValue).as("$max + 1 in LET context should be 11").isInstanceOf(Number.class);
      assertThat(((Number) computedValue).intValue()).isEqualTo(11);

      // Test with BEGIN/COMMIT - data is now committed, so this should also work
      final ResultSet result = database.command("sqlscript", """
          begin;
          let $max = SELECT max(seqid) FROM Invoice;
          let $nextId = $max + 1;
          let $invoice = CREATE VERTEX Invoice SET seqid = $nextId;
          commit;
          return $invoice;
          """);

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Object seqid = row.getProperty("seqid");

      // The seqid should be 11 (10 + 1), not a string like "com.arcadedb.query.sql.executor.InternalResultSet@..."
      assertThat(seqid).isNotNull();
      assertThat(seqid).isInstanceOf(Number.class);
      assertThat(((Number) seqid).intValue()).isEqualTo(11);
    });
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/2350
   * SELECT FROM variable containing nested property access with RID string
   */
  @Test
  void selectFromVariableWithNestedPropertyRidAccess() {
    database.command("sql", "CREATE DOCUMENT TYPE TestSelectFromNestedRid");

    database.transaction(() -> {
      // Create a document and get its RID
      final ResultSet insertResult = database.command("sql", "INSERT INTO TestSelectFromNestedRid SET name = " +
          "'nested_test'");
      assertThat(insertResult.hasNext()).isTrue();
      final String ridString = insertResult.next().getIdentity().get().toString();

      // Test: SELECT FROM a nested property containing a RID string
      final ResultSet result = database.command("sqlscript", """
          LET $batch_in = [{'source_id': '%s', 'target_id': '#4:0', 'features': {}, 'relation_type': 'in'}];
          LET $source = (SELECT FROM $batch_in[0].source_id);
          RETURN [$batch_in[0].source_id, $source];
          """.formatted(ridString));

      assertThat(result.hasNext()).isTrue();
    });
  }
}
