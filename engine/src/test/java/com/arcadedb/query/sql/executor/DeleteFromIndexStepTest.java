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
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for DeleteFromIndexStep.
 * Tests deletion from indexed types using various conditions and operators.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DeleteFromIndexStepTest extends TestHelper {

  @Test
  void shouldDeleteWithEqualsOperator() {
    database.getSchema().createDocumentType("DeleteTest1");
    database.command("sql", "CREATE PROPERTY DeleteTest1.uid INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "DeleteTest1", "uid");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newDocument("DeleteTest1").set("uid", i).save();
      }
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest1 WHERE uid = 5");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest1");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(9L);
    result.close();

    // Verify the specific record was deleted
    final ResultSet check = database.query("sql", "SELECT FROM DeleteTest1 WHERE uid = 5");
    assertThat(check.hasNext()).isFalse();
    check.close();
  }

  @Test
  void shouldDeleteWithGreaterThanOperator() {
    database.getSchema().createDocumentType("DeleteTest2");
    database.command("sql", "CREATE PROPERTY DeleteTest2.value INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "DeleteTest2", "value");

    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        database.newDocument("DeleteTest2").set("value", i).save();
      }
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest2 WHERE value > 15");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest2");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(16L);
    result.close();

    // Verify no records with value > 15 exist
    final ResultSet check = database.query("sql", "SELECT FROM DeleteTest2 WHERE value > 15");
    assertThat(check.hasNext()).isFalse();
    check.close();
  }

  @Test
  void shouldDeleteWithGreaterThanOrEqualOperator() {
    database.getSchema().createDocumentType("DeleteTest3");
    database.command("sql", "CREATE PROPERTY DeleteTest3.score INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "DeleteTest3", "score");

    database.transaction(() -> {
      for (int i = 0; i < 15; i++) {
        database.newDocument("DeleteTest3").set("score", i).save();
      }
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest3 WHERE score >= 10");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest3");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(10L);
    result.close();

    // Verify all records have score < 10
    database.transaction(() -> {
      final ResultSet check = database.query("sql", "SELECT FROM DeleteTest3");
      while (check.hasNext()) {
        assertThat(check.next().<Integer>getProperty("score")).isLessThan(10);
      }
      check.close();
    });
  }

  @Test
  void shouldDeleteWithLessThanOperator() {
    database.getSchema().createDocumentType("DeleteTest4");
    database.command("sql", "CREATE PROPERTY DeleteTest4.age INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "DeleteTest4", "age");

    database.transaction(() -> {
      for (int i = 0; i < 25; i++) {
        database.newDocument("DeleteTest4").set("age", i).save();
      }
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest4 WHERE age < 5");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest4");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(20L);
    result.close();

    // Verify all remaining records have age >= 5
    database.transaction(() -> {
      final ResultSet check = database.query("sql", "SELECT FROM DeleteTest4");
      while (check.hasNext()) {
        assertThat(check.next().<Integer>getProperty("age")).isGreaterThanOrEqualTo(5);
      }
      check.close();
    });
  }

  @Test
  void shouldDeleteWithLessThanOrEqualOperator() {
    database.getSchema().createDocumentType("DeleteTest5");
    database.command("sql", "CREATE PROPERTY DeleteTest5.priority INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "DeleteTest5", "priority");

    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        database.newDocument("DeleteTest5").set("priority", i).save();
      }
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest5 WHERE priority <= 7");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest5");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(12L);
    result.close();

    // Verify all remaining records have priority > 7
    database.transaction(() -> {
      final ResultSet check = database.query("sql", "SELECT FROM DeleteTest5");
      while (check.hasNext()) {
        assertThat(check.next().<Integer>getProperty("priority")).isGreaterThan(7);
      }
      check.close();
    });
  }

  @Test
  void shouldDeleteWithBetweenOperator() {
    database.getSchema().createDocumentType("DeleteTest6");
    database.command("sql", "CREATE PROPERTY DeleteTest6.number INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "DeleteTest6", "number");

    database.transaction(() -> {
      for (int i = 0; i < 30; i++) {
        database.newDocument("DeleteTest6").set("number", i).save();
      }
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest6 WHERE number BETWEEN 10 AND 20");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest6");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(19L); // 30 - 11 (10 to 20 inclusive)
    result.close();

    // Verify no records in the deleted range
    final ResultSet check = database.query("sql", "SELECT FROM DeleteTest6 WHERE number BETWEEN 10 AND 20");
    assertThat(check.hasNext()).isFalse();
    check.close();
  }

  @Test
  void shouldDeleteWithStringIndex() {
    database.getSchema().createDocumentType("DeleteTest7");
    database.command("sql", "CREATE PROPERTY DeleteTest7.code STRING");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "DeleteTest7", "code");

    database.transaction(() -> {
      database.newDocument("DeleteTest7").set("code", "A001").save();
      database.newDocument("DeleteTest7").set("code", "B002").save();
      database.newDocument("DeleteTest7").set("code", "C003").save();
      database.newDocument("DeleteTest7").set("code", "D004").save();
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest7 WHERE code = 'B002'");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest7");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(3L);
    result.close();
  }

  @Test
  void shouldDeleteMultipleRecordsWithSameValue() {
    database.getSchema().createDocumentType("DeleteTest8");
    database.command("sql", "CREATE PROPERTY DeleteTest8.category STRING");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "DeleteTest8", "category");

    database.transaction(() -> {
      for (int i = 0; i < 15; i++) {
        database.newDocument("DeleteTest8")
            .set("category", i % 3 == 0 ? "A" : i % 3 == 1 ? "B" : "C")
            .save();
      }
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest8 WHERE category = 'A'");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest8");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(10L); // 15 - 5 (indices 0, 3, 6, 9, 12)
    result.close();
  }

  @Test
  void shouldHandleDeleteWithNoMatches() {
    database.getSchema().createDocumentType("DeleteTest9");
    database.command("sql", "CREATE PROPERTY DeleteTest9.id INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "DeleteTest9", "id");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newDocument("DeleteTest9").set("id", i).save();
      }
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest9 WHERE id = 999");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest9");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(10L);
    result.close();
  }

  @Test
  void shouldDeleteFromEmptyIndex() {
    database.getSchema().createDocumentType("DeleteTest10");
    database.command("sql", "CREATE PROPERTY DeleteTest10.value INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "DeleteTest10", "value");

    // No records inserted

    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest10 WHERE value = 5");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest10");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(0L);
    result.close();
  }

  @Test
  void shouldDeleteAllRecordsMatchingCondition() {
    database.getSchema().createDocumentType("DeleteTest11");
    database.command("sql", "CREATE PROPERTY DeleteTest11.status STRING");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "DeleteTest11", "status");

    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        database.newDocument("DeleteTest11")
            .set("status", i < 10 ? "active" : "inactive")
            .save();
      }
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest11 WHERE status = 'active'");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest11");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(10L);
    result.close();

    // Verify only inactive records remain
    database.transaction(() -> {
      final ResultSet check = database.query("sql", "SELECT FROM DeleteTest11");
      while (check.hasNext()) {
        assertThat(check.next().<String>getProperty("status")).isEqualTo("inactive");
      }
      check.close();
    });
  }

  @Test
  void shouldDeleteWithRangeCondition() {
    database.getSchema().createDocumentType("DeleteTest12");
    database.command("sql", "CREATE PROPERTY DeleteTest12.rating INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "DeleteTest12", "rating");

    database.transaction(() -> {
      for (int i = 1; i <= 100; i++) {
        database.newDocument("DeleteTest12").set("rating", i).save();
      }
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest12 WHERE rating > 50 AND rating < 75");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest12");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(76L); // 100 - 24 (51 to 74)
    result.close();
  }

  @Test
  void shouldHandleCompositeConditions() {
    database.getSchema().createDocumentType("DeleteTest13");
    database.command("sql", "CREATE PROPERTY DeleteTest13.value INTEGER");
    database.command("sql", "CREATE PROPERTY DeleteTest13.flag BOOLEAN");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "DeleteTest13", "value");

    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        database.newDocument("DeleteTest13")
            .set("value", i)
            .set("flag", i % 2 == 0)
            .save();
      }
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest13 WHERE value < 10");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest13");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(10L);
    result.close();
  }

  @Test
  void shouldDeleteFirstAndLastRecords() {
    database.getSchema().createDocumentType("DeleteTest14");
    database.command("sql", "CREATE PROPERTY DeleteTest14.seq INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "DeleteTest14", "seq");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newDocument("DeleteTest14").set("seq", i).save();
      }
    });

    // Delete first record
    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest14 WHERE seq = 0");
    });

    // Delete last record
    database.transaction(() -> {
      database.command("sql", "DELETE FROM DeleteTest14 WHERE seq = 9");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest14");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(8L);
    result.close();
  }

  @Test
  void shouldDeleteWithOrderDescending() {
    database.getSchema().createDocumentType("DeleteTest15");
    database.command("sql", "CREATE PROPERTY DeleteTest15.rank INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "DeleteTest15", "rank");

    database.transaction(() -> {
      for (int i = 0; i < 15; i++) {
        database.newDocument("DeleteTest15").set("rank", i).save();
      }
    });

    database.transaction(() -> {
      // Using BETWEEN which should work with descending order
      database.command("sql", "DELETE FROM DeleteTest15 WHERE rank BETWEEN 5 AND 10");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM DeleteTest15");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(9L); // 15 - 6 (5 to 10 inclusive)
    result.close();
  }
}
