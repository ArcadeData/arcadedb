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
import com.arcadedb.query.sql.parser.ExplainResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that MAX and MIN aggregate functions use indexes when available.
 * Issue: https://github.com/ArcadeData/arcadedb/issues/3304
 */
class MaxMinFromIndexTest {

  @Test
  void maxUsesIndexOnLongProperty() throws Exception {
    TestHelper.executeInNewDatabase("maxMinFromIndexTest", (db) -> {
      // Create type with indexed Long property
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("value", Long.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      // Insert test data
      db.transaction(() -> {
        for (long i = 0; i < 100; i++) {
          db.newDocument("TestType").set("value", i).save();
        }
      });

      // Test MAX query returns correct result
      ResultSet rs = db.query("sql", "SELECT max(value) as maxValue FROM TestType");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      assertThat(result.<Long>getProperty("maxValue")).isEqualTo(99L);
      assertThat(rs.hasNext()).isFalse();

      // Verify execution plan uses index (not full scan)
      ExplainResultSet explain = (ExplainResultSet) db.command("sql", "EXPLAIN SELECT max(value) as maxValue FROM TestType");
      String plan = explain.getExecutionPlan().get().prettyPrint(0, 2);

      // The plan should use index-based MAX, not a full type scan
      assertThat(plan).contains("MAX FROM INDEX");
      assertThat(plan).doesNotContain("FETCH FROM TYPE");
    });
  }

  @Test
  void minUsesIndexOnLongProperty() throws Exception {
    TestHelper.executeInNewDatabase("minFromIndexTest", (db) -> {
      // Create type with indexed Long property
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("value", Long.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      // Insert test data (starting from 10 to verify MIN is actually computed)
      db.transaction(() -> {
        for (long i = 10; i < 110; i++) {
          db.newDocument("TestType").set("value", i).save();
        }
      });

      // Test MIN query returns correct result
      ResultSet rs = db.query("sql", "SELECT min(value) as minValue FROM TestType");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      assertThat(result.<Long>getProperty("minValue")).isEqualTo(10L);
      assertThat(rs.hasNext()).isFalse();

      // Verify execution plan uses index
      ExplainResultSet explain = (ExplainResultSet) db.command("sql", "EXPLAIN SELECT min(value) as minValue FROM TestType");
      String plan = explain.getExecutionPlan().get().prettyPrint(0, 2);

      assertThat(plan).contains("MIN FROM INDEX");
      assertThat(plan).doesNotContain("FETCH FROM TYPE");
    });
  }

  @Test
  void maxWithNullValues() throws Exception {
    TestHelper.executeInNewDatabase("maxNullValuesTest", (db) -> {
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("value", Long.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      db.transaction(() -> {
        // Insert some null values and some actual values
        db.newDocument("TestType").set("value", (Long) null).save();
        db.newDocument("TestType").set("value", 50L).save();
        db.newDocument("TestType").set("value", 100L).save();
        db.newDocument("TestType").set("value", (Long) null).save();
        db.newDocument("TestType").set("value", 25L).save();
      });

      ResultSet rs = db.query("sql", "SELECT max(value) as maxValue FROM TestType");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      assertThat(result.<Long>getProperty("maxValue")).isEqualTo(100L);
    });
  }

  @Test
  void minWithNullValues() throws Exception {
    TestHelper.executeInNewDatabase("minNullValuesTest", (db) -> {
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("value", Long.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      db.transaction(() -> {
        db.newDocument("TestType").set("value", (Long) null).save();
        db.newDocument("TestType").set("value", 50L).save();
        db.newDocument("TestType").set("value", 10L).save();
        db.newDocument("TestType").set("value", (Long) null).save();
        db.newDocument("TestType").set("value", 25L).save();
      });

      ResultSet rs = db.query("sql", "SELECT min(value) as minValue FROM TestType");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      assertThat(result.<Long>getProperty("minValue")).isEqualTo(10L);
    });
  }

  @Test
  void maxWithNoIndex() throws Exception {
    TestHelper.executeInNewDatabase("maxNoIndexTest", (db) -> {
      // Create type WITHOUT index
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("value", Long.class);

      db.transaction(() -> {
        for (long i = 0; i < 50; i++) {
          db.newDocument("TestType").set("value", i).save();
        }
      });

      // Should still work correctly, just via full scan
      ResultSet rs = db.query("sql", "SELECT max(value) as maxValue FROM TestType");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      assertThat(result.<Long>getProperty("maxValue")).isEqualTo(49L);

      // Verify execution plan uses full scan (no index available)
      ExplainResultSet explain = (ExplainResultSet) db.command("sql", "EXPLAIN SELECT max(value) as maxValue FROM TestType");
      String plan = explain.getExecutionPlan().get().prettyPrint(0, 2);

      assertThat(plan).contains("FETCH FROM TYPE");
    });
  }

  @Test
  void maxWithWhereClauseFallsBackToScan() throws Exception {
    TestHelper.executeInNewDatabase("maxWithWhereTest", (db) -> {
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("category", String.class);
      type.createProperty("value", Long.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      db.transaction(() -> {
        for (long i = 0; i < 50; i++) {
          db.newDocument("TestType").set("category", i % 2 == 0 ? "A" : "B").set("value", i).save();
        }
      });

      // MAX with WHERE clause - optimization should NOT apply
      ResultSet rs = db.query("sql", "SELECT max(value) as maxValue FROM TestType WHERE category = 'A'");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      assertThat(result.<Long>getProperty("maxValue")).isEqualTo(48L); // max even number < 50
    });
  }

  @Test
  void maxOnIntegerProperty() throws Exception {
    TestHelper.executeInNewDatabase("maxIntegerTest", (db) -> {
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("value", Integer.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      db.transaction(() -> {
        for (int i = 0; i < 100; i++) {
          db.newDocument("TestType").set("value", i).save();
        }
      });

      ResultSet rs = db.query("sql", "SELECT max(value) as maxValue FROM TestType");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      Number maxValue = result.getProperty("maxValue");
      assertThat(maxValue.intValue()).isEqualTo(99);
    });
  }

  @Test
  void maxOnDoubleProperty() throws Exception {
    TestHelper.executeInNewDatabase("maxDoubleTest", (db) -> {
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("value", Double.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      db.transaction(() -> {
        for (int i = 0; i < 100; i++) {
          db.newDocument("TestType").set("value", i * 1.5).save();
        }
      });

      ResultSet rs = db.query("sql", "SELECT max(value) as maxValue FROM TestType");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      Number maxValue = result.getProperty("maxValue");
      assertThat(maxValue.doubleValue()).isEqualTo(99 * 1.5);
    });
  }

  @Test
  void minOnStringProperty() throws Exception {
    TestHelper.executeInNewDatabase("minStringTest", (db) -> {
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("name", String.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");

      db.transaction(() -> {
        db.newDocument("TestType").set("name", "Charlie").save();
        db.newDocument("TestType").set("name", "Alice").save();
        db.newDocument("TestType").set("name", "Bob").save();
        db.newDocument("TestType").set("name", "David").save();
      });

      ResultSet rs = db.query("sql", "SELECT min(name) as minName FROM TestType");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      assertThat(result.<String>getProperty("minName")).isEqualTo("Alice");
    });
  }

  @Test
  void maxOnEmptyTable() throws Exception {
    TestHelper.executeInNewDatabase("maxEmptyTableTest", (db) -> {
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("value", Long.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      // No data inserted

      ResultSet rs = db.query("sql", "SELECT max(value) as maxValue FROM TestType");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      assertThat(result.<Long>getProperty("maxValue")).isNull();
    });
  }

  @Test
  void minOnEmptyTable() throws Exception {
    TestHelper.executeInNewDatabase("minEmptyTableTest", (db) -> {
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("value", Long.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      // No data inserted

      ResultSet rs = db.query("sql", "SELECT min(value) as minValue FROM TestType");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      assertThat(result.<Long>getProperty("minValue")).isNull();
    });
  }

  @Test
  void maxWithCompositeIndexFallsBackToScan() throws Exception {
    TestHelper.executeInNewDatabase("maxCompositeIndexTest", (db) -> {
      // Create type with composite index (not single property)
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("category", String.class);
      type.createProperty("value", Long.class);
      // Composite index on (category, value) - should NOT be used for max(value)
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "category", "value");

      db.transaction(() -> {
        for (long i = 0; i < 50; i++) {
          db.newDocument("TestType").set("category", "A").set("value", i).save();
        }
      });

      // MAX should still work correctly via full scan
      ResultSet rs = db.query("sql", "SELECT max(value) as maxValue FROM TestType");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      assertThat(result.<Long>getProperty("maxValue")).isEqualTo(49L);

      // Verify execution plan uses full scan (composite index not applicable)
      ExplainResultSet explain = (ExplainResultSet) db.command("sql", "EXPLAIN SELECT max(value) as maxValue FROM TestType");
      String plan = explain.getExecutionPlan().get().prettyPrint(0, 2);

      // Should NOT use MAX FROM INDEX since it's a composite index
      assertThat(plan).contains("FETCH FROM TYPE");
    });
  }

  @Test
  void maxOnPropertyWithBothSingleAndCompositeIndex() throws Exception {
    TestHelper.executeInNewDatabase("maxBothIndexTypesTest", (db) -> {
      final DocumentType type = db.getSchema().createDocumentType("TestType");
      type.createProperty("category", String.class);
      type.createProperty("value", Long.class);
      // Create BOTH a single-property index AND a composite index
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value"); // single
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "category", "value"); // composite

      db.transaction(() -> {
        for (long i = 0; i < 50; i++) {
          db.newDocument("TestType").set("category", "A").set("value", i).save();
        }
      });

      // Should use the single-property index
      ResultSet rs = db.query("sql", "SELECT max(value) as maxValue FROM TestType");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      assertThat(result.<Long>getProperty("maxValue")).isEqualTo(49L);

      // Verify execution plan uses the single-property index
      ExplainResultSet explain = (ExplainResultSet) db.command("sql", "EXPLAIN SELECT max(value) as maxValue FROM TestType");
      String plan = explain.getExecutionPlan().get().prettyPrint(0, 2);

      assertThat(plan).contains("MAX FROM INDEX");
      assertThat(plan).doesNotContain("FETCH FROM TYPE");
    });
  }

  /**
   * Test that matches the exact scenario from the original issue #3304:
   * A Long Property used as a 'High Water Mark' for tracking the processing of data.
   * The query "SELECT max(property) FROM type" should use the index, not a full scan.
   */
  @Test
  void issue3304HighWaterMarkPattern() throws Exception {
    TestHelper.executeInNewDatabase("issue3304Test", (db) -> {
      // Create type with indexed Long property (high water mark pattern)
      final DocumentType type = db.getSchema().createDocumentType("example");
      type.createProperty("p1", String.class);
      type.createProperty("p2", Long.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "p2");

      // Insert test data with incrementing high water mark values
      final AtomicLong hwm = new AtomicLong(0);
      db.transaction(() -> {
        for (int i = 0; i < 1000; i++) {
          db.newDocument("example")
              .set("p1", "data_" + i)
              .set("p2", hwm.getAndIncrement())
              .save();
        }
      });

      // Get current high water mark using MAX - this should use the index
      ResultSet rs = db.query("sql", "SELECT max(p2) as mp2 FROM example");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      assertThat(result.<Long>getProperty("mp2")).isEqualTo(999L);
      assertThat(rs.hasNext()).isFalse();

      // Verify execution plan uses index (not full scan)
      ExplainResultSet explain = (ExplainResultSet) db.command("sql", "EXPLAIN SELECT max(p2) as mp2 FROM example");
      String plan = explain.getExecutionPlan().get().prettyPrint(0, 2);

      // Should use MAX FROM INDEX, not FETCH FROM TYPE (full scan)
      assertThat(plan).contains("MAX FROM INDEX");
      assertThat(plan).doesNotContain("FETCH FROM TYPE");
    });
  }
}
