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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.parser.ExplainResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that MAX and MIN aggregate functions use indexes when available.
 * Issue: https://github.com/ArcadeData/arcadedb/issues/3304
 */
class MaxMinFromIndexTest {

  @Test
  void maxUsesIndexOnLongProperty() throws Exception {
    TestHelper.executeInNewDatabase("maxMinFromIndexTest", db -> {
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
    TestHelper.executeInNewDatabase("minFromIndexTest", db -> {
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
    TestHelper.executeInNewDatabase("maxNullValuesTest", db -> {
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
    TestHelper.executeInNewDatabase("minNullValuesTest", db -> {
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
    TestHelper.executeInNewDatabase("maxNoIndexTest", db -> {
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
    TestHelper.executeInNewDatabase("maxWithWhereTest", db -> {
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
    TestHelper.executeInNewDatabase("maxIntegerTest", db -> {
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
    TestHelper.executeInNewDatabase("maxDoubleTest", db -> {
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
    TestHelper.executeInNewDatabase("minStringTest", db -> {
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
    TestHelper.executeInNewDatabase("maxEmptyTableTest", db -> {
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
    TestHelper.executeInNewDatabase("minEmptyTableTest", db -> {
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
    TestHelper.executeInNewDatabase("maxCompositeIndexTest", db -> {
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
    TestHelper.executeInNewDatabase("maxBothIndexTypesTest", db -> {
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
   * Regression test for issue #3304: MAX/MIN optimization must also work when the type name
   * is provided as a positional parameter (e.g., SELECT max(p2) FROM ?).
   */
  @Test
  void maxUsesIndexWithPositionalParameter() throws Exception {
    TestHelper.executeInNewDatabase("maxPositionalParamTest", db -> {
      final DocumentType type = db.getSchema().createDocumentType("example");
      type.createProperty("p2", Long.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "p2");

      db.transaction(() -> {
        for (long i = 0; i < 100; i++)
          db.newDocument("example").set("p2", i).save();
      });

      // Test with positional parameter - should still use the index
      final ResultSet rs = db.command("sql", "select max(p2) as mp2 from ?", "example");
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.<Long>getProperty("mp2")).isEqualTo(99L);
      assertThat(rs.hasNext()).isFalse();

      // Verify execution plan uses index (not full scan) even with parameter
      final ExplainResultSet explain = (ExplainResultSet) db.command("sql", "explain select max(p2) as mp2 from ?", "example");
      final String plan = explain.getExecutionPlan().get().prettyPrint(0, 2);

      assertThat(plan).contains("MAX FROM INDEX");
      assertThat(plan).doesNotContain("FETCH FROM TYPE");
    });
  }

  /**
   * Regression test for issue #3304: MIN optimization with positional parameter.
   */
  @Test
  void minUsesIndexWithPositionalParameter() throws Exception {
    TestHelper.executeInNewDatabase("minPositionalParamTest", db -> {
      final DocumentType type = db.getSchema().createDocumentType("example");
      type.createProperty("p2", Long.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "p2");

      db.transaction(() -> {
        for (long i = 10; i < 110; i++)
          db.newDocument("example").set("p2", i).save();
      });

      final ResultSet rs = db.command("sql", "select min(p2) as mp2 from ?", "example");
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.<Long>getProperty("mp2")).isEqualTo(10L);
      assertThat(rs.hasNext()).isFalse();

      final ExplainResultSet explain = (ExplainResultSet) db.command("sql", "explain select min(p2) as mp2 from ?", "example");
      final String plan = explain.getExecutionPlan().get().prettyPrint(0, 2);

      assertThat(plan).contains("MIN FROM INDEX");
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
    TestHelper.executeInNewDatabase("issue3304Test", db -> {
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

  /**
   * MIN/MAX regression tests that run on the {@link TestHelper} database lifecycle (a fresh database per test). They live in a
   * nested class because the enclosing class creates its own databases with {@link TestHelper#executeInNewDatabase}.
   */
  @Nested
  class MinMaxQueries extends TestHelper {

    // ---- #8812: min()/max() over a range read the first index entry ----
    /*
     * SELECT min(a) FROM V WHERE a > x (and max with <) read every record of the range instead of the first entry of the index on
     * a. The answer has to stay the one the scan gives, and the plan has to stop at one index entry.
     */

    private void rangeMinMaxSetup() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE V");
        database.command("sql", "CREATE PROPERTY V.a LONG");
        database.command("sql", "CREATE PROPERTY V.b LONG");
        database.command("sql", "CREATE INDEX ON V (a) NOTUNIQUE");
        for (int i = 0; i < 2_000; i++)
          database.command("sql", "CREATE VERTEX V SET a = " + (i * 2) + ", b = " + (i % 7));
        // a vertex with no value: a range must never return it
        database.command("sql", "CREATE VERTEX V SET b = 1");
      });
    }

    /** Regression test for issue #8812. */
    @Test
    void minOverLowerBoundUsesTheIndex() {
      rangeMinMaxSetup();
      assertAnswer("SELECT min(a) AS c FROM V WHERE a > 1001", "SELECT min(a + 0) AS c FROM V WHERE a + 0 > 1001", 1002L);
      assertAnswer("SELECT min(a) AS c FROM V WHERE a >= 1002", "SELECT min(a + 0) AS c FROM V WHERE a + 0 >= 1002", 1002L);
      assertUsesOrderedIndexFetch("SELECT min(a) AS c FROM V WHERE a > 1001");
    }

    /** Regression test for issue #8812. */
    @Test
    void maxOverUpperBoundUsesTheIndex() {
      rangeMinMaxSetup();
      assertAnswer("SELECT max(a) AS c FROM V WHERE a < 1001", "SELECT max(a + 0) AS c FROM V WHERE a + 0 < 1001", 1000L);
      assertAnswer("SELECT max(a) AS c FROM V WHERE a <= 1000", "SELECT max(a + 0) AS c FROM V WHERE a + 0 <= 1000", 1000L);
      assertUsesOrderedIndexFetch("SELECT max(a) AS c FROM V WHERE a < 1001");
    }

    /** Regression test for issue #8812. */
    @Test
    void minAndMaxOverBothBounds() {
      rangeMinMaxSetup();
      assertAnswer("SELECT min(a) AS c FROM V WHERE a > 100 AND a < 200", "SELECT min(a + 0) AS c FROM V WHERE a + 0 > 100 AND a + 0 < 200",
          102L);
      assertAnswer("SELECT max(a) AS c FROM V WHERE a > 100 AND a < 200", "SELECT max(a + 0) AS c FROM V WHERE a + 0 > 100 AND a + 0 < 200",
          198L);
      assertAnswer("SELECT min(a) AS c FROM V WHERE a BETWEEN 101 AND 200", "SELECT min(a + 0) AS c FROM V WHERE a + 0 BETWEEN 101 AND 200",
          102L);
      assertAnswer("SELECT max(a) AS c FROM V WHERE a BETWEEN 101 AND 199", "SELECT max(a + 0) AS c FROM V WHERE a + 0 BETWEEN 101 AND 199",
          198L);
    }

    /** Regression test for issue #8812. */
    @Test
    void minOverTheOppositeBoundAndMaxOverTheSameBound() {
      rangeMinMaxSetup();
      assertAnswer("SELECT min(a) AS c FROM V WHERE a < 1001", "SELECT min(a + 0) AS c FROM V WHERE a + 0 < 1001", 0L);
      assertAnswer("SELECT max(a) AS c FROM V WHERE a > 1001", "SELECT max(a + 0) AS c FROM V WHERE a + 0 > 1001", 3998L);
    }

    /** Regression test for issue #8812. */
    @Test
    void emptyRangeStillAnswersOneNullRow() {
      rangeMinMaxSetup();
      try (final ResultSet rs = database.query("sql", "SELECT min(a) AS c FROM V WHERE a > 999999")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Object>getProperty("c")).isNull();
        assertThat(rs.hasNext()).isFalse();
      }
      try (final ResultSet rs = database.query("sql", "SELECT max(a) FROM V WHERE a < 0")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Object>getProperty("max(a)")).isNull();
        assertThat(rs.hasNext()).isFalse();
      }
    }

    /** Regression test for issue #8812. */
    @Test
    void columnNameIsTheOneTheStatementAsked() {
      rangeMinMaxSetup();
      try (final ResultSet rs = database.query("sql", "SELECT max(a) FROM V WHERE a < 100")) {
        final Result row = rs.next();
        assertThat(row.getPropertyNames()).containsExactly("max(a)");
        assertThat(row.<Long>getProperty("max(a)")).isEqualTo(98L);
      }
    }

    /** Regression test for issue #8812. */
    @Test
    void parametersAreReadAtEachExecution() {
      rangeMinMaxSetup();
      final String sql = "SELECT min(a) AS c FROM V WHERE a > :lo";
      for (final long lo : new long[] { 10, 1001, 3000 }) {
        try (final ResultSet rs = database.query("sql", sql, Map.of("lo", lo))) {
          final Object value = rs.next().getProperty("c");
          try (final ResultSet scan = database.query("sql", "SELECT min(a + 0) AS c FROM V WHERE a + 0 > :lo", Map.of("lo", lo))) {
            assertThat(value).isEqualTo(scan.next().getProperty("c"));
          }
        }
      }
    }

    /** Regression test for issue #8812. */
    @Test
    void otherShapesStillAggregate() {
      rangeMinMaxSetup();
      // a condition on another property, or an equality, is not a range of the aggregated property
      assertAnswer("SELECT min(a) AS c FROM V WHERE a > 100 AND b = 3", "SELECT min(a + 0) AS c FROM V WHERE a + 0 > 100 AND b = 3", null);
      assertAnswer("SELECT min(b) AS c FROM V WHERE a > 100", "SELECT min(b + 0) AS c FROM V WHERE a + 0 > 100", null);
    }

    /** Regression test for issue #8812. */
    @Test
    void aliasEqualToTheSourceColumnAndANullParameter() {
      rangeMinMaxSetup();
      try (final ResultSet rs = database.query("sql", "SELECT max(a) AS a FROM V WHERE a < 100")) {
        assertThat(rs.next().<Long>getProperty("a")).isEqualTo(98L);
      }
      try (final ResultSet rs = database.query("sql", "SELECT min(a) AS c FROM V WHERE a > :lo", Map.of("lo", Long.MIN_VALUE))) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(0L);
      }
      final HashMap<String, Object> nullParam = new HashMap<>();
      nullParam.put("lo", null);
      final Object viaIndex;
      try (final ResultSet rs = database.query("sql", "SELECT min(a) AS c FROM V WHERE a > :lo", nullParam)) {
        viaIndex = rs.next().getProperty("c");
      }
      try (final ResultSet rs = database.query("sql", "SELECT min(a + 0) AS c FROM V WHERE a + 0 > :lo", nullParam)) {
        assertThat(viaIndex).isEqualTo(rs.next().getProperty("c"));
      }
    }

    /** Regression test for issue #8812. */
    @Test
    void aNullBoundOfARangeMatchesNothing() {
      rangeMinMaxSetup();
      final HashMap<String, Object> nullParam = new HashMap<>();
      nullParam.put("lo", null);
      // a comparison with null is never true, through the index as through a scan: the null bound used to read as "no bound"
      try (final ResultSet rs = database.query("sql", "SELECT a FROM V WHERE a > :lo ORDER BY a LIMIT 1", nullParam)) {
        assertThat(rs.hasNext()).isFalse();
      }
      try (final ResultSet rs = database.query("sql", "SELECT a FROM V WHERE a < :lo", nullParam)) {
        assertThat(rs.hasNext()).isFalse();
      }
      try (final ResultSet rs = database.query("sql", "SELECT a FROM V WHERE a BETWEEN :lo AND 10", nullParam)) {
        assertThat(rs.hasNext()).isFalse();
      }
    }

    /** Regression test for issue #8812. */
    @Test
    void aNullBoundOnACompositeIndexMatchesNothingAndRealBoundsStillWork() {
      rangeMinMaxSetup();
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE C");
        database.command("sql", "CREATE PROPERTY C.x LONG");
        database.command("sql", "CREATE PROPERTY C.y LONG");
        database.command("sql", "CREATE INDEX ON C (x, y) NOTUNIQUE");
        for (int x = 0; x < 3; x++)
          for (int y = 0; y < 10; y++)
            database.command("sql", "CREATE VERTEX C SET x = " + x + ", y = " + y);
      });
      final HashMap<String, Object> nullParam = new HashMap<>();
      nullParam.put("p", null);
      for (final String where : new String[] { "x = 1 AND y > :p", "x = 1 AND y < :p", "x = 1 AND y >= :p", "x = 1 AND y BETWEEN :p AND 5" })
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM C WHERE " + where, nullParam)) {
          assertThat(rs.next().<Long>getProperty("c")).as(where).isEqualTo(0L);
        }
      for (final String where : new String[] { "x = 1 AND y > 4", "x = 1 AND y <= 4", "x = 1 AND y BETWEEN 2 AND 5" })
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM C WHERE " + where);
            final ResultSet scan = database.query("sql", "SELECT count(*) AS c FROM C WHERE " + where.replace("y", "y + 0"))) {
          assertThat(rs.next().<Long>getProperty("c")).as(where).isEqualTo(scan.next().<Long>getProperty("c"));
        }
    }

    /** Regression test for issue #8812. */
    @Test
    void everyFormOfANullRangeBoundMatchesNothingOnASingleAndACompositeIndex() {
      rangeMinMaxSetup();
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE K");
        database.command("sql", "CREATE PROPERTY K.a LONG");
        database.command("sql", "CREATE PROPERTY K.b LONG");
        database.command("sql", "CREATE INDEX ON K (a, b) NOTUNIQUE");
        for (int a = 0; a < 3; a++)
          for (int b = 0; b < 10; b++)
            database.command("sql", "CREATE VERTEX K SET a = " + a + ", b = " + b);
      });
      final HashMap<String, Object> nullParam = new HashMap<>();
      nullParam.put("p", null);
      for (final String where : new String[] { "a >= :p", "a <= :p", "a BETWEEN :p AND :p", "a = 1 AND b >= :p", "a = 1 AND b <= :p",
          "a = 1 AND b BETWEEN :p AND :p" })
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM K WHERE " + where, nullParam);
            final ResultSet scan = database.query("sql", "SELECT count(*) AS c FROM K WHERE " + where.replaceAll("\\b(a|b)\\b(?= [>=<B])", "$1 + 0"),
                nullParam)) {
          assertThat(rs.next().<Long>getProperty("c")).as(where).isEqualTo(scan.next().<Long>getProperty("c")).isEqualTo(0L);
        }
      // a null in the equality slot keeps what it did before: it is the same on the index and on a scan
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM K WHERE a = :p AND b > 5", nullParam);
          final ResultSet scan = database.query("sql", "SELECT count(*) AS c FROM K WHERE a + 0 = :p AND b + 0 > 5", nullParam)) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(scan.next().<Long>getProperty("c"));
      }
    }

    /** Regression test for issue #8812. */
    @Test
    void aParentIndexWithRecordsInASubtypeAnswersAsTheScanAndACaseInsensitiveIndexKeepsTheAggregate() {
      rangeMinMaxSetup();
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE P");
        database.command("sql", "CREATE PROPERTY P.a LONG");
        database.command("sql", "CREATE INDEX ON P (a) NOTUNIQUE");
        database.command("sql", "CREATE VERTEX TYPE S EXTENDS P");
        for (int i = 0; i < 50; i++) {
          database.command("sql", "CREATE VERTEX P SET a = " + (i * 2));
          database.command("sql", "CREATE VERTEX S SET a = " + (i * 2 + 1));
        }
        database.command("sql", "CREATE VERTEX TYPE T");
        database.command("sql", "CREATE PROPERTY T.s STRING");
        database.command("sql", "CREATE INDEX ON T (s COLLATE CI) NOTUNIQUE");
        for (final String s : new String[] { "b", "C", "a", "D", "E" })
          database.command("sql", "CREATE VERTEX T SET s = '" + s + "'");
      });
      assertAnswer("SELECT min(a) AS c FROM P WHERE a > 10", "SELECT min(a + 0) AS c FROM P WHERE a + 0 > 10", 11L);
      assertAnswer("SELECT max(a) AS c FROM P WHERE a < 10", "SELECT max(a + 0) AS c FROM P WHERE a + 0 < 10", 9L);
      assertAnswer("SELECT min(a) AS c FROM S WHERE a > 10", "SELECT min(a + 0) AS c FROM S WHERE a + 0 > 10", 11L);
      // a case-insensitive index holds folded keys, so its order is not the order of the values: the rewrite is not used
      for (final String sql : new String[] { "SELECT min(s) AS c FROM T WHERE s > 'B'", "SELECT max(s) AS c FROM T WHERE s < 'D'" })
        try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
          assertThat(rs.next().<String>getProperty("executionPlanAsString")).doesNotContain("FIRST ROW VALUE");
        }
    }

    /** Regression test for issue #8812. */
    @Test
    void anEqualityNullPrefixOfACompositeIndexIsNotANullRangeBound() {
      rangeMinMaxSetup();
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE N");
        database.command("sql", "CREATE PROPERTY N.x LONG");
        database.command("sql", "CREATE PROPERTY N.y LONG");
        database.command("sql", "CREATE INDEX ON N (x, y) NOTUNIQUE NULL_STRATEGY INDEX");
        for (int y = 0; y < 10; y++) {
          database.command("sql", "CREATE VERTEX N SET y = " + y);
          database.command("sql", "CREATE VERTEX N SET x = 1, y = " + y);
        }
      });
      for (final String where : new String[] { "x IS NULL AND y > 6", "x IS NULL AND y < 3", "x IS NULL AND y >= 6", "x IS NULL AND y BETWEEN 2 AND 4" })
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM N WHERE " + where);
            final ResultSet scan = database.query("sql", "SELECT count(*) AS c FROM N WHERE " + where.replace("y", "y + 0"))) {
          final long expected = scan.next().<Long>getProperty("c");
          assertThat(expected).as(where).isGreaterThan(0L);
          assertThat(rs.next().<Long>getProperty("c")).as(where).isEqualTo(expected);
        }
    }

    /** Regression test for issue #8812. */
    @Test
    void changesOfTheOpenTransactionAreSeen() {
      rangeMinMaxSetup();
      database.begin();
      try {
        database.command("sql", "CREATE VERTEX V SET a = 1003");
        database.command("sql", "DELETE FROM V WHERE a = 1002");
        assertAnswer("SELECT min(a) AS c FROM V WHERE a > 1001", "SELECT min(a + 0) AS c FROM V WHERE a + 0 > 1001", 1003L);
      } finally {
        database.rollback();
      }
      assertAnswer("SELECT min(a) AS c FROM V WHERE a > 1001", "SELECT min(a + 0) AS c FROM V WHERE a + 0 > 1001", 1002L);
    }

    private void assertAnswer(final String sql, final String checkSql, final Object expected) {
      final Object actual;
      final Object check;
      try (final ResultSet rs = database.query("sql", sql)) {
        actual = rs.next().getProperty("c");
      }
      try (final ResultSet rs = database.query("sql", checkSql)) {
        check = rs.next().getProperty("c");
      }
      assertThat(actual).as(sql).isEqualTo(check);
      if (expected != null)
        assertThat(actual).as(sql).isEqualTo(expected);
    }

    private void assertUsesOrderedIndexFetch(final String sql) {
      try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
        final String plan = rs.next().getProperty("executionPlanAsString");
        assertThat(plan).contains("FETCH FROM INDEX").contains("LIMIT 1").contains("FIRST ROW VALUE");
        assertThat(plan).doesNotContain("AGGREGAT");
      }
    }

    // ---- #8975: string min()/max() compare by code point ----
    /*
     * SQL min() and max() compared strings by UTF-16 code unit, so a supplementary character (an emoji) sorted below U+FF5A,
     * disagreeing with ORDER BY, the index and openCypher, which compare by code point.
     */

    private static final String FULLWIDTH_Z = "ｚ";
    private static final String EMOJI       = "😀";

    private void codePointSetup() {
      database.command("sql", "CREATE VERTEX TYPE S");
      database.command("sql", "CREATE PROPERTY S.s STRING");
      database.transaction(() -> {
        database.newVertex("S").set("s", FULLWIDTH_Z).save();
        database.newVertex("S").set("s", EMOJI).save();
      });
    }

    private String single(final String sql, final String name) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return rs.next().getProperty(name);
      }
    }

    private void assertCodePointOrder() {
      assertThat(single("SELECT max(s) AS v FROM S", "v")).isEqualTo(EMOJI);
      assertThat(single("SELECT min(s) AS v FROM S", "v")).isEqualTo(FULLWIDTH_Z);
      assertThat(single("SELECT min(s) AS mn, max(s) AS v FROM S", "v")).isEqualTo(EMOJI);
      assertThat(single("SELECT min(s) AS v, max(s) AS mx FROM S", "v")).isEqualTo(FULLWIDTH_Z);
      assertThat(single("SELECT s AS v FROM S ORDER BY s DESC LIMIT 1", "v")).isEqualTo(EMOJI);
    }

    /** Regression test for issue #8975. */
    @Test
    void minMaxWithoutIndex() {
      codePointSetup();
      assertCodePointOrder();
    }

    /** Regression test for issue #8975. */
    @Test
    void minMaxWithIndex() {
      codePointSetup();
      database.command("sql", "CREATE INDEX ON S (s) NOTUNIQUE");
      assertCodePointOrder();
    }

    /** Regression test for issue #8975. */
    @Test
    void openCypherAgrees() {
      codePointSetup();
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:S) RETURN min(n.s) AS mn, max(n.s) AS mx")) {
        final Result r = rs.next();
        assertThat(r.<String>getProperty("mn")).isEqualTo(FULLWIDTH_Z);
        assertThat(r.<String>getProperty("mx")).isEqualTo(EMOJI);
      }
    }

    // ---- #10710 (#10697, #10696): min()/max() over every type, and the grouping key of a temporal ----
    /*
     * SQL min() / max() ordered with a raw Comparable cast, so they refused a BINARY, ARRAY_OF_* or MAP column and a schemaless
     * column mixing two Java representations of one instant, which ORDER BY, the index and openCypher all order; and SQL
     * DISTINCT, GROUP BY and count(DISTINCT) keyed a temporal by its Java class, so one instant stored as a DATETIME and as a
     * ZONED_DATETIME was two groups where = and openCypher say it is one.
     */

    private static final long MS = 1_600_000_000_000L;

    /**
     * Regression test for issue #10710.
     * <p>
     * The census: for every property type, two rows, and {@code min()} / {@code max()} name the value {@code ORDER BY ... LIMIT 1}
     * names. A type added to {@link Type} without a case here fails the test, so the next one cannot be missed.
     */
    @Test
    void minAndMaxAgreeWithOrderByForEveryType() {
      database.command("sql", "CREATE DOCUMENT TYPE Target").close();
      database.command("sql", "CREATE DOCUMENT TYPE E").close();
      final RID[] links = new RID[2];
      database.transaction(() -> {
        links[0] = database.newDocument("Target").set("n", 0).save().getIdentity();
        links[1] = database.newDocument("Target").set("n", 1).save().getIdentity();
      });

      for (final Type type : Type.values()) {
        final String typeName = "T_" + type.name();
        database.command("sql", "CREATE DOCUMENT TYPE " + typeName).close();
        database.command("sql", "CREATE PROPERTY " + typeName + ".v " + type.name()).close();

        final Object[] values = switch (type) {
          case BOOLEAN -> new Object[] { false, true };
          case INTEGER -> new Object[] { 1, 2 };
          case SHORT -> new Object[] { (short) 1, (short) 2 };
          case LONG -> new Object[] { 1L, 2L };
          case FLOAT -> new Object[] { 1.5f, 2.5f };
          case DOUBLE -> new Object[] { 1.5d, 2.5d };
          case BYTE -> new Object[] { (byte) 1, (byte) 2 };
          case DECIMAL -> new Object[] { new BigDecimal("1.5"), new BigDecimal("2.5") };
          case STRING -> new Object[] { "a", "b" };
          case DATE -> new Object[] { LocalDate.of(2020, 1, 1), LocalDate.of(2021, 1, 1) };
          case DATETIME, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS ->
              new Object[] { LocalDateTime.of(2020, 1, 1, 0, 0), LocalDateTime.of(2021, 1, 1, 0, 0) };
          case ZONED_DATETIME -> new Object[] { ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
              ZonedDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC) };
          case LOCAL_TIME -> new Object[] { LocalTime.of(10, 0), LocalTime.of(11, 0) };
          case OFFSET_TIME -> new Object[] { OffsetTime.of(10, 0, 0, 0, ZoneOffset.UTC), OffsetTime.of(11, 0, 0, 0, ZoneOffset.UTC) };
          case DURATION -> new Object[] { new CypherDuration(0, 1, 0, 0), new CypherDuration(0, 2, 0, 0) };
          case BINARY -> new Object[] { new byte[] { 0, 9 }, new byte[] { 1, 2 } };
          case MAP -> new Object[] { new LinkedHashMap<>(Map.of("a", 0)), new LinkedHashMap<>(Map.of("a", 1)) };
          case LINK -> new Object[] { links[0], links[1] };
          // A collection argument of SQL min()/max() is the set of its elements: the smallest and the largest element of all the rows
          case LIST -> new Object[] { new ArrayList<>(List.of(0, 9)), new ArrayList<>(List.of(1, 2)) };
          case ARRAY_OF_SHORTS -> new Object[] { new short[] { 0, 9 }, new short[] { 1, 2 } };
          case ARRAY_OF_INTEGERS -> new Object[] { new int[] { 0, 9 }, new int[] { 1, 2 } };
          case ARRAY_OF_LONGS -> new Object[] { new long[] { 0, 9 }, new long[] { 1, 2 } };
          case ARRAY_OF_FLOATS -> new Object[] { new float[] { 0, 9 }, new float[] { 1, 2 } };
          case ARRAY_OF_DOUBLES -> new Object[] { new double[] { 0, 9 }, new double[] { 1, 2 } };
          case EMBEDDED -> null;
        };

        if (type == Type.EMBEDDED) {
          // an embedded document has no order of its own: min() refuses it with a message naming the type, rather than a cast error
          database.transaction(() -> {
            for (int i = 0; i < 2; i++) {
              final MutableDocument doc = database.newDocument(typeName);
              doc.newEmbeddedDocument("E", "v").set("x", i);
              doc.save();
            }
          });
          assertThatThrownBy(() -> single("SELECT min(v) AS m FROM " + typeName)).as(type.name())
              .hasStackTraceContaining("min()").hasStackTraceContaining("EmbeddedDocument");
          continue;
        }

        database.transaction(() -> {
          database.newDocument(typeName).set("k", "A").set("v", values[1]).save();
          database.newDocument(typeName).set("k", "B").set("v", values[0]).save();
        });

        final Object min = single("SELECT min(v) AS m FROM " + typeName);
        final Object max = single("SELECT max(v) AS m FROM " + typeName);

        if (type == Type.LIST || type.name().startsWith("ARRAY_OF_")) {
          assertThat(((Number) min).doubleValue()).as(type.name()).isEqualTo(0d);
          assertThat(((Number) max).doubleValue()).as(type.name()).isEqualTo(9d);
          // ORDER BY orders the sequences element by element: [0, 9] before [1, 2]
          assertThat(single("SELECT k AS m FROM " + typeName + " ORDER BY v LIMIT 1")).as(type.name()).isEqualTo("B");
          assertThat(single("SELECT k AS m FROM " + typeName + " ORDER BY v DESC LIMIT 1")).as(type.name()).isEqualTo("A");
          continue;
        }

        final Object first = single("SELECT v AS m FROM " + typeName + " ORDER BY v LIMIT 1");
        final Object last = single("SELECT v AS m FROM " + typeName + " ORDER BY v DESC LIMIT 1");
        assertThat(BinaryComparator.compareTo(first, last)).as(type.name()).isNegative();
        assertThat(BinaryComparator.compareTo(min, first)).as(type.name() + " min " + min + " vs " + first).isZero();
        assertThat(BinaryComparator.compareTo(max, last)).as(type.name() + " max " + max + " vs " + last).isZero();
      }
    }

    /**
     * Regression test for issue #10710.
     * <p>
     * A record and a RID are one kind, ordered by identity, not by the name of their classes.
     */
    @Test
    void aRecordAndARidOrderByIdentity() {
      database.command("sql", "CREATE DOCUMENT TYPE R").close();
      final RID[] rids = new RID[2];
      database.transaction(() -> {
        rids[0] = database.newDocument("R").set("n", 0).save().getIdentity();
        rids[1] = database.newDocument("R").set("n", 1).save().getIdentity();
      });
      final List<Object> values = List.of(rids[0], rids[1].asDocument());
      try (final ResultSet rs = database.query("sql", "SELECT max(?) AS m, min(?) AS n", values, values)) {
        final Result row = rs.next();
        assertThat(((Identifiable) row.getProperty("m")).getIdentity()).isEqualTo(rids[1]);
        assertThat(((Identifiable) row.getProperty("n")).getIdentity()).isEqualTo(rids[0]);
      }
    }

    /**
     * Regression test for issue #10710.
     * <p>
     * An array is a sequence, ordered against another array or a list element by element.
     */
    @Test
    void sequencesOrderElementByElement() {
      assertThat(BinaryComparator.compareTo(new int[] { 0, 9 }, new int[] { 1, 2 })).isNegative();
      assertThat(BinaryComparator.compareTo(new double[] { 1, 2 }, new long[] { 1, 2 })).isZero();
      assertThat(BinaryComparator.compareTo(new short[] { 1 }, new short[] { 1, 0 })).isNegative();
      assertThat(BinaryComparator.compareTo(new LinkedList<>(List.of(0, 9)), new int[] { 1, 2 })).isNegative();
      assertThat(BinaryComparator.compareTo(new float[] { 2 }, new ArrayList<>(List.of(1)))).isPositive();
    }

    /**
     * Regression test for issue #10710.
     * <p>
     * #10697: the repro of the report, over declared properties.
     */
    @Test
    @SuppressWarnings("unchecked")
    void minAndMaxOfABinaryAnArrayAndAMapColumn() {
      database.command("sql", "CREATE DOCUMENT TYPE T").close();
      database.command("sql", "CREATE PROPERTY T.b BINARY").close();
      database.command("sql", "CREATE PROPERTY T.m MAP").close();
      database.transaction(() -> {
        database.newDocument("T").set("k", "A").set("b", new byte[] { 1, 2 }).set("m", Map.of("a", 1)).save();
        database.newDocument("T").set("k", "B").set("b", new byte[] { 0, 9 }).set("m", Map.of("a", 0)).save();
      });

      assertThat((byte[]) single("SELECT min(b) AS m FROM T")).containsExactly(0, 9);
      assertThat((byte[]) single("SELECT max(b) AS m FROM T")).containsExactly(1, 2);
      assertThat((Map<String, Object>) single("SELECT min(m) AS m FROM T")).containsEntry("a", 0);
      assertThat((Map<String, Object>) single("SELECT max(m) AS m FROM T")).containsEntry("a", 1);
      // unsigned, as the index and ORDER BY order bytes
      database.transaction(() -> database.newDocument("T").set("k", "C").set("b", new byte[] { (byte) 0xFF }).save());
      assertThat((byte[]) single("SELECT max(b) AS m FROM T")).containsExactly(0xFF);
    }

    /**
     * Regression test for issue #10710.
     * <p>
     * #10696: one instant, three Java representations, a schemaless property.
     */
    @Test
    void oneInstantInThreeRepresentationsIsOneGroup() {
      database.command("sql", "CREATE VERTEX TYPE V").close();
      database.transaction(() -> {
        database.newVertex("V").set("k", "zoned", "v", ZonedDateTime.ofInstant(Instant.ofEpochMilli(MS), ZoneOffset.UTC)).save();
        database.newVertex("V").set("k", "plain", "v", new Date(MS)).save();
        database.newVertex("V").set("k", "plain2", "v", LocalDateTime.ofInstant(Instant.ofEpochMilli(MS), ZoneOffset.UTC)).save();
      });

      assertThat(count("SELECT DISTINCT v FROM V")).isEqualTo(1);
      assertThat(count("SELECT v, count(*) AS c FROM V GROUP BY v")).isEqualTo(1);
      assertThat(((Number) single("SELECT count(*) AS m FROM V GROUP BY v")).longValue()).isEqualTo(3L);
      assertThat(((Number) single("SELECT count(DISTINCT v) AS m FROM V")).longValue()).isEqualTo(1L);

      final Object min = single("SELECT min(v) AS m FROM V");
      final Object max = single("SELECT max(v) AS m FROM V");
      assertThat(min).isNotNull();
      assertThat(max).isNotNull();
      assertThat(QueryOperatorEquals.equals(min, new Date(MS))).isTrue();
      assertThat(QueryOperatorEquals.equals(max, new Date(MS))).isTrue();

      // and the same answers openCypher gives
      assertThat(((Number) cypher("MATCH (n:V) RETURN count(DISTINCT n.v) AS m")).longValue()).isEqualTo(1L);
    }

    /**
     * Regression test for issue #10710.
     * <p>
     * #10696: the seven-way probe. SQL answers the groups openCypher answers: the zone-less values join the instant in UTC.
     */
    @Test
    void sevenRepresentationsGroupAsInOpenCypher() {
      database.command("sql", "CREATE VERTEX TYPE W").close();
      final Instant instant = Instant.ofEpochMilli(MS);
      database.transaction(() -> {
        database.newVertex("W").set("k", "date", "v", new Date(MS)).save();
        database.newVertex("W").set("k", "instant", "v", instant).save();
        database.newVertex("W").set("k", "timestamp", "v", new Timestamp(MS)).save();
        database.newVertex("W").set("k", "ldtUTC", "v", LocalDateTime.ofInstant(instant, ZoneOffset.UTC)).save();
        database.newVertex("W").set("k", "zdtUTC", "v", ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)).save();
        database.newVertex("W").set("k", "odtUTC", "v", OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)).save();
        database.newVertex("W").set("k", "zdtParis", "v", ZonedDateTime.ofInstant(instant, ZoneId.of("Europe/Paris"))).save();
      });

      final long cypherGroups = ((Number) cypher("MATCH (n:W) RETURN count(DISTINCT n.v) AS m")).longValue();
      assertThat(cypherGroups).isEqualTo(2L);
      assertThat(((Number) single("SELECT count(DISTINCT v) AS m FROM W")).longValue()).isEqualTo(cypherGroups);
      assertThat(count("SELECT DISTINCT v FROM W")).isEqualTo(cypherGroups);
      assertThat(count("SELECT v, count(*) FROM W GROUP BY v")).isEqualTo(cypherGroups);

      assertThat(single("SELECT min(v) AS m FROM W")).isNotNull();
      assertThat(single("SELECT max(v) AS m FROM W")).isNotNull();
    }

    /**
     * Regression test for issue #10710.
     * <p>
     * A BINARY value and a temporal inside a list key by their content too, as the scalars do.
     */
    @Test
    void binaryAndTemporalElementsOfAListShareAKey() {
      database.command("sql", "CREATE DOCUMENT TYPE L").close();
      database.transaction(() -> {
        database.newDocument("L").set("b", new byte[] { 1, 2 }).set("t", new ArrayList<>(List.of(new Date(MS)))).save();
        database.newDocument("L").set("b", new byte[] { 1, 2 })
            .set("t", new ArrayList<>(List.of(LocalDateTime.ofInstant(Instant.ofEpochMilli(MS), ZoneOffset.UTC)))).save();
      });
      assertThat(count("SELECT DISTINCT b FROM L")).isEqualTo(1);
      assertThat(count("SELECT DISTINCT t FROM L")).isEqualTo(1);
      assertThat(count("SELECT t, count(*) FROM L GROUP BY t")).isEqualTo(1);
    }

    private Object single(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        assertThat(rs.hasNext()).as(sql).isTrue();
        final Object value = rs.next().getProperty("m");
        assertThat(rs.hasNext()).as(sql).isFalse();
        return value;
      }
    }

    private Object cypher(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return rs.next().getProperty("m");
      }
    }

    private long count(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return rs.stream().count();
      }
    }

    // ---- #10748: min()/max() over a subtype read through an ancestor's index ----
    /*
     * SELECT max(p) FROM T / min(p) is answered from the first entry of an ordered index, and for a subtype the index found is
     * routinely the one declared on an ancestor, which holds the entries of the ancestor and of every sibling subtype too:
     * max(v) FROM Child answered 901, a value held only by a record of Other. The step now reads the sub-indexes of the target's
     * own buckets only, and the planner hands it an index only when that index covers every one of them.
     *
     * The oracle is the same statement with a WHERE clause that defeats the pushdown, over a three-type hierarchy where each type
     * holds the extreme values in turn.
     */

    private static final List<String> TYPES = List.of("Parent", "Child", "Other", "GrandChild");

    private void createHierarchy(final String keyType, final String indexType) {
      database.command("sql", "CREATE DOCUMENT TYPE Parent").close();
      database.command("sql", "CREATE PROPERTY Parent.v " + keyType).close();
      database.command("sql", "CREATE INDEX ON Parent (v) " + indexType).close();
      database.command("sql", "CREATE DOCUMENT TYPE Child EXTENDS Parent").close();
      database.command("sql", "CREATE DOCUMENT TYPE Other EXTENDS Parent").close();
      database.command("sql", "CREATE DOCUMENT TYPE GrandChild EXTENDS Child").close();
    }

    private void insert(final String type, final Object... values) {
      database.transaction(() -> {
        for (final Object value : values)
          database.newDocument(type).set("v", value).save();
      });
    }

    private Object aggregate(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return rs.next().getProperty("m");
      }
    }

    private String plan(final String sql) {
      try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
        return rs.next().getProperty("executionPlanAsString");
      }
    }

    /** The hardwired answer agrees with the answer of the scan, for both functions and every type of the hierarchy. */
    private void assertEveryTypeAgreesWithTheScan() {
      for (final String type : TYPES)
        for (final String function : List.of("max", "min")) {
          final String hardwired = "SELECT " + function + "(v) AS m FROM " + type;
          assertThat(plan(hardwired)).as(hardwired).contains((function.equals("max") ? "MAX" : "MIN") + " FROM INDEX");
          assertThat(aggregate(hardwired)).as(hardwired).isEqualTo(aggregate(hardwired + " WHERE v IS NOT NULL OR v IS NULL"));
        }
    }

    /** Regression test for issue #10748. */
    @Test
    void theReportedHierarchy() {
      createHierarchy("INTEGER", "NOTUNIQUE");
      insert("Parent", 50, 60, 70);
      insert("Child", 5, 6, 7);
      insert("Other", 900, 901);

      assertThat(aggregate("SELECT max(v) AS m FROM Child")).isEqualTo(7);
      assertThat(aggregate("SELECT min(v) AS m FROM Child")).isEqualTo(5);
      assertThat(aggregate("SELECT max(v) AS m FROM Other")).isEqualTo(901);
      assertThat(aggregate("SELECT min(v) AS m FROM Other")).isEqualTo(900);
      assertThat(aggregate("SELECT max(v) AS m FROM Parent")).isEqualTo(901);
      assertThat(aggregate("SELECT min(v) AS m FROM Parent")).isEqualTo(5);
      // a subtype with no record answers null, as the scan does
      assertThat(aggregate("SELECT max(v) AS m FROM GrandChild")).isNull();
      assertEveryTypeAgreesWithTheScan();
    }

    /** Regression test for issue #10748. */
    @Test
    void eachTypeHoldsTheExtremesInTurn() {
      createHierarchy("INTEGER", "NOTUNIQUE");
      int base = 0;
      for (final String holder : TYPES) {
        for (final String type : TYPES)
          insert(type, base + (type.equals(holder) ? 1000 : 500), base + (type.equals(holder) ? -1000 : 0));
        assertEveryTypeAgreesWithTheScan();
        base += 10;
      }
    }

    /** Regression test for issue #10748. */
    @Test
    void aUniqueStringKey() {
      createHierarchy("STRING", "UNIQUE");
      insert("Parent", "mmm");
      insert("Child", "aaa", "bbb");
      insert("GrandChild", "ccc");
      insert("Other", "zzz", "000");
      assertThat(aggregate("SELECT max(v) AS m FROM Child")).isEqualTo("ccc");
      assertThat(aggregate("SELECT min(v) AS m FROM Child")).isEqualTo("aaa");
      assertThat(aggregate("SELECT max(v) AS m FROM GrandChild")).isEqualTo("ccc");
      assertEveryTypeAgreesWithTheScan();
    }

    /** Regression test for issue #10748. */
    @Test
    void deletedAndNullValuesAreNotTheExtreme() {
      createHierarchy("INTEGER", "NOTUNIQUE NULL_STRATEGY INDEX");
      insert("Child", 5, 6, 7);
      insert("Other", 900, 1);
      database.transaction(() -> {
        database.newDocument("Child").set("v", null).save();
        database.command("sql", "DELETE FROM Child WHERE v = 7").close();
      });
      assertThat(aggregate("SELECT max(v) AS m FROM Child")).isEqualTo(6);
      assertThat(aggregate("SELECT min(v) AS m FROM Child")).isEqualTo(5);
      assertEveryTypeAgreesWithTheScan();
    }

    /**
     * Regression test for issue #10748.
     * <p>
     * The ordered fetch through the same ancestor index answers the subtype's records only, in order, as the scan does.
     */
    @Test
    void anOrderByOverTheAncestorIndexReadsTheSubtypeOnly() {
      createHierarchy("INTEGER", "NOTUNIQUE");
      insert("Parent", 50, 60, 70);
      insert("Child", 5, 6, 7);
      insert("Other", 900, 901);
      for (final String direction : List.of("ASC", "DESC")) {
        final List<Object> values = new ArrayList<>();
        try (final ResultSet rs = database.query("sql", "SELECT v FROM Child ORDER BY v " + direction + " LIMIT 2", Map.of())) {
          rs.stream().forEach(r -> values.add(r.getProperty("v")));
        }
        assertThat(values).containsExactlyElementsOf(direction.equals("ASC") ? List.of(5, 6) : List.of(7, 6));
      }
    }
  }
}
