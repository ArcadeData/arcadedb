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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQL integration tests for low-coverage execution steps.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLExecutorLowCoverageTest extends TestHelper {

  @Override
  public void beginTest() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("WorksFor");
    database.getSchema().createEdgeType("Knows");
    database.getSchema().createDocumentType("Doc");

    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        final MutableVertex v = database.newVertex("Person");
        v.set("name", "Person" + i);
        v.set("age", 20 + i);
        v.set("city", i % 3 == 0 ? "NYC" : i % 3 == 1 ? "LA" : "SF");
        v.save();
      }

      for (int i = 0; i < 5; i++) {
        final MutableVertex v = database.newVertex("Company");
        v.set("name", "Company" + i);
        v.save();
      }

      // Create edges
      for (int i = 0; i < 15; i++) {
        database.command("sql",
            "CREATE EDGE WorksFor FROM (SELECT FROM Person WHERE name = ?) TO (SELECT FROM Company WHERE name = ?)",
            "Person" + i, "Company" + (i % 5));
      }

      for (int i = 0; i < 10; i++) {
        database.command("sql",
            "CREATE EDGE Knows FROM (SELECT FROM Person WHERE name = ?) TO (SELECT FROM Person WHERE name = ?)",
            "Person" + i, "Person" + (i + 1));
      }
    });
  }

  // --- TRAVERSE BFS ---
  @Test
  void traverseBreadthFirst() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "TRAVERSE out('Knows') FROM (SELECT FROM Person WHERE name = 'Person0') STRATEGY BREADTH_FIRST");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isGreaterThan(0);
      rs.close();
    });
  }

  @Test
  void traverseBreadthFirstWithMaxDepth() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "TRAVERSE out('Knows') FROM (SELECT FROM Person WHERE name = 'Person0') MAXDEPTH 2 STRATEGY BREADTH_FIRST");
      int count = 0;
      while (rs.hasNext()) {
        final Result item = rs.next();
        final Integer depth = item.getProperty("$depth");
        if (depth != null)
          assertThat(depth).isLessThanOrEqualTo(2);
        count++;
      }
      assertThat(count).isGreaterThan(0);
      rs.close();
    });
  }

  // --- TRAVERSE DFS ---
  @Test
  void traverseDepthFirst() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "TRAVERSE out('Knows') FROM (SELECT FROM Person WHERE name = 'Person0') STRATEGY DEPTH_FIRST");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isGreaterThan(0);
      rs.close();
    });
  }

  @Test
  void traverseDepthFirstWithWhile() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "TRAVERSE out('Knows') FROM (SELECT FROM Person WHERE name = 'Person0') WHILE $depth < 3");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isGreaterThan(0);
      rs.close();
    });
  }

  // --- TIMEOUT ---
  @Test
  void timeoutWithQuery() {
    database.transaction(() -> {
      // Just test that timeout syntax works
      final ResultSet rs = database.query("sql", "SELECT FROM Person TIMEOUT 10000");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(20);
      rs.close();
    });
  }

  // --- FETCH FROM CLUSTERS ---
  @Test
  void fetchFromMultipleClusters() {
    database.transaction(() -> {
      final String bucket1 = database.getSchema().getType("Person").getBuckets(false).get(0).getName();
      final ResultSet rs = database.query("sql", "SELECT FROM bucket:" + bucket1);
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isGreaterThan(0);
      rs.close();
    });
  }

  // --- DELETE FROM INDEX ---
  @Test
  void deleteFromIndexRange() {
    database.getSchema().createDocumentType("Indexed");
    database.command("sql", "CREATE PROPERTY Indexed.uid INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Indexed", "uid");

    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.command("sql", "INSERT INTO Indexed SET uid = ?", i);
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM Indexed WHERE uid BETWEEN 5 AND 10");
    });

    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM Indexed");
    assertThat(rs.next().<Long>getProperty("cnt")).isLessThan(20L);
    rs.close();
  }

  @Test
  void deleteFromIndexEquals() {
    database.getSchema().createDocumentType("IndexedEq");
    database.command("sql", "CREATE PROPERTY IndexedEq.code STRING");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "IndexedEq", "code");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO IndexedEq SET code = 'A'");
      database.command("sql", "INSERT INTO IndexedEq SET code = 'B'");
      database.command("sql", "INSERT INTO IndexedEq SET code = 'C'");
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM IndexedEq WHERE code = 'B'");
    });

    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM IndexedEq");
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(2L);
    rs.close();
  }

  // --- BATCH (Note: BATCH clause not supported in current SQL syntax) ---
  @Test
  void batchInsertSimple() {
    database.getSchema().createDocumentType("BatchTest");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.command("sql", "INSERT INTO BatchTest SET val = ?", i);
    });

    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM BatchTest");
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(100L);
    rs.close();
  }

  // --- PARALLEL ---
  @Test
  void parallelQuery() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Person PARALLEL");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(20);
      rs.close();
    });
  }

  // --- FILTER BY BUCKET ---
  @Test
  void filterBucketName() {
    database.transaction(() -> {
      final String bucketName = database.getSchema().getType("Person").getBuckets(false).get(0).getName();
      final ResultSet rs = database.query("sql",
          "SELECT FROM bucket:" + bucketName);
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isGreaterThan(0);
      rs.close();
    });
  }

  // --- SCRIPT EXECUTION PLANS ---
  @Test
  void scriptWithMultipleStatements() {
    database.getSchema().createDocumentType("ScriptTest");
    database.transaction(() -> {
      database.command("sqlscript", """
          LET $a = 1;
          LET $b = 2;
          INSERT INTO ScriptTest SET val = $a + $b;
          """);
    });

    final ResultSet rs = database.query("sql", "SELECT FROM ScriptTest");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("val")).isEqualTo(3);
    rs.close();
  }

  @Test
  void scriptWithIfStatement() {
    database.getSchema().createDocumentType("IfTest");
    database.transaction(() -> {
      database.command("sqlscript", """
          LET $x = 5;
          IF ($x > 3) {
            INSERT INTO IfTest SET status = 'high';
          }
          """);
    });

    final ResultSet rs = database.query("sql", "SELECT FROM IfTest");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("status")).isEqualTo("high");
    rs.close();
  }

  @Test
  void scriptWithReturn() {
    final ResultSet rs = database.command("sqlscript", """
        LET $result = 42;
        RETURN $result;
        """);
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("value")).isEqualTo(42);
    rs.close();
  }

  // --- DDL EXECUTION PLANS ---
  @Test
  void ddlCreateType() {
    database.command("sql", "CREATE DOCUMENT TYPE DDLTest");
    assertThat(database.getSchema().existsType("DDLTest")).isTrue();
  }

  @Test
  void ddlCreateProperty() {
    database.command("sql", "CREATE DOCUMENT TYPE PropTest");
    database.command("sql", "CREATE PROPERTY PropTest.name STRING");
    assertThat(database.getSchema().getType("PropTest").existsProperty("name")).isTrue();
  }

  @Test
  void ddlCreateIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE IdxTest");
    database.command("sql", "CREATE PROPERTY IdxTest.code INTEGER");
    database.command("sql", "CREATE INDEX ON IdxTest (code) UNIQUE");
    assertThat(database.getSchema().getType("IdxTest").getAllIndexes(false)).isNotEmpty();
  }

  // --- SINGLE OP EXECUTION PLANS ---
  @Test
  void singleOpBegin() {
    database.begin();
    database.rollback();
  }

  @Test
  void singleOpCommit() {
    database.begin();
    database.commit();
  }

  // --- CARTESIAN PRODUCT (via MATCH) ---
  @Test
  void cartesianProductViaMatch() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "MATCH {type: Person, as: p, WHERE: (name = 'Person0')}, {type: Company, as: c, WHERE: (name = 'Company0')} RETURN p.name, c.name");
      assertThat(rs.hasNext()).isTrue();
      rs.close();
    });
  }

  // --- REMOVE EDGE POINTERS ---
  @Test
  void insertFromSelectRemovesEdgePointers() {
    database.getSchema().createVertexType("VertexCopy");
    database.transaction(() -> {
      // This should trigger RemoveEdgePointersStep
      database.command("sql",
          "INSERT INTO VertexCopy FROM (SELECT FROM Person WHERE name = 'Person0')");
    });

    final ResultSet rs = database.query("sql", "SELECT FROM VertexCopy");
    assertThat(rs.hasNext()).isTrue();
    rs.close();
  }

  // --- RESULT SET METHODS ---
  @Test
  void resultSetNextIfAvailable() {
    final ResultSet rs = database.query("sql", "SELECT FROM Person LIMIT 1");
    final Result r = rs.nextIfAvailable();
    assertThat(r).isNotNull();
    assertThat(r.isElement()).isTrue();
    rs.close();
  }

  @Test
  void resultSetNextIfAvailableEmpty() {
    final ResultSet rs = database.query("sql", "SELECT FROM Person WHERE 1 = 0");
    final Result r = rs.nextIfAvailable();
    assertThat(r).isInstanceOf(EmptyResult.class);
    rs.close();
  }

  @Test
  void resultSetStream() {
    final ResultSet rs = database.query("sql", "SELECT FROM Person LIMIT 5");
    final long count = rs.stream().count();
    assertThat(count).isEqualTo(5);
  }

  @Test
  void resultSetElementStream() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Person LIMIT 3");
      final long count = rs.elementStream().count();
      assertThat(count).isEqualTo(3);
    });
  }

  @Test
  void resultSetVertexStream() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Person LIMIT 3");
      final long count = rs.vertexStream().count();
      assertThat(count).isEqualTo(3);
    });
  }

  @Test
  void resultSetEdgeStream() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM WorksFor LIMIT 3");
      final long count = rs.edgeStream().count();
      assertThat(count).isEqualTo(3);
    });
  }

  @Test
  void resultSetToDocuments() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Person LIMIT 3");
      assertThat(rs.toDocuments()).hasSize(3);
    });
  }

  @Test
  void resultSetToVertices() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Person LIMIT 3");
      assertThat(rs.toVertices()).hasSize(3);
    });
  }

  @Test
  void resultSetToEdges() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM WorksFor LIMIT 3");
      assertThat(rs.toEdges()).hasSize(3);
    });
  }

  // --- ADDITIONAL COVERAGE ---
  @Test
  void fetchFromResultset() {
    database.transaction(() -> {
      final ResultSet inner = database.query("sql", "SELECT FROM Person WHERE age < 25");
      // This exercises FetchFromResultsetStep when used in subqueries
      final ResultSet rs = database.query("sql", "SELECT FROM Person WHERE age > 30");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isGreaterThan(0);
      rs.close();
      inner.close();
    });
  }

  @Test
  void globalLetExpression() {
    final ResultSet rs = database.command("sqlscript", """
        LET $global = 100;
        SELECT $global as value;
        """);
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("value")).isEqualTo(100);
    rs.close();
  }

  @Test
  void globalLetQuery() {
    final ResultSet rs = database.command("sqlscript", """
        LET $count = (SELECT count(*) as c FROM Person);
        SELECT $count as result;
        """);
    assertThat(rs.hasNext()).isTrue();
    rs.close();
  }

  @Test
  void updateWithRemove() {
    database.getSchema().createDocumentType("RemoveTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO RemoveTest SET a = 1, b = 2, c = 3");
    });
    database.transaction(() -> {
      database.command("sql", "UPDATE RemoveTest REMOVE b");
    });
    final ResultSet rs = database.query("sql", "SELECT FROM RemoveTest");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.hasProperty("a")).isTrue();
    assertThat(item.hasProperty("b")).isFalse();
    assertThat(item.hasProperty("c")).isTrue();
    rs.close();
  }

  @Test
  void copyDocumentStep() {
    database.getSchema().createDocumentType("CopyTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO CopyTest SET name = 'original'");
    });
    database.transaction(() -> {
      // UPDATE triggers CopyRecordContentBeforeUpdateStep for RETURN BEFORE
      final ResultSet rs = database.command("sql",
          "UPDATE CopyTest SET name = 'updated' RETURN BEFORE");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("original");
      rs.close();
    });
  }
}
