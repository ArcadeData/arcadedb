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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional coverage tests for the SQL executor package.
 * Exercises executor steps that are not sufficiently covered by existing tests.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLExecutorAdditionalCoverageTest extends TestHelper {

  @Override
  public void beginTest() {
    database.getSchema().createVertexType("V1");
    database.getSchema().createVertexType("V2");
    database.getSchema().createEdgeType("E1");
    database.getSchema().createEdgeType("E2");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final MutableVertex v = database.newVertex("V1");
        v.set("name", "v1_" + i);
        v.set("idx", i);
        v.set("group", i % 3);
        v.set("value", i * 10);
        if (i % 2 == 0)
          v.set("tags", List.of("a", "b", "c"));
        else
          v.set("tags", List.of("x", "y"));
        v.save();
      }

      for (int i = 0; i < 5; i++) {
        final MutableVertex v = database.newVertex("V2");
        v.set("name", "v2_" + i);
        v.set("idx", i);
        v.save();
      }

      // Create edges: V1[0]->V2[0], V1[1]->V2[1], ..., V1[4]->V2[4]
      for (int i = 0; i < 5; i++) {
        database.command("sql",
            "CREATE EDGE E1 FROM (SELECT FROM V1 WHERE idx = ?) TO (SELECT FROM V2 WHERE idx = ?)", i, i);
      }
      // Create edges among V1: V1[0]->V1[1], V1[1]->V1[2], ..., V1[8]->V1[9]
      for (int i = 0; i < 9; i++) {
        database.command("sql",
            "CREATE EDGE E2 FROM (SELECT FROM V1 WHERE idx = ?) TO (SELECT FROM V1 WHERE idx = ?)", i, i + 1);
      }
    });
  }

  // --- FetchFromSchemaDatabaseStep ---
  @Test
  void fetchFromSchemaDatabase() {
    final ResultSet rs = database.query("sql", "SELECT FROM schema:database");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.getPropertyNames()).isNotEmpty();
    rs.close();
  }

  // --- FetchFromSchemaTypesStep ---
  @Test
  void fetchFromSchemaTypes() {
    final ResultSet rs = database.query("sql", "SELECT FROM schema:types");
    int count = 0;
    while (rs.hasNext()) {
      final Result item = rs.next();
      assertThat(item.<String>getProperty("name")).isNotNull();
      count++;
    }
    assertThat(count).isGreaterThanOrEqualTo(4); // V1, V2, E1, E2
    rs.close();
  }

  // --- FetchFromSchemaBucketsStep ---
  @Test
  void fetchFromSchemaBuckets() {
    final ResultSet rs = database.query("sql", "SELECT FROM schema:buckets");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isGreaterThan(0);
    rs.close();
  }

  // --- FetchFromSchemaIndexesStep ---
  @Test
  void fetchFromSchemaIndexes() {
    database.command("sql", "CREATE PROPERTY V1.idx INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "V1", "idx");
    final ResultSet rs = database.query("sql", "SELECT FROM schema:indexes");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isGreaterThan(0);
    rs.close();
  }

  // --- UnwindStep ---
  @Test
  void unwindField() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT name, tags FROM V1 WHERE idx = 0 UNWIND tags");
      int count = 0;
      while (rs.hasNext()) {
        final Result item = rs.next();
        assertThat(item.<String>getProperty("tags")).isNotNull();
        count++;
      }
      assertThat(count).isEqualTo(3); // ["a","b","c"] unwound
      rs.close();
    });
  }

  // --- ExpandStep ---
  @Test
  void expandOut() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT expand(out()) FROM V1 WHERE idx = 0");
      int count = 0;
      while (rs.hasNext()) {
        final Result item = rs.next();
        assertThat(item.getIdentity()).isPresent();
        count++;
      }
      assertThat(count).isGreaterThan(0);
      rs.close();
    });
  }

  // --- Projection with method call ---
  @Test
  void letExpression() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT name, name.toUpperCase() as upperName FROM V1 WHERE idx < 3");
      int count = 0;
      while (rs.hasNext()) {
        final Result item = rs.next();
        assertThat(item.<String>getProperty("upperName")).isNotNull();
        count++;
      }
      assertThat(count).isEqualTo(3);
      rs.close();
    });
  }

  // --- Subquery in projection ---
  @Test
  void letQuery() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT name, (SELECT count(*) FROM V2) as cnt FROM V1 WHERE idx = 0");
      assertThat(rs.hasNext()).isTrue();
      final Result item = rs.next();
      assertThat(item.<Object>getProperty("cnt")).isNotNull();
      rs.close();
    });
  }

  // --- SubQueryStep ---
  @Test
  void subQuery() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM (SELECT FROM V1 WHERE idx < 5)");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(5);
      rs.close();
    });
  }

  // --- Cross-type query via MATCH ---
  @Test
  void cartesianProduct() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "MATCH {type: V1, as: a, WHERE: (idx = 0)}.out('E1'){type: V2, as: b, WHERE: (idx = 0)} RETURN a.name as aName, b.name as bName");
      assertThat(rs.hasNext()).isTrue();
      rs.close();
    });
  }

  // --- FilterByClustersStep ---
  @Test
  void filterByBucket() {
    database.transaction(() -> {
      final String bucketName = database.getSchema().getType("V1").getBuckets(false).get(0).getName();
      final ResultSet rs = database.query("sql", "SELECT FROM bucket:" + bucketName);
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isGreaterThan(0);
      rs.close();
    });
  }

  // --- InsertValuesStep ---
  @Test
  void insertMultipleValues() {
    database.getSchema().createDocumentType("InsertValuesTest");
    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO InsertValuesTest (a, b) VALUES (1, 'x'), (2, 'y'), (3, 'z')");
    });
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM InsertValuesTest");
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(3L);
    rs.close();
  }

  // --- DeleteFromIndexStep ---
  @Test
  void deleteRecordWithIndex() {
    database.getSchema().createDocumentType("IndexedDoc");
    database.command("sql", "CREATE PROPERTY IndexedDoc.uid INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "IndexedDoc", "uid");
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO IndexedDoc SET uid = ?", i);
    });
    database.transaction(() -> {
      database.command("sql", "DELETE FROM IndexedDoc WHERE uid = 3");
    });
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM IndexedDoc");
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(4L);
    rs.close();
  }

  // --- OrderByStep ---
  @Test
  void orderByMultipleColumns() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT name, `group`, value FROM V1 ORDER BY `group` ASC, value DESC");
      int count = 0;
      Integer prevGroup = null;
      while (rs.hasNext()) {
        final Result item = rs.next();
        final Integer grp = item.getProperty("group");
        if (grp != null && prevGroup != null)
          assertThat(grp).isGreaterThanOrEqualTo(prevGroup);
        if (grp != null)
          prevGroup = grp;
        count++;
      }
      assertThat(count).isEqualTo(10);
      rs.close();
    });
  }

  @Test
  void orderByWithNulls() {
    database.getSchema().createDocumentType("OrderNullTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO OrderNullTest SET name = 'a', val = 1");
      database.command("sql", "INSERT INTO OrderNullTest SET name = 'b'");
      database.command("sql", "INSERT INTO OrderNullTest SET name = 'c', val = 3");
    });
    final ResultSet rs = database.query("sql", "SELECT FROM OrderNullTest ORDER BY val ASC");
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("name"));
    assertThat(names).hasSize(3);
    rs.close();
  }

  // --- ApplyDefaultsStep ---
  @Test
  void applyDefaults() {
    database.command("sql", "CREATE DOCUMENT TYPE DefaultsTest");
    database.command("sql", "CREATE PROPERTY DefaultsTest.status STRING (default 'active')");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO DefaultsTest SET name = 'test'");
    });
    final ResultSet rs = database.query("sql", "SELECT FROM DefaultsTest WHERE name = 'test'");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.<String>getProperty("status")).isEqualTo("active");
    rs.close();
  }

  // --- CopyRecordContentBeforeUpdateStep (UPDATE ... RETURN BEFORE) ---
  @Test
  void updateReturnBefore() {
    database.getSchema().createDocumentType("ReturnBeforeTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO ReturnBeforeTest SET name = 'original', val = 1");
    });
    database.transaction(() -> {
      final ResultSet rs = database.command("sql",
          "UPDATE ReturnBeforeTest SET name = 'updated' RETURN BEFORE WHERE val = 1");
      assertThat(rs.hasNext()).isTrue();
      final Result item = rs.next();
      assertThat(item.<String>getProperty("name")).isEqualTo("original");
      rs.close();
    });
  }

  // --- TimeoutStep ---
  @Test
  void selectWithTimeout() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM V1 TIMEOUT 10000");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(10);
      rs.close();
    });
  }

  // --- GuaranteeEmptyCountStep ---
  @Test
  void countOnEmptyType() {
    database.getSchema().createDocumentType("EmptyType");
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM EmptyType");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(0L);
    rs.close();
  }

  // --- EmptyDataGeneratorStep ---
  @Test
  void selectWithoutTarget() {
    final ResultSet rs = database.query("sql", "SELECT 1 as x, 'hello' as y, 2+3 as z");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.<Integer>getProperty("x")).isEqualTo(1);
    assertThat(item.<String>getProperty("y")).isEqualTo("hello");
    assertThat(item.<Integer>getProperty("z")).isEqualTo(5);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  // --- LimitExecutionStep ---
  @Test
  void selectWithLimit() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM V1 LIMIT 5");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(5);
      rs.close();
    });
  }

  // --- SkipExecutionStep ---
  @Test
  void selectWithSkipAndLimit() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM V1 ORDER BY idx ASC SKIP 2 LIMIT 3");
      int count = 0;
      int firstIdx = -1;
      while (rs.hasNext()) {
        final Result item = rs.next();
        if (firstIdx < 0)
          firstIdx = item.getProperty("idx");
        count++;
      }
      assertThat(count).isEqualTo(3);
      assertThat(firstIdx).isEqualTo(2);
      rs.close();
    });
  }

  // --- ProjectionCalculationStep ---
  @Test
  void projectionCalculation() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT name, idx * 2 as doubled FROM V1 WHERE idx = 5");
      assertThat(rs.hasNext()).isTrue();
      final Result item = rs.next();
      assertThat(item.<Integer>getProperty("doubled")).isEqualTo(10);
      rs.close();
    });
  }

  // --- AggregateProjectionCalculationStep ---
  @Test
  void aggregateWithGroupBy() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT `group`, count(*) as cnt, sum(value) as total, avg(value) as average FROM V1 GROUP BY `group` ORDER BY `group`");
      int count = 0;
      while (rs.hasNext()) {
        final Result item = rs.next();
        assertThat(item.<Long>getProperty("cnt")).isGreaterThan(0L);
        assertThat(item.<Object>getProperty("total")).isNotNull();
        assertThat(item.<Object>getProperty("average")).isNotNull();
        count++;
      }
      assertThat(count).isEqualTo(3); // groups 0, 1, 2
      rs.close();
    });
  }

  // --- DistinctExecutionStep ---
  @Test
  void distinctValues() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT DISTINCT `group` as grp FROM V1 ORDER BY grp");
      final List<Integer> groups = new ArrayList<>();
      while (rs.hasNext())
        groups.add(rs.next().getProperty("grp"));
      assertThat(groups).containsExactly(0, 1, 2);
      rs.close();
    });
  }

  // --- CheckIsVertexTypeStep ---
  @Test
  void createVertexValidatesType() {
    database.transaction(() -> {
      final ResultSet rs = database.command("sql", "CREATE VERTEX V1 SET name = 'newVertex', idx = 100");
      assertThat(rs.hasNext()).isTrue();
      rs.close();
    });
    final ResultSet check = database.query("sql", "SELECT FROM V1 WHERE idx = 100");
    assertThat(check.hasNext()).isTrue();
    check.close();
  }

  // --- CheckIsEdgeTypeStep / ConnectEdgeStep ---
  @Test
  void createEdgeValidatesType() {
    database.transaction(() -> {
      final ResultSet rs = database.command("sql",
          "CREATE EDGE E1 FROM (SELECT FROM V1 WHERE idx = 5) TO (SELECT FROM V2 WHERE idx = 0)");
      assertThat(rs.hasNext()).isTrue();
      rs.close();
    });
  }

  // --- CreateEdgesStep with IF NOT EXISTS ---
  @Test
  void createEdgeIfNotExists() {
    database.transaction(() -> {
      // First creation
      database.command("sql",
          "CREATE EDGE E1 FROM (SELECT FROM V1 WHERE idx = 6) TO (SELECT FROM V2 WHERE idx = 1)");
      // Second with IF NOT EXISTS: should not duplicate
      database.command("sql",
          "CREATE EDGE E1 FROM (SELECT FROM V1 WHERE idx = 6) TO (SELECT FROM V2 WHERE idx = 1) IF NOT EXISTS");
    });
    // Count edges from V1[6] to V2[1]
    final ResultSet rs = database.query("sql",
        "SELECT outE('E1').size() as cnt FROM V1 WHERE idx = 6");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("cnt")).isEqualTo(1);
    rs.close();
  }

  // --- CreateEdgesStep with UNIDIRECTIONAL ---
  @Test
  void createEdgeUnidirectional() {
    database.getSchema().buildEdgeType().withName("UniEdge").withBidirectional(false).create();
    database.transaction(() -> {
      database.command("sql",
          "CREATE EDGE UniEdge FROM (SELECT FROM V1 WHERE idx = 7) TO (SELECT FROM V2 WHERE idx = 2) UNIDIRECTIONAL");
    });
    // The edge should exist going out from V1[7]
    final ResultSet rs = database.query("sql",
        "SELECT outE('UniEdge').size() as cnt FROM V1 WHERE idx = 7");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("cnt")).isGreaterThanOrEqualTo(1);
    rs.close();
  }

  // --- FetchFromRidsStep ---
  @Test
  void fetchFromRids() {
    database.transaction(() -> {
      final ResultSet first = database.query("sql", "SELECT FROM V1 WHERE idx = 0");
      final RID rid1 = first.next().getIdentity().get();
      first.close();

      final ResultSet second = database.query("sql", "SELECT FROM V1 WHERE idx = 1");
      final RID rid2 = second.next().getIdentity().get();
      second.close();

      final ResultSet rs = database.query("sql", "SELECT FROM [" + rid1 + ", " + rid2 + "]");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(2);
      rs.close();
    });
  }

  // --- Delete edges via DELETE FROM ---
  @Test
  void deleteEdgeWithWhere() {
    database.transaction(() -> {
      // Create a specific edge to delete
      database.command("sql",
          "CREATE EDGE E1 FROM (SELECT FROM V1 WHERE idx = 8) TO (SELECT FROM V2 WHERE idx = 3)");
    });
    database.transaction(() -> {
      // Delete edges going from V1[8] by querying and deleting from edge type
      database.command("sql",
          "DELETE FROM (SELECT expand(outE('E1')) FROM V1 WHERE idx = 8)");
    });
    final ResultSet rs = database.query("sql", "SELECT outE('E1').size() as cnt FROM V1 WHERE idx = 8");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("cnt")).isEqualTo(0);
    rs.close();
  }

  // --- MoveVertexStep ---
  @Test
  void moveVertex() {
    database.getSchema().createVertexType("V3");
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX V1 SET name = 'toMove', idx = 200");
    });
    database.transaction(() -> {
      database.command("sql", "MOVE VERTEX (SELECT FROM V1 WHERE idx = 200) TO TYPE:V3");
    });
    final ResultSet rs = database.query("sql", "SELECT FROM V3 WHERE idx = 200");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("toMove");
    rs.close();
    // Should not be in V1 anymore
    final ResultSet rs2 = database.query("sql", "SELECT FROM V1 WHERE idx = 200");
    assertThat(rs2.hasNext()).isFalse();
    rs2.close();
  }

  // --- UpdateStep with various scenarios ---
  @Test
  void updateAddRemoveFromCollection() {
    database.getSchema().createDocumentType("CollTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO CollTest SET name = 'doc1', items = ['a', 'b']");
    });
    database.transaction(() -> {
      database.command("sql", "UPDATE CollTest SET items += 'c' WHERE name = 'doc1'");
    });
    ResultSet rs = database.query("sql", "SELECT FROM CollTest WHERE name = 'doc1'");
    assertThat(rs.hasNext()).isTrue();
    Result item = rs.next();
    final Collection<String> items = item.getProperty("items");
    assertThat(items).contains("a", "b", "c");
    rs.close();

    database.transaction(() -> {
      database.command("sql", "UPDATE CollTest SET items -= 'b' WHERE name = 'doc1'");
    });
    rs = database.query("sql", "SELECT FROM CollTest WHERE name = 'doc1'");
    assertThat(rs.hasNext()).isTrue();
    item = rs.next();
    final Collection<String> items2 = item.getProperty("items");
    assertThat(items2).contains("a", "c");
    assertThat(items2).doesNotContain("b");
    rs.close();
  }

  // --- UPDATE with PUT (map) ---
  @Test
  void updatePutMap() {
    database.getSchema().createDocumentType("MapTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO MapTest SET name = 'doc1', props = {'key1': 'val1'}");
    });
    database.transaction(() -> {
      database.command("sql", "UPDATE MapTest SET props.key2 = 'val2' WHERE name = 'doc1'");
    });
    final ResultSet rs = database.query("sql", "SELECT FROM MapTest WHERE name = 'doc1'");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.<Object>getProperty("props")).isNotNull();
    rs.close();
  }

  // --- UPDATE INCREMENT ---
  @Test
  void updateIncrement() {
    database.getSchema().createDocumentType("IncrTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO IncrTest SET name = 'counter', val = 10");
    });
    database.transaction(() -> {
      database.command("sql", "UPDATE IncrTest SET val += 5 WHERE name = 'counter'");
    });
    final ResultSet rs = database.query("sql", "SELECT FROM IncrTest WHERE name = 'counter'");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("val")).isEqualTo(15);
    rs.close();
  }

  // --- UPSERT ---
  @Test
  void upsert() {
    database.getSchema().createDocumentType("UpsertTest");
    database.command("sql", "CREATE PROPERTY UpsertTest.uid INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "UpsertTest", "uid");
    database.transaction(() -> {
      database.command("sql", "UPDATE UpsertTest SET uid = 1, name = 'first' UPSERT WHERE uid = 1");
    });
    database.transaction(() -> {
      database.command("sql", "UPDATE UpsertTest SET uid = 1, name = 'updated' UPSERT WHERE uid = 1");
    });
    final ResultSet rs = database.query("sql", "SELECT FROM UpsertTest WHERE uid = 1");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("updated");
    rs.close();

    final ResultSet cnt = database.query("sql", "SELECT count(*) as c FROM UpsertTest");
    assertThat(cnt.next().<Long>getProperty("c")).isEqualTo(1L);
    cnt.close();
  }

  // --- INSERT from SELECT ---
  @Test
  void insertFromSelect() {
    database.getSchema().createDocumentType("InsertFromSelTest");
    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO InsertFromSelTest FROM (SELECT name, idx FROM V1 WHERE idx < 3)");
    });
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM InsertFromSelTest");
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(3L);
    rs.close();
  }

  // --- EXPLAIN ---
  @Test
  void explainSelect() {
    final ResultSet rs = database.command("sql", "EXPLAIN SELECT FROM V1 WHERE idx = 0");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.getPropertyNames()).isNotEmpty();
    rs.close();
  }

  // --- PROFILE ---
  @Test
  void profileSelect() {
    database.transaction(() -> {
      final ResultSet rs = database.command("sql", "PROFILE SELECT FROM V1 WHERE idx = 0");
      assertThat(rs.hasNext()).isTrue();
      final Result item = rs.next();
      assertThat(item.getPropertyNames()).isNotEmpty();
      rs.close();
    });
  }


  // --- TRUNCATE TYPE ---
  @Test
  void truncateType() {
    database.getSchema().createDocumentType("TruncTest");
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO TruncTest SET val = ?", i);
    });
    database.transaction(() -> {
      database.command("sql", "TRUNCATE TYPE TruncTest");
    });
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM TruncTest");
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(0L);
    rs.close();
  }

  // --- Complex WHERE with AND/OR/NOT ---
  @Test
  void complexWhereConditions() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT FROM V1 WHERE (idx > 2 AND idx < 7) OR (idx = 0) ORDER BY idx");
      final List<Integer> indices = new ArrayList<>();
      while (rs.hasNext())
        indices.add(rs.next().getProperty("idx"));
      assertThat(indices).containsExactly(0, 3, 4, 5, 6);
      rs.close();
    });
  }

  @Test
  void whereNotCondition() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT FROM V1 WHERE NOT (idx >= 5) ORDER BY idx");
      final List<Integer> indices = new ArrayList<>();
      while (rs.hasNext())
        indices.add(rs.next().getProperty("idx"));
      assertThat(indices).containsExactly(0, 1, 2, 3, 4);
      rs.close();
    });
  }

  // --- WHERE with IN subquery ---
  @Test
  void whereInSubquery() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT FROM V1 WHERE idx IN (SELECT idx FROM V2)");
      int count = 0;
      while (rs.hasNext()) {
        final Result item = rs.next();
        assertThat(item.<Integer>getProperty("idx")).isBetween(0, 4);
        count++;
      }
      assertThat(count).isEqualTo(5);
      rs.close();
    });
  }

  // --- WHERE with BETWEEN ---
  @Test
  void whereBetween() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM V1 WHERE idx BETWEEN 3 AND 6 ORDER BY idx");
      final List<Integer> indices = new ArrayList<>();
      while (rs.hasNext())
        indices.add(rs.next().getProperty("idx"));
      assertThat(indices).containsExactly(3, 4, 5, 6);
      rs.close();
    });
  }

  // --- WHERE with LIKE ---
  @Test
  void whereLike() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM V1 WHERE name LIKE 'v1_1%'");
      assertThat(rs.hasNext()).isTrue();
      rs.close();
    });
  }

  // --- WHERE with INSTANCEOF ---
  @Test
  void whereInstanceOf() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM V1 WHERE @type = 'V1'");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(10);
      rs.close();
    });
  }

  // --- WHERE with IS NULL / IS NOT NULL ---
  @Test
  void whereIsNull() {
    database.getSchema().createDocumentType("NullTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO NullTest SET name = 'a', val = 1");
      database.command("sql", "INSERT INTO NullTest SET name = 'b'");
    });
    final ResultSet rs = database.query("sql", "SELECT FROM NullTest WHERE val IS NULL");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("b");
    assertThat(rs.hasNext()).isFalse();
    rs.close();

    final ResultSet rs2 = database.query("sql", "SELECT FROM NullTest WHERE val IS NOT NULL");
    assertThat(rs2.hasNext()).isTrue();
    assertThat(rs2.next().<String>getProperty("name")).isEqualTo("a");
    rs2.close();
  }

  // --- GROUP BY with HAVING (emulated via subquery) ---
  @Test
  void groupByHaving() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT FROM (SELECT `group`, count(*) as cnt FROM V1 GROUP BY `group`) WHERE cnt > 3 ORDER BY `group`");
      int count = 0;
      while (rs.hasNext()) {
        assertThat(rs.next().<Long>getProperty("cnt")).isGreaterThan(3L);
        count++;
      }
      assertThat(count).isGreaterThanOrEqualTo(1);
      rs.close();
    });
  }


  // --- Nested projections ---
  @Test
  void nestedProjection() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT name, idx, out('E1').size() as outCount, out('E2').size() as nextCount FROM V1 WHERE idx < 5 ORDER BY idx");
      int count = 0;
      while (rs.hasNext()) {
        final Result item = rs.next();
        assertThat(item.<String>getProperty("name")).isNotNull();
        count++;
      }
      assertThat(count).isEqualTo(5);
      rs.close();
    });
  }

  // --- FOREACH in script ---
  @Test
  void forEachInScript() {
    database.getSchema().createDocumentType("ForEachTest");
    database.transaction(() -> {
      database.command("sqlscript",
          "FOREACH ($i IN [1, 2, 3]) { INSERT INTO ForEachTest SET val = $i; }");
    });
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM ForEachTest");
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(3L);
    rs.close();
  }

  // --- WHILE in script ---
  @Test
  void whileInScript() {
    database.getSchema().createDocumentType("WhileTest");
    database.transaction(() -> {
      database.command("sqlscript", """
          LET $i = 0;
          WHILE ($i < 5) {
            INSERT INTO WhileTest SET val = $i;
            LET $i = $i + 1;
          }
          """);
    });
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM WhileTest");
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(5L);
    rs.close();
  }

  // --- ALTER TYPE ---
  @Test
  void alterType() {
    database.getSchema().createDocumentType("AlterTest");
    database.command("sql", "ALTER TYPE AlterTest CUSTOM myProp = 'myVal'");
    final String customVal = (String) database.getSchema().getType("AlterTest").getCustomValue("myProp");
    assertThat(customVal).isEqualTo("myVal");
  }

  // --- ALTER PROPERTY ---
  @Test
  void alterProperty() {
    database.getSchema().createDocumentType("AlterPropTest");
    database.command("sql", "CREATE PROPERTY AlterPropTest.name STRING");
    database.command("sql", "ALTER PROPERTY AlterPropTest.name MANDATORY true");
    assertThat(database.getSchema().getType("AlterPropTest").getProperty("name").isMandatory()).isTrue();
  }

  // --- DROP TYPE ---
  @Test
  void dropType() {
    database.getSchema().createDocumentType("DropMe");
    database.command("sql", "DROP TYPE DropMe");
    assertThat(database.getSchema().existsType("DropMe")).isFalse();
  }

  // --- DROP PROPERTY ---
  @Test
  void dropProperty() {
    database.getSchema().createDocumentType("DropPropTest");
    database.command("sql", "CREATE PROPERTY DropPropTest.name STRING");
    assertThat(database.getSchema().getType("DropPropTest").existsProperty("name")).isTrue();
    database.command("sql", "DROP PROPERTY DropPropTest.name");
    assertThat(database.getSchema().getType("DropPropTest").existsProperty("name")).isFalse();
  }

  // --- DROP INDEX ---
  @Test
  void dropIndex() {
    database.getSchema().createDocumentType("DropIdxTest");
    database.command("sql", "CREATE PROPERTY DropIdxTest.uid INTEGER");
    database.command("sql", "CREATE INDEX ON DropIdxTest (uid) UNIQUE");
    assertThat(database.getSchema().getType("DropIdxTest").getAllIndexes(false)).isNotEmpty();
    database.command("sql", "DROP INDEX `DropIdxTest[uid]`");
    assertThat(database.getSchema().getType("DropIdxTest").getAllIndexes(false)).isEmpty();
  }

  // --- CREATE BUCKET ---
  @Test
  void createAndDropBucket() {
    database.command("sql", "CREATE BUCKET extraBucket");
    assertThat(database.getSchema().existsBucket("extraBucket")).isTrue();
    database.command("sql", "DROP BUCKET extraBucket");
    assertThat(database.getSchema().existsBucket("extraBucket")).isFalse();
  }

  // --- UPDATE with RETURN AFTER ---
  @Test
  void updateReturnAfter() {
    database.getSchema().createDocumentType("ReturnAfterTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO ReturnAfterTest SET name = 'before', val = 1");
    });
    database.transaction(() -> {
      final ResultSet rs = database.command("sql",
          "UPDATE ReturnAfterTest SET name = 'after' RETURN AFTER WHERE val = 1");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("after");
      rs.close();
    });
  }

  // --- DELETE with LIMIT ---
  @Test
  void deleteWithLimit() {
    database.getSchema().createDocumentType("DelLimitTest");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO DelLimitTest SET val = ?", i);
    });
    database.transaction(() -> {
      database.command("sql", "DELETE FROM DelLimitTest LIMIT 3");
    });
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM DelLimitTest");
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(7L);
    rs.close();
  }

  // --- UPDATE with MERGE ---
  @Test
  void updateMerge() {
    database.getSchema().createDocumentType("MergeTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO MergeTest SET name = 'doc1', data = {'a': 1}");
    });
    database.transaction(() -> {
      database.command("sql", "UPDATE MergeTest MERGE {'b': 2} WHERE name = 'doc1'");
    });
    final ResultSet rs = database.query("sql", "SELECT FROM MergeTest WHERE name = 'doc1'");
    assertThat(rs.hasNext()).isTrue();
    rs.close();
  }

  // --- UPDATE with CONTENT ---
  @Test
  void updateContent() {
    database.getSchema().createDocumentType("ContentTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO ContentTest SET name = 'old', val = 1");
    });
    database.transaction(() -> {
      database.command("sql",
          "UPDATE ContentTest CONTENT {'name': 'new', 'val': 99} WHERE name = 'old'");
    });
    final ResultSet rs = database.query("sql", "SELECT FROM ContentTest WHERE name = 'new'");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("val")).isEqualTo(99);
    rs.close();
  }

  // --- Optional match (RemoveEmptyOptionalsStep) ---
  @Test
  void optionalMatch() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          """
          MATCH {type: V1, as: a, WHERE: (idx = 0)}.out('E1'){as: b} \
          RETURN a.name as aName, b.name as bName""");
      assertThat(rs.hasNext()).isTrue();
      rs.close();
    });
  }

  // --- Nested subqueries ---
  @Test
  void nestedSubqueries() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT FROM (SELECT FROM (SELECT FROM V1 WHERE idx < 5) WHERE idx > 2)");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(2); // idx 3, 4
      rs.close();
    });
  }

  // --- INSERT with embedded document ---
  @Test
  void insertWithEmbedded() {
    database.getSchema().createDocumentType("EmbeddedTest");
    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO EmbeddedTest SET name = 'doc1', address = {'street': '123 Main', 'city': 'Springfield'}");
    });
    final ResultSet rs = database.query("sql", "SELECT FROM EmbeddedTest WHERE name = 'doc1'");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.<Object>getProperty("address")).isNotNull();
    rs.close();
  }

  // --- SELECT with math expressions in projection ---
  @Test
  void mathExpressionsInProjection() {
    final ResultSet rs = database.query("sql",
        "SELECT 10 + 5 as sum, 10 - 3 as diff, 4 * 5 as prod, 20 / 4 as quot, 10 % 3 as modulo");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.<Integer>getProperty("sum")).isEqualTo(15);
    assertThat(item.<Integer>getProperty("diff")).isEqualTo(7);
    assertThat(item.<Integer>getProperty("prod")).isEqualTo(20);
    assertThat(item.<Integer>getProperty("quot")).isEqualTo(5);
    assertThat(item.<Integer>getProperty("modulo")).isEqualTo(1);
    rs.close();
  }

  // --- SELECT with CASE WHEN ---
  @Test
  void caseWhen() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT idx, CASE WHEN idx < 3 THEN 'low' WHEN idx < 7 THEN 'mid' ELSE 'high' END as category FROM V1 ORDER BY idx");
      int lowCount = 0, midCount = 0, highCount = 0;
      while (rs.hasNext()) {
        final String cat = rs.next().getProperty("category");
        switch (cat) {
        case "low" -> lowCount++;
        case "mid" -> midCount++;
        case "high" -> highCount++;
        }
      }
      assertThat(lowCount).isEqualTo(3);
      assertThat(midCount).isEqualTo(4);
      assertThat(highCount).isEqualTo(3);
      rs.close();
    });
  }

  // --- SELECT with parameter binding ---
  @Test
  void parameterBinding() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM V1 WHERE idx = ? AND name = ?", 3, "v1_3");
      assertThat(rs.hasNext()).isTrue();
      final Result item = rs.next();
      assertThat(item.<Integer>getProperty("idx")).isEqualTo(3);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    });
  }

  // --- SELECT count(*) with WHERE ---
  @Test
  void countWithWhere() {
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM V1 WHERE idx < 5");
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(5L);
    rs.close();
  }

  // --- UPDATE with RETURN COUNT ---
  @Test
  void updateReturnCount() {
    database.getSchema().createDocumentType("RetCountTest");
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO RetCountTest SET val = ?", i);
    });
    database.transaction(() -> {
      final ResultSet rs = database.command("sql", "UPDATE RetCountTest SET flag = true WHERE val < 3");
      assertThat(rs.hasNext()).isTrue();
      final Result item = rs.next();
      assertThat(item.<Long>getProperty("count")).isEqualTo(3L);
      rs.close();
    });
  }
}
