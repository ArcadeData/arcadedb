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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonGraphSerializer;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.CollectionUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static com.arcadedb.schema.Property.RID_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

class QueryTest extends TestHelper {
  private static final int TOT = 10000;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      if (!database.getSchema().existsType("V")) {
        database.getSchema().createVertexType("V");
      }

      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newVertex("V");
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner" + i);

        v.save();
      }
    });
  }

  @Test
  void scan() {

    database.transaction(() -> {
      final ResultSet rs = database.command("SQL", """
          SELECT
          -- this is a comment
          FROM V
          """, new HashMap<>());

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();

        final Set<String> prop = new HashSet<>(record.getPropertyNames());

        assertThat(record.getPropertyNames().size()).isEqualTo(3);
        assertThat(prop).contains("id", "name", "surname");

        total.incrementAndGet();
      }

      assertThat(total.get()).isEqualTo(TOT);

    });
  }

  @Test
  void equalsFiltering() {

    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":name", "Jay");
      params.put(":surname", "Miner123");
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE name = :name AND surname = :surname", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record.getPropertyNames().size()).isEqualTo(3);
        assertThat((int) record.<Integer>getProperty("id")).isEqualTo(123);
        assertThat(record.<String>getProperty("name")).isEqualTo("Jay");
        assertThat(record.<String>getProperty("surname")).isEqualTo("Miner123");

        total.incrementAndGet();
      }

      assertThat(total.get()).isEqualTo(1);
    });
  }

  @Test
  void nullSafeEqualsFiltering() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":name", "Jay");
      params.put(":surname", "Miner123");
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE notExistent <=> null");

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record.getPropertyNames().size()).isEqualTo(3);
        total.incrementAndGet();
      }

      assertThat(total.get()).isEqualTo(TOT);
    });
  }

  @Test
  void cachedStatementAndExecutionPlan() {

    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":name", "Jay");
      params.put(":surname", "Miner123");
      ResultSet rs = database.command("SQL", "SELECT FROM V WHERE name = :name AND surname = :surname", params);

      AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record.getPropertyNames().size()).isEqualTo(3);
        assertThat((int) record.<Integer>getProperty("id")).isEqualTo(123);
        assertThat(record.<String>getProperty("name")).isEqualTo("Jay");
        assertThat(record.<String>getProperty("surname")).isEqualTo("Miner123");

        total.incrementAndGet();
      }

      assertThat(total.get()).isEqualTo(1);

      // CHECK STATEMENT CACHE
      assertThat(((DatabaseInternal) database).getStatementCache()
          .contains("SELECT FROM V WHERE name = :name AND surname = :surname")).isTrue();

      // CHECK EXECUTION PLAN CACHE
      assertThat(((DatabaseInternal) database).getExecutionPlanCache()
          .contains("SELECT FROM V WHERE name = :name AND surname = :surname")).isTrue();

      // EXECUTE THE 2ND TIME
      rs = database.command("SQL", """
          SELECT id,
          -- this is a comment
          name, surname
          FROM V WHERE name = :name AND surname = :surname""", params);

      total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record).isNotNull();
        assertThat(record.getPropertyNames().size()).isEqualTo(3);
        assertThat((int) record.<Integer>getProperty("id")).isEqualTo(123);
        assertThat(record.<String>getProperty("name")).isEqualTo("Jay");
        assertThat(record.<String>getProperty("surname")).isEqualTo("Miner123");

        total.incrementAndGet();
      }

      assertThat(total.get()).isEqualTo(1);
    });
  }

  @Test
  void majorFiltering() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":id", TOT - 11);
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE id > :id", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record.<Integer>getProperty("id") > TOT - 11).isTrue();
        total.incrementAndGet();
      }
      assertThat(total.get()).isEqualTo(10);
    });
  }

  @Test
  void majorEqualsFiltering() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":id", TOT - 11);
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE id >= :id", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record.<Integer>getProperty("id") >= TOT - 11).isTrue();

        total.incrementAndGet();
      }
      assertThat(total.get()).isEqualTo(11);
    });
  }

  @Test
  void minorFiltering() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":id", 10);
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE id < :id", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record.<Integer>getProperty("id") < 10).isTrue();
        total.incrementAndGet();
      }
      assertThat(total.get()).isEqualTo(10);
    });
  }

  @Test
  void minorEqualsFiltering() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":id", 10);
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE id <= :id", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record.<Integer>getProperty("id") <= 10).isTrue();
        total.incrementAndGet();
      }
      assertThat(total.get()).isEqualTo(11);
    });
  }

  @Test
  void notFiltering() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":id", 10);
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE NOT( id > :id )", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record.<Integer>getProperty("id") <= 10).isTrue();
        total.incrementAndGet();
      }
      assertThat(total.get()).isEqualTo(11);
    });
  }

  @Test
  void createVertexType() {
    database.transaction(() -> {
      database.command("SQL", "CREATE VERTEX TYPE Foo");
      database.command("SQL", "CREATE VERTEX Foo SET name = 'foo'");
      database.command("SQL", "CREATE VERTEX Foo SET name = 'bar'");

      final ResultSet rs = database.query("SQL", "SELECT FROM Foo");
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).isFalse();

      rs.close();
    });
  }

  @Test
  void createEdge() {
    database.transaction(() -> {
      database.command("SQL", "CREATE VERTEX TYPE Foo");
      database.command("SQL", "CREATE EDGE TYPE TheEdge");
      database.command("SQL", "CREATE VERTEX Foo SET name = 'foo'");
      database.command("SQL", "CREATE VERTEX Foo SET name = 'bar'");
      database.command("SQL",
          "CREATE EDGE TheEdge FROM (SELECT FROM Foo WHERE name ='foo') TO (SELECT FROM Foo WHERE name ='bar')");

      final ResultSet rs = database.query("SQL", "SELECT FROM TheEdge");
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).isFalse();

      rs.close();
    });
  }

  @Test
  void queryEdge() {
    final String vertexClass = "testQueryEdge_V";
    final String edgeClass = "testQueryEdge_E";
    database.transaction(() -> {
      database.command("SQL", "CREATE VERTEX TYPE " + vertexClass);
      database.command("SQL", "CREATE EDGE TYPE " + edgeClass);
      database.command("SQL", "CREATE VERTEX " + vertexClass + " SET name = 'foo'");
      database.command("SQL", "CREATE VERTEX " + vertexClass + " SET name = 'bar'");
      database.command("SQL",
          "CREATE EDGE " + edgeClass + " FROM (SELECT FROM " + vertexClass + " WHERE name ='foo') TO (SELECT FROM " + vertexClass
              + " WHERE name ='bar')");

      ResultSet rs = database.query("SQL", "SELECT FROM " + edgeClass);
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).isFalse();

      rs.close();

      rs = database.query("SQL", "SELECT out()[0].name as name from " + vertexClass + " where name = 'foo'");
      assertThat(rs.hasNext()).isTrue();
      final Result item = rs.next();
      final String name = item.getProperty("name");
      assertThat(name.contains("bar")).isTrue();
      assertThat(rs.hasNext()).isFalse();

      rs.close();
    });
  }

  @Test
  void method() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", "SELECT 'bar'.prefix('foo') as name");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("foobar");

      assertThat(rs.hasNext()).isFalse();

      rs.close();
    });
  }

  @Test
  void match() {
    final String vertexClass = "testMatch_V";
    final String edgeClass = "testMatch_E";
    database.transaction(() -> {
      database.command("SQL", "CREATE VERTEX TYPE " + vertexClass);
      database.command("SQL", "CREATE EDGE TYPE " + edgeClass);
      database.command("SQL", "CREATE VERTEX " + vertexClass + " SET name = 'foo'");
      database.command("SQL", "CREATE VERTEX " + vertexClass + " SET name = 'bar'");
      database.command("SQL",
          "CREATE EDGE " + edgeClass + " FROM (SELECT FROM " + vertexClass + " WHERE name ='foo') TO (SELECT FROM " + vertexClass
              + " WHERE name ='bar')");

      ResultSet rs = database.query("SQL", "SELECT FROM " + edgeClass);
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).isFalse();

      rs.close();

      rs = database.query("SQL", "MATCH {type:" + vertexClass + ", as:a} -" + edgeClass + "->{}  RETURN $patterns");
      rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(rs.hasNext()).isTrue();
      final Result item = rs.next();
      assertThat(rs.hasNext()).isFalse();

      rs.close();
    });
  }

  @Test
  void anonMatch() {
    final String vertexClass = "testAnonMatch_V";
    final String edgeClass = "testAnonMatch_E";
    database.transaction(() -> {
      database.command("SQL", "CREATE VERTEX TYPE " + vertexClass);
      database.command("SQL", "CREATE EDGE TYPE " + edgeClass);
      database.command("SQL", "CREATE VERTEX " + vertexClass + " SET name = 'foo'");
      database.command("SQL", "CREATE VERTEX " + vertexClass + " SET name = 'bar'");
      database.command("SQL",
          "CREATE EDGE " + edgeClass + " FROM (SELECT FROM " + vertexClass + " WHERE name ='foo') TO (SELECT FROM " + vertexClass
              + " WHERE name ='bar')");

      ResultSet rs = database.query("SQL", "SELECT FROM " + edgeClass);
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).isFalse();

      rs.close();

      rs = database.query("SQL", "MATCH {type:" + vertexClass + ", as:a} --> {}  RETURN $patterns");
      rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(rs.hasNext()).isTrue();
      final Result item = rs.next();
      assertThat(rs.hasNext()).isFalse();

      rs.close();
    });
  }

  @Test
  void like() {
    database.transaction(() -> {
      ResultSet rs = database.command("SQL", "SELECT FROM V WHERE surname LIKE '%in%' LIMIT 1");
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).isFalse();
      rs.close();

      rs = database.command("SQL", "SELECT FROM V WHERE surname LIKE 'Mi?er%' LIMIT 1");
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).isFalse();
      rs.close();

      rs = database.command("SQL", "SELECT FROM V WHERE surname LIKE 'baz' LIMIT 1");
      assertThat(rs.hasNext()).isFalse();
      rs.close();

    });
  }

  /**
   * Test case for issue https://github.com/ArcadeData/arcadedb/issues/1581
   * Tests PostgreSQL-style semantic negation of LIKE and ILIKE operators.
   */
  @Test
  void notLikeAndNotIlike() {
    database.transaction(() -> {
      // Test NOT LIKE - should return records that do NOT match the pattern
      ResultSet rs = database.command("SQL", "SELECT FROM V WHERE surname NOT LIKE 'Miner999'");
      assertThat(rs.hasNext()).isTrue();
      int count = 0;
      while (rs.hasNext()) {
        Result r = rs.next();
        String surname = r.getProperty("surname");
        assertThat(surname).isNotEqualTo("Miner999");
        count++;
      }
      // Only "Miner999" should be excluded from TOT records
      assertThat(count).isEqualTo(TOT - 1);
      rs.close();

      // Test NOT ILIKE - case-insensitive NOT LIKE
      database.command("SQL", "insert into V set surname = 'ALLCAPS'");
      database.command("SQL", "insert into V set surname = 'lowercase'");

      rs = database.command("SQL", "SELECT FROM V WHERE surname NOT ILIKE '%caps%'");
      int capsExcludedCount = 0;
      while (rs.hasNext()) {
        Result r = rs.next();
        String surname = r.getProperty("surname");
        // Should not contain 'caps' (case-insensitive)
        assertThat(surname.toLowerCase()).doesNotContain("caps");
        capsExcludedCount++;
      }
      // ALLCAPS should be excluded, lowercase should be included
      // Total: TOT + 2 inserted - 1 excluded = TOT + 1
      assertThat(capsExcludedCount).isEqualTo(TOT + 1);
      rs.close();

      // Test expression form: SELECT ('abc' NOT LIKE 'a?c')
      rs = database.command("SQL", "SELECT ('abc' NOT LIKE 'a?c') as result");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      assertThat(result.<Boolean>getProperty("result")).isFalse(); // 'abc' LIKE 'a?c' is true, so NOT is false
      rs.close();

      // Test expression form: SELECT ('ABC' NOT ILIKE 'a?c')
      rs = database.command("SQL", "SELECT ('ABC' NOT ILIKE 'a?c') as result");
      assertThat(rs.hasNext()).isTrue();
      result = rs.next();
      assertThat(result.<Boolean>getProperty("result")).isFalse(); // 'ABC' ILIKE 'a?c' is true, so NOT is false
      rs.close();

      // Test that 'xyz' NOT LIKE 'a?c' returns true
      rs = database.command("SQL", "SELECT ('xyz' NOT LIKE 'a?c') as result");
      assertThat(rs.hasNext()).isTrue();
      result = rs.next();
      assertThat(result.<Boolean>getProperty("result")).isTrue(); // 'xyz' LIKE 'a?c' is false, so NOT is true
      rs.close();
    });
  }

  // Issue https://github.com/ArcadeData/arcadedb/issues/603
  @Test
  void likeEncoding() {
    database.transaction(() -> {
      database.command("SQL", "insert into V set age = '10%'");
      database.command("SQL", "insert into V set age = '100%'");

      ResultSet rs = database.command("SQL", "SELECT FROM V WHERE age LIKE '10\\%'");
      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      assertThat(result.<String>getProperty("age")).isEqualTo("10%");
      assertThat(rs.hasNext()).isFalse();
      rs.close();

      rs = database.command("SQL", "SELECT FROM V WHERE age LIKE \"10%\" ORDER BY age");
      assertThat(rs.hasNext()).isTrue();
      result = rs.next();
      assertThat(result.<String>getProperty("age")).isEqualTo("10%");
      assertThat(rs.hasNext()).isTrue();
      result = rs.next();
      assertThat(result.<String>getProperty("age")).isEqualTo("100%");
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    });
  }

  @Test
  void timeout() {
    database.transaction(() -> {
      try {
        for (int i = 0; i < TOT * 3; ++i) {
          final MutableDocument v = database.newVertex("V");
          v.set("id", TOT + i);
          v.set("name", "Jay");
          v.set("surname", "Miner" + (TOT + i));

          v.save();
        }

        final ResultSet rs = database.command("SQL", "SELECT FROM V ORDER BY ID DESC TIMEOUT 1", new HashMap<>());
        while (rs.hasNext()) {
          final Result record = rs.next();
          LogManager.instance().log(this, Level.INFO, "Found record %s", null, record);
        }
        fail("Timeout should be triggered");
      } catch (final TimeoutException e) {
        // OK
      }
    });
  }

  /**
   * Test case for issue https://github.com/ArcadeData/arcadedb/issues/725
   */
  @Test
  void orderByRID() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", "SELECT @rid, name FROM V order by @rid asc LIMIT 2");

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();

        final Set<String> prop = new HashSet<>(record.getPropertyNames());

        assertThat(record.getPropertyNames()).hasSize(2);
        assertThat(prop.contains(RID_PROPERTY)).isTrue();
        assertThat(prop.contains("name")).isTrue();

        total.incrementAndGet();
      }

      assertThat(total.get()).isEqualTo(2);
    });
  }

  @Test
  void flattenWhereCondition() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("test");
      StringBuilder query = new StringBuilder("SELECT FROM test WHERE (");
      for (int i = 1; i < 10; i++) {
        if (i > 1)
          query.append(" and ");
        query.append("((property").append(i).append(" is null) or (property").append(i).append(" = #107:150))");
      }
      query.append(")");

      try (ResultSet set = database.query("sql", query.toString())) {
        assertThat(set.stream().count()).isEqualTo(0);
      }
    });
  }

  @Test
  void collectionsInProjections() {
    try (ResultSet set = database.query("sql", "SELECT [\"a\",\"b\",\"c\"] as coll")) {
      Collection<String> coll = set.nextIfAvailable().getProperty("coll");
      assertThat(coll.size()).isEqualTo(3);
      assertThat(coll.contains("a")).isTrue();
      assertThat(coll.contains("b")).isTrue();
      assertThat(coll.contains("c")).isTrue();
    }
  }

  @Test
  void collectionsInProjectionsContains() {
    try (ResultSet set = database.query("sql", "SELECT ([\"a\",\"b\",\"c\"] CONTAINS (@this ILIKE \"C\")) as coll")) {
      final Object coll = set.nextIfAvailable().getProperty("coll");
      assertThat((Boolean) coll).isTrue();
    }
  }

  @Test
  void collectionsOfObjectsInProjectionsContains() {
    try (ResultSet set = database.query("sql",
        "SELECT ([{\"x\":\"a\"},{\"x\":\"b\"},{\"x\":\"c\"}] CONTAINS (x ILIKE \"C\")) as coll")) {
      final Object coll = set.nextIfAvailable().getProperty("coll");
      assertThat((Boolean) coll).isTrue();
    }
  }

  @Test
  void graphElementSize() {
    database.transaction(() -> {
      database.command("sqlscript",
          """
              CREATE VERTEX TYPE News;
              CREATE VERTEX TYPE Author;
              CREATE VERTEX TYPE User;
              CREATE EDGE TYPE Published;
              CREATE EDGE TYPE HasRead;
              INSERT INTO News CONTENT { "id": "1", "title": "News 1", "content": "Content 1" };
              INSERT INTO News CONTENT { "id": "2", "title": "News 2", "content": "Content 2" };
              INSERT INTO Author CONTENT { "id": "1", "name": "Author 1" };
              INSERT INTO User CONTENT { "id": "1", "name": "User 1" };
              CREATE EDGE Published FROM (SELECT FROM Author WHERE id = 1) TO (SELECT FROM News WHERE id = 1);
              CREATE EDGE Published FROM (SELECT FROM Author WHERE id = 1) TO (SELECT FROM News WHERE id = 2);
              """);
    });

    ResultSet resultSet = database.query("sql", "SELECT out(\"Published\").in(\"HasRead\") as output FROM Author");
    Assertions.assertThat(resultSet.hasNext()).isTrue();
    Result result = resultSet.next();
    Assertions.assertThat((List<?>) result.getProperty("output")).hasSize(0);

    resultSet = database.query("sql", "SELECT out(\"Published\").in(\"HasRead\").size() as output FROM Author");
    Assertions.assertThat(resultSet.hasNext()).isTrue();
    result = resultSet.next();
    Assertions.assertThat((Integer) result.getProperty("output")).isEqualTo(0);

    resultSet = database.query("sql",
        "SELECT bothE(\"Published\") as bothE, inE(\"Published\") as inE, outE(\"Published\") as outE FROM News");
    Assertions.assertThat(resultSet.hasNext()).isTrue();
    result = resultSet.next();
    Assertions.assertThat(CollectionUtils.countEntries(((Iterable<?>) result.getProperty("bothE")).iterator())).isEqualTo(1L);
    Assertions.assertThat(CollectionUtils.countEntries(((Iterable<?>) result.getProperty("inE")).iterator())).isEqualTo(1L);
    Assertions.assertThat(CollectionUtils.countEntries(((Iterable<?>) result.getProperty("outE")).iterator())).isEqualTo(0);

    final JsonGraphSerializer serializerImpl = JsonGraphSerializer.createJsonGraphSerializer()
        .setExpandVertexEdges(false);
    serializerImpl.setUseCollectionSize(false).setUseCollectionSizeForEdges(true);
    final JSONObject json = serializerImpl.serializeResult(database, result);

    Assertions.assertThat(json.getInt("bothE")).isEqualTo(1L);
    Assertions.assertThat(json.getInt("inE")).isEqualTo(1L);
    Assertions.assertThat(json.getInt("outE")).isEqualTo(0L);
  }

  /**
   * Test case for issue https://github.com/ArcadeData/arcadedb/issues/3315
   * Tests CREATE EDGE with property access on query result sets and LET variables.
   * The bug causes SuffixIdentifier to return wrapped Result objects instead of unwrapped values.
   */
  @Test
  void createEdgeWithPropertyAccessOnResultSet() {
    database.transaction(() -> {
      // Setup: Create types and test vertices
      database.command("SQL", "CREATE VERTEX TYPE TestVertex3315");
      database.command("SQL", "CREATE EDGE TYPE TestEdge3315");
      database.command("SQL", "CREATE VERTEX TestVertex3315 SET name = 'vertex1'");
      database.command("SQL", "CREATE VERTEX TestVertex3315 SET name = 'vertex2'");

      // Get the RIDs for verification
      final ResultSet vertices = database.query("SQL", "SELECT @rid FROM TestVertex3315 ORDER BY name");
      assertThat(vertices.hasNext()).isTrue();
      final Object rid1 = vertices.next().getProperty("@rid");
      assertThat(vertices.hasNext()).isTrue();
      final Object rid2 = vertices.next().getProperty("@rid");
      vertices.close();

      // Scenario 1: LET variable with property access (should fail with current bug)
      // This tests: LET $x = SELECT @rid FROM V; CREATE EDGE E FROM $x.@rid TO $x.@rid
      database.command("sqlscript", """
          LET $x = SELECT @rid FROM TestVertex3315 WHERE name = 'vertex1';
          CREATE EDGE TestEdge3315 FROM $x.@rid TO $x.@rid;
          """);

      // Verify edge was created correctly
      ResultSet edges = database.query("SQL", "SELECT FROM TestEdge3315");
      assertThat(edges.hasNext()).isTrue();
      Result edge = edges.next();
      assertThat((Object) edge.getProperty("@in")).isEqualTo(rid1);
      assertThat((Object) edge.getProperty("@out")).isEqualTo(rid1);
      edges.close();

      // Clean up for next scenario
      database.command("SQL", "DELETE FROM TestEdge3315");

      // Scenario 2: Direct subquery with property access (should fail with current bug)
      // This tests: CREATE EDGE E FROM (SELECT @rid FROM V).@rid TO (SELECT @rid FROM V).@rid
      database.command("SQL", """
          CREATE EDGE TestEdge3315
          FROM (SELECT @rid FROM TestVertex3315 WHERE name = 'vertex1').@rid
          TO (SELECT @rid FROM TestVertex3315 WHERE name = 'vertex2').@rid;
          """);

      // Verify edge was created correctly
      edges = database.query("SQL", "SELECT FROM TestEdge3315");
      assertThat(edges.hasNext()).isTrue();
      edge = edges.next();
      assertThat((Object) edge.getProperty("@in")).isEqualTo(rid2);
      assertThat((Object) edge.getProperty("@out")).isEqualTo(rid1);
      edges.close();

      // Clean up for next scenario
      database.command("SQL", "DELETE FROM TestEdge3315");

      // Scenario 3: Verify working case still works (baseline)
      // This tests: LET $x = (SELECT @rid FROM V).@rid; CREATE EDGE E FROM $x TO $x
      database.command("sqlscript", """
          LET $x = (SELECT @rid FROM TestVertex3315 WHERE name = 'vertex1').@rid;
          CREATE EDGE TestEdge3315 FROM $x TO $x;
          """);

      // Verify edge was created correctly
      edges = database.query("SQL", "SELECT FROM TestEdge3315");
      assertThat(edges.hasNext()).isTrue();
      edge = edges.next();
      assertThat((Object) edge.getProperty("@in")).isEqualTo(rid1);
      assertThat((Object) edge.getProperty("@out")).isEqualTo(rid1);
      edges.close();

      // Clean up for next scenario
      database.command("SQL", "DELETE FROM TestEdge3315");

      // Scenario 4: Multiple edges from result set with property access
      database.command("sqlscript", """
          LET $vertices = SELECT @rid FROM TestVertex3315;
          CREATE EDGE TestEdge3315 FROM $vertices.@rid TO $vertices.@rid;
          """);

      // Verify edges were created correctly (should create edges from each vertex to all vertices)
      edges = database.query("SQL", "SELECT count(*) as cnt FROM TestEdge3315");
      assertThat(edges.hasNext()).isTrue();
      final long edgeCount = edges.next().getProperty("cnt");
      assertThat(edgeCount).isGreaterThan(0);
      edges.close();
    });
  }

  // Issue #1625: SELECT with named parameter substituting the bucket name in bucket::param
  @Test
  void selectFromBucketWithNamedParameter() {
    database.transaction(() -> {
      database.command("sql", "CREATE BUCKET Customer_Europe IF NOT EXISTS");
      database.command("sql", "CREATE BUCKET Customer_Americas IF NOT EXISTS");
      database.command("sql", "CREATE DOCUMENT TYPE Customer BUCKET Customer_Europe,Customer_Americas");
      database.command("sql", "INSERT INTO bucket:Customer_Europe CONTENT { firstName: 'Enzo', lastName: 'Ferrari', region: 'Europe' }");
      database.command("sql", "INSERT INTO bucket:Customer_Americas CONTENT { firstName: 'Jack', lastName: 'Tramiel', region: 'Americas' }");

      final Map<String, Object> params = new HashMap<>();
      params.put("bucketName", "Customer_Europe");

      final ResultSet resultSet = database.query("sql", "SELECT FROM bucket::bucketName", params);

      assertThat(resultSet.hasNext()).isTrue();
      final Result record = resultSet.next();
      assertThat((String) record.getProperty("firstName")).isEqualTo("Enzo");
      assertThat((String) record.getProperty("region")).isEqualTo("Europe");
      assertThat(resultSet.hasNext()).isFalse();
    });
  }

  // Issue #1625: SELECT using a positional parameter for the bucket name (bucket:?)
  @Test
  void selectFromBucketWithPositionalParameter() {
    database.transaction(() -> {
      database.command("sql", "CREATE BUCKET Customer_Europe_Pos IF NOT EXISTS");
      database.command("sql", "CREATE BUCKET Customer_Americas_Pos IF NOT EXISTS");
      database.command("sql", "CREATE DOCUMENT TYPE CustomerPos BUCKET Customer_Europe_Pos,Customer_Americas_Pos");
      database.command("sql", "INSERT INTO bucket:Customer_Europe_Pos CONTENT { firstName: 'Enzo', lastName: 'Ferrari', region: 'Europe' }");
      database.command("sql", "INSERT INTO bucket:Customer_Americas_Pos CONTENT { firstName: 'Jack', lastName: 'Tramiel', region: 'Americas' }");

      final Map<String, Object> params = new HashMap<>();
      params.put("0", "Customer_Americas_Pos");

      final ResultSet resultSet = database.query("sql", "SELECT FROM bucket:?", params);

      assertThat(resultSet.hasNext()).isTrue();
      final Result record = resultSet.next();
      assertThat((String) record.getProperty("firstName")).isEqualTo("Jack");
      assertThat((String) record.getProperty("region")).isEqualTo("Americas");
      assertThat(resultSet.hasNext()).isFalse();
    });
  }

  // Issue #1625: SELECT with bucket parameter combined with a WHERE clause
  @Test
  void selectFromBucketParameterWithWhereClause() {
    database.transaction(() -> {
      database.command("sql", "CREATE BUCKET Customer_Europe_Where IF NOT EXISTS");
      database.command("sql", "CREATE DOCUMENT TYPE CustomerWhere BUCKET Customer_Europe_Where");
      database.command("sql", "INSERT INTO bucket:Customer_Europe_Where CONTENT { firstName: 'Enzo', lastName: 'Ferrari', region: 'Europe' }");

      final Map<String, Object> params = new HashMap<>();
      params.put("bucketName", "Customer_Europe_Where");
      params.put("lastName", "Ferrari");

      final ResultSet resultSet = database.query("sql",
          "SELECT FROM bucket::bucketName WHERE lastName = :lastName", params);

      assertThat(resultSet.hasNext()).isTrue();
      final Result record = resultSet.next();
      assertThat((String) record.getProperty("firstName")).isEqualTo("Enzo");
      assertThat(resultSet.hasNext()).isFalse();
    });
  }

  // Issue #1625: INSERT INTO bucket::param substitutes the bucket name from a parameter
  @Test
  void insertIntoBucketWithParameter() {
    database.transaction(() -> {
      database.command("sql", "CREATE BUCKET Customer_Americas_Ins IF NOT EXISTS");
      database.command("sql", "CREATE DOCUMENT TYPE CustomerIns BUCKET Customer_Americas_Ins");

      final Map<String, Object> params = new HashMap<>();
      params.put("bucketName", "Customer_Americas_Ins");
      params.put("firstName", "Steve");
      params.put("lastName", "Jobs");

      database.command("sql",
          "INSERT INTO bucket::bucketName CONTENT { firstName: :firstName, lastName: :lastName }", params);

      final ResultSet resultSet = database.query("sql",
          "SELECT FROM bucket:Customer_Americas_Ins WHERE lastName = 'Jobs'");

      assertThat(resultSet.hasNext()).isTrue();
      final Result record = resultSet.next();
      assertThat((String) record.getProperty("firstName")).isEqualTo("Steve");
      assertThat(resultSet.hasNext()).isFalse();
    });
  }

  // Issue #1898: SELECT with OR mixing an indexed equality and a non-indexed CONTAINS predicate
  @Test
  void selectWithOr() {
    database.transaction(() -> {
      database.command("sqlscript", """
          CREATE VERTEX TYPE Asset IF NOT EXISTS;
          CREATE PROPERTY Asset.id IF NOT EXISTS STRING (mandatory true);
          CREATE PROPERTY Asset.addresses IF NOT EXISTS LIST OF STRING;
          CREATE INDEX IF NOT EXISTS ON Asset (id) UNIQUE;
          """);
    });

    database.transaction(() -> {
      database.command("sqlscript", """
          INSERT INTO Asset CONTENT {"id":"first", "addresses":["192.168.10.10","192.168.20.10"]};
          INSERT INTO Asset CONTENT {"id":"second"};
          INSERT INTO Asset CONTENT {"id":"third"};
          """);
    });

    database.transaction(() -> {
      final ResultSet rs = database.command("SQL", """
          select from Asset WHERE addresses CONTAINS '192.168.10.10'
          """);
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.stream().count()).isEqualTo(1);
    });

    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", """
          select from Asset where
          id='wrong id'
          OR
          addresses CONTAINS '192.168.10.10'
          """);
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.nextIfAvailable().getProperty("id").equals("first")).isTrue();
      assertThat(rs.hasNext()).isFalse();
    });
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", """
          select from Asset where id='first' OR id='second'
          """);
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.stream().count()).isEqualTo(2);
    });
  }

  // Issue #1972: SELECT with LIMIT bound to a LET variable from a sqlscript
  @Test
  void limitWithLetVariable() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc IF NOT EXISTS");
      database.command("sql", "DELETE FROM doc");

      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO doc SET num = " + i);
      }

      final String script = """
          LET $t = 5;
          SELECT FROM doc LIMIT $t;
          """;

      final ResultSet rs = database.command("sqlscript", script);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }
      rs.close();

      assertThat(results).hasSize(5);
    });
  }

  // Issue #1972: SELECT with SKIP bound to a LET variable from a sqlscript
  @Test
  void skipWithLetVariable() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc2 IF NOT EXISTS");
      database.command("sql", "DELETE FROM doc2");

      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO doc2 SET num = " + i);
      }

      final String script = """
          LET $s = 3;
          SELECT FROM doc2 SKIP $s;
          """;

      final ResultSet rs = database.command("sqlscript", script);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }
      rs.close();

      assertThat(results).hasSize(7);
    });
  }

  // Issue #1972: SELECT with LIMIT bound to a named parameter
  @Test
  void limitWithNamedParameter() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc3 IF NOT EXISTS");
      database.command("sql", "DELETE FROM doc3");

      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO doc3 SET num = " + i);
      }

      final Map<String, Object> params = new HashMap<>();
      params.put("limit", 3);

      final ResultSet rs = database.query("sql", "SELECT FROM doc3 LIMIT :limit", params);

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }
      rs.close();

      assertThat(results).hasSize(3);
    });
  }

  // Issue #1972: SELECT with SKIP bound to a named parameter
  @Test
  void skipWithNamedParameter() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc4 IF NOT EXISTS");
      database.command("sql", "DELETE FROM doc4");

      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO doc4 SET num = " + i);
      }

      final Map<String, Object> params = new HashMap<>();
      params.put("skip", 5);

      final ResultSet rs = database.query("sql", "SELECT FROM doc4 SKIP :skip", params);

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }
      rs.close();

      assertThat(results).hasSize(5);
    });
  }

  // Issue #1972: SELECT combining SKIP and LIMIT bound to named parameters
  @Test
  void limitAndSkipWithParameters() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc5 IF NOT EXISTS");
      database.command("sql", "DELETE FROM doc5");

      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO doc5 SET num = " + i);
      }

      final Map<String, Object> params = new HashMap<>();
      params.put("skip", 2);
      params.put("limit", 5);

      final ResultSet rs = database.query("sql", "SELECT FROM doc5 SKIP :skip LIMIT :limit", params);

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }
      rs.close();

      assertThat(results).hasSize(5);
    });
  }

  // Issue #1972: SELECT combining SKIP and LIMIT bound to LET variables in a sqlscript
  @Test
  void limitAndSkipWithLetVariables() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc6 IF NOT EXISTS");
      database.command("sql", "DELETE FROM doc6");

      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO doc6 SET num = " + i);
      }

      final String script = """
          LET $skip = 3;
          LET $limit = 4;
          SELECT FROM doc6 SKIP $skip LIMIT $limit;
          """;

      final ResultSet rs = database.command("sqlscript", script);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }
      rs.close();

      assertThat(results).hasSize(4);
    });
  }

  // Issue #3516: SKIP accepts a named parameter called ':offset' (reserved keyword usable as param name)
  @Test
  void skipWithOffsetParam() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Brewery IF NOT EXISTS");
      database.command("sql", "DELETE FROM Brewery");

      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Brewery SET name = 'Brewery" + i + "'");

      final Map<String, Object> params = new HashMap<>();
      params.put("limit", 25);
      params.put("offset", 0);

      final ResultSet rs = database.query("sql", "select * from Brewery skip :offset limit :limit", params);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());
      rs.close();

      assertThat(results).hasSize(10);
    });
  }

  // Issue #3516: SKIP/LIMIT named params honour non-zero offset values
  @Test
  void skipWithOffsetParamNonZero() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Brewery2 IF NOT EXISTS");
      database.command("sql", "DELETE FROM Brewery2");

      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Brewery2 SET name = 'Brewery" + i + "'");

      final Map<String, Object> params = new HashMap<>();
      params.put("limit", 5);
      params.put("offset", 3);

      final ResultSet rs = database.query("sql", "select * from Brewery2 skip :offset limit :limit", params);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());
      rs.close();

      assertThat(results).hasSize(5);
    });
  }

  // Issue #3516: parser accepts LIMIT before SKIP with both bound to named parameters
  @Test
  void limitOffsetOrder() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Brewery3 IF NOT EXISTS");
      database.command("sql", "DELETE FROM Brewery3");

      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Brewery3 SET name = 'Brewery" + i + "'");

      final Map<String, Object> params = new HashMap<>();
      params.put("limit", 4);
      params.put("offset", 2);

      final ResultSet rs = database.query("sql", "select * from Brewery3 limit :limit skip :offset", params);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());
      rs.close();

      assertThat(results).hasSize(4);
    });
  }

  // Issue #3571: SELECT inE().@rid on a function-call result returns the edge RID list
  @Test
  void inEAtRidUnquoted() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571a IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571a IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571a");
      database.command("sql", "INSERT INTO V3571a");
      database.command("sql", "CREATE EDGE E3571a FROM (SELECT FROM V3571a) TO (SELECT FROM V3571a)");

      final ResultSet rs = database.query("sql", "SELECT inE().@rid FROM V3571a");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object ridList = row.getProperty("inE().@rid");
      assertThat(ridList).isNotNull();
      assertThat(ridList).isInstanceOf(List.class);
      final List<?> rids = (List<?>) ridList;
      assertThat(rids).hasSize(1);
      assertThat(rids.getFirst()).isInstanceOf(RID.class);
      rs.close();
    });
  }

  // Issue #3571: SELECT inE().`@rid` (backtick-quoted) on a function-call result returns the edge RID list
  @Test
  void inEAtRidQuoted() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571b IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571b IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571b");
      database.command("sql", "INSERT INTO V3571b");
      database.command("sql", "CREATE EDGE E3571b FROM (SELECT FROM V3571b) TO (SELECT FROM V3571b)");

      final ResultSet rs = database.query("sql", "SELECT inE().`@rid` FROM V3571b");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object ridList = row.getProperty("inE().@rid");
      assertThat(ridList).isNotNull();
      assertThat(ridList).isInstanceOf(List.class);
      final List<?> rids = (List<?>) ridList;
      assertThat(rids).hasSize(1);
      assertThat(rids.getFirst()).isInstanceOf(RID.class);
      rs.close();
    });
  }

  // Issue #3571: SELECT inE()[0].@rid (indexed) on a function-call result returns the single edge RID
  @Test
  void inEIndexedAtRidUnquoted() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571c IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571c IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571c");
      database.command("sql", "INSERT INTO V3571c");
      database.command("sql", "CREATE EDGE E3571c FROM (SELECT FROM V3571c) TO (SELECT FROM V3571c)");

      final ResultSet rs = database.query("sql", "SELECT inE()[0].@rid FROM V3571c");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object rid = row.getProperty("inE()[0].@rid");
      assertThat(rid).isNotNull();
      assertThat(rid).isInstanceOf(RID.class);
      rs.close();
    });
  }

  // Issue #3571: SELECT inE()[0].`@rid` (indexed, backtick-quoted) returns the single edge RID
  @Test
  void inEIndexedAtRidQuoted() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571d IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571d IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571d");
      database.command("sql", "INSERT INTO V3571d");
      database.command("sql", "CREATE EDGE E3571d FROM (SELECT FROM V3571d) TO (SELECT FROM V3571d)");

      final ResultSet rs = database.query("sql", "SELECT inE()[0].`@rid` FROM V3571d");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object rid = row.getProperty("inE()[0].@rid");
      assertThat(rid).isNotNull();
      assertThat(rid).isInstanceOf(RID.class);
      rs.close();
    });
  }

  // Issue #3571: SELECT inE()[0].@type returns the edge type name
  @Test
  void inEAtType() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571e IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571e IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571e");
      database.command("sql", "INSERT INTO V3571e");
      database.command("sql", "CREATE EDGE E3571e FROM (SELECT FROM V3571e) TO (SELECT FROM V3571e)");

      final ResultSet rs = database.query("sql", "SELECT inE()[0].@type FROM V3571e");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object type = row.getProperty("inE()[0].@type");
      assertThat(type).isNotNull();
      assertThat(type).isEqualTo("E3571e");
      rs.close();
    });
  }

  // Issue #3571: SELECT inE()[0].`@cat` returns 'e' for an edge via the isRecordAttributeName fallback
  @Test
  void inEAtCat() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571f IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571f IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571f");
      database.command("sql", "INSERT INTO V3571f");
      database.command("sql", "CREATE EDGE E3571f FROM (SELECT FROM V3571f) TO (SELECT FROM V3571f)");

      final ResultSet rs = database.query("sql", "SELECT inE()[0].`@cat` FROM V3571f");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object cat = row.getProperty("inE()[0].@cat");
      assertThat(cat).isNotNull();
      assertThat(cat).isEqualTo("e");
      rs.close();
    });
  }

  // Issue #3571: SELECT inE()[0].@in returns the RID of the edge's in-vertex
  @Test
  void inEIndexedAtIn() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571g IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571g IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571g");
      database.command("sql", "INSERT INTO V3571g");
      database.command("sql", "CREATE EDGE E3571g FROM (SELECT FROM V3571g) TO (SELECT FROM V3571g)");

      final ResultSet rs = database.query("sql", "SELECT inE()[0].@in FROM V3571g");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object inRid = row.getProperty("inE()[0].@in");
      assertThat(inRid).isNotNull();
      assertThat(inRid).isInstanceOf(RID.class);
      rs.close();
    });
  }

  // Issue #3571: SELECT inE()[0].@out returns the RID of the edge's out-vertex
  @Test
  void inEIndexedAtOut() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571h IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571h IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571h");
      database.command("sql", "INSERT INTO V3571h");
      database.command("sql", "CREATE EDGE E3571h FROM (SELECT FROM V3571h) TO (SELECT FROM V3571h)");

      final ResultSet rs = database.query("sql", "SELECT inE()[0].@out FROM V3571h");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object outRid = row.getProperty("inE()[0].@out");
      assertThat(outRid).isNotNull();
      assertThat(outRid).isInstanceOf(RID.class);
      rs.close();
    });
  }

  // Issue #3571: SELECT inE().@in (non-indexed) returns a list of in-vertex RIDs
  @Test
  void inEAtInList() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571i IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571i IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571i");
      database.command("sql", "INSERT INTO V3571i");
      database.command("sql", "CREATE EDGE E3571i FROM (SELECT FROM V3571i) TO (SELECT FROM V3571i)");

      final ResultSet rs = database.query("sql", "SELECT inE().@in FROM V3571i");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object inList = row.getProperty("inE().@in");
      assertThat(inList).isNotNull();
      assertThat(inList).isInstanceOf(List.class);
      final List<?> rids = (List<?>) inList;
      assertThat(rids).hasSize(1);
      assertThat(rids.getFirst()).isInstanceOf(RID.class);
      rs.close();
    });
  }

  // Issue #3571: map() built from ine.@rid.asString() keys and ine.@out values resolves internal properties correctly
  @Test
  void mapWithAtOut() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571j IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571j IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571j");
      database.command("sql", "INSERT INTO V3571j");
      database.command("sql", "CREATE EDGE E3571j FROM (SELECT FROM V3571j) TO (SELECT FROM V3571j)");

      final ResultSet rs = database.query("sql",
          "SELECT map(ine.@rid.asString(), ine.@out) FROM (SELECT inE()[0] AS ine FROM V3571j)");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object mapResult = row.getProperty("map(ine.@rid.asString(), ine.@out)");
      assertThat(mapResult).isNotNull();
      assertThat(mapResult).isInstanceOf(Map.class);
      final Map<?, ?> map = (Map<?, ?>) mapResult;
      assertThat(map).hasSize(1);
      assertThat(map.values().iterator().next()).isNotNull();
      assertThat(map.values().iterator().next()).isInstanceOf(RID.class);
      rs.close();
    });
  }

  // Issue #3581: CONTAINSANY accepts a method call (split) on the LHS without $current null errors
  @Test
  void containsAnyWithSplitOnLhs() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc3581 IF NOT EXISTS");
      database.command("sql", "INSERT INTO doc3581 SET txt = 'te st'");
      database.command("sql", "INSERT INTO doc3581 SET txt = 'hello world'");
      database.command("sql", "INSERT INTO doc3581 SET txt = 'no match here'");

      final ResultSet rs = database.query("sql", "SELECT FROM doc3581 WHERE txt.split(' ') CONTAINSANY 'te'");
      assertThat(rs.hasNext()).isTrue();
      final var result = rs.next();
      assertThat(result.<String>getProperty("txt")).isEqualTo("te st");
      assertThat(rs.hasNext()).isFalse();
    });
  }

  // Issue #3581: CONTAINSANY with split() LHS returns multiple matches when several rows qualify
  @Test
  void containsAnyWithSplitOnLhsMultipleMatches() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc3581b IF NOT EXISTS");
      database.command("sql", "INSERT INTO doc3581b SET txt = 'te st'");
      database.command("sql", "INSERT INTO doc3581b SET txt = 'te other'");
      database.command("sql", "INSERT INTO doc3581b SET txt = 'no match'");

      final ResultSet rs = database.query("sql", "SELECT FROM doc3581b WHERE txt.split(' ') CONTAINSANY 'te'");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  // Issue #3581: CONTAINSANY with split() LHS returns no rows when nothing matches
  @Test
  void containsAnyWithSplitOnLhsNoMatch() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc3581c IF NOT EXISTS");
      database.command("sql", "INSERT INTO doc3581c SET txt = 'hello world'");

      final ResultSet rs = database.query("sql", "SELECT FROM doc3581c WHERE txt.split(' ') CONTAINSANY 'te'");
      assertThat(rs.hasNext()).isFalse();
    });
  }

  // Issue #3581: CONTAINSANY with split() LHS treats missing field rows as non-matches without crashing
  @Test
  void containsAnyWithSplitOnLhsNullField() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc3581d IF NOT EXISTS");
      database.command("sql", "INSERT INTO doc3581d SET txt = 'te st'");
      database.command("sql", "INSERT INTO doc3581d SET other = 'no txt field'");

      final ResultSet rs = database.query("sql", "SELECT FROM doc3581d WHERE txt.split(' ') CONTAINSANY 'te'");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("txt")).isEqualTo("te st");
      assertThat(rs.hasNext()).isFalse();
    });
  }

  // Issue #3581: CONTAINSALL with a method call (split) on the LHS evaluates without $current null errors
  @Test
  void containsAllWithSplitOnLhs() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc3581e IF NOT EXISTS");
      database.command("sql", "INSERT INTO doc3581e SET txt = 'te st'");
      database.command("sql", "INSERT INTO doc3581e SET txt = 'te other'");

      final ResultSet rs = database.query("sql", "SELECT FROM doc3581e WHERE txt.split(' ') CONTAINSALL 'te'");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  // Issue #3583: chained replace().ilike() inside a LET subquery combined with UNIONALL evaluates without $current null
  @Test
  void replaceWithIlikeInLetSubquery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE NER IF NOT EXISTS");
      database.command("sql", "CREATE DOCUMENT TYPE THEME IF NOT EXISTS");

      database.command("sql", "INSERT INTO NER SET identity = ?", "hello\nworld");
      database.command("sql", "INSERT INTO NER SET identity = ?", "foo\tbar");
      database.command("sql", "INSERT INTO THEME SET identity = ?", "hello\tworld");
      database.command("sql", "INSERT INTO THEME SET identity = ?", "baz qux");

      final Map<String, Object> params = new HashMap<>();
      params.put("keyWordIdentifier_0", "hello");
      params.put("keyWordIdentifier_1", "world");

      final ResultSet rs = database.query("sql",
          """
          SELECT expand($c) \
          LET \
            $a = (SELECT identity, @rid as id FROM NER \
              WHERE identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_0 + '%') \
                AND identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_1 + '%')), \
            $b = (SELECT identity, @rid as id FROM THEME \
              WHERE identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_0 + '%') \
                AND identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_1 + '%')), \
            $c = UNIONALL($a, $b)""",
          params);

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      assertThat(results).hasSize(2);
    });
  }

  // Issue #3583: chained replace().ilike() inside a top-level WHERE clause works for ScanWithFilterStep
  @Test
  void replaceMethodCallInWhereClause() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TestReplace IF NOT EXISTS");
      database.command("sql", "INSERT INTO TestReplace SET identity = ?", "hello\nworld");
      database.command("sql", "INSERT INTO TestReplace SET identity = 'no match'");

      final ResultSet rs = database.query("sql",
          "SELECT identity FROM TestReplace WHERE identity.replace('\\n', ' ') ILIKE '%hello world%'");

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      assertThat(results).hasSize(1);
      assertThat(results.get(0).<String>getProperty("identity")).isEqualTo("hello\nworld");
    });
  }

  // Issue #3583: chained replace().ilike() works against an indexed property (FilterStep path)
  @Test
  void replaceWithIlikeAndIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE IndexedType IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY IndexedType.identity IF NOT EXISTS STRING");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON IndexedType (identity) NOTUNIQUE");

      database.command("sql", "INSERT INTO IndexedType SET identity = ?", "hello\nworld");
      database.command("sql", "INSERT INTO IndexedType SET identity = ?", "foo\tbar");
      database.command("sql", "INSERT INTO IndexedType SET identity = 'no match'");

      final ResultSet rs = database.query("sql",
          """
          SELECT identity FROM IndexedType \
          WHERE identity.replace('\\n', ' ').replace('\\t', ' ') ILIKE '%hello world%'""");

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      assertThat(results).hasSize(1);
      assertThat(results.get(0).<String>getProperty("identity")).isEqualTo("hello\nworld");
    });
  }

  // Issue #3583: chained replace().ilike() in a LET subquery still works when the underlying property is indexed
  @Test
  void replaceWithIlikeInLetSubqueryWithIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE NER2 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY NER2.identity IF NOT EXISTS STRING");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON NER2 (identity) NOTUNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE THEME2 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY THEME2.identity IF NOT EXISTS STRING");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON THEME2 (identity) NOTUNIQUE");

      database.command("sql", "INSERT INTO NER2 SET identity = ?", "hello\nworld");
      database.command("sql", "INSERT INTO NER2 SET identity = ?", "foo\tbar");
      database.command("sql", "INSERT INTO THEME2 SET identity = ?", "hello\tworld");
      database.command("sql", "INSERT INTO THEME2 SET identity = ?", "baz qux");

      final Map<String, Object> params = new HashMap<>();
      params.put("keyWordIdentifier_0", "hello");
      params.put("keyWordIdentifier_1", "world");

      final ResultSet rs = database.query("sql",
          """
          SELECT expand($c) \
          LET \
            $a = (SELECT identity, @rid as id FROM NER2 \
              WHERE identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_0 + '%') \
                AND identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_1 + '%')), \
            $b = (SELECT identity, @rid as id FROM THEME2 \
              WHERE identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_0 + '%') \
                AND identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_1 + '%')), \
            $c = UNIONALL($a, $b)""",
          params);

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      assertThat(results).hasSize(2);
    });
  }

  // Regression: CONTAINSALL applies type-aware equality so RID strings match Identifiables in the LHS list
  @Test
  void containsAllMatchesRidStringsAgainstIdentifiables() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Beer IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX TYPE Brewery IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE HasBrewery IF NOT EXISTS");

      final RID breweryRid = database.command("sql", "CREATE VERTEX Brewery SET name = 'Magic Hat'")
          .next().getIdentity().orElseThrow();

      database.command("sql", "CREATE VERTEX Beer SET name = 'Hocus Pocus'");
      database.command("sql",
          "CREATE EDGE HasBrewery FROM (SELECT FROM Beer WHERE name = 'Hocus Pocus') TO " + breweryRid);

      final String breweryRidStr = breweryRid.toString();

      try (final ResultSet rs = database.query("sql",
          "SELECT FROM (SELECT *, outE('HasBrewery').@in AS brewery FROM Beer WHERE name='Hocus Pocus') "
              + "WHERE brewery CONTAINSALL [\"" + breweryRidStr + "\"]")) {
        assertThat(rs.hasNext())
            .as("CONTAINSALL with a list of RID strings should match the inline 'brewery' list of vertices")
            .isTrue();
      }

      try (final ResultSet rs = database.query("sql",
          "SELECT FROM (SELECT *, outE('HasBrewery').@in AS brewery FROM Beer WHERE name='Hocus Pocus') "
              + "WHERE brewery CONTAINSALL " + breweryRidStr)) {
        assertThat(rs.hasNext())
            .as("CONTAINSALL with a single RID literal should match the inline 'brewery' list of vertices")
            .isTrue();
      }
    });
  }

  // Regression: CONTAINSALL with RID strings returns false when any required RID is absent from the LHS list
  @Test
  void containsAllReturnsFalseWhenRidStringIsMissing() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE BeerMissing IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX TYPE BreweryMissing IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE HasBreweryMissing IF NOT EXISTS");

      final RID linkedBreweryRid = database.command("sql", "CREATE VERTEX BreweryMissing SET name = 'LinkedOne'")
          .next().getIdentity().orElseThrow();
      final RID otherBreweryRid = database.command("sql", "CREATE VERTEX BreweryMissing SET name = 'OtherOne'")
          .next().getIdentity().orElseThrow();

      database.command("sql", "CREATE VERTEX BeerMissing SET name = 'PlainBeer'");
      database.command("sql",
          "CREATE EDGE HasBreweryMissing FROM (SELECT FROM BeerMissing WHERE name = 'PlainBeer') TO " + linkedBreweryRid);

      try (final ResultSet rs = database.query("sql",
          "SELECT FROM (SELECT *, outE('HasBreweryMissing').@in AS brewery FROM BeerMissing WHERE name='PlainBeer') "
              + "WHERE brewery CONTAINSALL [\"" + linkedBreweryRid + "\", \"" + otherBreweryRid + "\"]")) {
        assertThat(rs.hasNext())
            .as("CONTAINSALL must require every right-hand RID to be present in the brewery list")
            .isFalse();
      }
    });
  }

  // Regression: link.field projection resolves sub-properties on LINK columns across multiple dot-notations
  @Test
  void linkDotNotationProjection() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE AppUser IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX TYPE Product IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY AppUser.Email IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product.Name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product.CreatedBy IF NOT EXISTS LINK OF AppUser");
      database.command("sql", "CREATE PROPERTY Product.Owner IF NOT EXISTS LINK OF AppUser");
      database.command("sql", "CREATE PROPERTY Product.ModifiedBy IF NOT EXISTS LINK OF AppUser");

      database.command("sql", "INSERT INTO AppUser SET Email = 'alice@example.com'");
      database.command("sql", "INSERT INTO AppUser SET Email = 'bob@example.com'");
      database.command("sql", "INSERT INTO AppUser SET Email = 'charlie@example.com'");

      database.command("sql",
          """
          INSERT INTO Product SET Name = 'TestProduct', \
          CreatedBy = (SELECT FROM AppUser WHERE Email = 'alice@example.com'), \
          Owner = (SELECT FROM AppUser WHERE Email = 'bob@example.com'), \
          ModifiedBy = (SELECT FROM AppUser WHERE Email = 'charlie@example.com')""");

      try (final ResultSet rs = database.query("sql",
          "SELECT CreatedBy.Email as _CreatedBy_Email FROM Product")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<String>getProperty("_CreatedBy_Email")).isEqualTo("alice@example.com");
      }

      try (final ResultSet rs = database.query("sql",
          """
          SELECT CreatedBy.Email as _CreatedBy_Email, \
          ModifiedBy.Email as _ModifiedBy_Email, \
          Owner.Email as _Owner_Email FROM Product""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<String>getProperty("_CreatedBy_Email")).isEqualTo("alice@example.com");
        assertThat(row.<String>getProperty("_ModifiedBy_Email")).isEqualTo("charlie@example.com");
        assertThat(row.<String>getProperty("_Owner_Email")).isEqualTo("bob@example.com");
      }
    });
  }

  // Regression: link.field projection on a null LINK column returns null instead of throwing ClassCastException
  @Test
  void linkDotNotationWithNullLink() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE AppUser2 IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX TYPE Product2 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY AppUser2.Email IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product2.Name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product2.CreatedBy IF NOT EXISTS LINK OF AppUser2");

      database.command("sql", "INSERT INTO Product2 SET Name = 'TestProduct'");

      try (final ResultSet rs = database.query("sql",
          "SELECT CreatedBy.Email as _CreatedBy_Email FROM Product2")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<String>getProperty("_CreatedBy_Email")).isNull();
      }
    });
  }

  // Regression: link.field projection works against vertex-typed LINK columns
  @Test
  void linkDotNotationWithVertexLink() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE UserVertex IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX TYPE ProductVertex IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY UserVertex.Email IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY ProductVertex.Name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY ProductVertex.CreatedBy IF NOT EXISTS LINK OF UserVertex");
      database.command("sql", "CREATE PROPERTY ProductVertex.Owner IF NOT EXISTS LINK OF UserVertex");

      database.command("sql", "INSERT INTO UserVertex SET Email = 'alice@test.com'");
      database.command("sql", "INSERT INTO UserVertex SET Email = 'bob@test.com'");

      database.command("sql",
          """
          INSERT INTO ProductVertex SET Name = 'TestProduct', \
          CreatedBy = (SELECT FROM UserVertex WHERE Email = 'alice@test.com'), \
          Owner = (SELECT FROM UserVertex WHERE Email = 'bob@test.com')""");

      try (final ResultSet rs = database.query("sql",
          """
          SELECT CreatedBy.Email as _CreatedBy_Email, \
          Owner.Email as _Owner_Email FROM ProductVertex""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<String>getProperty("_CreatedBy_Email")).isEqualTo("alice@test.com");
        assertThat(row.<String>getProperty("_Owner_Email")).isEqualTo("bob@test.com");
      }
    });
  }

  // Regression: link.field projection survives a LET + UNIONALL pipeline mirroring real client queries
  @Test
  void linkDotNotationInLetSubquery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE AppUser3 IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX TYPE Product3 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY AppUser3.Email IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product3.Name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product3.CreatedBy IF NOT EXISTS LINK OF AppUser3");
      database.command("sql", "CREATE PROPERTY Product3.Owner IF NOT EXISTS LINK OF AppUser3");
      database.command("sql", "CREATE PROPERTY Product3.ModifiedBy IF NOT EXISTS LINK OF AppUser3");

      database.command("sql", "INSERT INTO AppUser3 SET Email = 'alice@example.com'");
      database.command("sql", "INSERT INTO AppUser3 SET Email = 'bob@example.com'");

      database.command("sql",
          """
          INSERT INTO Product3 SET Name = 'TestProduct', \
          CreatedBy = (SELECT FROM AppUser3 WHERE Email = 'alice@example.com'), \
          Owner = (SELECT FROM AppUser3 WHERE Email = 'bob@example.com'), \
          ModifiedBy = (SELECT FROM AppUser3 WHERE Email = 'alice@example.com')""");

      try (final ResultSet rs = database.query("sql",
          """
          SELECT ($c) LET \
          $a = (SELECT count(Name) FROM Product3), \
          $b = (SELECT *, CreatedBy.Email as _CreatedBy_Email, \
            ModifiedBy.Email as _ModifiedBy_Email, \
            Owner.Email as _Owner_Email FROM Product3), \
          $c = UNIONALL($a, $b) LIMIT -1""")) {
        assertThat(rs.hasNext()).isTrue();
      }
    });
  }

  // Regression: link.field projection with a mix of set and null LINK columns does not throw ClassCastException
  @Test
  void linkDotNotationMixedNullAndNonNull() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE AppUser4 IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX TYPE Product4 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY AppUser4.Email IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product4.Name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product4.CreatedBy IF NOT EXISTS LINK OF AppUser4");
      database.command("sql", "CREATE PROPERTY Product4.Owner IF NOT EXISTS LINK OF AppUser4");

      database.command("sql", "INSERT INTO AppUser4 SET Email = 'alice@example.com'");

      database.command("sql",
          """
          INSERT INTO Product4 SET Name = 'TestProduct', \
          CreatedBy = (SELECT FROM AppUser4 WHERE Email = 'alice@example.com')""");

      try (final ResultSet rs = database.query("sql",
          """
          SELECT CreatedBy.Email as _CreatedBy_Email, \
          Owner.Email as _Owner_Email FROM Product4""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<String>getProperty("_CreatedBy_Email")).isEqualTo("alice@example.com");
        assertThat(row.<String>getProperty("_Owner_Email")).isNull();
      }
    });
  }
}
