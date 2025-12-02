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
}
