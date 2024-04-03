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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

public class QueryTest extends TestHelper {
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
  public void testScan() {

    database.transaction(() -> {
      final ResultSet rs = database.command("SQL", "SELECT FROM V", new HashMap<>());

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        Assertions.assertNotNull(record);

        final Set<String> prop = new HashSet<>();
        prop.addAll(record.getPropertyNames());

        Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
        Assertions.assertTrue(prop.contains("id"));
        Assertions.assertTrue(prop.contains("name"));
        Assertions.assertTrue(prop.contains("surname"));

        total.incrementAndGet();
      }

      Assertions.assertEquals(TOT, total.get());
    });
  }

  @Test
  public void testEqualsFiltering() {

    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":name", "Jay");
      params.put(":surname", "Miner123");
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE name = :name AND surname = :surname", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        Assertions.assertNotNull(record);

        Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
        Assertions.assertEquals(123, (int) record.getProperty("id"));
        Assertions.assertEquals("Jay", record.getProperty("name"));
        Assertions.assertEquals("Miner123", record.getProperty("surname"));

        total.incrementAndGet();
      }

      Assertions.assertEquals(1, total.get());
    });
  }

  @Test
  public void testNullSafeEqualsFiltering() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":name", "Jay");
      params.put(":surname", "Miner123");
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE notExistent <=> null");

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        Assertions.assertNotNull(record);

        Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
        total.incrementAndGet();
      }

      Assertions.assertEquals(TOT, total.get());
    });
  }

  @Test
  public void testCachedStatementAndExecutionPlan() {

    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":name", "Jay");
      params.put(":surname", "Miner123");
      ResultSet rs = database.command("SQL", "SELECT FROM V WHERE name = :name AND surname = :surname", params);

      AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        Assertions.assertNotNull(record);

        Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
        Assertions.assertEquals(123, (int) record.getProperty("id"));
        Assertions.assertEquals("Jay", record.getProperty("name"));
        Assertions.assertEquals("Miner123", record.getProperty("surname"));

        total.incrementAndGet();
      }

      Assertions.assertEquals(1, total.get());

      // CHECK STATEMENT CACHE
      Assertions.assertTrue(
          ((DatabaseInternal) database).getStatementCache().contains("SELECT FROM V WHERE name = :name AND surname = :surname"));

      // CHECK EXECUTION PLAN CACHE
      Assertions.assertTrue(((DatabaseInternal) database).getExecutionPlanCache()
          .contains("SELECT FROM V WHERE name = :name AND surname = :surname"));

      // EXECUTE THE 2ND TIME
      rs = database.command("SQL", "SELECT FROM V WHERE name = :name AND surname = :surname", params);

      total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        Assertions.assertNotNull(record);

        Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
        Assertions.assertEquals(123, (int) record.getProperty("id"));
        Assertions.assertEquals("Jay", record.getProperty("name"));
        Assertions.assertEquals("Miner123", record.getProperty("surname"));

        total.incrementAndGet();
      }

      Assertions.assertEquals(1, total.get());
    });
  }

  @Test
  public void testMajorFiltering() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":id", TOT - 11);
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE id > :id", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        Assertions.assertNotNull(record);
        Assertions.assertTrue((int) record.getProperty("id") > TOT - 11);
        total.incrementAndGet();
      }
      Assertions.assertEquals(10, total.get());
    });
  }

  @Test
  public void testMajorEqualsFiltering() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":id", TOT - 11);
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE id >= :id", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        Assertions.assertNotNull(record);
        Assertions.assertTrue((int) record.getProperty("id") >= TOT - 11);
        total.incrementAndGet();
      }
      Assertions.assertEquals(11, total.get());
    });
  }

  @Test
  public void testMinorFiltering() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":id", 10);
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE id < :id", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        Assertions.assertNotNull(record);
        Assertions.assertTrue((int) record.getProperty("id") < 10);
        total.incrementAndGet();
      }
      Assertions.assertEquals(10, total.get());
    });
  }

  @Test
  public void testMinorEqualsFiltering() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":id", 10);
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE id <= :id", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        Assertions.assertNotNull(record);
        Assertions.assertTrue((int) record.getProperty("id") <= 10);
        total.incrementAndGet();
      }
      Assertions.assertEquals(11, total.get());
    });
  }

  @Test
  public void testNotFiltering() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":id", 10);
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE NOT( id > :id )", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        Assertions.assertNotNull(record);
        Assertions.assertTrue((int) record.getProperty("id") <= 10);
        total.incrementAndGet();
      }
      Assertions.assertEquals(11, total.get());
    });
  }

  @Test
  public void testCreateVertexType() {
    database.transaction(() -> {
      database.command("SQL", "CREATE VERTEX TYPE Foo");
      database.command("SQL", "CREATE VERTEX Foo SET name = 'foo'");
      database.command("SQL", "CREATE VERTEX Foo SET name = 'bar'");

      final ResultSet rs = database.query("SQL", "SELECT FROM Foo");
      Assertions.assertTrue(rs.hasNext());
      rs.next();
      Assertions.assertTrue(rs.hasNext());
      rs.next();
      Assertions.assertFalse(rs.hasNext());

      rs.close();
    });
  }

  @Test
  public void testCreateEdge() {
    database.transaction(() -> {
      database.command("SQL", "CREATE VERTEX TYPE Foo");
      database.command("SQL", "CREATE EDGE TYPE TheEdge");
      database.command("SQL", "CREATE VERTEX Foo SET name = 'foo'");
      database.command("SQL", "CREATE VERTEX Foo SET name = 'bar'");
      database.command("SQL",
          "CREATE EDGE TheEdge FROM (SELECT FROM Foo WHERE name ='foo') TO (SELECT FROM Foo WHERE name ='bar')");

      final ResultSet rs = database.query("SQL", "SELECT FROM TheEdge");
      Assertions.assertTrue(rs.hasNext());
      rs.next();
      Assertions.assertFalse(rs.hasNext());

      rs.close();
    });
  }

  @Test
  public void testQueryEdge() {
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
      Assertions.assertTrue(rs.hasNext());
      rs.next();
      Assertions.assertFalse(rs.hasNext());

      rs.close();

      rs = database.query("SQL", "SELECT out()[0].name as name from " + vertexClass + " where name = 'foo'");
      Assertions.assertTrue(rs.hasNext());
      final Result item = rs.next();
      final String name = item.getProperty("name");
      Assertions.assertTrue(name.contains("bar"));
      Assertions.assertFalse(rs.hasNext());

      rs.close();
    });
  }

  @Test
  public void testMethod() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", "SELECT 'bar'.prefix('foo') as name");
      Assertions.assertTrue(rs.hasNext());
      Assertions.assertEquals("foobar", rs.next().getProperty("name"));

      Assertions.assertFalse(rs.hasNext());

      rs.close();
    });
  }

  @Test
  public void testMatch() {
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
      Assertions.assertTrue(rs.hasNext());
      rs.next();
      Assertions.assertFalse(rs.hasNext());

      rs.close();

      rs = database.query("SQL", "MATCH {type:" + vertexClass + ", as:a} -" + edgeClass + "->{}  RETURN $patterns");
      rs.getExecutionPlan().get().prettyPrint(0, 2);
      Assertions.assertTrue(rs.hasNext());
      final Result item = rs.next();
      Assertions.assertFalse(rs.hasNext());

      rs.close();
    });
  }

  @Test
  public void testAnonMatch() {
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
      Assertions.assertTrue(rs.hasNext());
      rs.next();
      Assertions.assertFalse(rs.hasNext());

      rs.close();

      rs = database.query("SQL", "MATCH {type:" + vertexClass + ", as:a} --> {}  RETURN $patterns");
      rs.getExecutionPlan().get().prettyPrint(0, 2);
      Assertions.assertTrue(rs.hasNext());
      final Result item = rs.next();
      Assertions.assertFalse(rs.hasNext());

      rs.close();
    });
  }

  @Test
  public void testLike() {
    database.transaction(() -> {
      ResultSet rs = database.command("SQL", "SELECT FROM V WHERE surname LIKE '%in%' LIMIT 1");
      Assertions.assertTrue(rs.hasNext());
      rs.next();
      Assertions.assertFalse(rs.hasNext());
      rs.close();

      rs = database.command("SQL", "SELECT FROM V WHERE surname LIKE 'Mi?er%' LIMIT 1");
      Assertions.assertTrue(rs.hasNext());
      rs.next();
      Assertions.assertFalse(rs.hasNext());
      rs.close();

      rs = database.command("SQL", "SELECT FROM V WHERE surname LIKE 'baz' LIMIT 1");
      Assertions.assertFalse(rs.hasNext());
      rs.close();

    });
  }

  // Issue https://github.com/ArcadeData/arcadedb/issues/603
  @Test
  public void testLikeEncoding() {
    database.transaction(() -> {
      database.command("SQL", "insert into V set age = '10%'");
      database.command("SQL", "insert into V set age = '100%'");

      ResultSet rs = database.command("SQL", "SELECT FROM V WHERE age LIKE '10\\%'");
      Assertions.assertTrue(rs.hasNext());
      Result result = rs.next();
      Assertions.assertEquals("10%", result.getProperty("age"));
      Assertions.assertFalse(rs.hasNext());
      rs.close();

      rs = database.command("SQL", "SELECT FROM V WHERE age LIKE \"10%\" ORDER BY age");
      Assertions.assertTrue(rs.hasNext());
      result = rs.next();
      Assertions.assertEquals("10%", result.getProperty("age"));
      Assertions.assertTrue(rs.hasNext());
      result = rs.next();
      Assertions.assertEquals("100%", result.getProperty("age"));
      Assertions.assertFalse(rs.hasNext());
      rs.close();
    });
  }

  @Test
  public void testTimeout() {
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
        Assertions.fail("Timeout should be triggered");
      } catch (final TimeoutException e) {
        // OK
      }
    });
  }

  /**
   * Test case for issue https://github.com/ArcadeData/arcadedb/issues/725
   */
  @Test
  public void testOrderByRID() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", "SELECT @rid, name FROM V order by @rid asc LIMIT 2");

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        Assertions.assertNotNull(record);

        final Set<String> prop = new HashSet<>();
        prop.addAll(record.getPropertyNames());

        Assertions.assertEquals(2, record.getPropertyNames().size());
        Assertions.assertTrue(prop.contains("@rid"));
        Assertions.assertTrue(prop.contains("name"));

        total.incrementAndGet();
      }

      Assertions.assertEquals(2, total.get());
    });
  }

  @Test
  public void testFlattenWhereCondition() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("test");
      String query = "SELECT FROM test WHERE (";
      for (int i = 1; i < 10; i++) {
        if (i > 1)
          query += " and ";
        query += "((property" + i + " is null) or (property" + i + " = #107:150))";
      }
      query += ")";

      try (ResultSet set = database.query("sql", query)) {
        Assertions.assertEquals(set.stream().count(), 0);
      }
    });
  }

  @Test
  public void testCollectionsInProjections() {
    try (ResultSet set = database.query("sql", "SELECT [\"a\",\"b\",\"c\"] as coll")) {
      Collection coll = set.nextIfAvailable().getProperty("coll");
      Assertions.assertEquals(3, coll.size());
      Assertions.assertTrue(coll.contains("a"));
      Assertions.assertTrue(coll.contains("b"));
      Assertions.assertTrue(coll.contains("c"));
    }
  }

  @Test
  public void testCollectionsInProjectionsContains() {
    try (ResultSet set = database.query("sql", "SELECT ([\"a\",\"b\",\"c\"] CONTAINS (@this ILIKE \"C\")) as coll")) {
      final Object coll = set.nextIfAvailable().getProperty("coll");
      Assertions.assertTrue((Boolean) coll);
    }
  }

  @Test
  public void testCollectionsOfObjectsInProjectionsContains() {
    try (ResultSet set = database.query("sql",
        "SELECT ([{\"x\":\"a\"},{\"x\":\"b\"},{\"x\":\"c\"}] CONTAINS (x ILIKE \"C\")) as coll")) {
      final Object coll = set.nextIfAvailable().getProperty("coll");
      Assertions.assertTrue((Boolean) coll);
    }
  }

}
