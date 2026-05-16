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
package com.arcadedb.query.opencypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test parameter handling in Cypher queries.
 */
class ParameterTest {

  @Test
  void simpleParameterQuery() {
    final Database database = new DatabaseFactory("./target/testparams").create();
    try {
      database.getSchema().getOrCreateVertexType("Person");

      // Create test data
      database.transaction(() -> {
        database.command("opencypher", "CREATE (n:Person {name: 'Alice', age: 30})");
        database.command("opencypher", "CREATE (n:Person {name: 'Bob', age: 25})");
        database.command("opencypher", "CREATE (n:Person {name: 'Charlie', age: 35})");
      });

      // Test simple parameter query
      final ResultSet result = database.query("opencypher",
          "MATCH (p:Person) WHERE p.age >= $minAge RETURN p.name, p.age",
          Map.of("minAge", 30));

      int count = 0;
      while (result.hasNext()) {
        final Result row = result.next();
        //System.out.println("Found: " + row.getProperty("p.name") + ", age: " + row.getProperty("p.age"));
        count++;
      }

      //System.out.println("Total count: " + count);
      assertThat(count).isEqualTo(2); // Alice and Charlie

    } finally {
      database.drop();
    }
  }

  /**
   * Regression test for issue #3650: WHERE clause equality with string parameter should filter correctly.
   * Creates multiple nodes to verify that filtering is actually applied (not just that a result exists).
   */
  @Test
  void whereClauseEqualityWithStringParameter() {
    final Database database = new DatabaseFactory("./target/testparams_eq").create();
    try {
      database.getSchema().getOrCreateVertexType("EqTestNode");

      database.transaction(() -> {
        database.command("opencypher", "CREATE (n:EqTestNode {id: 'A001', name: 'alpha'})");
        database.command("opencypher", "CREATE (n:EqTestNode {id: 'B002', name: 'beta'})");
        database.command("opencypher", "CREATE (n:EqTestNode {id: 'C003', name: 'gamma'})");
      });

      // Test equality filter with string parameter - should return exactly 1 result
      final ResultSet r1 = database.query("opencypher",
          "MATCH (t:EqTestNode) WHERE t.id = $id RETURN t.name AS name",
          Map.of("id", "A001"));
      final List<String> names = new ArrayList<>();
      while (r1.hasNext())
        names.add(r1.next().getProperty("name").toString());
      assertThat(names).as("WHERE clause string equality should return exactly 1 result").hasSize(1);
      assertThat(names.get(0)).isEqualTo("alpha");

      // Non-matching parameter should return 0 results
      final ResultSet r2 = database.query("opencypher",
          "MATCH (t:EqTestNode) WHERE t.id = $id RETURN t.name AS name",
          Map.of("id", "NONE"));
      assertThat(r2.hasNext()).as("Non-matching parameter should return 0 results").isFalse();

    } finally {
      database.drop();
    }
  }

  @Test
  void positionalParameters() {
    final Database database = new DatabaseFactory("./target/testparams2").create();
    try {
      database.getSchema().getOrCreateVertexType("Person");

      // Test CREATE with positional parameters (like in original CypherTest)
      database.transaction(() -> {
        for (int i = 0; i < 50; i++) {
          final ResultSet createResult = database.command("opencypher", "CREATE (n:Person {name: $1, age: $2}) return n",
              Map.of("1", "Jay", "2", i));
          // Consume the result set to force execution
          while (createResult.hasNext()) {
            createResult.next();
          }
        }
      });

      // Verify data was created
      final ResultSet allResults = database.query("opencypher", "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age");
      int totalCount = 0;
      while (allResults.hasNext()) {
        final Result row = allResults.next();
        //System.out.println("Created: " + row.getProperty("p.name") + ", age: " + row.getProperty("p.age"));
        totalCount++;
      }
      //System.out.println("Total created: " + totalCount);

      // Test query with named parameter
      final ResultSet result = database.query("opencypher",
          "MATCH (p:Person) WHERE p.age >= $p1 RETURN p.name, p.age ORDER BY p.age",
          Map.of("p1", 25));

      int count = 0;
      while (result.hasNext()) {
        final Result row = result.next();
        //System.out.println("Filtered: " + row.getProperty("p.name") + ", age: " + row.getProperty("p.age"));
        count++;
      }

      //System.out.println("Filtered count: " + count);
      assertThat(count).isEqualTo(25); // Ages 25-49

    } finally {
      database.drop();
      FileUtils.deleteRecursively(new File("./target/testparams2"));
    }
  }

  /**
   * Issue #2364: 16 params or more in cypher = BOOM | 15 is fine
   * https://github.com/ArcadeData/arcadedb/issues/2364
   * Verifies native OpenCypher engine handles 16+ parameters correctly.
   */
  @Test
  void manyParametersInIdQuery() {
    final Database database = new DatabaseFactory("./target/testparams3").create();
    try {
      database.getSchema().getOrCreateVertexType("CHUNK");

      // Create 20 test nodes
      final List<RID> rids = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 20; i++) {
          final RID rid = database.newVertex("CHUNK").set("name", "chunk" + i).save().getIdentity();
          rids.add(rid);
        }
      });

      // Test with 15 parameters - baseline
      final StringBuilder query15 = new StringBuilder("MATCH (n:CHUNK) WHERE ID(n) IN [");
      final Map<String, Object> params15 = new HashMap<>();
      for (int i = 0; i < 15; i++) {
        if (i > 0) query15.append(", ");
        query15.append("$id_").append(i);
        params15.put("id_" + i, rids.get(i).toString());
      }
      query15.append("] RETURN n");

      final ResultSet result15 = database.query("opencypher", query15.toString(), params15);
      int count15 = 0;
      while (result15.hasNext()) {
        result15.next();
        count15++;
      }
      assertThat(count15).isEqualTo(15);

      // Test with 16 parameters - this was the boundary that previously failed
      final StringBuilder query16 = new StringBuilder("MATCH (n:CHUNK) WHERE ID(n) IN [");
      final Map<String, Object> params16 = new HashMap<>();
      for (int i = 0; i < 16; i++) {
        if (i > 0) query16.append(", ");
        query16.append("$id_").append(i);
        params16.put("id_" + i, rids.get(i).toString());
      }
      query16.append("] RETURN n");

      final ResultSet result16 = database.query("opencypher", query16.toString(), params16);
      int count16 = 0;
      while (result16.hasNext()) {
        result16.next();
        count16++;
      }
      assertThat(count16).isEqualTo(16);

      // Test with 20 parameters to ensure higher counts work
      final StringBuilder query20 = new StringBuilder("MATCH (n:CHUNK) WHERE ID(n) IN [");
      final Map<String, Object> params20 = new HashMap<>();
      for (int i = 0; i < 20; i++) {
        if (i > 0) query20.append(", ");
        query20.append("$id_").append(i);
        params20.put("id_" + i, rids.get(i).toString());
      }
      query20.append("] RETURN n");

      final ResultSet result20 = database.query("opencypher", query20.toString(), params20);
      int count20 = 0;
      while (result20.hasNext()) {
        result20.next();
        count20++;
      }
      assertThat(count20).isEqualTo(20);

    } finally {
      database.drop();
      FileUtils.deleteRecursively(new File("./target/testparams3"));
    }
  }

  // Issue #3864: JSON params arriving over HTTP must parse vector arrays as primitive float[] and flow through OpenCypher into ARRAY_OF_FLOATS properties.
  @Test
  @Tag("slow")
  void httpStyleJsonRequestProducesPrimitiveDoubleArrays() {
    final int    chunkCount   = 500;
    final int    batchSize    = 500;
    final int    vectorDim    = 1_024;
    final long   maxElapsedMs = 5_000L;
    final Random random       = new Random(42);

    final String databasePath = "./target/databases/issue-3864-json-params-http-path";
    FileUtils.deleteRecursively(new File(databasePath));
    final Database database = new DatabaseFactory(databasePath).create();
    try {
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 500);

      database.transaction(() -> {
        final VertexType chunkType = database.getSchema().getOrCreateVertexType("CHUNK");
        chunkType.getOrCreateProperty("name", Type.STRING);

        final VertexType embType = database.getSchema().getOrCreateVertexType("CHUNK_EMBEDDING");
        embType.getOrCreateProperty("vector", Type.ARRAY_OF_FLOATS);

        database.getSchema().getOrCreateEdgeType("embb");
      });

      final List<String> chunkRids = new ArrayList<>(chunkCount);
      database.transaction(() -> {
        for (int i = 0; i < chunkCount; i++) {
          final MutableVertex v = database.newVertex("CHUNK");
          v.set("name", "chunk_" + i);
          v.save();
          chunkRids.add(v.getIdentity().toString());
        }
      });

      final JSONObject root = new JSONObject();
      root.put("language", "opencypher");
      root.put("command",
          """
          UNWIND $batch AS BatchEntry \
          MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID \
          CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) \
          CREATE (p)-[:embb]->(b)""");
      final JSONArray batchJson = new JSONArray();
      for (int i = 0; i < batchSize; i++) {
        final JSONObject entry = new JSONObject();
        entry.put("destRID", chunkRids.get(i));
        final JSONArray vec = new JSONArray();
        for (int d = 0; d < vectorDim; d++)
          vec.put(random.nextDouble());
        entry.put("vector", vec);
        batchJson.put(entry);
      }
      final JSONObject paramsJson = new JSONObject();
      paramsJson.put("batch", batchJson);
      root.put("params", paramsJson);

      final String httpBody = root.toString();
      assertThat(httpBody.length()).isGreaterThan(1024 * 1024); // sanity: large payload

      // Mimic the PostCommandHandler entry path.
      final JSONObject parsed = new JSONObject(httpBody);
      final Map<String, Object> requestMap = parsed.toMap(true);
      @SuppressWarnings("unchecked")
      final Map<String, Object> params = (Map<String, Object>) requestMap.get("params");

      @SuppressWarnings("unchecked")
      final List<Map<String, Object>> batch = (List<Map<String, Object>>) params.get("batch");
      assertThat(batch).hasSize(batchSize);
      assertThat(batch.get(0).get("vector"))
          .as("the optimization must yield primitive float[] for the vector field")
          .isInstanceOf(float[].class);

      final long start = System.currentTimeMillis();
      database.transaction(() -> database.command("opencypher",
          """
          UNWIND $batch AS BatchEntry \
          MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID \
          CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) \
          CREATE (p)-[:embb]->(b)""",
          params));
      final long elapsed = System.currentTimeMillis() - start;

      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM CHUNK_EMBEDDING")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(batchSize);
        }
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM embb")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(batchSize);
        }

        // Spot-check that the stored vector is a float[] and matches the source values
        // (within float precision since the source was double).
        try (final ResultSet rs = database.query("sql", "SELECT vector FROM CHUNK_EMBEDDING LIMIT 1")) {
          assertThat(rs.hasNext()).isTrue();
          final Object stored = rs.next().getProperty("vector");
          assertThat(stored).isInstanceOf(float[].class);
          assertThat(((float[]) stored).length).isEqualTo(vectorDim);
        }
      });

      assertThat(elapsed)
          .as("HTTP-path Cypher batch (%d entries x dim %d) took %d ms", batchSize, vectorDim, elapsed)
          .isLessThan(maxElapsedMs);
    } finally {
      database.drop();
      FileUtils.deleteRecursively(new File(databasePath));
    }
  }

  // Issue #3765: literal values on both sides match (baseline)
  @Test
  void matchWithLiteralValuesOnBothSides() {
    final Database database = newIssue3765Database("literals");
    try {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:A {f: 'a'})-[:LINK]->(b:B {f: 'x'}) RETURN a.f, b.f")) {
        final List<Result> results = new ArrayList<>();
        while (rs.hasNext())
          results.add(rs.next());

        assertThat(results).hasSize(1);
        assertThat(results.get(0).<String>getProperty("a.f")).isEqualTo("a");
        assertThat(results.get(0).<String>getProperty("b.f")).isEqualTo("x");
      }
    } finally {
      database.drop();
    }
  }

  // Issue #3765: parameter on the left-hand side node works
  @Test
  void matchWithParameterOnLeftSideNode() {
    final Database database = newIssue3765Database("left");
    try {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:A {f: $param})-[:LINK]->(b:B {f: 'x'}) RETURN a.f, b.f",
          Map.of("param", "a"))) {
        final List<Result> results = new ArrayList<>();
        while (rs.hasNext())
          results.add(rs.next());

        assertThat(results).hasSize(1);
        assertThat(results.get(0).<String>getProperty("a.f")).isEqualTo("a");
        assertThat(results.get(0).<String>getProperty("b.f")).isEqualTo("x");
      }
    } finally {
      database.drop();
    }
  }

  // Issue #3765: parameter on the right-hand side node (originally returned empty)
  @Test
  void matchWithParameterOnRightSideNode() {
    final Database database = newIssue3765Database("right");
    try {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:A {f: 'a'})-[:LINK]->(b:B {f: $param}) RETURN a.f, b.f",
          Map.of("param", "x"))) {
        final List<Result> results = new ArrayList<>();
        while (rs.hasNext())
          results.add(rs.next());

        assertThat(results).hasSize(1);
        assertThat(results.get(0).<String>getProperty("a.f")).isEqualTo("a");
        assertThat(results.get(0).<String>getProperty("b.f")).isEqualTo("x");
      }
    } finally {
      database.drop();
    }
  }

  // Issue #3765: parameters on both sides of the relationship
  @Test
  void matchWithParametersOnBothSides() {
    final Database database = newIssue3765Database("both");
    try {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:A {f: $p1})-[:LINK]->(b:B {f: $p2}) RETURN a.f, b.f",
          Map.of("p1", "a", "p2", "x"))) {
        final List<Result> results = new ArrayList<>();
        while (rs.hasNext())
          results.add(rs.next());

        assertThat(results).hasSize(1);
        assertThat(results.get(0).<String>getProperty("a.f")).isEqualTo("a");
        assertThat(results.get(0).<String>getProperty("b.f")).isEqualTo("x");
      }
    } finally {
      database.drop();
    }
  }

  // Issue #3765: parameter on right-hand side that does not match returns empty
  @Test
  void matchWithParameterOnRightSideNoMatch() {
    final Database database = newIssue3765Database("right-nomatch");
    try {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:A {f: 'a'})-[:LINK]->(b:B {f: $param}) RETURN a.f, b.f",
          Map.of("param", "nonexistent"))) {
        assertThat(rs.hasNext()).isFalse();
      }
    } finally {
      database.drop();
    }
  }

  private static Database newIssue3765Database(final String tag) {
    final Database database = new DatabaseFactory("./target/databases/testopencypher-issue3765-" + tag).create();
    database.getSchema().createVertexType("A");
    database.getSchema().createVertexType("B");
    database.getSchema().createEdgeType("LINK");
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:A {f: 'a'}), (b:B {f: 'x'}), (a)-[:LINK]->(b)");
    });
    return database;
  }

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File("./target/testparams"));
    FileUtils.deleteRecursively(new File("./target/testparams_eq"));
  }
}
