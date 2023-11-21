/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.gremlin;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;
import java.util.stream.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class CypherQueryEngineTest {
  private static final String DB_PATH = "./target/testsql";

  @Test
  public void verifyProjectionWithCollectFunction() {
    final Configuration config = new BaseConfiguration();
    config.setProperty(ArcadeGraph.CONFIG_DIRECTORY, DB_PATH);

    final ArcadeGraph graph = ArcadeGraph.open(config);
    try (final BasicDatabase database = graph.getDatabase()) {
      database.transaction(() -> {
        final Schema schema = database.getSchema();
        schema.getOrCreateVertexType("V");
        schema.getOrCreateEdgeType("E");

        final MutableVertex v1 = database.newVertex("V").save();
        final MutableVertex v2 = database.newVertex("V").save();
        final MutableVertex v3 = database.newVertex("V").save();

        v1.newEdge("E", v2, true);
        v1.newEdge("E", v3, true);
        try (final ResultSet query = database.query("cypher",
            "match(parent:V)-[e:E]-(child:V) where id(parent) = $p return parent as parent, collect(child) as children", "p",
            v1.getIdentity().toString())) {

          // Ensure that the result (set) has the desired format
          final List<Result> results = IteratorUtils.toList(query, 1);
          assertThat(results, hasSize(1));

          final Result result = results.get(0);
          assertThat(result, notNullValue());
          assertThat(result.isProjection(), equalTo(true));
          assertThat(result.getPropertyNames(), hasItems("parent", "children"));

          // Transform rid from result to string as in vertex
          final Result parentAsResult = result.getProperty("parent");
          final Map<String, Object> parent = parentAsResult.toMap();
          parent.computeIfPresent("@rid", (k, v) -> Objects.toString(v));
          parent.put("@cat", "v");
          final Map<String, Object> vertexMap = v1.toJSON().toMap();
          assertThat(parent, equalTo(vertexMap));

          // Transform rid from result to string as in vertex
          final List<Result> childrenAsResult = result.getProperty("children");
          final List<Map<String, Object>> children = childrenAsResult.stream().map(Result::toMap).collect(Collectors.toList());
          children.forEach(c -> c.computeIfPresent("@rid", (k, v) -> Objects.toString(v)));
          children.forEach(c -> c.put("@cat", "v"));
          final List<Map<String, Object>> childVertices = Stream.of(v2, v3).map(MutableVertex::toJSON).map(JSONObject::toMap)
              .collect(Collectors.toList());
          assertThat(children, containsInAnyOrder(childVertices.toArray()));
        }

      });
    } finally {
      graph.drop();
    }
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/383
   */
  @Test
  public void returnPath() {
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    try (final BasicDatabase database = graph.getDatabase()) {
      database.transaction(() -> {
        database.command("cypher", "CREATE (n:Transaction {id:'T1'}) RETURN n");
        database.command("cypher", "CREATE (n:City {id:'C1'}) RETURN n");
        database.command("cypher",
            "MATCH (t:Transaction), (c:City) WHERE t.id = 'T1' AND c.id = 'C1' CREATE path = (t)-[r:IS_IN]->(c) RETURN type(r)");

        try (final ResultSet query = database.query("cypher",
            "MATCH path = (t:City{id:'C1'})-[r]-(c:Transaction{id:'T1'}) RETURN path")) {
          Assertions.assertTrue(query.hasNext());
          final Result r1 = query.next();
          Assertions.assertTrue(query.hasNext());
          final Result r2 = query.next();
          Assertions.assertTrue(query.hasNext());
          final Result r3 = query.next();
          Assertions.assertFalse(query.hasNext());
        }

      });
    } finally {
      graph.drop();
    }
  }

  /**
   * Test inheritance in Cypher (and therefore in Gremlin). Issue https://github.com/ArcadeData/arcadedb/issues/384.
   */
  @Test
  public void inheritance() {
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    try (final BasicDatabase database = graph.getDatabase()) {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Node");
        database.command("sql", "CREATE VERTEX TYPE Transaction EXTENDS Node");
        database.command("sql", "INSERT INTO Transaction set id = 'A'");

        try (final ResultSet query = database.query("cypher", "MATCH (n:Transaction) WHERE n.id = 'A' RETURN n")) {
          Assertions.assertTrue(query.hasNext());
          final Result r1 = query.next();
        }

        try (final ResultSet query = database.query("cypher", "MATCH (n:Node) WHERE n.id = 'A' RETURN n")) {
          Assertions.assertTrue(query.hasNext());
          final Result r1 = query.next();
        }
      });
    } finally {
      graph.drop();
    }
  }

  /**
   * Test null results are returned as null instead of `  cypher.null`. Issue https://github.com/ArcadeData/arcadedb/issues/804.
   */
  @Test
  public void testNullReturn() {
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    try (final BasicDatabase database = graph.getDatabase()) {
      database.transaction(() -> {
        try (final ResultSet query = database.command("cypher", "CREATE (n:Person) return n.name")) {
          Assertions.assertTrue(query.hasNext());
          final Result r1 = query.next();
          Assertions.assertNull(r1.getProperty("n.name"));
        }
      });
    } finally {
      graph.drop();
    }
  }

  /**
   * Cypher columns are returned in the wrong order. Issue https://github.com/ArcadeData/arcadedb/issues/818.
   */
  @Test
  public void testReturnOrder() {
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    try (final BasicDatabase database = graph.getDatabase()) {
      database.transaction(() -> {
        database.command("cypher", "CREATE (foo:Order {name: \"hi\", field1: \"value1\", field2: \"value2\"}) RETURN foo;\n");
        try (final ResultSet query = database.command("cypher", "MATCH (foo:Order) RETURN foo.name, foo.field2, foo.field1;")) {
          Assertions.assertTrue(query.hasNext());
          final Result r1 = query.next();

          final List<String> columns = new ArrayList<>(r1.toMap().keySet());
          Assertions.assertEquals("foo.name", columns.get(0));
          Assertions.assertEquals("foo.field2", columns.get(1));
          Assertions.assertEquals("foo.field1", columns.get(2));
        }
      });
    } finally {
      graph.drop();
    }
  }

  @BeforeEach
  @AfterEach
  public void clean() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }
}
