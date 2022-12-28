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
package org.apache.tinkerpop.gremlin.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;
import java.util.stream.*;

import static org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph.CONFIG_DIRECTORY;
import static org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph.CONFIG_EVALUATION_TIMEOUT;
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
    config.setProperty(CONFIG_DIRECTORY, DB_PATH);
    config.setProperty(CONFIG_EVALUATION_TIMEOUT, 180000);

    final ArcadeGraph graph = ArcadeGraph.open(config);
    try (Database database = graph.getDatabase()) {
      database.transaction(() -> {
        Schema schema = database.getSchema();
        schema.getOrCreateVertexType("V");
        schema.getOrCreateEdgeType("E");

        MutableVertex v1 = database.newVertex("V").save();
        MutableVertex v2 = database.newVertex("V").save();
        MutableVertex v3 = database.newVertex("V").save();

        v1.newEdge("E", v2, true);
        v1.newEdge("E", v3, true);
        try (ResultSet query = database.query("cypher",
            "match(parent:V)-[e:E]-(child:V) where id(parent) = $p return parent as parent, collect(child) as children", "p", v1.getIdentity().toString())) {

          // Ensure that the result (set) has the desired format
          List<Result> results = IteratorUtils.toList(query, 1);
          assertThat(results, hasSize(1));

          Result result = results.get(0);
          assertThat(result, notNullValue());
          assertThat(result.isProjection(), equalTo(true));
          assertThat(result.getPropertyNames(), hasItems("parent", "children"));

          // Transform rid from result to string as in vertex
          Result parentAsResult = result.getProperty("parent");
          Map<String, Object> parent = parentAsResult.toMap();
          parent.computeIfPresent("@rid", (k, v) -> Objects.toString(v));
          parent.put("@cat", "v");
          Map<String, Object> vertexMap = v1.toJSON().toMap();
          assertThat(parent, equalTo(vertexMap));

          // Transform rid from result to string as in vertex
          List<Result> childrenAsResult = result.getProperty("children");
          List<Map<String, Object>> children = childrenAsResult.stream().map(Result::toMap).collect(Collectors.toList());
          children.forEach(c -> c.computeIfPresent("@rid", (k, v) -> Objects.toString(v)));
          children.forEach(c -> c.put("@cat", "v"));
          List<Map<String, Object>> childVertices = Stream.of(v2, v3).map(MutableVertex::toJSON).map(JSONObject::toMap).collect(Collectors.toList());
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
    try (Database database = graph.getDatabase()) {
      database.transaction(() -> {
        database.command("cypher", "CREATE (n:Transaction {id:'T1'}) RETURN n");
        database.command("cypher", "CREATE (n:City {id:'C1'}) RETURN n");
        database.command("cypher", "MATCH (t:Transaction), (c:City) WHERE t.id = 'T1' AND c.id = 'C1' CREATE path = (t)-[r:IS_IN]->(c) RETURN type(r)");

        try (ResultSet query = database.query("cypher", "MATCH path = (t:City{id:'C1'})-[r]-(c:Transaction{id:'T1'}) RETURN path")) {
          Assertions.assertTrue(query.hasNext());
          Result r1 = query.next();
          Assertions.assertTrue(query.hasNext());
          Result r2 = query.next();
          Assertions.assertTrue(query.hasNext());
          Result r3 = query.next();
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
    try (Database database = graph.getDatabase()) {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Node");
        database.command("sql", "CREATE VERTEX TYPE Transaction EXTENDS Node");
        database.command("sql", "INSERT INTO Transaction set id = 'A'");

        try (ResultSet query = database.query("cypher", "MATCH (n:Transaction) WHERE n.id = 'A' RETURN n")) {
          Assertions.assertTrue(query.hasNext());
          Result r1 = query.next();
        }

        try (ResultSet query = database.query("cypher", "MATCH (n:Node) WHERE n.id = 'A' RETURN n")) {
          Assertions.assertTrue(query.hasNext());
          Result r1 = query.next();
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
