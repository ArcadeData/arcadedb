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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for Issue #3154: UNWIND with CREATE/MERGE treating property references as string literals
 * instead of evaluating them.
 * Related to Issue #3139.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3154Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./databases/test-issue-3154").create();
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createVertexType("CHUNK_EMBEDDING");
    database.getSchema().createVertexType("USER_RIGHTS");
    database.getSchema().createEdgeType("embb");
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void unwindWithCreateSimple() {
    // First create a CHUNK node to reference
    database.command("opencypher", "CREATE (c:CHUNK {name: 'test'})");

    // Simple test: UNWIND with property reference in CREATE
    final Map<String, Object> params = new HashMap<>();
    final List<Map<String, Object>> batch = new ArrayList<>();

    final Map<String, Object> entry = new HashMap<>();
    entry.put("name", "Alice");
    entry.put("age", 30);
    batch.add(entry);

    params.put("batch", batch);

    // This should create a node with name='Alice' and age=30, not name='entry.name' and age='entry.age'
    final ResultSet result = database.command("opencypher",
        """
        UNWIND $batch AS entry \
        CREATE (p:USER_RIGHTS {user_name: entry.name, age: entry.age}) \
        RETURN p""", params);

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    final Vertex vertex = (Vertex) row.toElement();

    // These should be the actual values, not the string "entry.name" or "entry.age"
    assertThat((String) vertex.get("user_name")).isEqualTo("Alice");
    assertThat((Integer) vertex.get("age")).isEqualTo(30);
  }

  @Test
  void unwindWithMergePropertyReferences() {
    // Test from Issue #3139
    final Map<String, Object> params = new HashMap<>();
    params.put("user_name", "root");
    params.put("right_0", "OWNER");

    final ResultSet result = database.command("opencypher",
        """
        UNWIND [{user_name: $user_name, right: $right_0}] AS node \
        MERGE (n:USER_RIGHTS {user_name: node.user_name, right: node.right}) \
        RETURN n""", params);

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    final Vertex vertex = (Vertex) row.toElement();

    // The bug was that these were set to "node.user_name" and "node.right" as strings
    assertThat((String) vertex.get("user_name")).isEqualTo("root");
    assertThat((String) vertex.get("right")).isEqualTo("OWNER");
  }

  @Test
  void unwindWithCreateAndMatch() {
    // Create a CHUNK node first
    final ResultSet createResult = database.command("opencypher", "CREATE (c:CHUNK {id: 1}) RETURN ID(c) as rid");
    assertThat(createResult.hasNext()).isTrue();
    final String chunkRid = createResult.next().getProperty("rid").toString();

    // Test similar to the original issue but with simple properties
    final Map<String, Object> params = new HashMap<>();
    final List<Map<String, Object>> batch = new ArrayList<>();

    final Map<String, Object> entry = new HashMap<>();
    entry.put("value", 42);
    entry.put("destRID", chunkRid);
    batch.add(entry);

    params.put("batch", batch);

    final ResultSet result = database.command("opencypher",
        """
        UNWIND $batch AS BatchEntry \
        MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID \
        CREATE (p:CHUNK_EMBEDDING {value: BatchEntry.value}) \
        CREATE (p)-[:embb]->(b) \
        RETURN p""", params);

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    final Vertex vertex = (Vertex) row.toElement();

    // Should be the actual value 42, not the string "BatchEntry.value"
    assertThat((Integer) vertex.get("value")).isEqualTo(42);
  }

  @Test
  void unwindWithCreateVectorProperty() {
    // Create a CHUNK node first
    final ResultSet createResult = database.command("opencypher", "CREATE (c:CHUNK {id: 1}) RETURN ID(c) as rid");
    assertThat(createResult.hasNext()).isTrue();
    final String chunkRid = createResult.next().getProperty("rid").toString();

    // Test with vector property (the original issue)
    final Map<String, Object> params = new HashMap<>();
    final List<Map<String, Object>> batch = new ArrayList<>();

    final Map<String, Object> entry = new HashMap<>();
    final float[] vector = new float[]{0.1f, 0.2f, 0.3f, 0.4f, 0.5f};
    entry.put("vector", vector);
    entry.put("destRID", chunkRid);
    batch.add(entry);

    params.put("batch", batch);

    final ResultSet result = database.command("opencypher",
        """
        UNWIND $batch AS BatchEntry \
        MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID \
        CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) \
        CREATE (p)-[:embb]->(b) \
        RETURN p""", params);

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    final Vertex vertex = (Vertex) row.toElement();

    // Should be the actual vector array, not the string "BatchEntry.vector"
    final Object vectorProp = vertex.get("vector");
    assertThat(vectorProp).isNotNull();
    assertThat(vectorProp).isNotEqualTo("BatchEntry.vector");

    // Verify it's actually a float array
    if (vectorProp instanceof float[])
      assertThat((float[]) vectorProp).containsExactly(0.1f, 0.2f, 0.3f, 0.4f, 0.5f);
    else if (vectorProp instanceof List) {
      @SuppressWarnings("unchecked")
      final List<Number> vectorList = (List<Number>) vectorProp;
      assertThat(vectorList).hasSize(5);
    }
  }

  @Test
  void originalIssue3154Scenario() {
    // Exact scenario from the original issue - create CHUNK node and use large vector
    final ResultSet createResult = database.command("opencypher", "CREATE (c:CHUNK {id: 1}) RETURN ID(c) as rid");
    assertThat(createResult.hasNext()).isTrue();
    final String chunkRid = createResult.next().getProperty("rid").toString();

    // Use a subset of the actual vector from the issue (first 10 elements)
    final Map<String, Object> params = new HashMap<>();
    final List<Map<String, Object>> batch = new ArrayList<>();

    final Map<String, Object> entry = new HashMap<>();
    final float[] vector = new float[]{
        -0.0159912109375f, 0.020538330078125f, -0.028778076171875f, 0.0081634521484375f,
        -0.0149993896484375f, -0.044219970703125f, 0.0160064697265625f, 0.0028247833251953125f,
        -0.006366729736328125f, 0.0203399658203125f
    };
    entry.put("vector", vector);
    entry.put("destRID", chunkRid);
    batch.add(entry);

    params.put("batch", batch);

    // Execute the exact query from the issue
    final ResultSet result = database.command("opencypher",
        """
        UNWIND $batch AS BatchEntry \
        MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID \
        CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) \
        CREATE (p)-[:embb]->(b) \
        RETURN p""", params);

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    final Vertex vertex = (Vertex) row.toElement();

    // Verify the vector was stored correctly (not as string "BatchEntry.vector")
    final Object vectorProp = vertex.get("vector");
    assertThat(vectorProp).isNotNull();
    assertThat(vectorProp).isNotEqualTo("BatchEntry.vector");
    assertThat(vectorProp).isNotInstanceOf(String.class);
  }

  @Test
  void issue3211Scenario() {
    // Issue #3211 - same problem as #3154, reported again after commit was reverted
    final ResultSet createResult = database.command("opencypher", "CREATE (c:CHUNK {id: 1}) RETURN ID(c) as rid");
    assertThat(createResult.hasNext()).isTrue();
    final String chunkRid = createResult.next().getProperty("rid").toString();

    // Use first 5 elements from issue #3211's vector
    final Map<String, Object> params = new HashMap<>();
    final List<Map<String, Object>> batch = new ArrayList<>();

    final Map<String, Object> entry = new HashMap<>();
    final float[] vector = new float[]{
        -0.01425933837890625f, 0.016937255859375f, -0.037384033203125f,
        -0.0006160736083984375f, 7.736682891845703e-05f
    };
    entry.put("vector", vector);
    entry.put("destRID", chunkRid);
    batch.add(entry);

    params.put("batch", batch);

    // Execute the exact query from issue #3211
    final ResultSet result = database.command("opencypher",
        """
        UNWIND $batch AS BatchEntry \
        MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID \
        CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) \
        CREATE (p)-[:embb]->(b) \
        RETURN p""", params);

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    final Vertex vertex = (Vertex) row.toElement();

    // Verify vector was stored correctly - this was the exact error in #3211:
    // "Expected float array or ComparableVector as key for vector index, got class java.lang.String"
    final Object vectorProp = vertex.get("vector");
    assertThat(vectorProp).isNotNull();
    assertThat(vectorProp).isNotEqualTo("BatchEntry.vector");
    assertThat(vectorProp).isNotInstanceOf(String.class);

    // Verify it's actually a float array or list
    if (vectorProp instanceof float[])
      assertThat((float[]) vectorProp).hasSize(5);
    else if (vectorProp instanceof List)
      assertThat((List<?>) vectorProp).hasSize(5);
  }
}
