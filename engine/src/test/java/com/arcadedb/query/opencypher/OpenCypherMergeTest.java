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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for MERGE clause in OpenCypher queries.
 */
public class OpenCypherMergeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-merge").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("WORKS_AT");
    database.getSchema().createEdgeType("in");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void mergeCreatesNodeWhenNotExists() {
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'Alice'})");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Alice'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    final Vertex v = (Vertex) verify.next().toElement();
    assertThat((String) v.get("name")).isEqualTo("Alice");
  }

  @Test
  void mergeFindsNodeWhenExists() {
    // Create node
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Bob'})");
    });

    // MERGE should find it, not create duplicate
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'Bob'})");
    });

    // Verify only one Bob exists
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Bob'}) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(1);
  }

  @Test
  void mergeWithReturn() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher", "MERGE (n:Person {name: 'Charlie'}) RETURN n");
      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().toElement();
      assertThat((String) v.get("name")).isEqualTo("Charlie");
    });
  }

  @Test
  void mergeMultipleTimes() {
    // First MERGE creates
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'David', age: 30})");
    });

    // Second MERGE finds
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'David', age: 30})");
    });

    // Third MERGE finds
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'David', age: 30})");
    });

    // Verify only one David exists
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'David'}) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(1);
  }

  @Test
  void mergeRelationship() {
    // Create nodes first
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Eve'})");
      database.command("opencypher", "CREATE (b:Person {name: 'Frank'})");
    });

    // MERGE relationship
    database.transaction(() -> {
      database.command("opencypher",
          "MERGE (a:Person {name: 'Eve'})-[r:KNOWS]->(b:Person {name: 'Frank'})");
    });

    // Verify relationship exists
    ResultSet verify = database.query("opencypher",
        "MATCH (a:Person {name: 'Eve'})-[r:KNOWS]->(b:Person {name: 'Frank'}) RETURN r");
    assertThat(verify.hasNext()).isTrue();

    // MERGE again - should find existing relationship
    database.transaction(() -> {
      database.command("opencypher",
          "MERGE (a:Person {name: 'Eve'})-[r:KNOWS]->(b:Person {name: 'Frank'})");
    });

    // Verify still only one relationship
    verify = database.query("opencypher",
        "MATCH (a:Person {name: 'Eve'})-[r:KNOWS]->(b:Person {name: 'Frank'}) RETURN r");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(1);
  }

  /**
   * Test that MERGE with label only (no properties) finds existing node instead of creating duplicates.
   * This is the pattern: MERGE (n:PIPELINE_CONFIG) ON CREATE SET n.pipelines = ["miaou"] ON MATCH SET n.pipelines = ["miaou"]
   */
  @Test
  void mergeLabelOnlyFindsExistingNode() {
    database.getSchema().createVertexType("PIPELINE_CONFIG");

    // First MERGE should create the node
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MERGE (n:PIPELINE_CONFIG) ON CREATE SET n.pipelines = ['miaou'] ON MATCH SET n.pipelines = ['miaou'] RETURN n.pipelines as pipelines");
      assertThat(result.hasNext()).isTrue();
      result.next();
    });

    // Second MERGE should find the existing node, not create a duplicate
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MERGE (n:PIPELINE_CONFIG) ON CREATE SET n.pipelines = ['miaou'] ON MATCH SET n.pipelines = ['miaou'] RETURN n.pipelines as pipelines");
      assertThat(result.hasNext()).isTrue();
      result.next();
    });

    // Verify only one node exists
    final ResultSet verify = database.query("opencypher",
        "MATCH (n:PIPELINE_CONFIG) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(1);
  }

  /**
   * Test that MERGE with label only (no properties) correctly triggers ON CREATE SET on first call
   * and ON MATCH SET on subsequent calls.
   */
  @Test
  void mergeLabelOnlyWithOnCreateAndOnMatchSet() {
    database.getSchema().createVertexType("SINGLETON");

    // First MERGE should create and apply ON CREATE SET
    ResultSet result = database.command("opencypher",
        "MERGE (n:SINGLETON) ON CREATE SET n.status = 'created', n.count = 1 ON MATCH SET n.status = 'matched', n.count = 2 RETURN n");
    assertThat(result.hasNext()).isTrue();
    Vertex v = (Vertex) result.next().toElement();
    assertThat(v.get("status")).isEqualTo("created");
    assertThat(((Number) v.get("count")).intValue()).isEqualTo(1);

    // Second MERGE should match and apply ON MATCH SET
    result = database.command("opencypher",
        "MERGE (n:SINGLETON) ON CREATE SET n.status = 'created', n.count = 1 ON MATCH SET n.status = 'matched', n.count = 2 RETURN n");
    assertThat(result.hasNext()).isTrue();
    v = (Vertex) result.next().toElement();
    assertThat(v.get("status")).isEqualTo("matched");
    assertThat(((Number) v.get("count")).intValue()).isEqualTo(2);
  }

  /**
   * Test for issue #3217: Backticks in relationship types should be treated as escape characters,
   * not included in the relationship type name.
   */
  @Test
  void mergeRelationshipWithBackticksInTypeName() {
    // Create nodes first
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (b:Company {name: 'TechCorp'})");
    });

    // MERGE relationship using backticks around the type name 'in' (which is a reserved keyword)
    database.transaction(() -> {
      database.command("opencypher",
          """
          MATCH (a:Person {name: 'Alice'}), (b:Company {name: 'TechCorp'}) \
          MERGE (a)-[r:`in`]->(b) RETURN a, b, r""");
    });

    // Verify the relationship type is "in" (without backticks)
    final ResultSet verify = database.query("opencypher",
        "MATCH (a:Person)-[r:`in`]->(b:Company) RETURN type(r) as relType");
    assertThat(verify.hasNext()).isTrue();
    final String relType = (String) verify.next().getProperty("relType");

    // The relationship type should be "in", NOT "`in`" (backticks should not be included)
    assertThat(relType).isEqualTo("in");
    assertThat(relType).doesNotContain("`");

    // MERGE again - should find the existing relationship (proves backticks are treated consistently)
    database.transaction(() -> {
      database.command("opencypher",
          """
          MATCH (a:Person {name: 'Alice'}), (b:Company {name: 'TechCorp'}) \
          MERGE (a)-[r2:`in`]->(b) RETURN r2""");
    });

    // Verify still only one relationship
    final ResultSet countVerify = database.query("opencypher",
        "MATCH (a:Person)-[r:`in`]->(b:Company) RETURN count(r) as cnt");
    assertThat(countVerify.hasNext()).isTrue();
    final Long count = (Long) countVerify.next().getProperty("cnt");
    assertThat(count).isEqualTo(1L);
  }

  /**
   * Test for issue #3217: Backticks in node labels should also be treated as escape characters.
   */
  @Test
  void createNodeWithBackticksInLabel() {
    // Create edge type for reserved keyword
    database.getSchema().createVertexType("select");

    // Create node using backticks around the label 'select' (which is a reserved keyword)
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:`select` {id: 1})");
    });

    // Verify the node label is "select" (without backticks)
    final ResultSet verify = database.query("opencypher",
        "MATCH (n:`select`) RETURN labels(n) as nodeLabels");
    assertThat(verify.hasNext()).isTrue();
    final Object labelsObj = verify.next().getProperty("nodeLabels");
    assertThat(labelsObj).isInstanceOf(List.class);

    @SuppressWarnings("unchecked")
    final List<String> labels = (List<String>) labelsObj;
    assertThat(labels).hasSize(1);
    assertThat(labels.get(0)).isEqualTo("select");
    assertThat(labels.get(0)).doesNotContain("`");
  }

  /** See issue #3998 */
  @Nested
  class MergeUnboundLabelEndpointRegression {
    @Test
    void unboundLabelOnlyEndpointCreatesNewNodeWhenSameLabelExists() {
      // Setup: Alice + an existing Company
      database.transaction(() -> {
        database.command("opencypher",
            "CREATE (:Person {name: 'Alice'}), (:Company {name: 'TechCorp', industry: 'Technology'})");
      });

      // c is unbound and label-only — MERGE must create a fresh Company, not reuse TechCorp
      database.transaction(() -> {
        database.command("opencypher",
            "MATCH (p:Person {name: 'Alice'}) MERGE (p)-[r:WORKS_AT {since: 2020}]->(c:Company)");
      });

      // Two Company nodes: TechCorp + the new label-only one
      final ResultSet countRs = database.query("opencypher", "MATCH (c:Company) RETURN count(c) AS cnt");
      assertThat(countRs.hasNext()).isTrue();
      assertThat(((Number) countRs.next().getProperty("cnt")).longValue()).isEqualTo(2L);

      // The merged Company was freshly created and has no properties
      final ResultSet mergedRs = database.query("opencypher",
          "MATCH (p:Person {name: 'Alice'})-[:WORKS_AT]->(c:Company) RETURN c");
      assertThat(mergedRs.hasNext()).isTrue();
      final Vertex mergedCompany = (Vertex) mergedRs.next().toElement();
      assertThat((Object) mergedCompany.get("name")).isNull();
    }

    @Test
    void unboundLabelOnlyEndpointCreatesNodeWhenNoSameLabelExists() {
      // Control case: no Company exists yet — both old and new code should create one
      database.transaction(() -> {
        database.command("opencypher", "CREATE (:Person {name: 'Alice'})");
      });

      database.transaction(() -> {
        database.command("opencypher",
            "MATCH (p:Person {name: 'Alice'}) MERGE (p)-[r:WORKS_AT {since: 2020}]->(c:Company)");
      });

      final ResultSet countRs = database.query("opencypher", "MATCH (c:Company) RETURN count(c) AS cnt");
      assertThat(((Number) countRs.next().getProperty("cnt")).longValue()).isEqualTo(1L);

      final ResultSet mergedRs = database.query("opencypher",
          "MATCH (:Person {name: 'Alice'})-[:WORKS_AT]->(c:Company) RETURN c");
      assertThat(mergedRs.hasNext()).isTrue();
      assertThat((Object) ((Vertex) mergedRs.next().toElement()).get("name")).isNull();
    }

    @Test
    void explicitlyBoundEndpointStillReusesExistingNode() {
      // Control case: c is explicitly bound via MATCH — must reuse TechCorp
      database.transaction(() -> {
        database.command("opencypher",
            "CREATE (:Person {name: 'Alice'}), (:Company {name: 'TechCorp', industry: 'Technology'})");
      });

      database.transaction(() -> {
        database.command("opencypher",
            """
            MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'TechCorp'}) \
            MERGE (p)-[r:WORKS_AT {since: 2020}]->(c)""");
      });

      final ResultSet countRs = database.query("opencypher", "MATCH (c:Company) RETURN count(c) AS cnt");
      assertThat(((Number) countRs.next().getProperty("cnt")).longValue()).isEqualTo(1L);

      final ResultSet mergedRs = database.query("opencypher",
          "MATCH (:Person {name: 'Alice'})-[:WORKS_AT]->(c:Company) RETURN c.name AS name");
      assertThat(mergedRs.hasNext()).isTrue();
      assertThat((String) mergedRs.next().getProperty("name")).isEqualTo("TechCorp");
    }

    @Test
    void unboundLabelOnlyEndpointCreatesNewNodeWithMultipleExistingSameLabel() {
      database.transaction(() -> {
        database.command("opencypher",
            """
            CREATE (:Person {name: 'Alice'}), \
            (:Company {name: 'TechCorp', industry: 'Technology'}), \
            (:Company {name: 'DataInc', industry: 'Analytics'})""");
      });

      database.transaction(() -> {
        database.command("opencypher",
            "MATCH (p:Person {name: 'Alice'}) MERGE (p)-[r:WORKS_AT {since: 2020}]->(c:Company)");
      });

      // TechCorp + DataInc + 1 new = 3 total
      final ResultSet countRs = database.query("opencypher", "MATCH (c:Company) RETURN count(c) AS cnt");
      assertThat(((Number) countRs.next().getProperty("cnt")).longValue()).isEqualTo(3L);

      // The freshly created Company has no properties
      final ResultSet mergedRs = database.query("opencypher",
          "MATCH (:Person {name: 'Alice'})-[:WORKS_AT]->(c:Company) RETURN c");
      assertThat(mergedRs.hasNext()).isTrue();
      assertThat((Object) ((Vertex) mergedRs.next().toElement()).get("name")).isNull();
    }

    @Test
    void secondMergeReusesCreatedPathAndDoesNotCreateAnotherNode() {
      // Idempotency: the second MERGE must find the path created by the first
      database.transaction(() -> {
        database.command("opencypher",
            "CREATE (:Person {name: 'Alice'}), (:Company {name: 'TechCorp', industry: 'Technology'})");
      });

      database.transaction(() -> {
        database.command("opencypher",
            "MATCH (p:Person {name: 'Alice'}) MERGE (p)-[r:WORKS_AT {since: 2020}]->(c:Company)");
      });

      database.transaction(() -> {
        database.command("opencypher",
            "MATCH (p:Person {name: 'Alice'}) MERGE (p)-[r:WORKS_AT {since: 2020}]->(c:Company)");
      });

      // TechCorp + 1 new; the second MERGE must not create a third
      final ResultSet countRs = database.query("opencypher", "MATCH (c:Company) RETURN count(c) AS cnt");
      assertThat(((Number) countRs.next().getProperty("cnt")).longValue()).isEqualTo(2L);
    }
  }

  /** Undirected MERGE must find edges regardless of storage direction */
  @Nested
  class MergeUndirectedPatternRegression {
    @Test
    void undirectedMergeFindsExistingEdgeInReverseOrientation() {
      // Create alice->bob directed edge
      database.transaction(() -> {
        database.command("opencypher",
            "CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})");
      });

      // Undirected MERGE with Bob on the left — must match the stored alice->bob edge
      database.transaction(() -> {
        database.command("opencypher",
            "MERGE (x:Person {name: 'Bob'})-[:KNOWS]-(y:Person {name: 'Alice'})");
      });

      // No new nodes or edges should have been created
      final ResultSet personCount = database.query("opencypher", "MATCH (p:Person) RETURN count(p) AS cnt");
      assertThat(((Number) personCount.next().getProperty("cnt")).longValue()).isEqualTo(2L);

      // Directed count to avoid double-counting from undirected match
      final ResultSet edgeCount = database.query("opencypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS cnt");
      assertThat(((Number) edgeCount.next().getProperty("cnt")).longValue()).isEqualTo(1L);
    }
  }

  /** Pre-bound intermediate node in a multi-hop MERGE must not be bypassed */
  @Nested
  class MergeIntermediateBoundNodeRegression {
    @Test
    void preBoundIntermediateNodeIsRespectedDuringPathTraversal() {
      // Setup: alice->dave->carol exists; no alice->bob->carol path yet
      database.transaction(() -> {
        database.command("opencypher",
            """
            CREATE (alice:Person {name: 'Alice'})-[:KNOWS]->(dave:Person {name: 'Dave'})-[:KNOWS]->(carol:Person {name: 'Carol'}),\
             (bob:Person {name: 'Bob'})""");
      });

      // MATCH binds alice and bob; MERGE the path through bob specifically
      database.transaction(() -> {
        database.command("opencypher",
            """
            MATCH (alice:Person {name: 'Alice'}), (bob:Person {name: 'Bob'}) \
            MERGE (alice)-[:KNOWS]->(bob)-[:KNOWS]->(carol:Person {name: 'Carol'})""");
      });

      // alice->bob edge must have been created (the dave path must NOT be accepted as a match for bob)
      final ResultSet rs = database.query("opencypher",
          "MATCH (:Person {name: 'Alice'})-[r:KNOWS]->(:Person {name: 'Bob'}) RETURN r");
      assertThat(rs.hasNext()).isTrue();
    }
  }

  /** See issue #3131 */
  @Nested
  class MatchIdThenMergeRelationshipRegression {
    private Database database;
    private RID sourceRid;
    private RID targetRid;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-3131").create();

      // Create schema
      database.getSchema().createVertexType("Node");

      // Create test nodes
      database.transaction(() -> {
        final MutableVertex source = database.newVertex("Node");
        source.set("name", "Source");
        source.save();
        sourceRid = source.getIdentity();

        final MutableVertex target = database.newVertex("Node");
        target.set("name", "Target");
        target.save();
        targetRid = target.getIdentity();
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void matchWithIdThenMergeRelationship() {
      // This query should:
      // 1. MATCH two nodes by ID
      // 2. MERGE a relationship between them
      // 3. RETURN all three elements

      final ResultSet rs = database.command("opencypher",
          """
          MATCH (a), (b) WHERE ID(a) = $source_id AND ID(b) = $target_id \
          MERGE (a)-[r:in]->(b) \
          RETURN a, b, r""",
          Map.of("source_id", sourceRid.toString(), "target_id", targetRid.toString()));

      assertThat(rs.hasNext()).as("Should return one result").isTrue();

      final Result result = rs.next();

      // Verify all three elements are returned
      assertThat(result.getPropertyNames()).as("Should return a, b, and r")
          .containsExactlyInAnyOrder("a", "b", "r");

      // Verify the nodes
      assertThat((Object) result.getProperty("a")).as("a should be a vertex").isNotNull();
      assertThat((Object) result.getProperty("b")).as("b should be a vertex").isNotNull();
      assertThat((Object) result.getProperty("r")).as("r should be an edge").isNotNull();

//    System.out.println("Result: " + result.toJSON());

      // No more results
      assertThat(rs.hasNext()).as("Should only return one result").isFalse();
    }

    @Test
    void mergeRelationshipIdempotency() {
      // First execution - creates the relationship
      database.command("opencypher",
          """
          MATCH (a), (b) WHERE ID(a) = $source_id AND ID(b) = $target_id \
          MERGE (a)-[r:in]->(b) \
          RETURN a, b, r""",
          Map.of("source_id", sourceRid.toString(), "target_id", targetRid.toString()));

      // Second execution - should find the existing relationship
      final ResultSet rs = database.command("opencypher",
          """
          MATCH (a), (b) WHERE ID(a) = $source_id AND ID(b) = $target_id \
          MERGE (a)-[r:in]->(b) \
          RETURN a, b, r""",
          Map.of("source_id", sourceRid.toString(), "target_id", targetRid.toString()));

      assertThat(rs.hasNext()).as("Should return one result").isTrue();

      final Result result = rs.next();
      assertThat(result.getPropertyNames()).containsExactlyInAnyOrder("a", "b", "r");

      // Verify only one relationship exists
      final ResultSet countRs = database.query("opencypher",
          "MATCH (a)-[r:in]->(b) WHERE ID(a) = $source_id AND ID(b) = $target_id RETURN count(r) AS count",
          Map.of("source_id", sourceRid.toString(), "target_id", targetRid.toString()));

      final long count = ((Number) countRs.next().getProperty("count")).longValue();
      assertThat(count).as("Should have exactly one relationship").isEqualTo(1);
    }
  }

  @Nested
  class MergeWithUniqueHashIndexRegression {
    private Database db;

    @BeforeEach
    void setUp() {
      db = new DatabaseFactory("./target/databases/issue-4089-merge-unique-hash").create();
      db.transaction(() -> {
        final VertexType type = db.getSchema().createVertexType("Experiment");
        type.createProperty("pk", Type.STRING).setMandatory(true).setNotNull(true);
        db.getSchema().createTypeIndex(Schema.INDEX_TYPE.HASH, true, "Experiment", "pk");
      });
    }

    @AfterEach
    void tearDown() {
      if (db != null) {
        db.drop();
        db = null;
      }
    }

    // Issue #4089: MERGE on a UNIQUE HASH-indexed property must find the existing vertex on the second invocation instead of attempting a duplicate insert.
    @Test
    void mergeFindsExistingVertexThroughUniqueHashIndex() {
      final String query = "MERGE (v:Experiment {pk: 'a'}) "
          + "ON CREATE SET v.status = 'created' "
          + "ON MATCH SET v.status = 'updated' "
          + "RETURN v";

      db.transaction(() -> {
        final ResultSet rs = db.command("opencypher", query);
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("v.status")).isIn("created", null);
      });

      db.transaction(() -> {
        final ResultSet rs = db.command("opencypher", query);
        assertThat(rs.hasNext()).isTrue();
        rs.next();
      });

      final ResultSet verify = db.query("opencypher", "MATCH (v:Experiment {pk: 'a'}) RETURN v.status AS status");
      int count = 0;
      String finalStatus = null;
      while (verify.hasNext()) {
        finalStatus = verify.next().<String>getProperty("status");
        count++;
      }
      assertThat(count).isEqualTo(1);
      assertThat(finalStatus).isEqualTo("updated");
    }

    // Issue #4089: MERGE on a UNIQUE LSM_TREE-indexed property must find the existing vertex on the second invocation instead of attempting a duplicate insert.
    @Test
    void mergeFindsExistingVertexThroughUniqueLsmTreeIndex() {
      db.transaction(() -> {
        db.getSchema().getType("Experiment").getIndexesByProperties("pk")
            .forEach(idx -> db.getSchema().dropIndex(idx.getName()));
        db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Experiment", "pk");
      });

      final String query = "MERGE (v:Experiment {pk: 'b'}) "
          + "ON CREATE SET v.status = 'created' "
          + "ON MATCH SET v.status = 'updated' "
          + "RETURN v";

      db.transaction(() -> db.command("opencypher", query));
      db.transaction(() -> db.command("opencypher", query));

      final ResultSet verify = db.query("opencypher",
          "MATCH (v:Experiment {pk: 'b'}) RETURN v.status AS status");
      int count = 0;
      String finalStatus = null;
      while (verify.hasNext()) {
        finalStatus = verify.next().<String>getProperty("status");
        count++;
      }
      assertThat(count).isEqualTo(1);
      assertThat(finalStatus).isEqualTo("updated");
    }
  }

  @Nested
  class MergeOnMatchStaleRegression {
    private Database db;

    @BeforeEach
    void setUp() {
      db = new DatabaseFactory("./target/databases/issue-4103-merge-on-match").create();
      db.transaction(() -> {
        db.command("opencypher", "CREATE (:Traveler {name:'Alice'})");
        db.command("opencypher", "CREATE (:Town {name:'London', population: 8900000})");
      });
    }

    @AfterEach
    void tearDown() {
      if (db != null) {
        db.drop();
        db = null;
      }
    }

    // Issue #4103: MERGE ... ON MATCH SET must materialize the updated property value in every returned row of a multi-row pipeline, including the first.
    @Test
    void mergeOnMatchSetPersistsForAllRows() {
      db.transaction(() -> {
        final ResultSet rs = db.command("opencypher",
            "MERGE (p:Traveler {name:'Alice'}) "
                + "WITH p "
                + "UNWIND [1, 2, 3] AS i "
                + "MERGE (p)-[:VISITS]->(c:Town {name:'London'}) "
                + "ON MATCH SET c.population = 9000000 "
                + "RETURN i, c.population AS population "
                + "ORDER BY i");
        final List<Long> populations = new ArrayList<>();
        while (rs.hasNext()) {
          final Result r = rs.next();
          populations.add(r.<Number>getProperty("population").longValue());
        }
        assertThat(populations).containsExactly(9000000L, 9000000L, 9000000L);
      });
    }

    // Issue #4103: MERGE ... ON MATCH SET persists the updated value to storage after a multi-row pipeline completes.
    @Test
    void persistedValueIsCorrect() {
      db.transaction(() -> db.command("opencypher",
          "MERGE (p:Traveler {name:'Alice'}) "
              + "WITH p "
              + "UNWIND [1, 2, 3] AS i "
              + "MERGE (p)-[:VISITS]->(c:Town {name:'London'}) "
              + "ON MATCH SET c.population = 9000000"));

      final ResultSet rs = db.query("opencypher",
          "MATCH (:Traveler {name:'Alice'})-[:VISITS]->(c:Town {name:'London'}) RETURN c.population AS population");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("population").longValue()).isEqualTo(9000000L);
    }
  }

  @Nested
  class MergeReusedEndpointRegression {
    private Database db;

    @BeforeEach
    void setUp() {
      db = new DatabaseFactory("./target/databases/issue-4104-merge-reused-endpoint").create();
      db.transaction(() -> {
        db.command("opencypher", "CREATE (:MergeP {name:'Alice', score:10.0})");
        db.command("opencypher", "CREATE (:MergeP {name:'Bob', score:20.0})");
        db.command("opencypher", "CREATE (:MergeP {name:'Charlie', score:30.0})");
      });
    }

    @AfterEach
    void tearDown() {
      if (db != null) {
        db.drop();
        db = null;
      }
    }

    // Issue #4104: MERGE with an unbound endpoint and identical inline keys across rows must create one endpoint per unmatched row instead of collapsing onto an earlier-row endpoint.
    @Test
    void mergeCreatesOneEndpointPerUnmatchedRow() {
      db.transaction(() -> {
        final ResultSet rs = db.command("opencypher",
            "MATCH (p:MergeP) "
                + "WITH p, p.score / 2.0 AS halfScore "
                + "MERGE (p)-[:FRIEND]->(f:MergeF {name:''}) "
                + "ON CREATE SET f.score = halfScore + 5.0 "
                + "RETURN p.name AS person_name, f.score AS friend_score "
                + "ORDER BY person_name");
        final List<Double> scores = new ArrayList<>();
        while (rs.hasNext()) {
          final Result r = rs.next();
          scores.add(r.<Number>getProperty("friend_score").doubleValue());
        }
        assertThat(scores).containsExactly(10.0, 15.0, 20.0);
      });

      final ResultSet count = db.query("opencypher",
          "MATCH (f:MergeF) RETURN count(f) AS friend_nodes");
      assertThat(count.hasNext()).isTrue();
      assertThat(count.next().<Number>getProperty("friend_nodes").longValue()).isEqualTo(3L);
    }

    // Issue #4104: Single-row MERGE with an unbound endpoint and inline keys creates the endpoint and applies ON CREATE SET correctly.
    @Test
    void singleRowMergeCreatesEndpoint() {
      db.transaction(() -> {
        final ResultSet rs = db.command("opencypher",
            "MATCH (p:MergeP {name:'Alice'}) "
                + "WITH p, p.score / 2.0 AS halfScore "
                + "MERGE (p)-[:FRIEND]->(f:MergeF {name:''}) "
                + "ON CREATE SET f.score = halfScore + 5.0 "
                + "RETURN f.score AS friend_score");
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("friend_score").doubleValue()).isEqualTo(10.0);
      });
    }
  }

  @Nested
  class MergeUnwindSingleQuoteRegression {
    private Database db;

    @BeforeEach
    void setUp() {
      db = new DatabaseFactory("./target/databases/issue-4210-merge-unwind-single-quote").create();
    }

    @AfterEach
    void tearDown() {
      if (db != null) {
        db.drop();
        db = null;
      }
    }

    // Issue #4210: UNWIND $batch MERGE with a property value that is a single quote (or any string starting and ending with a quote) must not throw StringIndexOutOfBoundsException.
    @Test
    void mergeWithSingleQuotePropertyValueDoesNotCrash() {
      final String query = """
          UNWIND $batch AS BatchEntry
          MERGE (n:NER {identity: BatchEntry.identity, name: BatchEntry.name})
          ON CREATE SET n._temp_created = true
          ON MATCH SET n._temp_created = false
          WITH n, n._temp_created AS created
          REMOVE n._temp_created
          RETURN ID(n) AS id, created
          """;

      final List<Map<String, Object>> batch = new ArrayList<>();

      final Map<String, Object> entry1 = new HashMap<>();
      entry1.put("identity", "normal");
      entry1.put("name", "normal");
      batch.add(entry1);

      // Single quote character: the original crash case
      final Map<String, Object> entryQuote = new HashMap<>();
      entryQuote.put("identity", "'");
      entryQuote.put("name", "'");
      batch.add(entryQuote);

      // Starts and ends with quote but length > 1
      final Map<String, Object> entryQuotedString = new HashMap<>();
      entryQuotedString.put("identity", "'wrapped'");
      entryQuotedString.put("name", "'wrapped'");
      batch.add(entryQuotedString);

      // Contains a quote but not both start+end
      final Map<String, Object> entryPartialQuote = new HashMap<>();
      entryPartialQuote.put("identity", "it's");
      entryPartialQuote.put("name", "it's");
      batch.add(entryPartialQuote);

      final Map<String, Object> params = new HashMap<>();
      params.put("batch", batch);

      db.transaction(() -> {
        try (final ResultSet rs = db.command("opencypher", query, params)) {
          int count = 0;
          while (rs.hasNext()) {
            rs.next();
            count++;
          }
          assertThat(count).isEqualTo(4);
        }
      });

      // Second run - all must match (ON MATCH path)
      db.transaction(() -> {
        try (final ResultSet rs = db.command("opencypher", query, params)) {
          int count = 0;
          while (rs.hasNext()) {
            final var row = rs.next();
            assertThat(row.<Boolean>getProperty("created")).isFalse();
            count++;
          }
          assertThat(count).isEqualTo(4);
        }
      });
    }

    // Issue #4210: MERGE with a single-quote property value preserves the exact literal value (no quote stripping).
    @Test
    void mergeWithQuotePropertyPreservesExactValue() {
      final String query = """
          UNWIND $batch AS entry
          MERGE (n:Token {value: entry.value})
          RETURN n.value AS val
          """;

      final Map<String, Object> entry = new HashMap<>();
      entry.put("value", "'");
      final List<Object> batch = List.of(entry);

      final Map<String, Object> params = new HashMap<>();
      params.put("batch", batch);

      db.transaction(() -> db.command("opencypher", query, params).close());

      try (final ResultSet rs = db.query("opencypher", "MATCH (n:Token) RETURN n.value AS val")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("val")).isEqualTo("'");
      }
    }

    // Issue #4210: MatchNodeStep.matchesProperties must not strip quotes from UNWIND-supplied node property filter values.
    @Test
    void matchNodeWithSingleQuotePropertyViaUnwindDoesNotCrash() {
      // Creates nodes and then MATCHes them using UNWIND-supplied property values.
      // MatchNodeStep.matchesProperties must not strip quotes from evaluated values.
      db.transaction(() -> {
        db.command("opencypher", "CREATE (:Tag {name: \"'\"})");
        db.command("opencypher", "CREATE (:Tag {name: \"'wrapped'\"})");
        db.command("opencypher", "CREATE (:Tag {name: 'plain'})");
      });

      final List<Map<String, Object>> batch = List.of(
          Map.of("name", "'"),
          Map.of("name", "'wrapped'"),
          Map.of("name", "plain")
      );
      final Map<String, Object> params = new HashMap<>();
      params.put("batch", batch);

      try (final ResultSet rs = db.query("opencypher",
          "UNWIND $batch AS item MATCH (n:Tag {name: item.name}) RETURN n.name AS name", params)) {
        int count = 0;
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat(row.<String>getProperty("name")).isNotNull();
          count++;
        }
        assertThat(count).isEqualTo(3);
      }
    }

    // Issue #4210: ExpandPathStep.matchesTargetProperties (variable-length path endpoint) must compare quote-containing literals as-is rather than stripping the surrounding quotes.
    @Test
    void variableLengthPathEndpointWithSingleQuotePropertyDoesNotCrash() {
      // ExpandPathStep.matchesTargetProperties: VLP endpoint with a quote-containing literal.
      // Pre-fix, quote-stripping would have transformed the literal "'" into the empty string,
      // causing zero matches; post-fix, the literal is compared as-is.
      db.getSchema().createVertexType("Node");
      db.getSchema().createEdgeType("LINK");

      db.transaction(() -> {
        db.command("opencypher", "CREATE (:Node {tag: 'start'})");
        db.command("opencypher", "CREATE (:Node {tag: \"'\"})");
        db.command("opencypher",
            "MATCH (a:Node {tag: 'start'}), (b:Node {tag: \"'\"}) CREATE (a)-[:LINK]->(b)");
      });

      try (final ResultSet rs = db.query("opencypher",
          "MATCH (:Node {tag: 'start'})-[:LINK*1..3]->(m:Node {tag: \"'\"}) RETURN m.tag AS tag")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("tag")).isEqualTo("'");
      }
    }

    // Issue #4210: MatchRelationshipStep.matchesEdgeProperties must round-trip an inline edge property literal value that starts and ends with a quote character.
    @Test
    void matchRelationshipWithQuoteLiteralEdgePropertyDoesNotCrash() {
      // MatchRelationshipStep.matchesEdgeProperties: inline edge property filter with a literal
      // value that starts and ends with a quote character. Pre-fix this would have been stripped
      // to the empty string; post-fix it round-trips correctly.
      db.getSchema().createVertexType("Person");
      db.getSchema().createEdgeType("KNOWS");

      db.transaction(() -> {
        db.command("opencypher", "CREATE (:Person {name: 'Alice'})");
        db.command("opencypher", "CREATE (:Person {name: 'Bob'})");
        db.command("opencypher",
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {note: \"'\"}]->(b)");
      });

      try (final ResultSet rs = db.query("opencypher",
          "MATCH ()-[r:KNOWS {note: \"'\"}]->() RETURN r.note AS note")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("note")).isEqualTo("'");
      }
    }

    // Issue #4210: MatchRelationshipStep.matchesTargetProperties must not strip quotes when filtering relationship endpoint nodes by UNWIND-supplied property values.
    @Test
    void matchRelationshipEndpointWithSingleQuotePropertyViaUnwindDoesNotCrash() {
      // Creates edges and MATCHes them using UNWIND-supplied endpoint node property filters.
      // MatchRelationshipStep.matchesTargetProperties evaluates expression values and must not
      // strip quotes when the evaluated value starts and ends with a quote character.
      db.getSchema().createVertexType("Person");
      db.getSchema().createEdgeType("KNOWS");

      db.transaction(() -> {
        db.command("opencypher", "CREATE (:Person {name: 'Alice'})");
        db.command("opencypher", "CREATE (:Person {name: \"'\"})");
        db.command("opencypher", "CREATE (:Person {name: 'plain'})");
        db.command("opencypher",
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: \"'\"}) CREATE (a)-[:KNOWS]->(b)");
        db.command("opencypher",
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'plain'}) CREATE (a)-[:KNOWS]->(b)");
      });

      final List<Map<String, Object>> batch = List.of(
          Map.of("name", "'"),
          Map.of("name", "plain")
      );
      final Map<String, Object> params = new HashMap<>();
      params.put("batch", batch);

      try (final ResultSet rs = db.query("opencypher",
          "UNWIND $batch AS item MATCH (:Person)-[:KNOWS]->(m:Person {name: item.name}) RETURN m.name AS name", params)) {
        int count = 0;
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat(row.<String>getProperty("name")).isNotNull();
          count++;
        }
        assertThat(count).isEqualTo(2);
      }
    }
  }

  @Nested
  class OptionalMatchNullEndpointMergeRegression {
    private Database db;

    @BeforeEach
    void setUp() {
      db = new DatabaseFactory("./target/databases/issue-4213-optional-match-null-endpoint").create();
      db.getSchema().createVertexType("Node");
      db.getSchema().createEdgeType("KNOWS");
      db.transaction(() -> {
        db.command("opencypher", "CREATE (:Node {id: 1})");
        db.command("opencypher", "CREATE (:Node {id: 2})");
      });
    }

    @AfterEach
    void tearDown() {
      if (db != null) {
        db.drop();
        db = null;
      }
    }

    // Issue #4213: OPTIONAL MATCH that finds no match leaves the endpoint null; a subsequent MERGE referencing that null variable must produce zero rows instead of rebinding or emitting a spurious result.
    @Test
    void mergeWithNullEndpointFromOptionalMatchProducesNoRows() {
      final ResultSet rs = db.command("opencypher",
          "MATCH (a:Node {id: 1}) "
              + "OPTIONAL MATCH (a)-[:KNOWS]->(b:Node {id: 2}) "
              + "WITH a, b "
              + "MERGE (a)-[:KNOWS]->(b) "
              + "RETURN a.id AS aid, b.id AS bid");

      assertThat(rs.hasNext())
          .as("MERGE with null endpoint from OPTIONAL MATCH must produce no rows")
          .isFalse();
    }

    // Issue #4213: OPTIONAL MATCH must still expose a null value for unmatched endpoint variables - control case for the regression.
    @Test
    void optionalMatchAloneReturnsNullEndpoint() {
      final ResultSet rs = db.query("opencypher",
          "MATCH (a:Node {id: 1}) "
              + "OPTIONAL MATCH (a)-[:KNOWS]->(b:Node {id: 2}) "
              + "WITH a, b "
              + "RETURN a.id AS aid, b.id AS bid");

      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(row.<Number>getProperty("aid").longValue()).isEqualTo(1L);
      assertThat(row.<Object>getProperty("bid")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }

    // Issue #4213: An explicit MATCH after the OPTIONAL MATCH rebinds the endpoint and a subsequent MERGE succeeds normally returning one row.
    @Test
    void mergeAfterExplicitRebindSucceeds() {
      final ResultSet rs = db.command("opencypher",
          "MATCH (a:Node {id: 1}) "
              + "OPTIONAL MATCH (a)-[:KNOWS]->(b:Node {id: 2}) "
              + "WITH a, b "
              + "MATCH (c:Node {id: 2}) "
              + "MERGE (a)-[:KNOWS]->(c) "
              + "RETURN a.id AS aid, c.id AS cid");

      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(row.<Number>getProperty("aid").longValue()).isEqualTo(1L);
      assertThat(row.<Number>getProperty("cid").longValue()).isEqualTo(2L);
      assertThat(rs.hasNext()).isFalse();
    }

    // Issue #4213: MERGE with both endpoints directly bound by a non-optional MATCH creates exactly one relationship and returns one row.
    @Test
    void mergeWithBothEndpointsBoundCreatesRelationship() {
      final ResultSet rs = db.command("opencypher",
          "MATCH (a:Node {id: 1}), (b:Node {id: 2}) "
              + "MERGE (a)-[:KNOWS]->(b) "
              + "RETURN count(*) AS cnt");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);
    }

    // Issue #4213: When OPTIONAL MATCH succeeds and binds the endpoint, the subsequent MERGE creates the relationship and returns one row.
    @Test
    void mergeWithNonNullEndpointFromOptionalMatchCreatesRelationship() {
      // Pre-create the KNOWS relationship so OPTIONAL MATCH will match.
      db.transaction(() ->
          db.command("opencypher",
              "MATCH (a:Node {id: 1}), (b:Node {id: 2}) CREATE (a)-[:KNOWS]->(b)"));

      final ResultSet rs = db.command("opencypher",
          "MATCH (a:Node {id: 1}) "
              + "OPTIONAL MATCH (a)-[:KNOWS]->(b:Node {id: 2}) "
              + "WITH a, b "
              + "MERGE (a)-[:KNOWS]->(b) "
              + "RETURN a.id AS aid, b.id AS bid");

      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(row.<Number>getProperty("aid").longValue()).isEqualTo(1L);
      assertThat(row.<Number>getProperty("bid").longValue()).isEqualTo(2L);
      assertThat(rs.hasNext()).isFalse();
    }

    // Issue #4213: The skipped MERGE row must not create spurious Node vertices or KNOWS relationships as a side effect.
    @Test
    void mergeWithNullEndpointDoesNotCreateSpuriousVertex() {
      // Execute the buggy query (which must now produce 0 rows and no side effects).
      db.command("opencypher",
          "MATCH (a:Node {id: 1}) "
              + "OPTIONAL MATCH (a)-[:KNOWS]->(b:Node {id: 2}) "
              + "WITH a, b "
              + "MERGE (a)-[:KNOWS]->(b) "
              + "RETURN a.id AS aid, b.id AS bid");

      final ResultSet nodeCount = db.query("opencypher", "MATCH (n:Node) RETURN count(n) AS cnt");
      assertThat(nodeCount.next().<Number>getProperty("cnt").longValue())
          .as("No spurious Node vertex must be created by the skipped MERGE")
          .isEqualTo(2L);

      final ResultSet relCount = db.query("opencypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS cnt");
      assertThat(relCount.next().<Number>getProperty("cnt").longValue())
          .as("No KNOWS relationship must be created when endpoint is null")
          .isEqualTo(0L);
    }

    // Issue #4213: Multi-hop MERGE with a null intermediate node from OPTIONAL MATCH must drop the row even when the endpoints are bound.
    @Test
    void mergeMultiHopPatternWithNullIntermediateProducesNoRows() {
      final ResultSet rs = db.command("opencypher",
          "MATCH (a:Node {id: 1}) "
              + "OPTIONAL MATCH (a)-[:KNOWS]->(b:Node {id: 99}) "
              + "WITH a, b "
              + "MATCH (c:Node {id: 2}) "
              + "MERGE (a)-[:KNOWS]->(b)-[:KNOWS]->(c) "
              + "RETURN a.id AS aid, b.id AS bid, c.id AS cid");

      assertThat(rs.hasNext())
          .as("MERGE with null intermediate node must produce no rows")
          .isFalse();

      final ResultSet relCount = db.query("opencypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS cnt");
      assertThat(relCount.next().<Number>getProperty("cnt").longValue())
          .as("No KNOWS relationship must be created when an intermediate node is null")
          .isEqualTo(0L);
    }
  }

  @Nested
  class MergeBoundAnchorRegression {
    private Database db;

    @BeforeEach
    void setUp() {
      db = new DatabaseFactory("./target/databases/issue-4226-merge-bound-anchor").create();
      db.getSchema().createVertexType("DOCUMENT");
      db.getSchema().createVertexType("CHUNK");
      db.getSchema().createEdgeType("in");
    }

    @AfterEach
    void tearDown() {
      if (db != null) {
        db.drop();
        db = null;
      }
    }

    // Issue #4226: Baseline correctness: MERGE with a bound anchor creates a new chunk and edge when the pattern does not exist, leaving unrelated chunks untouched.
    @Test
    void mergeWithBoundAnchorCreatesWhenMissing() {
      db.transaction(() -> {
        db.command("opencypher", "CREATE (:DOCUMENT {name: 'parentA'})");
        db.command("opencypher", "CREATE (:DOCUMENT {name: 'parentB'})");

        db.command("opencypher",
            "MATCH (b:DOCUMENT {name:'parentB'}) "
                + "UNWIND range(1, 5) AS i "
                + "CREATE (c:CHUNK {name:'B_'+toString(i)}) "
                + "CREATE (c)-[:in]->(b)");
      });

      db.transaction(() -> {
        db.command("opencypher",
            "MATCH (a:DOCUMENT {name:'parentA'}) "
                + "MERGE (n:CHUNK {name:'A_only'})-[:in]->(a)");
      });

      final ResultSet rs = db.query("opencypher",
          "MATCH (n:CHUNK {name:'A_only'})-[:in]->(a:DOCUMENT {name:'parentA'}) RETURN count(n) AS cnt");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);

      // The unrelated chunks under parentB must remain intact.
      final ResultSet total = db.query("opencypher", "MATCH (n:CHUNK) RETURN count(n) AS cnt");
      assertThat(total.next().<Number>getProperty("cnt").longValue()).isEqualTo(6L);
    }

    // Issue #4226: MERGE with a bound anchor must not match a noise node connected to a different parent even when the inline property values coincide; the edge anchor scopes the match.
    @Test
    void mergeWithBoundAnchorDoesNotMatchNoiseUnderOtherParent() {
      db.transaction(() -> {
        db.command("opencypher", "CREATE (:DOCUMENT {name: 'parentA'})");
        db.command("opencypher", "CREATE (:DOCUMENT {name: 'parentB'})");

        // Identical CHUNK property values, but anchored to parentB.
        db.command("opencypher",
            "MATCH (b:DOCUMENT {name:'parentB'}) "
                + "CREATE (c:CHUNK {name:'shared', subtype:'CHUNK'})-[:in]->(b)");
      });

      db.transaction(() -> {
        // Should NOT match the parentB-attached chunk; should create a new one under parentA.
        db.command("opencypher",
            "MATCH (a:DOCUMENT {name:'parentA'}) "
                + "MERGE (n:CHUNK {name:'shared', subtype:'CHUNK'})-[:in]->(a)");
      });

      // Two CHUNK nodes exist now: the original one under parentB and the new one under parentA.
      final ResultSet total = db.query("opencypher", "MATCH (n:CHUNK) RETURN count(n) AS cnt");
      assertThat(total.next().<Number>getProperty("cnt").longValue()).isEqualTo(2L);

      final ResultSet underA = db.query("opencypher",
          "MATCH (n:CHUNK)-[:in]->(:DOCUMENT {name:'parentA'}) RETURN count(n) AS cnt");
      assertThat(underA.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);

      final ResultSet underB = db.query("opencypher",
          "MATCH (n:CHUNK)-[:in]->(:DOCUMENT {name:'parentB'}) RETURN count(n) AS cnt");
      assertThat(underB.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);
    }

    // Issue #4226: MERGE with a bound anchor must MATCH (not create) when an edge from the anchor to a matching chunk already exists; the chunk count must not grow on re-run.
    @Test
    void mergeWithBoundAnchorMatchesExistingChunk() {
      db.transaction(() -> {
        db.command("opencypher", "CREATE (:DOCUMENT {name: 'parentA'})");

        db.command("opencypher",
            "MATCH (a:DOCUMENT {name:'parentA'}) "
                + "CREATE (c:CHUNK {name:'existing'})-[:in]->(a)");
      });

      db.transaction(() -> {
        db.command("opencypher",
            "MATCH (a:DOCUMENT {name:'parentA'}) "
                + "MERGE (n:CHUNK {name:'existing'})-[:in]->(a)");
      });

      final ResultSet rs = db.query("opencypher", "MATCH (n:CHUNK) RETURN count(n) AS cnt");
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);

      final ResultSet edges = db.query("opencypher",
          "MATCH (n:CHUNK)-[r:in]->(:DOCUMENT) RETURN count(r) AS cnt");
      assertThat(edges.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);
    }

    // Issue #4226: Inverted direction (bound)-[:in]->(unbound) - MERGE must anchor correctly when the bound endpoint is the source.
    @Test
    void mergeWithBoundAnchorIncomingDirection() {
      db.transaction(() -> {
        db.command("opencypher", "CREATE (:DOCUMENT {name: 'src'})");
        // Pre-existing chunk pointed to from src.
        db.command("opencypher",
            "MATCH (s:DOCUMENT {name:'src'}) "
                + "CREATE (s)-[:in]->(c:CHUNK {name:'target'})");
      });

      db.transaction(() -> {
        db.command("opencypher",
            "MATCH (s:DOCUMENT {name:'src'}) "
                + "MERGE (s)-[:in]->(n:CHUNK {name:'target'})");
      });

      // Must MATCH (not create) the existing chunk.
      final ResultSet rs = db.query("opencypher", "MATCH (n:CHUNK) RETURN count(n) AS cnt");
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);
    }

    // Issue #4226: UNWIND + MERGE with a bound anchor must match-or-create exactly one chunk per row even with noise edges of the same type pointing to other parents.
    @Test
    void mergeWithBoundAnchorInsideUnwindCreatesOnePerRow() {
      db.transaction(() -> {
        db.command("opencypher", "CREATE (:DOCUMENT {name:'parentA'})");
        db.command("opencypher", "CREATE (:DOCUMENT {name:'parentB'})");

        // 50 noise edges under parentB.
        db.command("opencypher",
            "MATCH (b:DOCUMENT {name:'parentB'}) "
                + "UNWIND range(1,50) AS i "
                + "CREATE (c:CHUNK {name:'noise_'+toString(i)})-[:in]->(b)");
      });

      db.transaction(() -> {
        db.command("opencypher",
            "MATCH (a:DOCUMENT {name:'parentA'}) "
                + "UNWIND range(1,10) AS i "
                + "MERGE (n:CHUNK {name:'A_'+toString(i)})-[:in]->(a)");
      });

      final ResultSet underA = db.query("opencypher",
          "MATCH (n:CHUNK)-[:in]->(:DOCUMENT {name:'parentA'}) RETURN count(n) AS cnt");
      assertThat(underA.next().<Number>getProperty("cnt").longValue()).isEqualTo(10L);

      final ResultSet underB = db.query("opencypher",
          "MATCH (n:CHUNK)-[:in]->(:DOCUMENT {name:'parentB'}) RETURN count(n) AS cnt");
      assertThat(underB.next().<Number>getProperty("cnt").longValue()).isEqualTo(50L);

      // Re-run MERGE: nothing new should be created.
      db.transaction(() -> {
        db.command("opencypher",
            "MATCH (a:DOCUMENT {name:'parentA'}) "
                + "UNWIND range(1,10) AS i "
                + "MERGE (n:CHUNK {name:'A_'+toString(i)})-[:in]->(a)");
      });

      final ResultSet total = db.query("opencypher", "MATCH (n:CHUNK) RETURN count(n) AS cnt");
      assertThat(total.next().<Number>getProperty("cnt").longValue()).isEqualTo(60L);
    }

    // Issue #4226: Performance regression - MERGE with a bound anchor must scale with the anchor's edge degree, not the global edge count of the relationship type.
    @Test
    @Tag("slow")
    void mergeWithBoundAnchorScalesWithAnchorDegree() {
      db.transaction(() -> {
        db.command("opencypher", "CREATE (:DOCUMENT {name:'parentA'})");
        db.command("opencypher", "CREATE (:DOCUMENT {name:'parentB'})");

        // Large noise under parentB to exaggerate the cost of a full edge-type scan.
        db.command("opencypher",
            "MATCH (b:DOCUMENT {name:'parentB'}) "
                + "UNWIND range(1,200000) AS i "
                + "CREATE (c:CHUNK {name:'noise_'+toString(i)})-[:in]->(b)");
      });

      // Run a batch of MERGEs scoped to parentA; each must NOT touch parentB's edges.
      final long start = System.nanoTime();
      db.transaction(() -> {
        for (int i = 0; i < 30; i++) {
          db.command("opencypher",
              "MATCH (a:DOCUMENT {name:'parentA'}) "
                  + "MERGE (n:CHUNK {name:'A_unique_" + i + "'})-[:in]->(a)");
        }
      });
      final long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

      final ResultSet underA = db.query("opencypher",
          "MATCH (n:CHUNK)-[:in]->(:DOCUMENT {name:'parentA'}) RETURN count(n) AS cnt");
      assertThat(underA.next().<Number>getProperty("cnt").longValue()).isEqualTo(30L);

      // With 200k noise edges of type :in the buggy implementation does roughly
      // 30 * 200,000 = 6M edge-property iterations on the hot path - several
      // seconds on the observed CI hardware.  The optimized walker only touches
      // the anchor's 0..N incoming edges per MERGE and finishes in well under
      // 1 s.  The 2 s threshold is loose enough to absorb GC pauses yet tight
      // enough to catch a regression to the buggy O(edges-of-type) path.
      assertThat(elapsedMs)
          .as("MERGE with bound anchor should not scale with noise edge count (elapsed: " + elapsedMs + "ms)")
          .isLessThan(2000L);
    }

    // Issue #4226: Sanity check - the "match by bound first" pattern (a)-[:in]->(n) with anchor on the source continues to work after the fix.
    @Test
    void mergeWithBoundAnchorAtStartStillWorks() {
      db.transaction(() -> {
        db.command("opencypher", "CREATE (:DOCUMENT {name:'src'})");
        db.command("opencypher",
            "MATCH (s:DOCUMENT {name:'src'}) "
                + "CREATE (s)-[:in]->(:CHUNK {name:'pre'})");
      });

      db.transaction(() -> {
        db.command("opencypher",
            "MATCH (s:DOCUMENT {name:'src'}) "
                + "MERGE (s)-[:in]->(n:CHUNK {name:'pre'}) "
                + "RETURN n.name AS name");
      });

      final ResultSet rs = db.query("opencypher", "MATCH (n:CHUNK) RETURN count(n) AS cnt");
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);
    }

    // Issue #4226: When the MERGE pattern returns the bound anchor in a projection, both vertex variables must resolve to the correct identities.
    @Test
    void mergeWithBoundAnchorReturnsBoundVariable() {
      db.transaction(() -> {
        db.command("opencypher", "CREATE (:DOCUMENT {name:'parentA'})");
      });

      db.transaction(() -> {
        final ResultSet rs = db.command("opencypher",
            "MATCH (a:DOCUMENT {name:'parentA'}) "
                + "MERGE (n:CHUNK {name:'A_1'})-[:in]->(a) "
                + "RETURN a.name AS parent_name, n.name AS chunk_name");
        assertThat(rs.hasNext()).isTrue();
        final Result r = rs.next();
        assertThat(r.<String>getProperty("parent_name")).isEqualTo("parentA");
        assertThat(r.<String>getProperty("chunk_name")).isEqualTo("A_1");
      });
    }

    // Issue #4226: Three-node path with the bound anchor in the middle - the bidirectional walker must match the existing chain without creating duplicates.
    @Test
    void mergeWithBoundAnchorInMiddleOfPath() {
      db.transaction(() -> {
        db.getSchema().createVertexType("A");
        db.getSchema().createVertexType("B");
        db.getSchema().createVertexType("C");
        db.getSchema().createEdgeType("R1");
        db.getSchema().createEdgeType("R2");

        db.command("opencypher", "CREATE (a:A {name:'a1'})-[:R1]->(b:B {name:'b1'})-[:R2]->(c:C {name:'c1'})");
        // Noise on B not connected via R1/R2 to anything else.
        db.command("opencypher", "CREATE (:B {name:'b2'})");
      });

      db.transaction(() -> {
        // Bound anchor is b in the middle; the path should match the existing chain.
        db.command("opencypher",
            "MATCH (b:B {name:'b1'}) "
                + "MERGE (a:A {name:'a1'})-[:R1]->(b)-[:R2]->(c:C {name:'c1'})");
      });

      final ResultSet rs = db.query("opencypher", "MATCH (b:B) RETURN count(b) AS cnt");
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(2L);

      final ResultSet aCount = db.query("opencypher", "MATCH (a:A) RETURN count(a) AS cnt");
      assertThat(aCount.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);

      final ResultSet cCount = db.query("opencypher", "MATCH (c:C) RETURN count(c) AS cnt");
      assertThat(cCount.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);
    }
  }
}
