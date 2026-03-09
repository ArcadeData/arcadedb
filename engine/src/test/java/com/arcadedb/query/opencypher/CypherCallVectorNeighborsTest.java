package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for calling vector search from Cypher via CALL.
 * Covers both the ArcadeDB-native vector.neighbors function and
 * the Neo4j-compatible db.index.vector.queryNodes procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCallVectorNeighborsTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Doc.name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (name) UNIQUE");

      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR
          METADATA {
            dimensions: 3,
            similarity: 'COSINE',
            idPropertyName: 'name'
          }""");
    });

    database.transaction(() -> {
      database.newVertex("Doc").set("name", "docA").set("embedding", new float[]{1.0f, 0.0f, 0.0f}).save();
      database.newVertex("Doc").set("name", "docB").set("embedding", new float[]{0.9f, 0.1f, 0.0f}).save();
      database.newVertex("Doc").set("name", "docC").set("embedding", new float[]{0.0f, 1.0f, 0.0f}).save();
      database.newVertex("Doc").set("name", "docD").set("embedding", new float[]{0.1f, 0.9f, 0.0f}).save();
      database.newVertex("Doc").set("name", "docE").set("embedding", new float[]{0.0f, 0.0f, 1.0f}).save();
    });
  }

  // ============================================================
  // vector.neighbors (ArcadeDB-native SQL function via CALL)
  // ============================================================

  @Test
  void callVectorNeighborsWithRawVector() {
    final Map<String, Object> params = new HashMap<>();
    params.put("vec", new float[]{0.0f, 0.0f, 1.0f});
    params.put("k", 3);

    try (ResultSet results = database.query("opencypher",
        "CALL vector.neighbors('Doc[embedding]', $vec, $k) YIELD distance, name RETURN name, distance ORDER BY distance",
        params)) {

      final List<String> names = new ArrayList<>();
      while (results.hasNext()) {
        final Result row = results.next();
        names.add(row.getProperty("name"));
        assertThat((Object) row.getProperty("distance")).isNotNull();
      }

      assertThat(names).isNotEmpty();
      assertThat(names.size()).isLessThanOrEqualTo(3);
      // docE has vector [0,0,1] which is the query vector, so it should be first (distance ~0)
      assertThat(names.get(0)).isEqualTo("docE");
    }
  }

  @Test
  void callVectorNeighborsWithVertexId() {
    try (ResultSet results = database.query("opencypher",
        "CALL vector.neighbors('Doc[embedding]', 'docA', 3) YIELD distance, name RETURN name, distance ORDER BY distance")) {

      final List<String> names = new ArrayList<>();
      while (results.hasNext()) {
        final Result row = results.next();
        names.add(row.getProperty("name"));
      }

      assertThat(names).isNotEmpty();
      // docB should be among neighbors of docA (similar vectors)
      assertThat(names).contains("docB");
    }
  }

  @Test
  void callVectorNeighborsReturnWithScore() {
    final Map<String, Object> params = new HashMap<>();
    params.put("vec", new float[]{1.0f, 0.0f, 0.0f});
    params.put("k", 2);

    try (ResultSet results = database.query("opencypher",
        "CALL vector.neighbors('Doc[embedding]', $vec, $k) YIELD name, distance RETURN name AS title, (1 - distance) AS score ORDER BY score DESC",
        params)) {

      assertThat(results.hasNext()).isTrue();
      final Result first = results.next();
      assertThat((String) first.getProperty("title")).isEqualTo("docA");
      assertThat(((Number) first.getProperty("score")).doubleValue()).isGreaterThan(0.9);
    }
  }

  // ============================================================
  // db.index.vector.queryNodes (Neo4j-compatible procedure)
  // ============================================================

  @Test
  void queryNodesNeo4jCompatible() {
    // Neo4j syntax: CALL db.index.vector.queryNodes(indexName, k, vector) YIELD node, score
    final Map<String, Object> params = new HashMap<>();
    params.put("vec", new float[]{0.0f, 0.0f, 1.0f});
    params.put("k", 3);

    try (ResultSet results = database.query("opencypher",
        "CALL db.index.vector.queryNodes('Doc[embedding]', $k, $vec) YIELD node, score RETURN node.name AS name, score ORDER BY score DESC",
        params)) {

      final List<String> names = new ArrayList<>();
      final List<Double> scores = new ArrayList<>();
      while (results.hasNext()) {
        final Result row = results.next();
        names.add(row.getProperty("name"));
        scores.add(((Number) row.getProperty("score")).doubleValue());
      }

      assertThat(names).isNotEmpty();
      assertThat(names.size()).isLessThanOrEqualTo(3);
      // docE has vector [0,0,1] which is the query vector, so it should be first with score ~1.0
      assertThat(names.get(0)).isEqualTo("docE");
      assertThat(scores.get(0)).isGreaterThan(0.9);

      // Scores should be in descending order
      for (int i = 1; i < scores.size(); i++)
        assertThat(scores.get(i)).isLessThanOrEqualTo(scores.get(i - 1));
    }
  }

  @Test
  void queryNodesYieldsDocumentNode() {
    // Verify that the 'node' yield is an actual document with accessible properties
    final Map<String, Object> params = new HashMap<>();
    params.put("vec", new float[]{1.0f, 0.0f, 0.0f});

    try (ResultSet results = database.query("opencypher",
        "CALL db.index.vector.queryNodes('Doc[embedding]', 1, $vec) YIELD node, score RETURN node, score",
        params)) {

      assertThat(results.hasNext()).isTrue();
      final Result row = results.next();

      final Object node = row.getProperty("node");
      assertThat(node).isInstanceOf(Document.class);
      assertThat(((Document) node).getString("name")).isEqualTo("docA");

      final double score = ((Number) row.getProperty("score")).doubleValue();
      assertThat(score).isGreaterThan(0.9);
    }
  }

  @Test
  void queryNodesWithReturnPattern() {
    // Exact Neo4j pattern from the user's benchmark query
    final Map<String, Object> params = new HashMap<>();
    params.put("vec", new float[]{1.0f, 0.0f, 0.0f});
    params.put("k", 2);

    try (ResultSet results = database.query("opencypher",
        "CALL db.index.vector.queryNodes('Doc[embedding]', $k, $vec) YIELD node, score RETURN node.name AS title, score",
        params)) {

      assertThat(results.hasNext()).isTrue();
      final Result first = results.next();
      assertThat((String) first.getProperty("title")).isEqualTo("docA");
      assertThat(((Number) first.getProperty("score")).doubleValue()).isGreaterThan(0.9);
    }
  }
}
