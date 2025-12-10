package com.arcadedb.query.sql.function.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SQLFunctionVectorNeighborsTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Doc.name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (name) UNIQUE");

      // Create the vector index before inserting data (needed for automatic indexing)
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR
          METADATA {
            dimensions: 3,
            similarity: 'COSINE',
            idPropertyName: 'name'
          }""");
    });

    // Insert some documents with known vectors
    database.transaction(() -> {
      database.newVertex("Doc").set("name", "docA").set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      database.newVertex("Doc").set("name", "docB").set("embedding", new float[] { 0.9f, 0.1f, 0.0f }).save(); // Close to A
      database.newVertex("Doc").set("name", "docC").set("embedding", new float[] { 0.0f, 1.0f, 0.0f })
          .save(); // Far from A, close to D
      database.newVertex("Doc").set("name", "docD").set("embedding", new float[] { 0.1f, 0.9f, 0.0f }).save(); // Close to C
      database.newVertex("Doc").set("name", "docE").set("embedding", new float[] { 0.0f, 0.0f, 1.0f }).save(); // Far from all
    });
  }

  @Test
  void programmaticVectorSearchWithRawVector() {

    final SQLFunctionVectorNeighbors function = new SQLFunctionVectorNeighbors();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Search with a raw vector (similar to docE)
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> results = (List<Map<String, Object>>) function.execute(null, null, null,
        new Object[] { "Doc[embedding]", new float[] { 0.0f, 0.0f, 1.0f }, 3 },
        context);

    assertThat(results).as("Results should not be null").isNotNull();
    assertThat(results.isEmpty()).as("Should find at least one neighbor").isFalse();
    assertThat(results.size() <= 3).as("Should return at most 3 results").isTrue();

    // Verify results contain the expected structure
    for (Map<String, Object> result : results) {
      assertThat(result.containsKey("vertex")).as("Result should contain 'vertex' key").isTrue();
      assertThat(result.containsKey("distance")).as("Result should contain 'distance' key").isTrue();
      assertThat(result.get("distance")).as("Distance should not be null").isNotNull();
    }
  }

  @Test
  void programmaticVectorSearchWithVertexId() {

    final SQLFunctionVectorNeighbors function = new SQLFunctionVectorNeighbors();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Search using a vertex identifier (docA)
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> results = (List<Map<String, Object>>) function.execute(null, null, null,
        new Object[] { "Doc[embedding]", "docA", 3 },
        context);

    assertThat(results).isNotNull();
    assertThat(results.isEmpty()).as("Should find neighbors").isFalse();
    assertThat(results.size() <= 3).as("Should return at most 3 results").isTrue();

    // docB should be close to docA (both have similar vectors)
    boolean foundDocB = results.stream()
        .anyMatch(r -> r.get("vertex").toString().contains("docB"));
    assertThat(foundDocB).as("DocB should be found as a neighbor of DocA").isTrue();
  }

  @Test
  void sqlVectorNeighborsWithRawVector() {

    // SQL query with raw vector
    String query = "SELECT vectorNeighbors('Doc[embedding]', [0.0, 0.0, 1.0], 3) as neighbors";
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).as("Query should return results").isTrue();

      var result = results.next();
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> neighbors = result.getProperty("neighbors");

      assertThat(neighbors).isNotNull();
      assertThat(neighbors.isEmpty()).as("Should find at least one neighbor").isFalse();
      assertThat(neighbors.size() <= 3).as("Should return at most 3 results").isTrue();
    }
  }

  @Test
  void sqlVectorNeighborsWithVertexId() {

    // SQL query with vertex identifier
    String query = "SELECT vectorNeighbors('Doc[embedding]', 'docC', 2) as neighbors";
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).as("Query should return results").isTrue();

      var result = results.next();
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> neighbors = result.getProperty("neighbors");

      assertThat(neighbors).isNotNull();
      assertThat(neighbors).as("Should find neighbors for docC").isNotEmpty();
      assertThat(neighbors).as("Should return at most 2 results").hasSizeLessThanOrEqualTo(2);
    }
  }

  @Test
  void sqlVectorNeighborsResultsAreSortedByDistance() {

    // SQL query using vectorNeighbors in a subquery
    String query = """
        SELECT vectorNeighbors('Doc[embedding]', 'docA', 5) as neighbors
        """;

    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).as("Query should return results").isTrue();

      var result = results.next();
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> neighbors = result.getProperty("neighbors");

      assertThat(neighbors).as("Neighbors should not be null").isNotNull();
      assertThat(neighbors).as("Should find neighbors").isNotEmpty();

      // Verify neighbors are sorted by distance (closer distances first)
      float previousDistance = -1;
      for (Map<String, Object> neighbor : neighbors) {
        float distance = ((Number) neighbor.get("distance")).floatValue();
        assertThat(distance ).as("Results should be ordered by distance").isGreaterThanOrEqualTo(previousDistance);
        previousDistance = distance;
      }
    }
  }

  @Test
  void vectorNeighborsLimitParameter() {

    final SQLFunctionVectorNeighbors function = new SQLFunctionVectorNeighbors();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Test with limit of 2
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resultsLimit2 = (List<Map<String, Object>>) function.execute(null, null, null,
        new Object[] { "Doc[embedding]", new float[] { 1.0f, 0.0f, 0.0f }, 2 },
        context);

    // Test with limit of 5
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resultsLimit5 = (List<Map<String, Object>>) function.execute(null, null, null,
        new Object[] { "Doc[embedding]", new float[] { 1.0f, 0.0f, 0.0f }, 5 },
        context);

    assertThat(resultsLimit2).as("Should respect limit of 2").hasSizeLessThanOrEqualTo(2);
    assertThat(resultsLimit5).as("Should respect limit of 5").hasSizeLessThanOrEqualTo(5);
    assertThat(resultsLimit5).as("Larger limit should return more or equal results")
        .hasSizeGreaterThanOrEqualTo(resultsLimit2.size());
  }
}
