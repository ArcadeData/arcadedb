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
 */
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.api.Assertions;
import static org.assertj.core.api.Assertions.within;

/**
 * Comprehensive tests for OpenCypher Vector functions based on Neo4j Cypher documentation.
 * Tests cover: vector(), vector.similarity.cosine(), vector.similarity.euclidean(),
 * vector_dimension_count(), vector_distance(), vector_norm()
 */
class OpenCypherVectorFunctionsComprehensiveTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/testOpenCypherVectorFunctions");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  // ==================== vector() Tests ====================

  @Test
  void vectorFromList() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector([1, 2, 3], 3, INTEGER) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void vectorFromString() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector('[1.05000e+00, 0.123, 5]', 3, FLOAT) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void vectorInteger8() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector([1, 2, 3], 3, INTEGER8) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void vectorFloat32() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector([1.0, 2.5, 3.7], 3, FLOAT32) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void vectorFloat64() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector([1.0, 2.5, 3.7], 3, FLOAT64) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void vectorNullValue() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector(null, 3, FLOAT32) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void vectorNullDimension() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector([1, 2, 3], null, INTEGER8) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== vector.similarity.cosine() Tests ====================

  @Test
  void vectorSimilarityCosineIdentical() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector.similarity.cosine([1, 2, 3], [1, 2, 3]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double similarity = (Double) result.next().getProperty("result");
    assertThat(similarity).isCloseTo(1.0, within(0.0001));
  }

  @Test
  void vectorSimilarityCosineOrthogonal() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector.similarity.cosine([1, 0, 0], [0, 1, 0]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double similarity = (Double) result.next().getProperty("result");
    assertThat(similarity).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void vectorSimilarityCosineWithVectorType() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector.similarity.cosine(vector([1, 2, 3], 3, FLOAT32), vector([1, 2, 4], 3, FLOAT32)) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double similarity = (Double) result.next().getProperty("result");
    Assertions.assertThat(similarity != null).isTrue();
    assertThat(similarity).isGreaterThan(0.0);
    assertThat(similarity).isLessThanOrEqualTo(1.0);
  }

  @Test
  void vectorSimilarityCosineNull() {
    ResultSet result = database.command("opencypher",
        "RETURN vector.similarity.cosine(null, [1, 2, 3]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher",
        "RETURN vector.similarity.cosine([1, 2, 3], null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== vector.similarity.euclidean() Tests ====================

  @Test
  void vectorSimilarityEuclideanIdentical() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector.similarity.euclidean([1, 2, 3], [1, 2, 3]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double similarity = (Double) result.next().getProperty("result");
    assertThat(similarity).isCloseTo(1.0, within(0.0001));
  }

  @Test
  void vectorSimilarityEuclideanDifferent() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector.similarity.euclidean([1, 2, 3], [4, 5, 6]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double similarity = (Double) result.next().getProperty("result");
    Assertions.assertThat(similarity != null).isTrue();
    assertThat(similarity).isGreaterThan(0.0);
    assertThat(similarity).isLessThan(1.0);
  }

  @Test
  void vectorSimilarityEuclideanWithVectorType() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector.similarity.euclidean(vector([1.0, 4.0, 2.0], 3, FLOAT32), vector([3.0, -2.0, 1.0], 3, FLOAT32)) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double similarity = (Double) result.next().getProperty("result");
    Assertions.assertThat(similarity != null).isTrue();
    assertThat(similarity).isGreaterThan(0.0);
    assertThat(similarity).isLessThanOrEqualTo(1.0);
  }

  @Test
  void vectorSimilarityEuclideanNull() {
    ResultSet result = database.command("opencypher",
        "RETURN vector.similarity.euclidean(null, [1, 2, 3]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher",
        "RETURN vector.similarity.euclidean([1, 2, 3], null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== vector_dimension_count() Tests ====================

  @Test
  void vectorDimensionCountBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector_dimension_count(vector([1, 2, 3], 3, INTEGER8)) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(3);
  }

  @Test
  void vectorDimensionCountLargeDimension() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector_dimension_count(vector([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 10, FLOAT32)) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(10);
  }

  @Test
  void vectorDimensionCountSingleDimension() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector_dimension_count(vector([42], 1, INTEGER)) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(1);
  }

  // ==================== vector_distance() Tests ====================

  @Test
  void vectorDistanceEuclidean() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector_distance(vector([1.0, 5.0, 3.0, 6.7], 4, FLOAT32), vector([5.0, 2.5, 3.1, 9.0], 4, FLOAT32), EUCLIDEAN) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Number distance = (Number) result.next().getProperty("result");
    assertThat(distance.doubleValue()).isCloseTo(5.248, within(0.01));
  }

  @Test
  void vectorDistanceEuclideanSquared() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector_distance(vector([1, 2, 3], 3, INTEGER8), vector([4, 5, 6], 3, INTEGER8), EUCLIDEAN_SQUARED) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Number distance = (Number) result.next().getProperty("result");
    assertThat(distance.doubleValue()).isCloseTo(27.0, within(0.01));
  }

  @Test
  void vectorDistanceManhattan() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector_distance(vector([1, 2, 3], 3, INTEGER8), vector([4, 5, 6], 3, INTEGER8), MANHATTAN) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Number distance = (Number) result.next().getProperty("result");
    assertThat(distance.doubleValue()).isCloseTo(9.0, within(0.01));
  }

  @Test
  void vectorDistanceCosine() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector_distance(vector([1, 2, 3], 3, INTEGER8), vector([1, 2, 4], 3, INTEGER8), COSINE) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Number distance = (Number) result.next().getProperty("result");
    assertThat(distance.doubleValue()).isCloseTo(0.008539, within(0.001));
  }

  @Test
  void vectorDistanceDot() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector_distance(vector([1, 2, 3], 3, INTEGER8), vector([4, 5, 6], 3, INTEGER8), DOT) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Number distance = (Number) result.next().getProperty("result");
    Assertions.assertThat(distance != null).isTrue();
  }

  @Test
  void vectorDistanceHamming() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector_distance(vector([1, 2, 3], 3, INTEGER8), vector([1, 2, 4], 3, INTEGER8), HAMMING) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Number distance = (Number) result.next().getProperty("result");
    assertThat(distance.doubleValue()).isCloseTo(1.0, within(0.01));
  }

  @Test
  void vectorDistanceIdenticalVectors() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector_distance(vector([1, 2, 3], 3, INTEGER8), vector([1, 2, 3], 3, INTEGER8), EUCLIDEAN) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Number distance = (Number) result.next().getProperty("result");
    assertThat(distance.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  // ==================== vector_norm() Tests ====================

  @Test
  void vectorNormEuclidean() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector_norm(vector([1.0, 5.0, 3.0, 6.7], 4, FLOAT32), EUCLIDEAN) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Number norm = (Number) result.next().getProperty("result");
    assertThat(norm.doubleValue()).isCloseTo(8.938, within(0.01));
  }

  @Test
  void vectorNormManhattan() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector_norm(vector([1.0, 5.0, 3.0, 6.7], 4, FLOAT32), MANHATTAN) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Number norm = (Number) result.next().getProperty("result");
    assertThat(norm.doubleValue()).isCloseTo(15.7, within(0.01));
  }

  @Test
  void vectorNormZeroVector() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector_norm(vector([0, 0, 0], 3, FLOAT32), EUCLIDEAN) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Number norm = (Number) result.next().getProperty("result");
    assertThat(norm.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void vectorNormUnitVector() {
    final ResultSet result = database.command("opencypher",
        "RETURN vector_norm(vector([1, 0, 0], 3, FLOAT32), EUCLIDEAN) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Number norm = (Number) result.next().getProperty("result");
    assertThat(norm.doubleValue()).isCloseTo(1.0, within(0.0001));
  }

  // ==================== Combined/Integration Tests ====================

  @Test
  void vectorSimilarityAndDistanceComparison() {
    // For identical vectors, similarity should be 1 and distance should be 0
    final ResultSet result = database.command("opencypher",
        "WITH vector([1, 2, 3], 3, FLOAT32) AS v " +
            "RETURN vector.similarity.cosine(v, v) AS cosSim, " +
            "       vector.similarity.euclidean(v, v) AS eucSim, " +
            "       vector_distance(v, v, EUCLIDEAN) AS eucDist");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    final Number cosSim = (Number) row.getProperty("cosSim");
    final Number eucSim = (Number) row.getProperty("eucSim");
    final Number eucDist = (Number) row.getProperty("eucDist");
    assertThat(cosSim.doubleValue()).isCloseTo(1.0, within(0.0001));
    assertThat(eucSim.doubleValue()).isCloseTo(1.0, within(0.0001));
    assertThat(eucDist.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void vectorDimensionAndSizeConsistency() {
    final ResultSet result = database.command("opencypher",
        "WITH vector([1, 2, 3, 4, 5], 5, INTEGER) AS v " +
            "RETURN vector_dimension_count(v) AS dimCount, size(v) AS sizeResult");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat(((Number) row.getProperty("dimCount")).intValue()).isEqualTo(5);
    assertThat(((Number) row.getProperty("sizeResult")).intValue()).isEqualTo(5);
  }

  @Test
  void vectorNormAndDistanceRelationship() {
    // For a vector, its norm should equal the distance from origin
    final ResultSet result = database.command("opencypher",
        "WITH vector([3, 4], 2, FLOAT32) AS v " +
            "RETURN vector_norm(v, EUCLIDEAN) AS norm, " +
            "       vector_distance(v, vector([0, 0], 2, FLOAT32), EUCLIDEAN) AS distFromOrigin");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    final Number norm = (Number) row.getProperty("norm");
    final Number distFromOrigin = (Number) row.getProperty("distFromOrigin");
    assertThat(norm.doubleValue()).isCloseTo(distFromOrigin.doubleValue(), within(0.0001));
  }

  @Test
  void vectorKNearestNeighbors() {
    // Create sample vectors and find k-nearest neighbors
    database.command("opencypher",
        "CREATE (:Node {id: 1, vector: vector([1.0, 4.0, 2.0], 3, FLOAT32)})");
    database.command("opencypher",
        "CREATE (:Node {id: 2, vector: vector([3.0, -2.0, 1.0], 3, FLOAT32)})");
    database.command("opencypher",
        "CREATE (:Node {id: 3, vector: vector([2.0, 8.0, 3.0], 3, FLOAT32)})");

    final ResultSet result = database.command("opencypher",
        "MATCH (node:Node) " +
            "WITH node, vector.similarity.euclidean([4.0, 5.0, 6.0], node.vector) AS score " +
            "RETURN node.id AS id, score " +
            "ORDER BY score DESC " +
            "LIMIT 2");

    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row1 = result.next();
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row2 = result.next();

    // Verify we got 2 results
    Assertions.assertThat(row1.getProperty("id") != null).isTrue();
    Assertions.assertThat(row2.getProperty("id") != null).isTrue();
    Assertions.assertThat(row1.getProperty("score") != null).isTrue();
    Assertions.assertThat(row2.getProperty("score") != null).isTrue();
  }

  @Test
  void vectorMultipleDistanceMetrics() {
    final ResultSet result = database.command("opencypher",
        "WITH vector([1, 2, 3], 3, FLOAT32) AS v1, vector([4, 5, 6], 3, FLOAT32) AS v2 " +
            "RETURN vector_distance(v1, v2, EUCLIDEAN) AS euclidean, " +
            "       vector_distance(v1, v2, MANHATTAN) AS manhattan, " +
            "       vector_distance(v1, v2, COSINE) AS cosine");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("euclidean") != null).isTrue();
    Assertions.assertThat(row.getProperty("manhattan") != null).isTrue();
    Assertions.assertThat(row.getProperty("cosine") != null).isTrue();

    final Number euclidean = (Number) row.getProperty("euclidean");
    final Number manhattan = (Number) row.getProperty("manhattan");
    assertThat(euclidean.doubleValue()).isGreaterThan(0.0);
    assertThat(manhattan.doubleValue()).isGreaterThan(0.0);
  }
}
