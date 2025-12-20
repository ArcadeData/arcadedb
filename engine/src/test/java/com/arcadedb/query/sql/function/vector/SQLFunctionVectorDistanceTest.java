package com.arcadedb.query.sql.function.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SQLFunctionVectorDistanceTest extends TestHelper {

  @Test
  void distanceCosineWithFloatArrays() {
    final SQLFunctionVectorDistance function = new SQLFunctionVectorDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Test cosine distance between two identical vectors (should be 0)
    final float[] v1 = new float[] { 1.0f, 0.0f, 0.0f };
    final float[] v2 = new float[] { 1.0f, 0.0f, 0.0f };

    final Object result = function.execute(null, null, null,
        new Object[] { v1, v2, "COSINE" },
        context);

    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).floatValue()).isCloseTo(0.0f, org.assertj.core.data.Offset.offset(0.001f));
  }

  @Test
  void distanceCosineWithDifferentVectors() {
    final SQLFunctionVectorDistance function = new SQLFunctionVectorDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Orthogonal vectors should have cosine distance close to 1.0 or higher
    final float[] v1 = new float[] { 1.0f, 0.0f, 0.0f };
    final float[] v2 = new float[] { 0.0f, 1.0f, 0.0f };

    final Object result = function.execute(null, null, null,
        new Object[] { v1, v2, "COSINE" },
        context);

    assertThat(result).isInstanceOf(Number.class);
    final float distance = ((Number) result).floatValue();
    assertThat(distance).isGreaterThan(0.5f); // Orthogonal vectors should have high distance
  }

  @Test
  void distanceEuclidean() {
    final SQLFunctionVectorDistance function = new SQLFunctionVectorDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Euclidean distance between (0, 0) and (3, 4) should be 5
    final float[] v1 = new float[] { 0.0f, 0.0f };
    final float[] v2 = new float[] { 3.0f, 4.0f };

    final Object result = function.execute(null, null, null,
        new Object[] { v1, v2, "EUCLIDEAN" },
        context);

    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).floatValue()).isCloseTo(5.0f, org.assertj.core.data.Offset.offset(0.001f));
  }

  @Test
  void distanceManhattan() {
    final SQLFunctionVectorDistance function = new SQLFunctionVectorDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Manhattan distance between (0, 0) and (3, 4) should be 7
    final float[] v1 = new float[] { 0.0f, 0.0f };
    final float[] v2 = new float[] { 3.0f, 4.0f };

    final Object result = function.execute(null, null, null,
        new Object[] { v1, v2, "MANHATTAN" },
        context);

    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).floatValue()).isCloseTo(7.0f, org.assertj.core.data.Offset.offset(0.001f));
  }

  @Test
  void distanceChebyshev() {
    final SQLFunctionVectorDistance function = new SQLFunctionVectorDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Chebyshev distance between (0, 0) and (3, 4) should be 4 (max of abs differences)
    final float[] v1 = new float[] { 0.0f, 0.0f };
    final float[] v2 = new float[] { 3.0f, 4.0f };

    final Object result = function.execute(null, null, null,
        new Object[] { v1, v2, "CHEBYSHEV" },
        context);

    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).floatValue()).isCloseTo(4.0f, org.assertj.core.data.Offset.offset(0.001f));
  }

  @Test
  void distanceWithListParameters() {
    final SQLFunctionVectorDistance function = new SQLFunctionVectorDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Test with lists instead of arrays
    final java.util.List<Number> v1 = java.util.List.of(0.0f, 0.0f);
    final java.util.List<Number> v2 = java.util.List.of(3.0f, 4.0f);

    final Object result = function.execute(null, null, null,
        new Object[] { v1, v2, "EUCLIDEAN" },
        context);

    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).floatValue()).isCloseTo(5.0f, org.assertj.core.data.Offset.offset(0.001f));
  }

  @Test
  void distanceWithObjectArrays() {
    final SQLFunctionVectorDistance function = new SQLFunctionVectorDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Test with Object arrays containing Numbers
    final Object[] v1 = new Object[] { 0.0, 0.0 };
    final Object[] v2 = new Object[] { 3.0, 4.0 };

    final Object result = function.execute(null, null, null,
        new Object[] { v1, v2, "EUCLIDEAN" },
        context);

    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).floatValue()).isCloseTo(5.0f, org.assertj.core.data.Offset.offset(0.001f));
  }

  @Test
  void sqlDistanceWithRawVectors() {
    String query = "SELECT vectorDistance([1.0, 0.0, 0.0], [1.0, 0.0, 0.0], 'COSINE') as dist";
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).as("Query should return results").isTrue();

      var result = results.next();
      final Number distance = result.getProperty("dist");

      assertThat(distance).isNotNull();
      assertThat(distance.floatValue()).isCloseTo(0.0f, org.assertj.core.data.Offset.offset(0.001f));
    }
  }

  @Test
  void sqlDistanceEuclidean() {
    String query = "SELECT vectorDistance([0.0, 0.0], [3.0, 4.0], 'EUCLIDEAN') as dist";
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).as("Query should return results").isTrue();

      var result = results.next();
      final Number distance = result.getProperty("dist");

      assertThat(distance).isNotNull();
      assertThat(distance.floatValue()).isCloseTo(5.0f, org.assertj.core.data.Offset.offset(0.001f));
    }
  }

  @Test
  void sqlDistanceMultipleAlgorithms() {
    String query = """
        SELECT
          vectorDistance([0.0, 0.0], [3.0, 4.0], 'EUCLIDEAN') as euclidean,
          vectorDistance([0.0, 0.0], [3.0, 4.0], 'MANHATTAN') as manhattan,
          vectorDistance([0.0, 0.0], [3.0, 4.0], 'CHEBYSHEV') as chebyshev
        """;

    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).as("Query should return results").isTrue();

      var result = results.next();
      final Number euclidean = result.getProperty("euclidean");
      final Number manhattan = result.getProperty("manhattan");
      final Number chebyshev = result.getProperty("chebyshev");

      assertThat(euclidean.floatValue()).isCloseTo(5.0f, org.assertj.core.data.Offset.offset(0.001f));
      assertThat(manhattan.floatValue()).isCloseTo(7.0f, org.assertj.core.data.Offset.offset(0.001f));
      assertThat(chebyshev.floatValue()).isCloseTo(4.0f, org.assertj.core.data.Offset.offset(0.001f));
    }
  }

  @Test
  void distanceNullVectorThrowsException() {
    final SQLFunctionVectorDistance function = new SQLFunctionVectorDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { null, new float[] { 1.0f }, "COSINE" },
        context))
        .isInstanceOf(com.arcadedb.exception.CommandSQLParsingException.class)
        .hasMessageContaining("Vectors cannot be null");
  }

  @Test
  void distanceDifferentDimensionsThrowsException() {
    final SQLFunctionVectorDistance function = new SQLFunctionVectorDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 0.0f }, new float[] { 1.0f }, "COSINE" },
        context))
        .isInstanceOf(com.arcadedb.exception.CommandSQLParsingException.class)
        .hasMessageContaining("same dimension");
  }

  @Test
  void distanceWrongParameterCountThrowsException() {
    final SQLFunctionVectorDistance function = new SQLFunctionVectorDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 1.0f }, new float[] { 1.0f } },
        context))
        .isInstanceOf(com.arcadedb.exception.CommandSQLParsingException.class);
  }

  @Test
  void distanceInvalidAlgorithmThrowsException() {
    final SQLFunctionVectorDistance function = new SQLFunctionVectorDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 1.0f }, new float[] { 1.0f }, "INVALID_ALGO" },
        context))
        .isInstanceOf(com.arcadedb.exception.CommandSQLParsingException.class)
        .hasMessageContaining("Unknown distance algorithm");
  }

  @Test
  void distanceSyntax() {
    final SQLFunctionVectorDistance function = new SQLFunctionVectorDistance();
    assertThat(function.getSyntax()).contains("vectorDistance").contains("vector").contains("algorithm");
  }
}
