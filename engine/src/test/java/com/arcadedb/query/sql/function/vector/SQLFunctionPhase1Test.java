package com.arcadedb.query.sql.function.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SQLFunctionPhase1Test extends TestHelper {

  // ========== Phase 1.1: Basic Operations Tests ==========

  @Test
  void vectorNormalizeWithFloatArray() {
    final SQLFunctionVectorNormalize function = new SQLFunctionVectorNormalize();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Test normalizing a simple vector [3, 4] should give [0.6, 0.8] (magnitude = 5)
    final float[] result = (float[]) function.execute(null, null, null,
        new Object[] { new float[] { 3.0f, 4.0f } },
        context);

    assertThat(result).hasSize(2);
    assertThat(result[0]).isCloseTo(0.6f, Offset.offset(0.001f));
    assertThat(result[1]).isCloseTo(0.8f, Offset.offset(0.001f));
  }

  @Test
  void vectorNormalizeWithList() {
    final SQLFunctionVectorNormalize function = new SQLFunctionVectorNormalize();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final float[] result = (float[]) function.execute(null, null, null,
        new Object[] { List.of(1.0, 0.0, 0.0) },
        context);

    assertThat(result).hasSize(3);
    assertThat(result[0]).isCloseTo(1.0f, Offset.offset(0.001f));
    assertThat(result[1]).isCloseTo(0.0f, Offset.offset(0.001f));
    assertThat(result[2]).isCloseTo(0.0f, Offset.offset(0.001f));
  }

  @Test
  void vectorNormalizeZeroVector() {
    final SQLFunctionVectorNormalize function = new SQLFunctionVectorNormalize();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Zero vector should return itself
    final float[] result = (float[]) function.execute(null, null, null,
        new Object[] { new float[] { 0.0f, 0.0f, 0.0f } },
        context);

    assertThat(result).containsExactly(0.0f, 0.0f, 0.0f);
  }

  @Test
  void vectorMagnitude() {
    final SQLFunctionVectorMagnitude function = new SQLFunctionVectorMagnitude();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Magnitude of [3, 4] should be 5
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 3.0f, 4.0f } },
        context);

    assertThat(result).isCloseTo(5.0f, Offset.offset(0.001f));
  }

  @Test
  void vectorMagnitudeUnit() {
    final SQLFunctionVectorMagnitude function = new SQLFunctionVectorMagnitude();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Magnitude of [1, 0, 0] should be 1
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 0.0f, 0.0f } },
        context);

    assertThat(result).isCloseTo(1.0f, Offset.offset(0.001f));
  }

  @Test
  void vectorDimension() {
    final SQLFunctionVectorDimension function = new SQLFunctionVectorDimension();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final int dims = (int) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f, 3.0f, 4.0f } },
        context);

    assertThat(dims).isEqualTo(4);
  }

  @Test
  void vectorDimensionWithList() {
    final SQLFunctionVectorDimension function = new SQLFunctionVectorDimension();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final int dims = (int) function.execute(null, null, null,
        new Object[] { List.of(1, 2, 3) },
        context);

    assertThat(dims).isEqualTo(3);
  }

  @Test
  void vectorDotProduct() {
    final SQLFunctionVectorDotProduct function = new SQLFunctionVectorDotProduct();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Dot product of [1, 2, 3] and [4, 5, 6] = 1*4 + 2*5 + 3*6 = 32
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f, 3.0f }, new float[] { 4.0f, 5.0f, 6.0f } },
        context);

    assertThat(result).isCloseTo(32.0f, Offset.offset(0.001f));
  }

  @Test
  void vectorDotProductOrthogonal() {
    final SQLFunctionVectorDotProduct function = new SQLFunctionVectorDotProduct();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Orthogonal vectors should have dot product of 0
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 0.0f, 0.0f }, new float[] { 0.0f, 1.0f, 0.0f } },
        context);

    assertThat(result).isCloseTo(0.0f, Offset.offset(0.001f));
  }

  // ========== Phase 1.2: Similarity Scoring Tests ==========

  @Test
  void vectorCosineSimilarityIdentical() {
    final SQLFunctionVectorCosineSimilarity function = new SQLFunctionVectorCosineSimilarity();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Identical vectors should have cosine similarity of 1.0
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 0.0f, 0.0f }, new float[] { 1.0f, 0.0f, 0.0f } },
        context);

    assertThat(result).isCloseTo(1.0f, Offset.offset(0.001f));
  }

  @Test
  void vectorCosineSimilarityOrthogonal() {
    final SQLFunctionVectorCosineSimilarity function = new SQLFunctionVectorCosineSimilarity();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Orthogonal vectors should have cosine similarity of 0.0
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 0.0f, 0.0f }, new float[] { 0.0f, 1.0f, 0.0f } },
        context);

    assertThat(result).isCloseTo(0.0f, Offset.offset(0.001f));
  }

  @Test
  void vectorCosineSimilarityOpposite() {
    final SQLFunctionVectorCosineSimilarity function = new SQLFunctionVectorCosineSimilarity();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Opposite vectors should have cosine similarity of -1.0
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 0.0f, 0.0f }, new float[] { -1.0f, 0.0f, 0.0f } },
        context);

    assertThat(result).isCloseTo(-1.0f, Offset.offset(0.001f));
  }

  @Test
  void vectorL2Distance() {
    final SQLFunctionVectorL2Distance function = new SQLFunctionVectorL2Distance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // L2 distance between [0, 0] and [3, 4] should be 5
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 0.0f, 0.0f }, new float[] { 3.0f, 4.0f } },
        context);

    assertThat(result).isCloseTo(5.0f, Offset.offset(0.001f));
  }

  @Test
  void vectorL2DistanceZero() {
    final SQLFunctionVectorL2Distance function = new SQLFunctionVectorL2Distance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Distance between identical vectors should be 0
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f, 3.0f }, new float[] { 1.0f, 2.0f, 3.0f } },
        context);

    assertThat(result).isCloseTo(0.0f, Offset.offset(0.001f));
  }

  @Test
  void vectorInnerProduct() {
    final SQLFunctionVectorDotProduct function = new SQLFunctionVectorDotProduct();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Inner product of [1, 2, 3] and [4, 5, 6] = 32
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f, 3.0f }, new float[] { 4.0f, 5.0f, 6.0f } },
        context);

    assertThat(result).isCloseTo(32.0f, Offset.offset(0.001f));
  }

  @Test
  void vectorInnerProductNegative() {
    final SQLFunctionVectorDotProduct function = new SQLFunctionVectorDotProduct();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Inner product with opposing vectors can be negative
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 0.0f }, new float[] { -1.0f, 0.0f } },
        context);

    assertThat(result).isCloseTo(-1.0f, Offset.offset(0.001f));
  }

  // ========== SQL Integration Tests ==========

  @Test
  void sqlVectorNormalize() {
    String query = "SELECT vectorNormalize([3.0, 4.0]) as normalized";
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();

      var result = results.next();
      final float[] normalized = result.getProperty("normalized");

      assertThat(normalized).hasSize(2);
      assertThat(normalized[0]).isCloseTo(0.6f, Offset.offset(0.001f));
      assertThat(normalized[1]).isCloseTo(0.8f, Offset.offset(0.001f));
    }
  }

  @Test
  void sqlVectorMagnitude() {
    String query = "SELECT vectorMagnitude([3.0, 4.0]) as magnitude";
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();

      var result = results.next();
      final float magnitude = result.getProperty("magnitude");

      assertThat(magnitude).isCloseTo(5.0f, Offset.offset(0.001f));
    }
  }

  @Test
  void sqlVectorDimension() {
    String query = "SELECT vectorDimension([1.0, 2.0, 3.0, 4.0, 5.0]) as dims";
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();

      var result = results.next();
      final int dims = result.getProperty("dims");

      assertThat(dims).isEqualTo(5);
    }
  }

  @Test
  void sqlVectorDotProduct() {
    String query = "SELECT vectorDotProduct([1.0, 2.0, 3.0], [4.0, 5.0, 6.0]) as dot";
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();

      var result = results.next();
      final float dot = result.getProperty("dot");

      assertThat(dot).isCloseTo(32.0f, Offset.offset(0.001f));
    }
  }

  @Test
  void sqlVectorCosineSimilarity() {
    String query = "SELECT vectorCosineSimilarity([1.0, 0.0, 0.0], [1.0, 0.0, 0.0]) as similarity";
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();

      var result = results.next();
      final float similarity = result.getProperty("similarity");

      assertThat(similarity).isCloseTo(1.0f, Offset.offset(0.001f));
    }
  }

  @Test
  void sqlVectorL2Distance() {
    String query = "SELECT vectorL2Distance([0.0, 0.0], [3.0, 4.0]) as distance";
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();

      var result = results.next();
      final float distance = result.getProperty("distance");

      assertThat(distance).isCloseTo(5.0f, Offset.offset(0.001f));
    }
  }

  @Test
  void sqlPhase1Combined() {
    String query = """
        SELECT
          vectorDimension([1.0, 2.0, 3.0]) as dims,
          vectorMagnitude([1.0, 2.0]) as magnitude,
          vectorDotProduct([1.0, 2.0], [3.0, 4.0]) as dot,
          vectorL2Distance([0.0, 0.0], [3.0, 4.0]) as distance,
          vectorCosineSimilarity([1.0, 0.0], [1.0, 0.0]) as cosine
        """;

    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();

      var result = results.next();
      assertThat(result.<Integer>getProperty("dims")).isEqualTo(3);
      assertThat(result.<Float>getProperty("magnitude")).isCloseTo(2.236f, Offset.offset(0.01f));
      assertThat(result.<Float>getProperty("dot")).isCloseTo(11.0f, Offset.offset(0.001f));
      assertThat(result.<Float>getProperty("distance")).isCloseTo(5.0f, Offset.offset(0.001f));
      assertThat(result.<Float>getProperty("cosine")).isCloseTo(1.0f, Offset.offset(0.001f));
    }
  }

  // ========== Error Handling Tests ==========

  @Test
  void vectorNormalizeNullVector() {
    final SQLFunctionVectorNormalize function = new SQLFunctionVectorNormalize();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { null },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("null");
  }

  @Test
  void vectorDotProductDimensionMismatch() {
    final SQLFunctionVectorDotProduct function = new SQLFunctionVectorDotProduct();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f }, new float[] { 1.0f, 2.0f, 3.0f } },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("dimension");
  }

  @Test
  void vectorL2DistanceDimensionMismatch() {
    final SQLFunctionVectorL2Distance function = new SQLFunctionVectorL2Distance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 1.0f }, new float[] { 1.0f, 2.0f } },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("dimension");
  }
}
