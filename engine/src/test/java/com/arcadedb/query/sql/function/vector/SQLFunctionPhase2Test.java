package com.arcadedb.query.sql.function.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SQLFunctionPhase2Test extends TestHelper {

  // ========== Phase 2.1: Vector Arithmetic Tests ==========

  @Test
  void vectorAdd() {
    final SQLFunctionVectorAdd function = new SQLFunctionVectorAdd();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // [1, 2, 3] + [4, 5, 6] = [5, 7, 9]
    final float[] result = (float[]) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f, 3.0f }, new float[] { 4.0f, 5.0f, 6.0f } },
        context);

    assertThat(result).containsExactly(5.0f, 7.0f, 9.0f);
  }

  @Test
  void vectorSubtract() {
    final SQLFunctionVectorSubtract function = new SQLFunctionVectorSubtract();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // [5, 7, 9] - [1, 2, 3] = [4, 5, 6]
    final float[] result = (float[]) function.execute(null, null, null,
        new Object[] { new float[] { 5.0f, 7.0f, 9.0f }, new float[] { 1.0f, 2.0f, 3.0f } },
        context);

    assertThat(result).containsExactly(4.0f, 5.0f, 6.0f);
  }

  @Test
  void vectorMultiply() {
    final SQLFunctionVectorMultiply function = new SQLFunctionVectorMultiply();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // [2, 3, 4] * [1, 2, 3] = [2, 6, 12]
    final float[] result = (float[]) function.execute(null, null, null,
        new Object[] { new float[] { 2.0f, 3.0f, 4.0f }, new float[] { 1.0f, 2.0f, 3.0f } },
        context);

    assertThat(result).containsExactly(2.0f, 6.0f, 12.0f);
  }

  @Test
  void vectorScale() {
    final SQLFunctionVectorScale function = new SQLFunctionVectorScale();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // [1, 2, 3] * 2.5 = [2.5, 5.0, 7.5]
    final float[] result = (float[]) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f, 3.0f }, 2.5f },
        context);

    assertThat(result).containsExactly(2.5f, 5.0f, 7.5f);
  }

  @Test
  void vectorScaleWithNegative() {
    final SQLFunctionVectorScale function = new SQLFunctionVectorScale();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // [1, 2, 3] * -1.0 = [-1, -2, -3]
    final float[] result = (float[]) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f, 3.0f }, -1.0f },
        context);

    assertThat(result).containsExactly(-1.0f, -2.0f, -3.0f);
  }

  @Test
  void vectorScaleZero() {
    final SQLFunctionVectorScale function = new SQLFunctionVectorScale();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // [1, 2, 3] * 0 = [0, 0, 0]
    final float[] result = (float[]) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f, 3.0f }, 0.0f },
        context);

    assertThat(result).containsExactly(0.0f, 0.0f, 0.0f);
  }

  // ========== Phase 2.2: Vector Aggregation Tests ==========

  @Test
  void vectorSum() {
    final SQLFunctionVectorSum function = new SQLFunctionVectorSum();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Sum: [1, 2] + [2, 3] + [3, 4] = [6, 9]
    function.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f } }, context);
    function.execute(null, null, null, new Object[] { new float[] { 2.0f, 3.0f } }, context);
    function.execute(null, null, null, new Object[] { new float[] { 3.0f, 4.0f } }, context);

    final float[] result = (float[]) function.getResult();
    assertThat(result).containsExactly(6.0f, 9.0f);
  }

  @Test
  void vectorAvg() {
    final SQLFunctionVectorAvg function = new SQLFunctionVectorAvg();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Average: ([1, 2] + [2, 3] + [3, 4]) / 3 = [2.0, 3.0]
    function.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f } }, context);
    function.execute(null, null, null, new Object[] { new float[] { 2.0f, 3.0f } }, context);
    function.execute(null, null, null, new Object[] { new float[] { 3.0f, 4.0f } }, context);

    final float[] result = (float[]) function.getResult();
    assertThat(result[0]).isCloseTo(2.0f, Offset.offset(0.001f));
    assertThat(result[1]).isCloseTo(3.0f, Offset.offset(0.001f));
  }

  @Test
  void vectorMin() {
    final SQLFunctionVectorMin function = new SQLFunctionVectorMin();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Min: min([1, 5], [2, 3], [3, 2]) = [1, 2]
    function.execute(null, null, null, new Object[] { new float[] { 1.0f, 5.0f } }, context);
    function.execute(null, null, null, new Object[] { new float[] { 2.0f, 3.0f } }, context);
    function.execute(null, null, null, new Object[] { new float[] { 3.0f, 2.0f } }, context);

    final float[] result = (float[]) function.getResult();
    assertThat(result).containsExactly(1.0f, 2.0f);
  }

  @Test
  void vectorMax() {
    final SQLFunctionVectorMax function = new SQLFunctionVectorMax();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Max: max([1, 5], [2, 3], [3, 2]) = [3, 5]
    function.execute(null, null, null, new Object[] { new float[] { 1.0f, 5.0f } }, context);
    function.execute(null, null, null, new Object[] { new float[] { 2.0f, 3.0f } }, context);
    function.execute(null, null, null, new Object[] { new float[] { 3.0f, 2.0f } }, context);

    final float[] result = (float[]) function.getResult();
    assertThat(result).containsExactly(3.0f, 5.0f);
  }

  // ========== SQL Integration Tests ==========

  @Test
  void sqlVectorAdd() {
    String query = "SELECT vectorAdd( [1.0, 2.0, 3.0], [4.0, 5.0, 6.0] ) as sum";
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();

      var result = results.next();
      final float[] sum = result.getProperty("sum");

      assertThat(sum).containsExactly(5.0f, 7.0f, 9.0f);
    }
  }

  @Test
  void sqlVectorSubtract() {
    String query = "SELECT vectorSubtract([5.0, 7.0, 9.0], [1.0, 2.0, 3.0]) as diff";
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();

      var result = results.next();
      final float[] diff = result.getProperty("diff");

      assertThat(diff).containsExactly(4.0f, 5.0f, 6.0f);
    }
  }

  @Test
  void sqlVectorMultiply() {
    String query = "SELECT vectorMultiply([2.0, 3.0, 4.0], [1.0, 2.0, 3.0]) as prod";
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();

      var result = results.next();
      final float[] prod = result.getProperty("prod");

      assertThat(prod).containsExactly(2.0f, 6.0f, 12.0f);
    }
  }

  @Test
  void sqlVectorScale() {
    String query = "SELECT vectorScale([1.0, 2.0, 3.0], 2.5) as scaled";
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();

      var result = results.next();
      final float[] scaled = result.getProperty("scaled");

      assertThat(scaled).containsExactly(2.5f, 5.0f, 7.5f);
    }
  }

  @Test
  void sqlVectorArithmeticChained() {
    // (([1, 2] + [3, 4]) * 2) = [8, 12]
    String query = """
        SELECT vectorScale(vectorAdd([1.0, 2.0], [3.0, 4.0]), 2.0) as result
        """;
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();

      var result = results.next();
      final float[] res = result.getProperty("result");

      assertThat(res).containsExactly(8.0f, 12.0f);
    }
  }

  @Test
  void sqlVectorArithmeticComplex() {
    // Complex: normalize(vectorAdd([1,0], [0,1])) should be approximately [0.707, 0.707]
    String query = """
        SELECT vectorNormalize(vectorAdd([1.0, 0.0], [0.0, 1.0])) as normalized
        """;
    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();

      var result = results.next();
      final float[] norm = result.getProperty("normalized");

      assertThat(norm[0]).isCloseTo(0.707f, Offset.offset(0.01f));
      assertThat(norm[1]).isCloseTo(0.707f, Offset.offset(0.01f));
    }
  }

  // ========== Error Handling Tests ==========

  @Test
  void vectorAddDimensionMismatch() {
    final SQLFunctionVectorAdd function = new SQLFunctionVectorAdd();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f }, new float[] { 1.0f, 2.0f, 3.0f } },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("dimension");
  }

  @Test
  void vectorScaleInvalidScalar() {
    final SQLFunctionVectorScale function = new SQLFunctionVectorScale();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f }, "not a number" },
        context))
        .isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void vectorMultiplyDimensionMismatch() {
    final SQLFunctionVectorMultiply function = new SQLFunctionVectorMultiply();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 1.0f }, new float[] { 1.0f, 2.0f } },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("dimension");
  }

  @Test
  void vectorSumNull() {
    final SQLFunctionVectorSum function = new SQLFunctionVectorSum();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Null input should be skipped
    final Object result1 = function.execute(null, null, null, new Object[] { null }, context);
    assertThat(result1).isNull();

    final Object result2 = function.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f } }, context);
    assertThat(result2).isNull();

    final float[] finalResult = (float[]) function.getResult();
    assertThat(finalResult).containsExactly(1.0f, 2.0f);
  }

  @Test
  void vectorAvgDimensionMismatch() {
    final SQLFunctionVectorAvg function = new SQLFunctionVectorAvg();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    function.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f } }, context);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f, 3.0f } },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("dimension");
  }

  @Test
  void sqlPhase2Combined() {
    String query = """
        SELECT
          vectorAdd([1.0, 2.0], [3.0, 4.0]) as added,
          vectorSubtract([5.0, 6.0], [1.0, 1.0]) as subtracted,
          vectorScale([1.0, 2.0], 3.0) as scaled,
          vectorMultiply([2.0, 3.0], [2.0, 2.0]) as multiplied
        """;

    try (ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();

      var result = results.next();
      final float[] added = result.getProperty("added");
      final float[] subtracted = result.getProperty("subtracted");
      final float[] scaled = result.getProperty("scaled");
      final float[] multiplied = result.getProperty("multiplied");

      assertThat(added).containsExactly(4.0f, 6.0f);
      assertThat(subtracted).containsExactly(4.0f, 5.0f);
      assertThat(scaled).containsExactly(3.0f, 6.0f);
      assertThat(multiplied).containsExactly(4.0f, 6.0f);
    }
  }
}
