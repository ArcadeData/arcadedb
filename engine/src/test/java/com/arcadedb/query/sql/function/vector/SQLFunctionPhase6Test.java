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
package com.arcadedb.query.sql.function.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 6 - Vector Analysis & Validation Functions Tests
 * Tests for: L1Norm, LInfNorm, Variance, StdDev, Sparsity,
 *            IsNormalized, HasNaN, HasInf, Clip, ToString
 */
class SQLFunctionPhase6Test extends TestHelper {

  // ===== Phase 6.1: Vector Analysis Functions =====

  @Test
  void vectorL1NormBasic() {
    final SQLFunctionVectorL1Norm function = new SQLFunctionVectorL1Norm();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // L1 norm = sum of absolute values
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 3, 4 } },
        context);

    assertThat(result).isCloseTo(7.0f, Assertions.offset(0.001f));
  }

  @Test
  void vectorL1NormNegativeValues() {
    final SQLFunctionVectorL1Norm function = new SQLFunctionVectorL1Norm();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { -1, -2, 3 } },
        context);

    assertThat(result).isCloseTo(6.0f, Assertions.offset(0.001f));
  }

  @Test
  void vectorLInfNormBasic() {
    final SQLFunctionVectorLInfNorm function = new SQLFunctionVectorLInfNorm();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // L∞ norm = max absolute value
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 3, 4, 2 } },
        context);

    assertThat(result).isCloseTo(4.0f, Assertions.offset(0.001f));
  }

  @Test
  void vectorLInfNormNegativeValues() {
    final SQLFunctionVectorLInfNorm function = new SQLFunctionVectorLInfNorm();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { -1, -5, 3 } },
        context);

    assertThat(result).isCloseTo(5.0f, Assertions.offset(0.001f));
  }

  @Test
  void vectorVarianceBasic() {
    final SQLFunctionVectorVariance function = new SQLFunctionVectorVariance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Variance of [1, 2, 3, 4, 5] = 2.0
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 1, 2, 3, 4, 5 } },
        context);

    assertThat(result).isCloseTo(2.0f, Assertions.offset(0.001f));
  }

  @Test
  void vectorVarianceUniformVector() {
    final SQLFunctionVectorVariance function = new SQLFunctionVectorVariance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Variance of uniform vector = 0
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 5, 5, 5, 5, 5 } },
        context);

    assertThat(result).isCloseTo(0.0f, Assertions.offset(0.001f));
  }

  @Test
  void vectorStdDevBasic() {
    final SQLFunctionVectorStdDev function = new SQLFunctionVectorStdDev();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // StdDev of [1, 2, 3, 4, 5] = sqrt(2) ≈ 1.414
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 1, 2, 3, 4, 5 } },
        context);

    assertThat(result).isCloseTo(1.414f, Assertions.offset(0.01f));
  }

  @Test
  void vectorStdDevUniformVector() {
    final SQLFunctionVectorStdDev function = new SQLFunctionVectorStdDev();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // StdDev of uniform vector = 0
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 5, 5, 5, 5, 5 } },
        context);

    assertThat(result).isCloseTo(0.0f, Assertions.offset(0.001f));
  }

  @Test
  void vectorSparsityBasic() {
    final SQLFunctionVectorSparsity function = new SQLFunctionVectorSparsity();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Sparsity with threshold 0.06: [0.01, 0.1, 0.05, 0.02]
    // Below threshold: 0.01, 0.05, 0.02 = 3 out of 4
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 0.01f, 0.1f, 0.05f, 0.02f }, 0.06f },
        context);

    assertThat(result).isCloseTo(0.75f, Assertions.offset(0.001f));
  }

  @Test
  void vectorSparsityAllSparse() {
    final SQLFunctionVectorSparsity function = new SQLFunctionVectorSparsity();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // All values below threshold
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 0.01f, 0.02f, 0.03f }, 0.1f },
        context);

    assertThat(result).isCloseTo(1.0f, Assertions.offset(0.001f));
  }

  @Test
  void vectorSparsityNoneSparse() {
    final SQLFunctionVectorSparsity function = new SQLFunctionVectorSparsity();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // No values below threshold
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 0.5f, 0.6f, 0.7f }, 0.1f },
        context);

    assertThat(result).isCloseTo(0.0f, Assertions.offset(0.001f));
  }

  // ===== Phase 6.2: Vector Validation Functions =====

  @Test
  void vectorIsNormalizedTrue() {
    final SQLFunctionVectorIsNormalized function = new SQLFunctionVectorIsNormalized();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // [0.6, 0.8] has L2 norm = 1.0
    final boolean result = (boolean) function.execute(null, null, null,
        new Object[] { new float[] { 0.6f, 0.8f } },
        context);

    assertThat(result).isTrue();
  }

  @Test
  void vectorIsNormalizedFalse() {
    final SQLFunctionVectorIsNormalized function = new SQLFunctionVectorIsNormalized();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // [1, 1] has L2 norm ≈ 1.414
    final boolean result = (boolean) function.execute(null, null, null,
        new Object[] { new float[] { 1, 1 } },
        context);

    assertThat(result).isFalse();
  }

  @Test
  void vectorIsNormalizedWithTolerance() {
    final SQLFunctionVectorIsNormalized function = new SQLFunctionVectorIsNormalized();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // [0.6, 0.8] with large tolerance should be true
    final boolean result = (boolean) function.execute(null, null, null,
        new Object[] { new float[] { 0.6f, 0.8f }, 0.5f },
        context);

    assertThat(result).isTrue();
  }

  @Test
  void vectorHasNaNFalse() {
    final SQLFunctionVectorHasNaN function = new SQLFunctionVectorHasNaN();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final boolean result = (boolean) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f, 3.0f } },
        context);

    assertThat(result).isFalse();
  }

  @Test
  void vectorHasNaNTrue() {
    final SQLFunctionVectorHasNaN function = new SQLFunctionVectorHasNaN();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final boolean result = (boolean) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, Float.NaN, 3.0f } },
        context);

    assertThat(result).isTrue();
  }

  @Test
  void vectorHasInfFalse() {
    final SQLFunctionVectorHasInf function = new SQLFunctionVectorHasInf();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final boolean result = (boolean) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f, 3.0f } },
        context);

    assertThat(result).isFalse();
  }

  @Test
  void vectorHasInfTrue() {
    final SQLFunctionVectorHasInf function = new SQLFunctionVectorHasInf();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final boolean result = (boolean) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, Float.POSITIVE_INFINITY, 3.0f } },
        context);

    assertThat(result).isTrue();
  }

  @Test
  void vectorClipBasic() {
    final SQLFunctionVectorClip function = new SQLFunctionVectorClip();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Clip [1, 5, 10] to [2, 8]
    final float[] result = (float[]) function.execute(null, null, null,
        new Object[] { new float[] { 1, 5, 10 }, 2, 8 },
        context);

    assertThat(result).containsExactly(2.0f, 5.0f, 8.0f);
  }

  @Test
  void vectorClipAllBelow() {
    final SQLFunctionVectorClip function = new SQLFunctionVectorClip();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // All values below min
    final float[] result = (float[]) function.execute(null, null, null,
        new Object[] { new float[] { 1, 2, 3 }, 5, 10 },
        context);

    assertThat(result).containsExactly(5.0f, 5.0f, 5.0f);
  }

  @Test
  void vectorClipAllAbove() {
    final SQLFunctionVectorClip function = new SQLFunctionVectorClip();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // All values above max
    final float[] result = (float[]) function.execute(null, null, null,
        new Object[] { new float[] { 8, 9, 10 }, 2, 5 },
        context);

    assertThat(result).containsExactly(5.0f, 5.0f, 5.0f);
  }

  @Test
  void vectorToStringCompact() {
    final SQLFunctionVectorToString function = new SQLFunctionVectorToString();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final String result = (String) function.execute(null, null, null,
        new Object[] { new float[] { 0.5f, 0.25f, 0.75f } },
        context);

    assertThat(result).contains("[").contains("]").contains(",");
    assertThat(result).contains("0.5");
    assertThat(result).contains("0.25");
    assertThat(result).contains("0.75");
  }

  @Test
  void vectorToStringPretty() {
    final SQLFunctionVectorToString function = new SQLFunctionVectorToString();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final String result = (String) function.execute(null, null, null,
        new Object[] { new float[] { 0.5f, 0.25f }, "PRETTY" },
        context);

    assertThat(result).contains("[").contains("]").contains("\n");
  }

  @Test
  void vectorToStringPython() {
    final SQLFunctionVectorToString function = new SQLFunctionVectorToString();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final String result = (String) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f }, "PYTHON" },
        context);

    assertThat(result).contains("[").contains("]");
  }

  @Test
  void vectorToStringMatlab() {
    final SQLFunctionVectorToString function = new SQLFunctionVectorToString();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final String result = (String) function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f }, "MATLAB" },
        context);

    assertThat(result).contains("[").contains("]");
  }

  @Test
  void vectorToStringInvalidFormat() {
    final SQLFunctionVectorToString function = new SQLFunctionVectorToString();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 1.0f }, "INVALID" },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Unknown format");
  }

  @Test
  void vectorL1NormNull() {
    final SQLFunctionVectorL1Norm function = new SQLFunctionVectorL1Norm();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final Object result = function.execute(null, null, null,
        new Object[] { null },
        context);

    assertThat(result).isNull();
  }

  @Test
  void vectorSparsityInvalidThreshold() {
    final SQLFunctionVectorSparsity function = new SQLFunctionVectorSparsity();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 0.1f, 0.2f }, -1.0f },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Threshold must be >= 0");
  }

  @Test
  void vectorClipInvalidRange() {
    final SQLFunctionVectorClip function = new SQLFunctionVectorClip();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 1, 2, 3 }, 10, 5 },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("must be <=");
  }
}
