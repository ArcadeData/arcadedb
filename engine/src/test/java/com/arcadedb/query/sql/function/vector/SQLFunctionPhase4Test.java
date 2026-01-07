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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SQLFunctionPhase4Test extends TestHelper {

  // ========== Phase 4.1: Sparse Vector Tests ==========

  @Test
  void sparseVectorCreate() {
    final SQLFunctionSparseVectorCreate function = new SQLFunctionSparseVectorCreate();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Create sparse vector from indices [0, 2] and values [0.5, 0.3]
    final SparseVector result = (SparseVector) function.execute(null, null, null,
        new Object[] { new int[] { 0, 2 }, new float[] { 0.5f, 0.3f } },
        context);

    assertThat(result).isNotNull();
    assertThat(result.getDimensions()).isEqualTo(3); // max index = 2, so dimension = 3
    assertThat(result.getNonZeroCount()).isEqualTo(2);
    assertThat(result.get(0)).isEqualTo(0.5f);
    assertThat(result.get(1)).isEqualTo(0.0f);
    assertThat(result.get(2)).isEqualTo(0.3f);
  }

  @Test
  void sparseVectorCreateWithDimensions() {
    final SQLFunctionSparseVectorCreate function = new SQLFunctionSparseVectorCreate();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Create sparse vector with explicit dimensions
    final SparseVector result = (SparseVector) function.execute(null, null, null,
        new Object[] { new int[] { 1, 3 }, new float[] { 0.2f, 0.8f }, 5 },
        context);

    assertThat(result).isNotNull();
    assertThat(result.getDimensions()).isEqualTo(5);
    assertThat(result.getNonZeroCount()).isEqualTo(2);
    assertThat(result.get(1)).isEqualTo(0.2f);
    assertThat(result.get(3)).isEqualTo(0.8f);
  }

  @Test
  void sparseVectorDot() {
    final SQLFunctionSparseVectorDot function = new SQLFunctionSparseVectorDot();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Create two sparse vectors
    final SparseVector v1 = new SparseVector(new int[] { 0, 2 }, new float[] { 0.5f, 0.3f }, 3);
    final SparseVector v2 = new SparseVector(new int[] { 1, 2 }, new float[] { 0.2f, 0.8f }, 3);

    // Dot product: only index 2 matches: 0.3 * 0.8 = 0.24
    final float result = (float) function.execute(null, null, null,
        new Object[] { v1, v2 },
        context);

    assertThat(result).isCloseTo(0.24f, Offset.offset(0.001f));
  }

  @Test
  void sparseVectorDotIdentical() {
    final SQLFunctionSparseVectorDot function = new SQLFunctionSparseVectorDot();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Same vector dot product with itself
    final SparseVector v = new SparseVector(new int[] { 0, 2 }, new float[] { 0.5f, 0.3f }, 3);

    // Dot product: 0.5*0.5 + 0.3*0.3 = 0.25 + 0.09 = 0.34
    final float result = (float) function.execute(null, null, null,
        new Object[] { v, v },
        context);

    assertThat(result).isCloseTo(0.34f, Offset.offset(0.001f));
  }

  @Test
  void sparseVectorMagnitude() {
    // Create sparse vector [0.5, 0.0, 0.3] with indices [0, 2]
    final SparseVector v = new SparseVector(new int[] { 0, 2 }, new float[] { 0.5f, 0.3f }, 3);

    // Magnitude: sqrt(0.5^2 + 0.3^2) = sqrt(0.25 + 0.09) = sqrt(0.34) ≈ 0.583
    final float magnitude = v.magnitude();

    assertThat(magnitude).isCloseTo((float) Math.sqrt(0.34), Offset.offset(0.001f));
  }

  @Test
  void sparseVectorToDense() {
    final SQLFunctionSparseVectorToDense function = new SQLFunctionSparseVectorToDense();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Create sparse vector [0.5, 0.0, 0.3]
    final SparseVector sparse = new SparseVector(new int[] { 0, 2 }, new float[] { 0.5f, 0.3f }, 3);

    // Convert to dense
    final float[] dense = (float[]) function.execute(null, null, null,
        new Object[] { sparse },
        context);

    assertThat(dense).containsExactly(0.5f, 0.0f, 0.3f);
  }

  @Test
  void denseVectorToSparse() {
    final SQLFunctionDenseVectorToSparse function = new SQLFunctionDenseVectorToSparse();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Convert dense vector [0.5, 0.0, 0.3, 0.0] to sparse
    final SparseVector sparse = (SparseVector) function.execute(null, null, null,
        new Object[] { new float[] { 0.5f, 0.0f, 0.3f, 0.0f } },
        context);

    assertThat(sparse).isNotNull();
    assertThat(sparse.getDimensions()).isEqualTo(4);
    assertThat(sparse.getNonZeroCount()).isEqualTo(2);
    assertThat(sparse.get(0)).isEqualTo(0.5f);
    assertThat(sparse.get(2)).isEqualTo(0.3f);
  }

  @Test
  void denseVectorToSparseWithThreshold() {
    final SQLFunctionDenseVectorToSparse function = new SQLFunctionDenseVectorToSparse();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Convert with threshold 0.2: filters out 0.1
    final SparseVector sparse = (SparseVector) function.execute(null, null, null,
        new Object[] { new float[] { 0.5f, 0.1f, 0.3f }, 0.2f },
        context);

    assertThat(sparse).isNotNull();
    assertThat(sparse.getNonZeroCount()).isEqualTo(2); // Only 0.5 and 0.3 are > 0.2
    assertThat(sparse.get(0)).isEqualTo(0.5f);
    assertThat(sparse.get(2)).isEqualTo(0.3f);
  }

  @Test
  void sparseVectorNormalize() {
    // Create sparse vector [0.5, 0.0, 0.3]
    final SparseVector v = new SparseVector(new int[] { 0, 2 }, new float[] { 0.5f, 0.3f }, 3);

    // Normalize: magnitude = sqrt(0.34)
    final SparseVector normalized = v.normalize();

    assertThat(normalized).isNotNull();
    assertThat(normalized.getDimensions()).isEqualTo(3);
    assertThat(normalized.getNonZeroCount()).isEqualTo(2);

    // Check normalized values
    final float mag = (float) Math.sqrt(0.34);
    assertThat(normalized.get(0)).isCloseTo(0.5f / mag, Offset.offset(0.001f));
    assertThat(normalized.get(2)).isCloseTo(0.3f / mag, Offset.offset(0.001f));
  }

  // ========== Phase 4.2: Multi-Vector Tests ==========

  @Test
  void multiVectorScoreMax() {
    final SQLFunctionMultiVectorScore function = new SQLFunctionMultiVectorScore();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // ColBERT style: max of scores [0.9, 0.7, 0.8]
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 0.9f, 0.7f, 0.8f }, "MAX" },
        context);

    assertThat(result).isCloseTo(0.9f, Offset.offset(0.001f));
  }

  @Test
  void multiVectorScoreMin() {
    final SQLFunctionMultiVectorScore function = new SQLFunctionMultiVectorScore();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Min of scores [0.9, 0.7, 0.8]
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 0.9f, 0.7f, 0.8f }, "MIN" },
        context);

    assertThat(result).isCloseTo(0.7f, Offset.offset(0.001f));
  }

  @Test
  void multiVectorScoreAvg() {
    final SQLFunctionMultiVectorScore function = new SQLFunctionMultiVectorScore();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Average of scores [0.9, 0.7, 0.8] = 2.4 / 3 = 0.8
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 0.9f, 0.7f, 0.8f }, "AVG" },
        context);

    assertThat(result).isCloseTo(0.8f, Offset.offset(0.001f));
  }

  @Test
  void multiVectorScoreWeighted() {
    final SQLFunctionMultiVectorScore function = new SQLFunctionMultiVectorScore();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Weighted average: (0.9*0.5 + 0.7*0.3 + 0.8*0.2) / (0.5+0.3+0.2)
    // = (0.45 + 0.21 + 0.16) / 1.0 = 0.82
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 0.9f, 0.7f, 0.8f }, "WEIGHTED", new float[] { 0.5f, 0.3f, 0.2f } },
        context);

    assertThat(result).isCloseTo(0.82f, Offset.offset(0.001f));
  }

  @Test
  void multiVectorScoreSingleValue() {
    final SQLFunctionMultiVectorScore function = new SQLFunctionMultiVectorScore();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Single score should return that score
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 0.75f }, "AVG" },
        context);

    assertThat(result).isCloseTo(0.75f, Offset.offset(0.001f));
  }

  @Test
  void multiVectorScoreMaxCaseSensitive() {
    final SQLFunctionMultiVectorScore function = new SQLFunctionMultiVectorScore();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Test lowercase method
    final float result = (float) function.execute(null, null, null,
        new Object[] { new float[] { 0.9f, 0.7f, 0.8f }, "max" },
        context);

    assertThat(result).isCloseTo(0.9f, Offset.offset(0.001f));
  }

  // ========== Error Handling Tests ==========

  @Test
  void sparseVectorCreateMismatchedArrays() {
    final SQLFunctionSparseVectorCreate function = new SQLFunctionSparseVectorCreate();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new int[] { 0, 2 }, new float[] { 0.5f } },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("same length");
  }

  @Test
  void sparseVectorCreateNegativeIndex() {
    assertThatThrownBy(() -> new SparseVector(new int[] { -1, 2 }, new float[] { 0.5f, 0.3f }))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-negative");
  }

  @Test
  void sparseVectorDotDimensionMismatch() {
    final SQLFunctionSparseVectorDot function = new SQLFunctionSparseVectorDot();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final SparseVector v1 = new SparseVector(new int[] { 0 }, new float[] { 0.5f }, 3);
    final SparseVector v2 = new SparseVector(new int[] { 0 }, new float[] { 0.5f }, 5);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { v1, v2 },
        context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dimensions");
  }

  @Test
  void multiVectorScoreInvalidMethod() {
    final SQLFunctionMultiVectorScore function = new SQLFunctionMultiVectorScore();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 0.5f }, "INVALID" },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Unknown fusion method");
  }

  @Test
  void multiVectorScoreWeightedMismatch() {
    final SQLFunctionMultiVectorScore function = new SQLFunctionMultiVectorScore();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 0.5f, 0.7f }, "WEIGHTED", new float[] { 0.5f } },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("same length");
  }

  @Test
  void sparseVectorToDenseNull() {
    final SQLFunctionSparseVectorToDense function = new SQLFunctionSparseVectorToDense();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final Object result = function.execute(null, null, null, new Object[] { null }, context);
    assertThat(result).isNull();
  }

  @Test
  void denseVectorToSparseNull() {
    final SQLFunctionDenseVectorToSparse function = new SQLFunctionDenseVectorToSparse();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final Object result = function.execute(null, null, null, new Object[] { null }, context);
    assertThat(result).isNull();
  }

  @Test
  void multiVectorScoreNull() {
    final SQLFunctionMultiVectorScore function = new SQLFunctionMultiVectorScore();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final Object result = function.execute(null, null, null, new Object[] { null, "MAX" }, context);
    assertThat(result).isNull();
  }

  @Test
  void sparseVectorRoundTrip() {
    // Test converting dense -> sparse -> dense preserves values
    final SQLFunctionDenseVectorToSparse toSparse = new SQLFunctionDenseVectorToSparse();
    final SQLFunctionSparseVectorToDense toDense = new SQLFunctionSparseVectorToDense();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final float[] original = new float[] { 0.5f, 0.0f, 0.3f, 0.0f, 0.2f };

    // Dense -> Sparse
    final SparseVector sparse = (SparseVector) toSparse.execute(null, null, null,
        new Object[] { original },
        context);

    // Sparse -> Dense
    final float[] recovered = (float[]) toDense.execute(null, null, null,
        new Object[] { sparse },
        context);

    assertThat(recovered).containsExactly(original);
  }
}
