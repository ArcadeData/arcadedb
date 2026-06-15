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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the issue #3099 follow-up enhancements: quick wins (LInfNorm/StdDev), new aliases
 * (vectorL2Norm, vectorClamp), the renamed function classes, and new small features (vector.hasNull,
 * scalar broadcasting on add/subtract, LN/TANH score transforms, vector.sparsity modes/default).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
class SQLFunctionVectorEnhancementsTest extends TestHelper {

  private BasicCommandContext ctx() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  private float scalar(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("r")).floatValue();
    }
  }

  // ========== Quick wins ==========

  @Test
  void lInfNormReturnsMaxAbsValue() {
    final SQLFunctionVectorLInfNorm fn = new SQLFunctionVectorLInfNorm();
    assertThat((float) fn.execute(null, null, null, new Object[] { new float[] { -1.0f, -5.0f, 3.0f } }, ctx()))
        .isEqualTo(5.0f);
  }

  @Test
  void stdDevIsSqrtOfVariance() {
    final float[] v = { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f };
    final float variance = (float) new SQLFunctionVectorVariance().execute(null, null, null, new Object[] { v }, ctx());
    final float stdDev = (float) new SQLFunctionVectorStdDev().execute(null, null, null, new Object[] { v }, ctx());
    assertThat(stdDev).isCloseTo((float) Math.sqrt(variance), Offset.offset(1e-6f));
    assertThat(variance).isCloseTo(2.0f, Offset.offset(1e-5f));
  }

  // ========== New aliases ==========

  @Test
  void l2NormAliasEqualsMagnitude() {
    // magnitude([3,4]) = 5; reachable as vector.l2Norm and vectorL2Norm
    assertThat(scalar("SELECT `vector.l2Norm`([3.0, 4.0]) as r")).isCloseTo(5.0f, Offset.offset(0.001f));
    assertThat(scalar("SELECT vectorL2Norm([3.0, 4.0]) as r")).isCloseTo(5.0f, Offset.offset(0.001f));
    assertThat(scalar("SELECT vectorMagnitude([3.0, 4.0]) as r")).isCloseTo(5.0f, Offset.offset(0.001f));
  }

  @Test
  void clampAliasEqualsClip() {
    try (final ResultSet rs = database.query("sql", "SELECT vectorClamp([1.0, 5.0, 10.0], 2, 8) as r")) {
      assertThat(rs.next().<float[]>getProperty("r")).containsExactly(2.0f, 5.0f, 8.0f);
    }
    try (final ResultSet rs = database.query("sql", "SELECT `vector.clamp`([1.0, 5.0, 10.0], 2, 8) as r")) {
      assertThat(rs.next().<float[]>getProperty("r")).containsExactly(2.0f, 5.0f, 8.0f);
    }
  }

  // ========== Renamed classes still resolve by SQL name ==========

  @Test
  void renamedFunctionsResolveBySqlName() {
    try (final ResultSet rs = database.query("sql", "SELECT `vector.sparseCreate`([0, 2], [1.0, 3.0], 4) as r")) {
      assertThat(rs.hasNext()).isTrue();
    }
    // vector.multiScore over two score lists with MAX fusion
    try (final ResultSet rs = database.query("sql", "SELECT `vector.multiScore`([0.2, 0.8], 'MAX') as r")) {
      assertThat(rs.next().<Float>getProperty("r")).isCloseTo(0.8f, Offset.offset(0.001f));
    }
  }

  // ========== vector.hasNull ==========

  @Test
  void hasNullDetectsNullElement() {
    try (final ResultSet rs = database.query("sql", "SELECT `vector.hasNull`([1.0, sqrt(-1.0), 3.0]) as r")) {
      assertThat(rs.next().<Boolean>getProperty("r")).isTrue();
    }
  }

  @Test
  void hasNullFalseForCleanVector() {
    try (final ResultSet rs = database.query("sql", "SELECT vectorHasNull([1.0, 2.0, 3.0]) as r")) {
      assertThat(rs.next().<Boolean>getProperty("r")).isFalse();
    }
  }

  @Test
  void hasNullFalseForPrimitiveArray() {
    final SQLFunctionVectorHasNull fn = new SQLFunctionVectorHasNull();
    assertThat((Boolean) fn.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f } }, ctx())).isFalse();
  }

  // ========== Scalar broadcasting ==========

  @Test
  void addBroadcastsScalar() {
    final SQLFunctionVectorAdd fn = new SQLFunctionVectorAdd();
    assertThat((float[]) fn.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f }, 4.0f }, ctx()))
        .containsExactly(5.0f, 6.0f, 7.0f);
    // commutative: scalar + vector
    assertThat((float[]) fn.execute(null, null, null, new Object[] { 4.0f, new float[] { 1.0f, 2.0f, 3.0f } }, ctx()))
        .containsExactly(5.0f, 6.0f, 7.0f);
  }

  @Test
  void subtractBroadcastsScalarPreservingOrder() {
    final SQLFunctionVectorSubtract fn = new SQLFunctionVectorSubtract();
    // vector - scalar
    assertThat((float[]) fn.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f }, 1.0f }, ctx()))
        .containsExactly(0.0f, 1.0f, 2.0f);
    // scalar - vector
    assertThat((float[]) fn.execute(null, null, null, new Object[] { 10.0f, new float[] { 1.0f, 2.0f, 3.0f } }, ctx()))
        .containsExactly(9.0f, 8.0f, 7.0f);
  }

  @Test
  void addRejectsTwoScalars() {
    final SQLFunctionVectorAdd fn = new SQLFunctionVectorAdd();
    assertThatThrownBy(() -> fn.execute(null, null, null, new Object[] { 1.0f, 2.0f }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("must be a vector");
  }

  @Test
  void addStillDoesElementWise() {
    final SQLFunctionVectorAdd fn = new SQLFunctionVectorAdd();
    assertThat((float[]) fn.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f, 3.0f }, new float[] { 4.0f, 5.0f, 6.0f } }, ctx()))
        .containsExactly(5.0f, 7.0f, 9.0f);
  }

  // ========== Score transform LN / TANH ==========

  @Test
  void scoreTransformLnEqualsLog() {
    final float ln = scalar("SELECT `vector.scoreTransform`(2.5, 'LN') as r");
    final float log = scalar("SELECT `vector.scoreTransform`(2.5, 'LOG') as r");
    assertThat(ln).isCloseTo((float) Math.log(2.5), Offset.offset(1e-6f));
    assertThat(ln).isEqualTo(log);
  }

  @Test
  void scoreTransformTanh() {
    assertThat(scalar("SELECT `vector.scoreTransform`(0.5, 'TANH') as r"))
        .isCloseTo((float) Math.tanh(0.5), Offset.offset(1e-6f));
  }

  // ========== vector.sparsity default threshold + modes ==========

  @Test
  void sparsityDefaultThreshold() {
    // one zero out of four with the default (tiny) threshold -> 0.25
    final SQLFunctionVectorSparsity fn = new SQLFunctionVectorSparsity();
    assertThat((float) fn.execute(null, null, null, new Object[] { new float[] { 0.0f, 1.0f, 2.0f, 3.0f } }, ctx()))
        .isCloseTo(0.25f, Offset.offset(1e-6f));
  }

  @Test
  void sparsityL0Mode() {
    // L0 = count of significant (>= threshold) elements = 3
    final SQLFunctionVectorSparsity fn = new SQLFunctionVectorSparsity();
    assertThat((int) fn.execute(null, null, null,
        new Object[] { new float[] { 0.0f, 1.0f, 2.0f, 3.0f }, 0.5f, "L0" }, ctx()))
        .isEqualTo(3);
  }

  @Test
  void sparsityGeometricMeanMode() {
    // gmean([1,2,4]) = (1*2*4)^(1/3) = 2.0
    final SQLFunctionVectorSparsity fn = new SQLFunctionVectorSparsity();
    assertThat((float) fn.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f, 4.0f }, 0.0f, "GMEAN" }, ctx()))
        .isCloseTo(2.0f, Offset.offset(1e-5f));
    // any zero element -> 0
    assertThat((float) fn.execute(null, null, null,
        new Object[] { new float[] { 0.0f, 2.0f, 4.0f }, 0.0f, "GMEAN" }, ctx()))
        .isEqualTo(0.0f);
  }
}
