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
import com.arcadedb.query.sql.method.conversion.SQLMethodAsSparse;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsVector;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

  // ========== asString() vector support + vectorToString delegation ==========

  private String str(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("r");
    }
  }

  @Test
  void asStringFormatsCollectionLiteral() {
    assertThat(str("SELECT [1.0, 2.0, 3.0].asString() as r")).isEqualTo("[1.0, 2.0, 3.0]");
    assertThat(str("SELECT [1.0, 2.0, 3.0].asString('MATLAB') as r")).isEqualTo("[1.0 2.0 3.0]");
    assertThat(str("SELECT [1.0, 2.0, 3.0].asString('NUMPY') as r")).isEqualTo("1.0, 2.0, 3.0");
    assertThat(str("SELECT [1.0, 2.0, 3.0].asString('JULIA') as r")).isEqualTo("[1.0, 2.0, 3.0]");
  }

  @Test
  void asStringFormatsStoredFloatArray() {
    database.command("sql", "CREATE DOCUMENT TYPE Emb2");
    database.command("sql", "CREATE PROPERTY Emb2.v ARRAY_OF_FLOATS");
    database.transaction(() -> database.command("sql", "INSERT INTO Emb2 SET v = [0.5, 0.25, 0.75]"));

    assertThat(str("SELECT v.asString('PYTHON') as r FROM Emb2")).isEqualTo("[0.5, 0.25, 0.75]");
  }

  @Test
  void asStringLeavesNonVectorsUnchanged() {
    // A plain string is returned as-is, not parsed/reformatted as a vector.
    assertThat(str("SELECT 'hello'.asString() as r")).isEqualTo("hello");
  }

  @Test
  void asStringAndVectorToStringAgree() {
    assertThat(str("SELECT [0.5, 0.25].asString('PRETTY') as r"))
        .isEqualTo(str("SELECT `vector.toString`([0.5, 0.25], 'PRETTY') as r"));
  }

  @Test
  void vectorToStringSupportsJuliaAndNumpy() {
    assertThat(str("SELECT `vector.toString`([1.0, 2.0], 'JULIA') as r")).isEqualTo("[1.0, 2.0]");
    assertThat(str("SELECT `vector.toString`([1.0, 2.0], 'NUMPY') as r")).isEqualTo("1.0, 2.0");
  }

  // ========== asVector(): the inverse of asString() ==========

  private float[] vec(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("r");
    }
  }

  @Test
  void asVectorParsesEveryAsStringFormat() {
    // COMPACT / PYTHON / JULIA (bracketed, comma)
    assertThat(vec("SELECT '[1.0, 2.0, 3.0]'.asVector() as r")).containsExactly(1.0f, 2.0f, 3.0f);
    // MATLAB (bracketed, space-separated)
    assertThat(vec("SELECT '[1.0 2.0 3.0]'.asVector() as r")).containsExactly(1.0f, 2.0f, 3.0f);
    // NUMPY (bare, comma-separated)
    assertThat(vec("SELECT '1.0, 2.0, 3.0'.asVector() as r")).containsExactly(1.0f, 2.0f, 3.0f);
    // PRETTY (multi-line) - exercised via the method directly since a raw newline cannot be a SQL string literal
    final Object pretty = new SQLMethodAsVector().execute("[\n  1.0,\n  2.0,\n  3.0\n]", null, ctx(), new Object[0]);
    assertThat((float[]) pretty).containsExactly(1.0f, 2.0f, 3.0f);
  }

  @Test
  void asVectorRoundTripsWithAsStringForAllFormats() {
    for (final String fmt : new String[] { "COMPACT", "PRETTY", "PYTHON", "MATLAB", "JULIA", "NUMPY" }) {
      final float[] back = vec("SELECT [0.5, 0.25, 0.75].asString('" + fmt + "').asVector() as r");
      assertThat(back).as("round-trip via %s", fmt).containsExactly(0.5f, 0.25f, 0.75f);
    }
  }

  @Test
  void asVectorAcceptsListAndScalar() {
    assertThat(vec("SELECT [1.0, 2.0, 3.0].asVector() as r")).containsExactly(1.0f, 2.0f, 3.0f);
    assertThat(vec("SELECT (5.0).asVector() as r")).containsExactly(5.0f);
  }

  // ========== vectorRRFScore: trailing number is a rank, not k (issue #3099) ==========

  @Test
  void rrfScoreTreatsAllPositionalNumbersAsRanks() {
    final SQLFunctionVectorRRFScore fn = new SQLFunctionVectorRRFScore();
    // Three lists where the last rank is 75 (>= 60). Previously 75 was silently swallowed as k; now it is
    // a rank with the default k=60.
    final float result = (float) fn.execute(null, null, null, new Object[] { 1L, 5L, 75L }, ctx());
    assertThat(result).isCloseTo((1.0f / 61) + (1.0f / 65) + (1.0f / 135), Offset.offset(1e-5f));
  }

  @Test
  void rrfScoreKSetOnlyViaOptionsMap() {
    final SQLFunctionVectorRRFScore fn = new SQLFunctionVectorRRFScore();
    final float result = (float) fn.execute(null, null, null,
        new Object[] { 1L, 5L, 10L, Map.of("k", 100L) }, ctx());
    assertThat(result).isCloseTo((1.0f / 101) + (1.0f / 105) + (1.0f / 110), Offset.offset(1e-5f));
  }

  // ========== vectorDequantizeBinary ==========

  @Test
  void dequantizeBinaryRoundTripsSignsThroughQuantize() {
    final SQLFunctionVectorQuantizeBinary quantize = new SQLFunctionVectorQuantizeBinary();
    final SQLFunctionVectorDequantizeBinary dequantize = new SQLFunctionVectorDequantizeBinary();

    // median of [0.1, 0.5, 0.9] is 0.5: bits = [0, 1, 1] -> [-1, 1, 1] by default
    final Object q = quantize.execute(null, null, null, new Object[] { new float[] { 0.1f, 0.5f, 0.9f } }, ctx());
    assertThat((float[]) dequantize.execute(null, null, null, new Object[] { q }, ctx()))
        .containsExactly(-1.0f, 1.0f, 1.0f);

    // custom low/high reconstruction values
    final Object q2 = quantize.execute(null, null, null, new Object[] { new float[] { 0.1f, 0.5f, 0.9f } }, ctx());
    assertThat((float[]) dequantize.execute(null, null, null, new Object[] { q2, 0.0f, 1.0f }, ctx()))
        .containsExactly(0.0f, 1.0f, 1.0f);
  }

  @Test
  void dequantizeBinaryRejectsNonResult() {
    final SQLFunctionVectorDequantizeBinary fn = new SQLFunctionVectorDequantizeBinary();
    assertThatThrownBy(() -> fn.execute(null, null, null, new Object[] { new float[] { 1.0f } }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("vector.quantizeBinary");
  }

  // ========== vectorNormalizeScores uniform-input micro-opt ==========

  @Test
  void normalizeScoresUniformReturnsMidpoints() {
    final SQLFunctionVectorNormalizeScores fn = new SQLFunctionVectorNormalizeScores();
    assertThat((float[]) fn.execute(null, null, null, new Object[] { new float[] { 3.0f, 3.0f, 3.0f } }, ctx()))
        .containsExactly(0.5f, 0.5f, 0.5f);
  }

  // ========== int8 quantization ergonomics (#8/#9/#10) ==========

  @Test
  void dequantizeInt8AcceptsResultObjectDirectly() {
    final SQLFunctionVectorQuantizeInt8 q = new SQLFunctionVectorQuantizeInt8();
    final SQLFunctionVectorDequantizeInt8 dq = new SQLFunctionVectorDequantizeInt8();

    final Object qr = q.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f } }, ctx());
    // Pass the result object directly - no need to unpack quantized()/min()/max().
    final float[] back = (float[]) dq.execute(null, null, null, new Object[] { qr }, ctx());

    assertThat(back).hasSize(3);
    assertThat(back[0]).isCloseTo(1.0f, Offset.offset(0.05f));
    assertThat(back[1]).isCloseTo(2.0f, Offset.offset(0.05f));
    assertThat(back[2]).isCloseTo(3.0f, Offset.offset(0.05f));
  }

  @Test
  void dequantizeInt8AcceptsResultObjectInThreeArgFormToo() {
    // The exact form from the issue: result object passed with redundant min/max must not error.
    final SQLFunctionVectorQuantizeInt8 q = new SQLFunctionVectorQuantizeInt8();
    final SQLFunctionVectorDequantizeInt8 dq = new SQLFunctionVectorDequantizeInt8();

    final Object qr = q.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f } }, ctx());
    final float[] back = (float[]) dq.execute(null, null, null, new Object[] { qr, 1.0f, 3.0f }, ctx());
    assertThat(back).hasSize(3);
    assertThat(back[0]).isCloseTo(1.0f, Offset.offset(0.05f));
    assertThat(back[2]).isCloseTo(3.0f, Offset.offset(0.05f));
  }

  @Test
  void approxDistanceInfersTypeFromResultObjects() {
    final SQLFunctionVectorQuantizeInt8 qi = new SQLFunctionVectorQuantizeInt8();
    final SQLFunctionVectorQuantizeBinary qb = new SQLFunctionVectorQuantizeBinary();
    final SQLFunctionVectorApproxDistance ad = new SQLFunctionVectorApproxDistance();

    // INT8 inferred from QuantizationResult objects; identical vectors -> distance 0
    final Object i1 = qi.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f } }, ctx());
    final Object i2 = qi.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f } }, ctx());
    assertThat((float) ad.execute(null, null, null, new Object[] { i1, i2 }, ctx())).isEqualTo(0.0f);

    // Differently-shaped vectors -> non-zero distance (int8 is normalized per-vector, so the distance
    // reflects the relative shape; [1,2,3] and [1,1,3] differ at index 1).
    final Object i3 = qi.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f } }, ctx());
    final Object i4 = qi.execute(null, null, null, new Object[] { new float[] { 1.0f, 1.0f, 3.0f } }, ctx());
    assertThat((float) ad.execute(null, null, null, new Object[] { i3, i4 }, ctx())).isGreaterThan(0.0f);

    // BINARY inferred from BinaryQuantizationResult objects; identical -> 0
    final Object b1 = qb.execute(null, null, null, new Object[] { new float[] { 0.1f, 0.5f, 0.9f } }, ctx());
    final Object b2 = qb.execute(null, null, null, new Object[] { new float[] { 0.1f, 0.5f, 0.9f } }, ctx());
    assertThat((float) ad.execute(null, null, null, new Object[] { b1, b2 }, ctx())).isEqualTo(0.0f);

    // Mirrored vectors -> two bits differ -> normalized Hamming distance 2/3
    final Object b3 = qb.execute(null, null, null, new Object[] { new float[] { 0.1f, 0.5f, 0.9f } }, ctx());
    final Object b4 = qb.execute(null, null, null, new Object[] { new float[] { 0.9f, 0.5f, 0.1f } }, ctx());
    assertThat((float) ad.execute(null, null, null, new Object[] { b3, b4 }, ctx())).isCloseTo(2.0f / 3, Offset.offset(1e-5f));
  }

  @Test
  void approxDistanceExplicitTypeWithResultObjects() {
    final SQLFunctionVectorQuantizeInt8 qi = new SQLFunctionVectorQuantizeInt8();
    final SQLFunctionVectorQuantizeBinary qb = new SQLFunctionVectorQuantizeBinary();
    final SQLFunctionVectorApproxDistance ad = new SQLFunctionVectorApproxDistance();

    // 3-arg explicit form must also accept the result objects
    final Object i1 = qi.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f } }, ctx());
    final Object i2 = qi.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f } }, ctx());
    assertThat((float) ad.execute(null, null, null, new Object[] { i1, i2, "INT8" }, ctx())).isEqualTo(0.0f);

    final Object b1 = qb.execute(null, null, null, new Object[] { new float[] { 0.1f, 0.5f, 0.9f } }, ctx());
    final Object b2 = qb.execute(null, null, null, new Object[] { new float[] { 0.1f, 0.5f, 0.9f } }, ctx());
    assertThat((float) ad.execute(null, null, null, new Object[] { b1, b2, "BINARY" }, ctx())).isEqualTo(0.0f);
  }

  @Test
  void approxDistanceTwoArgFormRequiresResultObjects() {
    final SQLFunctionVectorQuantizeInt8 qi = new SQLFunctionVectorQuantizeInt8();
    final SQLFunctionVectorApproxDistance ad = new SQLFunctionVectorApproxDistance();

    // raw arrays cannot be inferred
    assertThatThrownBy(() -> ad.execute(null, null, null, new Object[] { new byte[] { 1, 2 }, new byte[] { 1, 2 } }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Cannot infer");

    // mixing a result object with a raw array is also rejected in the 2-arg form
    final Object i1 = qi.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f } }, ctx());
    assertThatThrownBy(() -> ad.execute(null, null, null, new Object[] { i1, new byte[] { 1, 2, 3 } }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Cannot infer");
  }

  @Test
  void approxDistanceRejectsMixedQuantizationTypes() {
    final SQLFunctionVectorQuantizeInt8 qi = new SQLFunctionVectorQuantizeInt8();
    final SQLFunctionVectorQuantizeBinary qb = new SQLFunctionVectorQuantizeBinary();
    final SQLFunctionVectorApproxDistance ad = new SQLFunctionVectorApproxDistance();

    final Object int8 = qi.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f } }, ctx());
    final Object binary = qb.execute(null, null, null, new Object[] { new float[] { 0.1f, 0.5f, 0.9f } }, ctx());

    assertThatThrownBy(() -> ad.execute(null, null, null, new Object[] { binary, int8 }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Cannot mix");
  }

  // ========== asSparse() method (#4) ==========

  @Test
  void asSparseConvertsDenseDroppingZeros() {
    final SQLMethodAsSparse m = new SQLMethodAsSparse();
    final SparseVector sv = (SparseVector) m.execute(new float[] { 0.5f, 0.0f, 0.3f }, null, ctx(), new Object[0]);
    assertThat(sv.getDimensions()).isEqualTo(3);
    assertThat(sv.getNonZeroCount()).isEqualTo(2);
    assertThat(sv.get(0)).isEqualTo(0.5f);
    assertThat(sv.get(1)).isEqualTo(0.0f);
    assertThat(sv.get(2)).isEqualTo(0.3f);
  }

  @Test
  void asSparseHonoursThresholdAndWorksInSql() {
    final SQLMethodAsSparse m = new SQLMethodAsSparse();
    final SparseVector sv = (SparseVector) m.execute(new float[] { 0.5f, 0.05f, 0.3f }, null, ctx(), new Object[] { 0.1f });
    assertThat(sv.getNonZeroCount()).isEqualTo(2); // 0.05 dropped by the 0.1 threshold

    try (final ResultSet rs = database.query("sql", "SELECT [0.5, 0.0, 0.3].asSparse() as r")) {
      assertThat(rs.hasNext()).isTrue();
      final SparseVector fromSql = (SparseVector) rs.next().getProperty("r");
      assertThat(fromSql.getDimensions()).isEqualTo(3);
      assertThat(fromSql.getNonZeroCount()).isEqualTo(2);
      assertThat(fromSql.get(0)).isEqualTo(0.5f);
      assertThat(fromSql.get(1)).isEqualTo(0.0f);
      assertThat(fromSql.get(2)).isEqualTo(0.3f);
    }
  }

  // ========== RRF array-of-ranks input (#14) ==========

  @Test
  void rrfScoreAcceptsArrayOfRanks() {
    final SQLFunctionVectorRRFScore fn = new SQLFunctionVectorRRFScore();
    assertThat((float) fn.execute(null, null, null, new Object[] { new int[] { 1, 5, 10 } }, ctx()))
        .isCloseTo((1.0f / 61) + (1.0f / 65) + (1.0f / 70), Offset.offset(1e-5f));
    assertThat((float) fn.execute(null, null, null,
        new Object[] { List.of(1L, 5L, 10L), Map.of("k", 100L) }, ctx()))
        .isCloseTo((1.0f / 101) + (1.0f / 105) + (1.0f / 110), Offset.offset(1e-5f));
  }

  @Test
  void rrfScoreSkipsNullRanksConsistentlyInBothForms() {
    final SQLFunctionVectorRRFScore fn = new SQLFunctionVectorRRFScore();
    final float expected = (1.0f / 61) + (1.0f / 65);
    // A null rank means "this item is absent from that ranking list" and is skipped (contributes 0), so only
    // ranks 1 and 5 score. Here the null is in position 0 - the item is absent from the FIRST ranking list,
    // not ranked last - and the result must equal scoring just [1, 5].
    final List<Object> variadic = new ArrayList<>(Arrays.asList(null, 1L, 5L));
    assertThat((float) fn.execute(null, null, null, variadic.toArray(), ctx())).isCloseTo(expected, Offset.offset(1e-5f));
    // array form with the same null element must behave identically.
    assertThat((float) fn.execute(null, null, null, new Object[] { variadic }, ctx())).isCloseTo(expected, Offset.offset(1e-5f));
  }

  @Test
  void rrfScoreRejectsNonIntegerRanksInBothForms() {
    final SQLFunctionVectorRRFScore fn = new SQLFunctionVectorRRFScore();
    // array form (a list/Object[] of ranks)
    assertThatThrownBy(() -> fn.execute(null, null, null, new Object[] { List.of(1.5, 5.0) }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("integers");
    // variadic form - no longer silently truncates 1.5 -> 1
    assertThatThrownBy(() -> fn.execute(null, null, null, new Object[] { 1.5, 5.0 }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("integers");
  }

  @Test
  void rrfScoreRejectsFloatArrayAsNonNumberRank() {
    // A float[]/double[] is a score/embedding vector, not a list of integer ranks: it is not treated as an
    // array of ranks, so it is rejected as a non-number rank (clearer than "must be integers" per element).
    final SQLFunctionVectorRRFScore fn = new SQLFunctionVectorRRFScore();
    assertThatThrownBy(() -> fn.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f } }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("numbers");
    assertThatThrownBy(() -> fn.execute(null, null, null, new Object[] { new double[] { 1.0, 2.0 } }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("numbers");
  }

  // ========== MATLAB_COLUMN format (#21) ==========

  @Test
  void matlabColumnFormatAndRoundTrip() {
    assertThat(str("SELECT `vector.toString`([1.0, 2.0, 3.0], 'MATLAB_COLUMN') as r")).isEqualTo("[1.0; 2.0; 3.0]");
    // semicolons parse back like the other separators
    assertThat(vec("SELECT '[1.0; 2.0; 3.0]'.asVector() as r")).containsExactly(1.0f, 2.0f, 3.0f);
    // and without the space after the semicolon (the [,;\s]+ separator regex must collapse it)
    assertThat(vec("SELECT '[1.0;2.0;3.0]'.asVector() as r")).containsExactly(1.0f, 2.0f, 3.0f);
  }

  // ========== review follow-ups: edge cases ==========

  @Test
  void dequantizeInt8ThreeArgRejectsConflictingMinMaxWhenGivenResult() {
    // A QuantizationResult carries its own authoritative min/max. The redundant 3-arg form is supported
    // (issue #3099) only when the explicit scalars match the embedded values; scalars that conflict would
    // dequantize to a wrong scale, so they are rejected loudly rather than silently ignored.
    final SQLFunctionVectorQuantizeInt8 q = new SQLFunctionVectorQuantizeInt8();
    final SQLFunctionVectorDequantizeInt8 dq = new SQLFunctionVectorDequantizeInt8();

    final Object qr = q.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f } }, ctx());
    assertThatThrownBy(() -> dq.execute(null, null, null, new Object[] { qr, -99.0f, 99.0f }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("conflicts with the result's embedded");
  }

  @Test
  void rrfScoreRejectsNonFiniteRanks() {
    final SQLFunctionVectorRRFScore fn = new SQLFunctionVectorRRFScore();
    assertThatThrownBy(() -> fn.execute(null, null, null, new Object[] { new Object[] { Double.NaN } }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("finite");
    assertThatThrownBy(() -> fn.execute(null, null, null, new Object[] { Double.POSITIVE_INFINITY }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("finite");
  }

  @Test
  void approxDistanceExplicitTypeRejectsMixedTypesWithHelpfulMessage() {
    final SQLFunctionVectorQuantizeInt8 qi = new SQLFunctionVectorQuantizeInt8();
    final SQLFunctionVectorQuantizeBinary qb = new SQLFunctionVectorQuantizeBinary();
    final SQLFunctionVectorApproxDistance ad = new SQLFunctionVectorApproxDistance();

    final Object int8 = qi.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f } }, ctx());
    final Object binary = qb.execute(null, null, null, new Object[] { new float[] { 0.1f, 0.5f, 0.9f } }, ctx());

    // Explicit INT8 with a binary result for the second arg: the error must name the expected INT8 inputs.
    assertThatThrownBy(() -> ad.execute(null, null, null, new Object[] { int8, binary, "INT8" }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("INT8");
  }

  @Test
  void dequantizeInt8ThreeArgWithResultAndNullScalarsStillUsesEmbeddedMinMax() {
    // (result, null, null): the result object's embedded min/max must win - the null scalars must NOT
    // short-circuit the whole call to null (the QuantizationResult check runs before the scalar null-guard).
    final SQLFunctionVectorQuantizeInt8 q = new SQLFunctionVectorQuantizeInt8();
    final SQLFunctionVectorDequantizeInt8 dq = new SQLFunctionVectorDequantizeInt8();

    final Object qr = q.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f } }, ctx());
    final float[] expected = (float[]) dq.execute(null, null, null, new Object[] { qr }, ctx());
    final float[] withNullScalars = (float[]) dq.execute(null, null, null, new Object[] { qr, null, null }, ctx());
    assertThat(withNullScalars).isNotNull().containsExactly(expected);
  }

  @Test
  void approxDistanceExplicitBinaryWithInt8ResultsGivesParsingError() {
    // The BINARY path is reached for (int8, int8, 'BINARY') and (int8, binary, 'BINARY'). It must surface a
    // CommandSQLParsingException naming BinaryQuantizationResult, never a raw ClassCastException. The asserted
    // message text is owned by SQLFunctionVectorApproxDistance.computeBinaryDistance() - keep them in sync.
    final SQLFunctionVectorQuantizeInt8 qi = new SQLFunctionVectorQuantizeInt8();
    final SQLFunctionVectorQuantizeBinary qb = new SQLFunctionVectorQuantizeBinary();
    final SQLFunctionVectorApproxDistance ad = new SQLFunctionVectorApproxDistance();

    final Object int8a = qi.execute(null, null, null, new Object[] { new float[] { 1.0f, 2.0f, 3.0f } }, ctx());
    final Object int8b = qi.execute(null, null, null, new Object[] { new float[] { 1.0f, 1.0f, 3.0f } }, ctx());
    final Object binary = qb.execute(null, null, null, new Object[] { new float[] { 0.1f, 0.5f, 0.9f } }, ctx());

    assertThatThrownBy(() -> ad.execute(null, null, null, new Object[] { int8a, int8b, "BINARY" }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Expected BinaryQuantizationResult");
    assertThatThrownBy(() -> ad.execute(null, null, null, new Object[] { int8a, binary, "BINARY" }, ctx()))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Expected BinaryQuantizationResult");
  }
}
