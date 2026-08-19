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

import com.arcadedb.query.sql.executor.BasicCommandContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression tests for issue #6386: {@code vector.approxDistance} computed wrong distances for
 * both quantization modes, silently corrupting top-k ordering. The binary (Hamming) path
 * sign-extended {@code byte} operands into {@link Integer#bitCount}; the int8 path masked
 * signed, order-preserving codes with {@code & 0xFF}, breaking the ordering across the
 * mid-range.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6386VectorApproxDistanceTest {

  private final SQLFunctionVectorQuantizeBinary quantizeBinary = new SQLFunctionVectorQuantizeBinary();
  private final SQLFunctionVectorApproxDistance approxDistance = new SQLFunctionVectorApproxDistance();
  private final BasicCommandContext             context        = new BasicCommandContext();

  @Test
  void binaryApproxDistanceIgnoresTheByteSignBit() {
    // Both quantize to 8 bits with median 1.0; the two vectors differ only on the sign of the
    // last component, so only bit 7 differs -> true Hamming distance is 1, not ~25.
    final Object q1 = quantizeBinary.execute(null, null, null,
        new Object[] { new float[] { 1, 1, 1, 1, 1, 1, 1, 9.0f } }, context);
    final Object q2 = quantizeBinary.execute(null, null, null,
        new Object[] { new float[] { 1, 1, 1, 1, 1, 1, 1, -9.0f } }, context);

    final Object result = approxDistance.execute(null, null, null, new Object[] { q1, q2 }, context);
    assertThat((Float) result).isEqualTo(0.125f, within(0.0001f)); // 1 / 8 bits
  }

  @Test
  void int8ApproxDistanceUsesSignedDifferenceAcrossTheMidpoint() {
    // Codes straddling the signed/unsigned midpoint (-1 and 0): the true (pre-shift) values are
    // 127 and 128, one apart. Masking with & 0xFF would instead read them as 255 and 0.
    final byte[] q1 = { -1 };
    final byte[] q2 = { 0 };

    final Object result = approxDistance.execute(null, null, null, new Object[] { q1, q2, "INT8" }, context);
    assertThat((Float) result).isEqualTo(1.0f, within(0.0001f));
  }

  @Test
  void int8ApproxDistancePreservesTopKOrderingAcrossTheMidpoint() {
    // Query code -1 (raw 127) should rank a neighbor at 0 (raw 128, distance 1) closer than one
    // at 100 (raw 228, distance 101). The pre-fix "& 0xFF" reinterpretation inverted this: it
    // read -1 as 255, making the near neighbor (0) look like the far one (distance 255).
    final byte[] query = { -1 };
    final byte[] near = { 0 };
    final byte[] far = { 100 };

    final float distanceToNear = (Float) approxDistance.execute(null, null, null, new Object[] { query, near, "INT8" }, context);
    final float distanceToFar = (Float) approxDistance.execute(null, null, null, new Object[] { query, far, "INT8" }, context);

    assertThat(distanceToNear).isLessThan(distanceToFar);
  }
}
