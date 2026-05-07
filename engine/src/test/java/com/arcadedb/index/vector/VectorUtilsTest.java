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
package com.arcadedb.index.vector;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for {@link VectorUtils} INT8-related conversions: pin the dequantization edge cases
 * (Cohere/OpenAI calibration {@code value / 127.0f} with the {@code -128 -> -127} clamp) and the
 * strict / encoding-aware split for {@code toFloatArray}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class VectorUtilsTest {

  @Test
  void dequantizeInt8PinsBoundaryValues() {
    final byte[] in = { -128, -127, -1, 0, 1, 127 };
    final float[] out = VectorUtils.dequantizeInt8ToFloat(in);

    assertThat(out).hasSize(6);
    // -128 must be clamped up to -127 to keep the result inside [-1, 1] for COSINE similarity.
    // The remaining values follow the v / 127.0f Cohere/OpenAI calibration.
    assertThat(out[0]).isEqualTo(-127f / 127f);
    assertThat(out[1]).isEqualTo(-1.0f);
    assertThat(out[2]).isCloseTo(-1f / 127f, within(1e-6f));
    assertThat(out[3]).isEqualTo(0.0f);
    assertThat(out[4]).isCloseTo(1f / 127f, within(1e-6f));
    assertThat(out[5]).isEqualTo(1.0f);
  }

  @Test
  void dequantizeInt8EmptyVectorReturnsEmptyArray() {
    assertThat(VectorUtils.dequantizeInt8ToFloat(new byte[0])).isEmpty();
  }

  @Test
  void toFloatArrayStrictRejectsByteArray() {
    // Safety contract: the bare overload rejects byte[] up front to keep non-INT8 callers from
    // silently dequantizing stray binary data.
    assertThatThrownBy(() -> VectorUtils.toFloatArray(new byte[] { 1, 2, 3 }))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("byte[] vector is only supported in an INT8-encoded index context");
  }

  @Test
  void toFloatArrayWithFloat32EncodingRejectsByteArray() {
    // Encoding-aware overload: under FLOAT32 the rejection still applies. This pins the safety
    // contract so a future refactor that accidentally widens the byte[] branch is caught.
    assertThatThrownBy(() -> VectorUtils.toFloatArray(new byte[] { 1, 2, 3 }, VectorEncoding.FLOAT32))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("byte[] vector is only supported in an INT8-encoded index context");
  }

  @Test
  void toFloatArrayWithInt8EncodingDequantizesByteArray() {
    final byte[] in = { 0, 64, 127, -1 };
    final float[] out = VectorUtils.toFloatArray(in, VectorEncoding.INT8);
    assertThat(out).hasSize(4);
    assertThat(out[0]).isEqualTo(0.0f);
    assertThat(out[1]).isCloseTo(64f / 127f, within(1e-6f));
    assertThat(out[2]).isEqualTo(1.0f);
    assertThat(out[3]).isCloseTo(-1f / 127f, within(1e-6f));
  }

  @Test
  void toFloatArrayPassesThroughFloatArrayUnchanged() {
    // The encoding-aware overload only intercepts byte[] inputs - any other type, including
    // float[], delegates straight to the bare overload, which returns the input array reference
    // verbatim (no defensive copy). Asserting identity on all three call shapes pins the
    // pass-through contract so a future widening of the byte[] branch (e.g. accepting
    // double[] under INT8) would surface as a failing test rather than as a silent reallocation.
    final float[] in = { 0.1f, 0.2f, 0.3f };
    assertThat(VectorUtils.toFloatArray(in)).isSameAs(in);
    assertThat(VectorUtils.toFloatArray(in, VectorEncoding.FLOAT32)).isSameAs(in);
    assertThat(VectorUtils.toFloatArray(in, VectorEncoding.INT8)).isSameAs(in);
  }
}
