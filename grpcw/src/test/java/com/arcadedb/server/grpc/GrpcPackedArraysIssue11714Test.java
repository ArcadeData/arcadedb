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
package com.arcadedb.server.grpc;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11714: a vector travelled as a {@code GrpcList} of one {@code GrpcValue} per element. The packed kinds
 * {@code float_array_value} / {@code double_array_value} decode straight into a {@code float[]} / {@code double[]}, and a
 * {@code float[]} / {@code double[]} result is encoded packed only for a call that asked for it, since a client generated from
 * an older proto reads an unknown oneof member as a null.
 */
class GrpcPackedArraysIssue11714Test {

  // Canonical NaN, a NaN with a payload, both zeros, the infinities and the subnormal edge: compared by their raw bits
  private static final float[]  FLOATS  = { 1.5f, Float.NaN, Float.intBitsToFloat(0x7fc00001), -0.0f, 0.0f, Float.POSITIVE_INFINITY,
      Float.NEGATIVE_INFINITY, Float.MIN_VALUE, -3.25e-12f };
  private static final double[] DOUBLES = { 1.5, Double.NaN, Double.longBitsToDouble(0x7ff8000000000001L), -0.0, 0.0,
      Double.NEGATIVE_INFINITY, Double.MIN_VALUE, Math.PI };

  @Test
  void aPackedFloatArrayDecodesToTheSameFloatsBitForBit() {
    final Object decoded = GrpcTypeConverter.fromGrpcValue(GrpcPackedArrays.encode(FLOATS));

    assertThat(decoded).isInstanceOf(float[].class);
    assertThat(rawBits((float[]) decoded)).containsExactly(rawBits(FLOATS));
  }

  @Test
  void aPackedDoubleArrayDecodesToTheSameDoublesBitForBit() {
    final Object decoded = GrpcTypeConverter.fromGrpcValue(GrpcPackedArrays.encode(DOUBLES));

    assertThat(decoded).isInstanceOf(double[].class);
    assertThat(rawBits((double[]) decoded)).containsExactly(rawBits(DOUBLES));
  }

  @Test
  void anEmptyPackedArrayIsAnEmptyArrayNotANull() {
    final GrpcValue empty = GrpcPackedArrays.encode(new float[0]);

    assertThat(empty.getKindCase()).isEqualTo(GrpcValue.KindCase.FLOAT_ARRAY_VALUE);
    assertThat(GrpcTypeConverter.fromGrpcValue(empty)).isEqualTo(new float[0]);
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcPackedArrays.encode(new double[0]))).isEqualTo(new double[0]);
  }

  @Test
  void thePackedFormIsFourBytesAnElement() {
    final float[] vector = new float[768];
    for (int i = 0; i < vector.length; i++)
      vector[i] = i * 0.001f;

    final GrpcValue packed = GrpcPackedArrays.encode(vector);
    final GrpcValue list = GrpcTypeConverter.toGrpcValue(vector);

    // Tag and length of the packed run, then 768 x 4 bytes; the list spends a tag, a length and a nested message per element
    assertThat(packed.getFloatArrayValue().getSerializedSize()).isEqualTo(3 + 768 * 4);
    assertThat(list.getSerializedSize()).isGreaterThan(768 * 7);
  }

  @Test
  void theOldListEncodingStillDecodesToAList() {
    final GrpcValue list = GrpcValue.newBuilder().setListValue(GrpcList.newBuilder()
        .addValues(GrpcValue.newBuilder().setFloatValue(0.5f)).addValues(GrpcValue.newBuilder().setFloatValue(-1f))).build();

    assertThat(GrpcTypeConverter.fromGrpcValue(list)).isEqualTo(List.of(0.5f, -1f));
  }

  @Test
  void withoutTheConsentAVectorResultStaysAList() {
    final GrpcValue floats = GrpcTypeConverter.toGrpcValue(FLOATS);
    final GrpcValue doubles = GrpcTypeConverter.toGrpcValue(DOUBLES);

    assertThat(floats.getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
    assertThat(floats.getListValue().getValuesCount()).isEqualTo(FLOATS.length);
    assertThat(doubles.getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
  }

  @Test
  void withTheConsentAVectorResultIsPackedWhereverItSits() throws Exception {
    final GrpcValue floats = GrpcPackedArraysInterceptor.callWith(true, () -> GrpcTypeConverter.toGrpcValue(FLOATS));
    final GrpcValue doubles = GrpcPackedArraysInterceptor.callWith(true, () -> GrpcTypeConverter.toGrpcValue(DOUBLES));
    final GrpcValue nested = GrpcPackedArraysInterceptor.callWith(true,
        () -> GrpcTypeConverter.toGrpcValue(Map.of("rows", List.of(Map.of("v", FLOATS)))));

    assertThat(floats.getKindCase()).isEqualTo(GrpcValue.KindCase.FLOAT_ARRAY_VALUE);
    assertThat(rawBits((float[]) GrpcTypeConverter.fromGrpcValue(floats))).containsExactly(rawBits(FLOATS));
    assertThat(doubles.getKindCase()).isEqualTo(GrpcValue.KindCase.DOUBLE_ARRAY_VALUE);
    assertThat(rawBits((double[]) GrpcTypeConverter.fromGrpcValue(doubles))).containsExactly(rawBits(DOUBLES));
    final GrpcValue inner = nested.getMapValue().getEntriesOrThrow("rows").getListValue().getValues(0).getMapValue()
        .getEntriesOrThrow("v");
    assertThat(inner.getKindCase()).isEqualTo(GrpcValue.KindCase.FLOAT_ARRAY_VALUE);
    // The consent is the call's only: once the task is over, the thread encodes lists again
    assertThat(GrpcTypeConverter.toGrpcValue(FLOATS).getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
  }

  @Test
  void theConsentLeavesTheOtherArraysAsLists() throws Exception {
    final GrpcValue ints = GrpcPackedArraysInterceptor.callWith(true, () -> GrpcTypeConverter.toGrpcValue(new int[] { 1, 2 }));

    assertThat(ints.getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
  }

  private static int[] rawBits(final float[] values) {
    final int[] bits = new int[values.length];
    for (int i = 0; i < values.length; i++)
      bits[i] = Float.floatToRawIntBits(values[i]);
    return bits;
  }

  private static long[] rawBits(final double[] values) {
    final long[] bits = new long[values.length];
    for (int i = 0; i < values.length; i++)
      bits[i] = Double.doubleToRawLongBits(values[i]);
    return bits;
  }
}
