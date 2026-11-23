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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12189: a primitive numeric array ({@code ARRAY_OF_SHORTS/INTEGERS/LONGS/FLOATS/DOUBLES}) travels as a list like any
 * other, so the client read an {@code ArrayList} of boxed numbers where the embedded API returns the primitive array. The server
 * now tags the list with the Java name of the array's class ({@link GrpcArrayCodec}) and the client decodes a tagged list back to
 * the array. A list that is not tagged, or whose tag the codec does not know, stays the list it always was.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12189GrpcArrayWireTest {

  @Test
  void everyPrimitiveNumericArrayIsTaggedWithItsClass() {
    assertTagged(GrpcTypeConverter.toGrpcValue(new short[] { 1, 2 }), GrpcArrayCodec.SHORT_ARRAY, 2);
    assertTagged(GrpcTypeConverter.toGrpcValue(new int[] { 1, 2 }), GrpcArrayCodec.INT_ARRAY, 2);
    assertTagged(GrpcTypeConverter.toGrpcValue(new long[] { 1L, 5_000_000_000L }), GrpcArrayCodec.LONG_ARRAY, 2);
    assertTagged(GrpcTypeConverter.toGrpcValue(new float[] { 1.5F }), GrpcArrayCodec.FLOAT_ARRAY, 1);
    assertTagged(GrpcTypeConverter.toGrpcValue(new double[] { 2.5D }), GrpcArrayCodec.DOUBLE_ARRAY, 1);
  }

  @Test
  void anEmptyPrimitiveArrayIsTaggedToo() {
    assertTagged(GrpcTypeConverter.toGrpcValue(new long[0]), GrpcArrayCodec.LONG_ARRAY, 0);
    assertTagged(GrpcTypeConverter.toGrpcValue(new float[0]), GrpcArrayCodec.FLOAT_ARRAY, 0);
  }

  @Test
  void whatIsNotAPrimitiveNumericArrayIsNotTagged() {
    assertUntaggedList(GrpcTypeConverter.toGrpcValue(new Long[] { 1L, 2L }));
    assertUntaggedList(GrpcTypeConverter.toGrpcValue(new Float[] { 1F }));
    assertUntaggedList(GrpcTypeConverter.toGrpcValue(new String[] { "a" }));
    assertUntaggedList(GrpcTypeConverter.toGrpcValue(new boolean[] { true }));
    assertUntaggedList(GrpcTypeConverter.toGrpcValue(List.of(1L, 2L)));
    assertUntaggedList(GrpcTypeConverter.toGrpcValue(List.of(1.5F)));
  }

  @Test
  void aNestedPrimitiveArrayIsTaggedWhereverItSits() {
    final GrpcValue list = GrpcTypeConverter.toGrpcValue(List.of(new long[] { 1L }, new double[] { 2D }));
    assertThat(list.getLogicalType()).isEmpty();
    assertThat(list.getListValue().getValues(0).getLogicalType()).isEqualTo(GrpcArrayCodec.LONG_ARRAY);
    assertThat(list.getListValue().getValues(1).getLogicalType()).isEqualTo(GrpcArrayCodec.DOUBLE_ARRAY);
  }

  @Test
  void logicalTypeOfNamesNoOtherValue() {
    assertThat(GrpcArrayCodec.logicalTypeOf(null)).isNull();
    assertThat(GrpcArrayCodec.logicalTypeOf("long[]")).isNull();
    assertThat(GrpcArrayCodec.logicalTypeOf(1L)).isNull();
    assertThat(GrpcArrayCodec.logicalTypeOf(new byte[] { 1 })).isNull();
    assertThat(GrpcArrayCodec.logicalTypeOf(new Long[] { 1L })).isNull();
    assertThat(GrpcArrayCodec.logicalTypeOf(new long[0])).isEqualTo(GrpcArrayCodec.LONG_ARRAY);
  }

  @Test
  void aTaggedListIsTheArrayAgain() {
    assertThat((short[]) decode(new short[] { 1, -2 })).containsExactly((short) 1, (short) -2);
    assertThat((int[]) decode(new int[] { 1, Integer.MIN_VALUE })).containsExactly(1, Integer.MIN_VALUE);
    assertThat((long[]) decode(new long[] { 1L, 5_000_000_000L, Long.MIN_VALUE })).containsExactly(1L, 5_000_000_000L,
        Long.MIN_VALUE);
    assertThat((float[]) decode(new float[] { 1.5F, Float.NaN, Float.MAX_VALUE })).containsExactly(1.5F, Float.NaN, Float.MAX_VALUE);
    assertThat((double[]) decode(new double[] { 2.5D, Double.NEGATIVE_INFINITY })).containsExactly(2.5D, Double.NEGATIVE_INFINITY);
  }

  @Test
  void anEmptyTaggedListIsAnEmptyArray() {
    assertThat((long[]) decode(new long[0])).isEmpty();
    assertThat((double[]) decode(new double[0])).isEmpty();
  }

  @Test
  void aListWithoutATagOrWithAnUnknownOneIsNotAnArray() {
    final List<GrpcValue> ints = List.of(int32(1), int32(2));

    assertThat(GrpcArrayCodec.toArray(ints, null)).isNull();
    assertThat(GrpcArrayCodec.toArray(ints, "")).isNull();
    assertThat(GrpcArrayCodec.toArray(ints, "bigint[]")).isNull();
    assertThat(GrpcArrayCodec.toArray(ints, "date")).isNull();
    // the tag is the class name of the array: the declared name of the type is not one
    assertThat(GrpcArrayCodec.toArray(ints, "Long[]")).isNull();
    assertThat(GrpcArrayCodec.toArray(ints, "ARRAY_OF_LONGS")).isNull();
  }

  @Test
  void aTaggedListHoldingANonNumberIsNotAnArray() {
    final GrpcValue text = GrpcValue.newBuilder().setStringValue("1").build();
    final GrpcValue bool = GrpcValue.newBuilder().setBoolValue(true).build();
    final GrpcValue absent = GrpcValue.getDefaultInstance();

    for (final String tag : List.of(GrpcArrayCodec.SHORT_ARRAY, GrpcArrayCodec.INT_ARRAY, GrpcArrayCodec.LONG_ARRAY,
        GrpcArrayCodec.FLOAT_ARRAY, GrpcArrayCodec.DOUBLE_ARRAY)) {
      assertThat(GrpcArrayCodec.toArray(List.of(int32(1), text), tag)).as(tag).isNull();
      assertThat(GrpcArrayCodec.toArray(List.of(bool, int32(1)), tag)).as(tag).isNull();
      assertThat(GrpcArrayCodec.toArray(List.of(int32(1), absent), tag)).as(tag).isNull();
    }
  }

  private static void assertTagged(final GrpcValue encoded, final String tag, final int size) {
    assertThat(encoded.getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
    assertThat(encoded.getLogicalType()).isEqualTo(tag);
    assertThat(encoded.getListValue().getValuesCount()).isEqualTo(size);
  }

  private static void assertUntaggedList(final GrpcValue encoded) {
    assertThat(encoded.getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
    assertThat(encoded.getLogicalType()).isEmpty();
  }

  /** What the client does with what the server sent: the tag read back to the array. */
  private static Object decode(final Object array) {
    final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(array);
    final Object decoded = GrpcArrayCodec.toArray(encoded.getListValue().getValuesList(), encoded.getLogicalType());
    assertThat(decoded).isNotNull();
    assertThat(decoded.getClass()).isEqualTo(array.getClass());
    return decoded;
  }

  private static GrpcValue int32(final int value) {
    return GrpcValue.newBuilder().setInt32Value(value).build();
  }
}
