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
package com.arcadedb.remote.grpc.utils;

import com.arcadedb.server.grpc.GrpcArrayCodec;
import com.arcadedb.server.grpc.GrpcList;
import com.arcadedb.server.grpc.GrpcMap;
import com.arcadedb.server.grpc.GrpcValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12189, client side: {@link ProtoUtils#fromGrpcValue} decodes a list the server tagged with the class of a primitive
 * numeric array back to that array, as the embedded API returns it, and leaves every other list the {@code ArrayList} it always
 * was. The values here are exactly what the server's encoder emits for the five array types.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12189ProtoUtilsArrayDecodeTest {

  @Test
  void everyTaggedListIsThePrimitiveArray() {
    final Object shorts = ProtoUtils.fromGrpcValue(tagged(GrpcArrayCodec.SHORT_ARRAY, int32(1), int32(2)));
    assertThat(shorts).isInstanceOf(short[].class);
    assertThat((short[]) shorts).containsExactly((short) 1, (short) 2);

    final Object ints = ProtoUtils.fromGrpcValue(tagged(GrpcArrayCodec.INT_ARRAY, int32(1), int32(2)));
    assertThat(ints).isInstanceOf(int[].class);
    assertThat((int[]) ints).containsExactly(1, 2);

    final Object longs = ProtoUtils.fromGrpcValue(tagged(GrpcArrayCodec.LONG_ARRAY, int64(1L), int64(5_000_000_000L)));
    assertThat(longs).isInstanceOf(long[].class);
    assertThat((long[]) longs).containsExactly(1L, 5_000_000_000L);

    final Object floats = ProtoUtils.fromGrpcValue(tagged(GrpcArrayCodec.FLOAT_ARRAY, float32(1.5F)));
    assertThat(floats).isInstanceOf(float[].class);
    assertThat((float[]) floats).containsExactly(1.5F);

    final Object doubles = ProtoUtils.fromGrpcValue(tagged(GrpcArrayCodec.DOUBLE_ARRAY, float64(2.5D)));
    assertThat(doubles).isInstanceOf(double[].class);
    assertThat((double[]) doubles).containsExactly(2.5D);
  }

  @Test
  void anEmptyTaggedListIsAnEmptyArray() {
    assertThat(ProtoUtils.fromGrpcValue(tagged(GrpcArrayCodec.LONG_ARRAY))).isInstanceOf(long[].class);
    assertThat((long[]) ProtoUtils.fromGrpcValue(tagged(GrpcArrayCodec.LONG_ARRAY))).isEmpty();
  }

  @Test
  void anUntaggedListIsStillAList() {
    final Object decoded = ProtoUtils.fromGrpcValue(tagged("", int32(1), int32(2)));

    assertThat(decoded).isInstanceOf(List.class).isEqualTo(List.of(1, 2));
  }

  @Test
  void aListWithATagOfAnotherKindIsStillAList() {
    final Object decoded = ProtoUtils.fromGrpcValue(tagged("bigint[]", int64(1L), int64(2L)));

    assertThat(decoded).isInstanceOf(List.class).isEqualTo(List.of(1L, 2L));
  }

  @Test
  void aTaggedListWithANonNumberIsStillAListOfWhatItHolds() {
    final Object decoded = ProtoUtils.fromGrpcValue(
        tagged(GrpcArrayCodec.LONG_ARRAY, int64(1L), GrpcValue.newBuilder().setStringValue("x").build()));

    assertThat(decoded).isInstanceOf(List.class).isEqualTo(List.of(1L, "x"));
  }

  @Test
  void anArrayNestedInAListOrAMapIsDecodedToo() {
    final GrpcValue list = GrpcValue.newBuilder().setListValue(
        GrpcList.newBuilder().addValues(tagged(GrpcArrayCodec.LONG_ARRAY, int64(7L))).addValues(tagged("", int32(8)))).build();
    final GrpcValue map = GrpcValue.newBuilder().setMapValue(
        GrpcMap.newBuilder().putEntries("vector", tagged(GrpcArrayCodec.FLOAT_ARRAY, float32(0.5F)))).build();

    final List<?> decodedList = (List<?>) ProtoUtils.fromGrpcValue(list);
    assertThat(decodedList.get(0)).isInstanceOf(long[].class);
    assertThat((long[]) decodedList.get(0)).containsExactly(7L);
    assertThat(decodedList.get(1)).isInstanceOf(List.class);

    final Map<?, ?> decodedMap = (Map<?, ?>) ProtoUtils.fromGrpcValue(map);
    assertThat(decodedMap.get("vector")).isInstanceOf(float[].class);
    assertThat((float[]) decodedMap.get("vector")).containsExactly(0.5F);
  }

  private static GrpcValue tagged(final String logicalType, final GrpcValue... items) {
    final GrpcList.Builder list = GrpcList.newBuilder();
    for (final GrpcValue item : items)
      list.addValues(item);
    return GrpcValue.newBuilder().setListValue(list).setLogicalType(logicalType).build();
  }

  private static GrpcValue int32(final int value) {
    return GrpcValue.newBuilder().setInt32Value(value).build();
  }

  private static GrpcValue int64(final long value) {
    return GrpcValue.newBuilder().setInt64Value(value).build();
  }

  private static GrpcValue float32(final float value) {
    return GrpcValue.newBuilder().setFloatValue(value).build();
  }

  private static GrpcValue float64(final double value) {
    return GrpcValue.newBuilder().setDoubleValue(value).build();
  }
}
