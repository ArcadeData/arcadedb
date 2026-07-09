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

import com.arcadedb.server.grpc.GrpcValue;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression tests for issue #5044 (COR-8 + COR-16, client encoder path).
 * <p>
 * Java arrays - in particular {@code float[]}/{@code double[]} vectors - had no branch in
 * {@link ProtoUtils#toGrpcValue(Object)}, so they fell through to the {@code String.valueOf(...)}
 * fallback (e.g. {@code "[F@6d03e736"}), destroying the value. They must encode to a
 * {@code GrpcList} exactly like a {@link java.util.Collection} does.
 * <p>
 * {@link ProtoUtils#fromGrpcValue(GrpcValue)} also lacked the null guard the other two decoders
 * have, so a {@code null} input threw an NPE instead of decoding to {@code null}.
 */
class ProtoUtilsArrayEncoderTest {

  @Test
  void floatArrayEncodesToListNotString() {
    final float[] vector = { 0.1f, 0.2f, 0.3f };

    final GrpcValue value = ProtoUtils.toGrpcValue(vector);

    assertThat(value.hasListValue()).as("float[] vector must serialize to LIST_VALUE, not STRING_VALUE").isTrue();
    assertThat(value.getListValue().getValuesCount()).isEqualTo(3);
    assertThat(value.getListValue().getValues(0).getFloatValue()).isCloseTo(0.1f, within(0.0001f));
    assertThat(value.getListValue().getValues(2).getFloatValue()).isCloseTo(0.3f, within(0.0001f));
  }

  @Test
  void floatArrayRoundTripsToNumericList() {
    final float[] vector = { 1.5f, 2.5f, 3.5f };

    final Object decoded = ProtoUtils.fromGrpcValue(ProtoUtils.toGrpcValue(vector));

    assertThat(decoded).isInstanceOf(List.class);
    @SuppressWarnings("unchecked")
    final List<Object> list = (List<Object>) decoded;
    assertThat(list).hasSize(3);
    assertThat((Float) list.get(0)).isCloseTo(1.5f, within(0.0001f));
    assertThat((Float) list.get(1)).isCloseTo(2.5f, within(0.0001f));
    assertThat((Float) list.get(2)).isCloseTo(3.5f, within(0.0001f));
  }

  @Test
  void doubleArrayEncodesToList() {
    final double[] vector = { 0.25, 0.5, 0.75 };

    final GrpcValue value = ProtoUtils.toGrpcValue(vector);

    assertThat(value.hasListValue()).as("double[] vector must serialize to LIST_VALUE").isTrue();
    assertThat(value.getListValue().getValuesCount()).isEqualTo(3);
    assertThat(value.getListValue().getValues(1).getDoubleValue()).isCloseTo(0.5, within(0.000001));
  }

  @Test
  void intArrayEncodesToList() {
    final int[] ints = { 10, 20, 30 };

    final GrpcValue value = ProtoUtils.toGrpcValue(ints);

    assertThat(value.hasListValue()).isTrue();
    assertThat(value.getListValue().getValuesCount()).isEqualTo(3);
    assertThat(value.getListValue().getValues(0).getInt32Value()).isEqualTo(10);
    assertThat(value.getListValue().getValues(2).getInt32Value()).isEqualTo(30);
  }

  @Test
  void longArrayEncodesToList() {
    final long[] longs = { 1L, 9876543210L };

    final GrpcValue value = ProtoUtils.toGrpcValue(longs);

    assertThat(value.hasListValue()).isTrue();
    assertThat(value.getListValue().getValuesCount()).isEqualTo(2);
    assertThat(value.getListValue().getValues(1).getInt64Value()).isEqualTo(9876543210L);
  }

  @Test
  void objectArrayEncodesToList() {
    final Object[] mixed = { 1, "two", 3.0 };

    final GrpcValue value = ProtoUtils.toGrpcValue(mixed);

    assertThat(value.hasListValue()).isTrue();
    assertThat(value.getListValue().getValuesCount()).isEqualTo(3);
    assertThat(value.getListValue().getValues(0).getInt32Value()).isEqualTo(1);
    assertThat(value.getListValue().getValues(1).getStringValue()).isEqualTo("two");
    assertThat(value.getListValue().getValues(2).getDoubleValue()).isCloseTo(3.0, within(0.000001));
  }

  @Test
  void byteArrayStillEncodesToBytes() {
    // byte[] keeps its dedicated BYTES branch (checked before the generic array branch).
    final byte[] bytes = { 1, 2, 3 };

    final GrpcValue value = ProtoUtils.toGrpcValue(bytes);

    assertThat(value.hasBytesValue()).as("byte[] must remain BYTES_VALUE, not LIST_VALUE").isTrue();
    assertThat(value.getBytesValue().toByteArray()).isEqualTo(bytes);
  }

  @Test
  void fromGrpcValueNullReturnsNullWithoutNpe() {
    // COR-16: fromGrpcValue lacked the null guard the other two decoders have.
    assertThat(ProtoUtils.fromGrpcValue(null)).isNull();
  }
}
