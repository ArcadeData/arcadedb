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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression tests for issue #5044 (COR-8, static converter path).
 * <p>
 * {@link GrpcTypeConverter#toGrpcValue(Object)} handled {@link java.util.List} but not Java arrays,
 * so a {@code float[]}/{@code double[]} vector fell through to the {@code String.valueOf(...)}
 * fallback. Arrays must encode to a {@code GrpcList}, matching the live server encoder.
 */
class GrpcTypeConverterArrayEncoderTest {

  @Test
  void floatArrayEncodesToListNotString() {
    final float[] vector = { 0.1f, 0.2f, 0.3f };

    final GrpcValue value = GrpcTypeConverter.toGrpcValue(vector);

    assertThat(value.hasListValue()).as("float[] vector must serialize to LIST_VALUE, not STRING_VALUE").isTrue();
    assertThat(value.getListValue().getValuesCount()).isEqualTo(3);
    assertThat(value.getListValue().getValues(0).getFloatValue()).isCloseTo(0.1f, within(0.0001f));
    assertThat(value.getListValue().getValues(2).getFloatValue()).isCloseTo(0.3f, within(0.0001f));
  }

  @Test
  void doubleArrayEncodesToList() {
    final double[] vector = { 0.25, 0.5, 0.75 };

    final GrpcValue value = GrpcTypeConverter.toGrpcValue(vector);

    assertThat(value.hasListValue()).as("double[] vector must serialize to LIST_VALUE").isTrue();
    assertThat(value.getListValue().getValuesCount()).isEqualTo(3);
    assertThat(value.getListValue().getValues(1).getDoubleValue()).isCloseTo(0.5, within(0.000001));
  }

  @Test
  void intArrayEncodesToList() {
    final int[] ints = { 10, 20, 30 };

    final GrpcValue value = GrpcTypeConverter.toGrpcValue(ints);

    assertThat(value.hasListValue()).isTrue();
    assertThat(value.getListValue().getValuesCount()).isEqualTo(3);
    assertThat(value.getListValue().getValues(0).getInt32Value()).isEqualTo(10);
  }

  @Test
  void byteArrayStillEncodesToBytes() {
    final byte[] bytes = { 1, 2, 3 };

    final GrpcValue value = GrpcTypeConverter.toGrpcValue(bytes);

    assertThat(value.hasBytesValue()).as("byte[] must remain BYTES_VALUE, not LIST_VALUE").isTrue();
    assertThat(value.getBytesValue().toByteArray()).isEqualTo(bytes);
  }
}
