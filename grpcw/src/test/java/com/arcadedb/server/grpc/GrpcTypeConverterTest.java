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

import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcTypeConverterTest {

  @Test
  void tsToMillisConvertsCorrectly() {
    Timestamp ts = Timestamp.newBuilder()
        .setSeconds(1000)
        .setNanos(500_000_000)
        .build();

    long millis = GrpcTypeConverter.tsToMillis(ts);

    assertThat(millis).isEqualTo(1000_500L);
  }

  @Test
  void msToTimestampConvertsCorrectly() {
    long millis = 1000_500L;

    Timestamp ts = GrpcTypeConverter.msToTimestamp(millis);

    assertThat(ts.getSeconds()).isEqualTo(1000L);
    assertThat(ts.getNanos()).isEqualTo(500_000_000);
  }

  @Test
  void timestampRoundTrip() {
    long original = System.currentTimeMillis();

    Timestamp ts = GrpcTypeConverter.msToTimestamp(original);
    long result = GrpcTypeConverter.tsToMillis(ts);

    assertThat(result).isEqualTo(original);
  }
}
