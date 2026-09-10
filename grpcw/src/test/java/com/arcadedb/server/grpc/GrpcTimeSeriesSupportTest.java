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

import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The server-side half of the tag-value rule (claude-review on PR #7323, issue #7305).
 * <p>
 * {@code TimeSeriesPoint} refuses an unstorable tag in the Java client, but the proto types tags as
 * {@code map<string, GrpcValue>} and any gRPC client can put a bytes, list or map value there. The server has
 * to refuse it too, or a byte array reaches storage as {@code [B@6bc7c054} - a different meaningless tag on
 * every run.
 */
class GrpcTimeSeriesSupportTest {

  private static TimeSeriesPoint.Builder point() {
    return TimeSeriesPoint.newBuilder().setType("weather").setTimestamp(1_000L)
        .putFields("temperature", GrpcValue.newBuilder().setDoubleValue(22.5).build());
  }

  @Test
  void aBytesTagIsRefusedRatherThanStoredAsAnObjectIdentity() {
    final TimeSeriesPoint bytesTag = point()
        .putTags("host", GrpcValue.newBuilder().setBytesValue(ByteString.copyFrom(new byte[] { 1, 2 })).build())
        .build();

    assertThatThrownBy(() -> GrpcTimeSeriesSupport.toSamples(List.of(bytesTag), "weather",
        TimeSeriesPrecision.TS_PRECISION_MILLISECONDS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host");
  }

  @Test
  void aListTagIsRefused() {
    final TimeSeriesPoint listTag = point()
        .putTags("host", GrpcValue.newBuilder().setListValue(GrpcList.newBuilder()
            .addValues(GrpcValue.newBuilder().setStringValue("a").build())).build())
        .build();

    assertThatThrownBy(() -> GrpcTimeSeriesSupport.toSamples(List.of(listTag), "weather",
        TimeSeriesPrecision.TS_PRECISION_MILLISECONDS))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aScalarTagIsConvertedToItsTextFormAsTheLineProtocolPathDoes() {
    // The two protocols must store a tag identically: LineProtocolParser models tags as Map<String, String>
    // and the column coerces that text, so gRPC converts the same way rather than passing a typed value
    // through.
    final TimeSeriesPoint numericTag = point()
        .putTags("rack", GrpcValue.newBuilder().setInt32Value(7).build())
        .build();

    final List<Sample> samples = GrpcTimeSeriesSupport.toSamples(List.of(numericTag), "weather",
        TimeSeriesPrecision.TS_PRECISION_MILLISECONDS);

    assertThat(samples).hasSize(1);
    assertThat(samples.getFirst().getTags()).containsEntry("rack", "7");
    assertThat(samples.getFirst().getFields()).containsEntry("temperature", 22.5);
    assertThat(samples.getFirst().getTimestampMs()).isEqualTo(1_000L);
  }

  @Test
  void aPrecisionOtherThanMillisecondsIsConvertedNotAssumed() {
    final TimeSeriesPoint seconds = TimeSeriesPoint.newBuilder().setType("weather").setTimestamp(5L)
        .putFields("temperature", GrpcValue.newBuilder().setDoubleValue(1.0).build())
        .build();

    assertThat(GrpcTimeSeriesSupport.toSamples(List.of(seconds), "weather",
        TimeSeriesPrecision.TS_PRECISION_SECONDS).getFirst().getTimestampMs()).isEqualTo(5_000L);

    // The zero value is MILLISECONDS, so an unset precision must not be rescaled.
    assertThat(GrpcTimeSeriesSupport.toSamples(List.of(seconds), "weather",
        TimeSeriesPrecision.TS_PRECISION_MILLISECONDS).getFirst().getTimestampMs()).isEqualTo(5L);
  }

  @Test
  void aPointNamingNoTypeAndHavingNoDefaultIsRefused() {
    final TimeSeriesPoint untyped = TimeSeriesPoint.newBuilder().setTimestamp(1L)
        .putFields("temperature", GrpcValue.newBuilder().setDoubleValue(1.0).build())
        .build();

    assertThatThrownBy(() -> GrpcTimeSeriesSupport.toSamples(List.of(untyped), "",
        TimeSeriesPrecision.TS_PRECISION_MILLISECONDS))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode())
            .isEqualTo(Status.Code.INVALID_ARGUMENT));
  }
}
