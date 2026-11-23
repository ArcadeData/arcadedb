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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The server-side half of the tag-value rule (code review on PR #7323, issue #7305).
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

  /**
   * Issue #8563, the gRPC sibling of the line protocol's empty-tag-key rule: an empty key names no column, so the
   * sample would be stored with that tag silently dropped, under a different series than the one the client sent.
   */
  @Test
  void anEmptyTagKeyIsRefused() {
    final TimeSeriesPoint emptyKey = point()
        .putTags("", GrpcValue.newBuilder().setStringValue("east").build())
        .putTags("host", GrpcValue.newBuilder().setStringValue("a").build())
        .build();

    assertThatThrownBy(() -> GrpcTimeSeriesSupport.toSamples(List.of(emptyKey), "weather",
        TimeSeriesPrecision.TS_PRECISION_MILLISECONDS))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(e -> {
          assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
          assertThat(((StatusRuntimeException) e).getStatus().getDescription()).contains("tag key");
        });
  }

  /**
   * Issue #8647, the gRPC sibling of the line protocol's empty-tag-value rule: an empty-string tag value is refused,
   * as {@code cpu,host= value=1} is refused over HTTP, instead of being stored as {@code host=""}. An unset value is
   * still an absent tag.
   */
  @Test
  void anEmptyTagValueIsRefused() {
    final TimeSeriesPoint emptyValue = point()
        .putTags("host", GrpcValue.newBuilder().setStringValue("").build())
        .build();

    assertThatThrownBy(() -> GrpcTimeSeriesSupport.toSamples(List.of(emptyValue), "weather",
        TimeSeriesPrecision.TS_PRECISION_MILLISECONDS))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(e -> {
          assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
          assertThat(((StatusRuntimeException) e).getStatus().getDescription()).contains("Tag 'host'").contains("empty value");
        });

    final TimeSeriesPoint unsetValue = point().putTags("host", GrpcValue.getDefaultInstance()).build();
    assertThat(GrpcTimeSeriesSupport.toSamples(List.of(unsetValue), "weather",
        TimeSeriesPrecision.TS_PRECISION_MILLISECONDS).getFirst().getTags()).isEmpty();
  }

  @Test
  void anEmptyFieldKeyIsRefused() {
    final TimeSeriesPoint emptyKey = point()
        .putFields("", GrpcValue.newBuilder().setDoubleValue(1.0).build())
        .build();

    assertThatThrownBy(() -> GrpcTimeSeriesSupport.toSamples(List.of(emptyKey), "weather",
        TimeSeriesPrecision.TS_PRECISION_MILLISECONDS))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(e -> {
          assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
          assertThat(((StatusRuntimeException) e).getStatus().getDescription()).contains("field key");
        });
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

  /*
   * Issue #7394 item 2: {@code TS_MAX_BATCH_SIZE} bounded a streamed message by row count while its javadoc
   * claimed it bounded the message's bytes.
   * <p>
   * A row count is not a byte count. A series with many tags or long string values crosses gRPC's 4 MiB default
   * inbound limit at a row count well inside the 10,000 cap, and the client rejects the frame rather than
   * receiving a status it can act on. The batch is now flushed on whichever bound is reached first.
   * <p>
   * The predicate is tested directly rather than by streaming megabytes through a live server: what needs
   * pinning is that the byte budget can fire while the row count is nowhere near its bound, and that it does so
   * at a size that leaves room under 4 MiB for the parts of the message the accumulator does not count.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7394">issue #7394</a>
   */
  /** gRPC's own default for {@code maxInboundMessageSize}, which is what an unconfigured client enforces. */
  private static final int GRPC_DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 4 * 1024 * 1024;

  // Regression: #7394
  @Test
  void aBatchWellUnderTheRowCapIsStillFlushedOnceItReachesTheByteBudget() {
    // Two rows, no more, but three megabytes of them: the row cap says "keep going", the byte budget must not.
    assertThat(ArcadeDbGrpcService.timeSeriesBatchIsFull(2, 3 * 1024 * 1024, 10_000)).isTrue();
  }

  // Regression: #7394
  @Test
  void aSmallBatchIsNotFlushedByEitherBound() {
    assertThat(ArcadeDbGrpcService.timeSeriesBatchIsFull(2, 1_024, 10_000)).isFalse();
  }

  // Regression: #7394
  @Test
  void theRowBoundStillFiresForNarrowRowsThatNeverReachTheByteBudget() {
    assertThat(ArcadeDbGrpcService.timeSeriesBatchIsFull(1_000, 8_000, 1_000)).isTrue();
    assertThat(ArcadeDbGrpcService.timeSeriesBatchIsFull(999, 8_000, 1_000)).isFalse();
  }

  // Regression: #7394
  /**
   * The whole point of the budget: a batch emitted at the moment it is declared full is a message a client
   * with gRPC's default inbound limit can receive, with margin left for the type name, the column names and
   * the protobuf framing that the accumulator does not count.
   */
  @Test
  void theByteBudgetLeavesHeadroomUnderTheDefaultInboundLimit() {
    int bytes = 0;
    int rows = 0;
    // Grow a batch a realistic wide row at a time until the predicate says to flush, then check what would
    // have gone on the wire.
    while (!ArcadeDbGrpcService.timeSeriesBatchIsFull(rows, bytes, 10_000)) {
      rows++;
      bytes += 4_096;
    }

    assertThat(bytes).isLessThan(GRPC_DEFAULT_MAX_INBOUND_MESSAGE_SIZE);
    // ... and it was the byte bound that fired, not the row cap, which is the case the old code missed.
    assertThat(rows).isLessThan(10_000);
  }

  // Regression: #7394
  /**
   * A row larger than the whole budget cannot be split, so it is emitted alone. The predicate has to say
   * "flush" for it rather than loop forever trying to get under a bound one row already exceeds.
   */
  @Test
  void oneOversizedRowIsFlushedOnItsOwnRatherThanAccumulating() {
    assertThat(ArcadeDbGrpcService.timeSeriesBatchIsFull(1, 8 * 1024 * 1024, 10_000)).isTrue();
  }

  // Regression: #7394
  /**
   * The accumulator matches what goes on the wire: the sum of the rows' serialized sizes is what the message
   * carries, plus the framing the margin covers. Pinned against real protobuf messages so the budget is not
   * being compared against a number of a different kind.
   */
  @Test
  void theAccumulatedSizeIsTheSerializedSizeOfTheRowsTheMessageCarries() {
    final List<TimeSeriesRow> rows = new ArrayList<>();
    int accumulated = 0;
    for (int i = 0; i < 50; i++) {
      final TimeSeriesRow row = TimeSeriesRow.newBuilder()
          .addValues(GrpcValue.newBuilder().setInt64Value(1_000L + i).build())
          .addValues(GrpcValue.newBuilder().setStringValue("host-" + i + "-with-a-realistically-long-tag").build())
          .addValues(GrpcValue.newBuilder().setDoubleValue(22.5 + i).build())
          .build();
      rows.add(row);
      accumulated += row.getSerializedSize();
    }

    final TimeSeriesQueryResult message = TimeSeriesQueryResult.newBuilder()
        .setType("weather")
        .addAllRows(rows)
        .build();

    // The message is the rows plus its own framing and header fields, never less than what was accumulated.
    assertThat(message.getSerializedSize()).isGreaterThanOrEqualTo(accumulated);
  }
}
