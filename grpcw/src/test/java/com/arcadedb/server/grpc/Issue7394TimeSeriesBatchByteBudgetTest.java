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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue7394TimeSeriesBatchByteBudgetTest {

  /** gRPC's own default for {@code maxInboundMessageSize}, which is what an unconfigured client enforces. */
  private static final int GRPC_DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 4 * 1024 * 1024;

  @Test
  void aBatchWellUnderTheRowCapIsStillFlushedOnceItReachesTheByteBudget() {
    // Two rows, no more, but three megabytes of them: the row cap says "keep going", the byte budget must not.
    assertThat(ArcadeDbGrpcService.timeSeriesBatchIsFull(2, 3 * 1024 * 1024, 10_000)).isTrue();
  }

  @Test
  void aSmallBatchIsNotFlushedByEitherBound() {
    assertThat(ArcadeDbGrpcService.timeSeriesBatchIsFull(2, 1_024, 10_000)).isFalse();
  }

  @Test
  void theRowBoundStillFiresForNarrowRowsThatNeverReachTheByteBudget() {
    assertThat(ArcadeDbGrpcService.timeSeriesBatchIsFull(1_000, 8_000, 1_000)).isTrue();
    assertThat(ArcadeDbGrpcService.timeSeriesBatchIsFull(999, 8_000, 1_000)).isFalse();
  }

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

  /**
   * A row larger than the whole budget cannot be split, so it is emitted alone. The predicate has to say
   * "flush" for it rather than loop forever trying to get under a bound one row already exceeds.
   */
  @Test
  void oneOversizedRowIsFlushedOnItsOwnRatherThanAccumulating() {
    assertThat(ArcadeDbGrpcService.timeSeriesBatchIsFull(1, 8 * 1024 * 1024, 10_000)).isTrue();
  }

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
