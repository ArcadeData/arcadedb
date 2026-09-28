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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.QueryHeapBudgetExceededException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #8591: the heap budget the in-memory buffers of all the running queries share - the JVM-wide counter, the
 * per-query tracker that reserves from it in chunks, the per-operation limit that charges the tracker, and the
 * estimate of what an element takes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class QueryHeapBudgetTest {
  private static final long MB = 1024 * 1024;

  private long baseline;

  @BeforeEach
  void setUp() {
    // THE BUDGET IS JVM-WIDE: A TRACKER ANOTHER TEST ABANDONED MAY STILL HOLD A RESERVATION UNTIL IT IS COLLECTED
    baseline = settledReservedBytes();
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
  }

  @Test
  void aQueryBufferingLittleNeverTouchesTheBudget() {
    final QueryHeapTracker tracker = new QueryHeapTracker();
    tracker.charge(QueryHeapTracker.UNRESERVED_BYTES, "test");

    assertThat(tracker.getUsedBytes()).isEqualTo(QueryHeapTracker.UNRESERVED_BYTES);
    assertThat(tracker.getReservedBytes()).isZero();
    assertThat(QueryHeapBudget.getReservedBytes()).isEqualTo(baseline);

    tracker.release(QueryHeapTracker.UNRESERVED_BYTES);
    assertThat(tracker.getUsedBytes()).isZero();
  }

  @Test
  void reservesInChunksAndGivesEverythingBackOnceTheBuffersFitTheAllowance() {
    final QueryHeapTracker tracker = new QueryHeapTracker();
    tracker.charge(QueryHeapTracker.UNRESERVED_BYTES + 10, "test");

    assertThat(tracker.getReservedBytes()).as("a whole chunk, not the 10 bytes past the allowance")
        .isEqualTo(QueryHeapTracker.MIN_CHUNK_BYTES);
    assertThat(QueryHeapBudget.getReservedBytes()).isEqualTo(baseline + QueryHeapTracker.MIN_CHUNK_BYTES);

    // Growing within the chunk takes nothing more
    tracker.charge(QueryHeapTracker.MIN_CHUNK_BYTES / 2, "test");
    assertThat(tracker.getReservedBytes()).isEqualTo(QueryHeapTracker.MIN_CHUNK_BYTES);

    // Past it a chunk as large as what is missing
    tracker.charge(5 * MB, "test");
    assertThat(tracker.getReservedBytes()).isGreaterThanOrEqualTo(tracker.getUsedBytes() - QueryHeapTracker.UNRESERVED_BYTES);

    tracker.release(tracker.getUsedBytes() - 100);
    assertThat(tracker.getReservedBytes()).as("back within the allowance: nothing stays reserved").isZero();
    assertThat(QueryHeapBudget.getReservedBytes()).isEqualTo(baseline);
  }

  @Test
  void aShrinkingBufferKeepsOnlyAChunkOfSurplus() {
    final QueryHeapTracker tracker = new QueryHeapTracker();
    tracker.charge(QueryHeapTracker.UNRESERVED_BYTES + 20 * MB, "test");
    tracker.release(10 * MB);

    final long needed = tracker.getUsedBytes() - QueryHeapTracker.UNRESERVED_BYTES;
    assertThat(tracker.getReservedBytes()).isBetween(needed, needed + 2 * QueryHeapTracker.MIN_CHUNK_BYTES);

    tracker.close();
    assertThat(tracker.getReservedBytes()).isZero();
    assertThat(QueryHeapBudget.getReservedBytes()).isEqualTo(baseline);
  }

  @Test
  void aQueryIsRefusedWhileOthersHoldTheBudgetAndServedOnceTheyRelease() {
    setBudgetAboveBaseline(4);
    final QueryHeapTracker hog = holdAllBut(MB / 2);
    final long refusalsBefore = QueryHeapBudget.getRefusals();

    final QueryHeapTracker query = new QueryHeapTracker();
    assertThatThrownBy(() -> query.charge(QueryHeapTracker.UNRESERVED_BYTES + MB, "ORDER BY"))
        .isInstanceOf(QueryHeapBudgetExceededException.class)
        .hasMessageContaining("ORDER BY")
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
    assertThat(query.getUsedBytes()).as("a refused charge is not accounted").isZero();
    assertThat(query.getReservedBytes()).isZero();
    assertThat(QueryHeapBudget.getRefusals()).isEqualTo(refusalsBefore + 1);

    hog.close();
    query.charge(QueryHeapTracker.UNRESERVED_BYTES + MB, "ORDER BY");
    assertThat(query.getReservedBytes()).isGreaterThanOrEqualTo(MB);

    query.close();
    assertThat(QueryHeapBudget.getReservedBytes()).isEqualTo(baseline);
  }

  @Test
  void aChunkThatDoesNotFitFallsBackToWhatIsMissing() {
    setBudgetAboveBaseline(20);
    final QueryHeapTracker hog = holdAllBut(MB / 2);

    // A chunk would be 1MB, which no longer fits: the 64KB really missing does
    final QueryHeapTracker query = new QueryHeapTracker();
    query.charge(QueryHeapTracker.UNRESERVED_BYTES + 64 * 1024, "ORDER BY");
    assertThat(query.getReservedBytes()).isEqualTo(64 * 1024);

    hog.close();
    query.close();
  }

  @Test
  void aQueryLargerThanTheWholeBudgetIsRefusedForGood() {
    // The verdict compares the query with the whole budget, so nothing else may hold a share of it here
    await().atMost(Duration.ofSeconds(30)).until(() -> {
      System.gc();
      return QueryHeapBudget.getReservedBytes() == 0;
    });
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(2L);

    final QueryHeapTracker query = new QueryHeapTracker();
    query.charge(QueryHeapTracker.UNRESERVED_BYTES + MB + MB / 2, "GROUP BY");

    assertThatThrownBy(() -> query.charge(MB, "GROUP BY"))
        .isInstanceOf(CommandExecutionException.class)
        .isNotInstanceOf(QueryHeapBudgetExceededException.class)
        .hasMessageContaining("more than the whole budget")
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());

    query.close();
  }

  @Test
  void anAbandonedQueryGivesItsReservationBackOnceCollected() {
    abandonTrackerHolding(8 * MB);
    assertThat(QueryHeapBudget.getReservedBytes()).isGreaterThanOrEqualTo(baseline + 8 * MB);

    await().atMost(Duration.ofSeconds(30)).until(() -> {
      System.gc();
      return QueryHeapBudget.getReservedBytes() <= baseline;
    });
  }

  @Test
  void aDisabledBudgetChargesNothing() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(0L);
    final OperationHeapLimit limit = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY");
    assertThat(limit.isCharging()).isFalse();
    limit.add(1, "a value");
    assertThat(limit.getChargedBytes()).isZero();
  }

  @Test
  void operationChargesReachTheTrackerInBatches() {
    final BasicCommandContext context = new BasicCommandContext();
    final QueryHeapTracker tracker = context.getQueryHeapTracker();
    final OperationHeapLimit limit = OperationHeapLimit.of(context, "ORDER BY");
    assertThat(limit.isCharging()).isTrue();

    limit.charge(OperationHeapLimit.FORWARD_BYTES - 1);
    assertThat(limit.getChargedBytes()).isEqualTo(OperationHeapLimit.FORWARD_BYTES - 1);
    assertThat(tracker.getUsedBytes()).as("below the batch, nothing reached the tracker").isZero();

    limit.charge(1);
    assertThat(tracker.getUsedBytes()).isEqualTo(OperationHeapLimit.FORWARD_BYTES);

    limit.charge(100);
    limit.release(50);
    assertThat(limit.getChargedBytes()).isEqualTo(OperationHeapLimit.FORWARD_BYTES + 50);
    assertThat(tracker.getUsedBytes()).as("a partial release takes from the pending bytes first")
        .isEqualTo(OperationHeapLimit.FORWARD_BYTES);

    limit.release();
    assertThat(limit.getChargedBytes()).isZero();
    assertThat(tracker.getUsedBytes()).isZero();

    limit.release();
    assertThat(tracker.getUsedBytes()).as("idempotent").isZero();
  }

  @Test
  void aNestedOperationChargesAndReleasesThroughItsParent() {
    final BasicCommandContext context = new BasicCommandContext();
    final OperationHeapLimit groupBy = OperationHeapLimit.of(context, "GROUP BY");
    final OperationHeapLimit collect = groupBy.child("collect()");

    collect.add(1, "a value");
    assertThat(collect.getChargedBytes()).isPositive();
    assertThat(groupBy.getChargedBytes()).isEqualTo(collect.getChargedBytes());

    // The parent releases what its children charged with its own buffer
    groupBy.release();
    assertThat(groupBy.getChargedBytes()).isZero();
    collect.release();
    assertThat(groupBy.getChargedBytes()).as("a late release of the child takes nothing more").isZero();
    assertThat(context.getQueryHeapTracker().getUsedBytes()).isZero();
  }

  @Test
  void aRefusedOperationGivesBackOnlyWhatReachedTheTracker() {
    setBudgetAboveBaseline(2);
    final QueryHeapTracker hog = holdAllBut(1024);

    final BasicCommandContext context = new BasicCommandContext();
    final QueryHeapTracker tracker = context.getQueryHeapTracker();
    tracker.charge(QueryHeapTracker.UNRESERVED_BYTES, "earlier operation");
    final OperationHeapLimit limit = OperationHeapLimit.of(context, "DISTINCT");

    assertThatThrownBy(() -> limit.charge(OperationHeapLimit.FORWARD_BYTES)).isInstanceOf(QueryHeapBudgetExceededException.class);
    limit.release();
    assertThat(tracker.getUsedBytes()).isEqualTo(QueryHeapTracker.UNRESERVED_BYTES);

    hog.close();
    tracker.close();
  }

  @Test
  void theElementCapStillApplies() {
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(3L);
    try {
      final OperationHeapLimit limit = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY");
      limit.add(3, "a");
      assertThatThrownBy(() -> limit.add(4, "b"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
      limit.release();
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.reset();
    }
  }

  @Test
  void theContextsOfOneQueryShareItsTracker() {
    final BasicCommandContext root = new BasicCommandContext();
    final BasicCommandContext subQuery = new BasicCommandContext();
    subQuery.setParent(root);

    assertThat(subQuery.getQueryHeapTracker()).isSameAs(root.getQueryHeapTracker());
    assertThat(root.copy().getQueryHeapTracker()).as("a parallel-scan worker's copy").isSameAs(root.getQueryHeapTracker());
    assertThat(subQuery.copy().getQueryHeapTracker()).isSameAs(root.getQueryHeapTracker());
    assertThat(new BasicCommandContext().getQueryHeapTracker()).as("another query").isNotSameAs(root.getQueryHeapTracker());
  }

  @Test
  void estimatesWhatAValueTakes() {
    assertThat(HeapEstimator.estimate(null)).isZero();
    assertThat(HeapEstimator.estimate(Boolean.TRUE)).isZero();
    assertThat(HeapEstimator.estimate("0123456789")).isEqualTo(50);
    assertThat(HeapEstimator.estimate(42L)).isEqualTo(24);

    // A large collection is extrapolated from its first elements
    final List<Object> strings = new ArrayList<>(Collections.nCopies(10_000, "0123456789"));
    assertThat(HeapEstimator.estimate(strings)).isEqualTo(40 + 10_000L * (HeapEstimator.REFERENCE_BYTES + 50));

    final ResultInternal row = new ResultInternal();
    row.setProperty("name", "0123456789");
    row.setProperty("age", 42);
    assertThat(HeapEstimator.estimate(row)).isEqualTo(HeapEstimator.RESULT_BYTES + 2L * HeapEstimator.HASH_ENTRY_BYTES + 50 + 16);

    // Nested values count, down to a depth
    final List<Object> deep = new ArrayList<>();
    List<Object> level = deep;
    for (int i = 0; i < 10; i++) {
      final List<Object> next = new ArrayList<>();
      next.add("x".repeat(1000));
      level.add(next);
      level = next;
    }
    assertThat(HeapEstimator.estimate(deep)).isLessThan(10_000);
  }

  private static void abandonTrackerHolding(final long bytes) {
    new QueryHeapTracker().charge(QueryHeapTracker.UNRESERVED_BYTES + bytes, "abandoned");
  }

  /** A tracker standing for the other running queries, holding all the budget but {@code freeBytes}. */
  private static QueryHeapTracker holdAllBut(final long freeBytes) {
    final QueryHeapTracker hog = new QueryHeapTracker();
    hog.charge(QueryHeapTracker.UNRESERVED_BYTES + QueryHeapBudget.getLimitBytes() - QueryHeapBudget.getReservedBytes() - freeBytes,
        "other queries");
    return hog;
  }

  /** Sets the budget to the megabytes others already hold plus {@code freeMegabytes}. */
  private void setBudgetAboveBaseline(final long freeMegabytes) {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue((baseline + MB - 1) / MB + freeMegabytes);
  }

  /** The reservations once the ones held by trackers nobody references anymore were given back. */
  private static long settledReservedBytes() {
    System.gc();
    long previous;
    long current = QueryHeapBudget.getReservedBytes();
    for (int i = 0; i < 20; i++) {
      previous = current;
      System.gc();
      try {
        Thread.sleep(20);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      current = QueryHeapBudget.getReservedBytes();
      if (current == 0 || current == previous && i > 3)
        break;
    }
    return current;
  }
}
