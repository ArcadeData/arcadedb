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
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.exception.QueryHeapBudgetExceededException;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.SQLAggregatedFunction;
import com.arcadedb.function.sql.SQLFunctionDefinition;
import com.arcadedb.function.sql.SQLFunctionLibraryDefinition;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #8591: the heap budget the in-memory buffers of all the running queries share - the JVM-wide counter, the
 * per-query tracker that reserves from it in chunks, the per-operation limit that charges the tracker, and the
 * estimate of what an element takes.
 * <p>
 * The nested classes run whole statements against a database: every buffer that grows with the input is under both heap
 * limits (#9979), and so is a subquery written inline in an expression (#10080).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class QueryHeapBudgetTest {
  private static final long MB = 1024 * 1024;

  /**
   * What the budget held reserved when the test started. Another test's result set the garbage collector reclaims
   * during this one can only lower the shared counter, so "nothing left behind" is asserted as at most this.
   */
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
  void chargesMoveBetweenOperationsOfOneQueryWithoutTouchingTheBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1024L);
    final BasicCommandContext context = new BasicCommandContext();
    final OperationHeapLimit worker = OperationHeapLimit.of(context, "worker");
    final OperationHeapLimit holder = OperationHeapLimit.of(context, "holder");
    worker.charge(8 * MB);
    // What this query holds reserved, read from its own tracker: another test's tracker the garbage collector reclaims
    // meanwhile moves the shared counter, not this one
    final long reserved = context.getQueryHeapTracker().getReservedBytes();
    assertThat(reserved).isGreaterThan(0L);

    // The holder takes the worker's buffer over: no byte goes back to the budget for another query to take meanwhile
    assertThat(holder.transferFrom(worker)).isTrue();
    assertThat(worker.getChargedBytes()).isZero();
    assertThat(holder.getChargedBytes()).isEqualTo(8 * MB);
    assertThat(context.getQueryHeapTracker().getReservedBytes()).isEqualTo(reserved);

    holder.release();
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);

    // Another query's operation is not one to take over
    final OperationHeapLimit otherQuery = OperationHeapLimit.of(new BasicCommandContext(), "other");
    otherQuery.charge(MB);
    assertThat(holder.transferFrom(otherQuery)).isFalse();
    assertThat(otherQuery.getChargedBytes()).isEqualTo(MB);
    otherQuery.release();
  }

  @Test
  void aQueryBufferingLittleNeverTouchesTheBudget() {
    final QueryHeapTracker tracker = new QueryHeapTracker();
    tracker.charge(QueryHeapTracker.UNRESERVED_BYTES, "test");

    assertThat(tracker.getUsedBytes()).isEqualTo(QueryHeapTracker.UNRESERVED_BYTES);
    assertThat(tracker.getReservedBytes()).isZero();
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);

    tracker.release(QueryHeapTracker.UNRESERVED_BYTES);
    assertThat(tracker.getUsedBytes()).isZero();
  }

  @Test
  void reservesInChunksAndGivesEverythingBackOnceTheBuffersFitTheAllowance() {
    final QueryHeapTracker tracker = new QueryHeapTracker();
    tracker.charge(QueryHeapTracker.UNRESERVED_BYTES + 10, "test");

    assertThat(tracker.getReservedBytes()).as("a whole chunk, not the 10 bytes past the allowance")
        .isEqualTo(QueryHeapTracker.MIN_CHUNK_BYTES);
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline + QueryHeapTracker.MIN_CHUNK_BYTES);

    // Growing within the chunk takes nothing more
    tracker.charge(QueryHeapTracker.MIN_CHUNK_BYTES / 2, "test");
    assertThat(tracker.getReservedBytes()).isEqualTo(QueryHeapTracker.MIN_CHUNK_BYTES);

    // Past it a chunk as large as what is missing
    tracker.charge(5 * MB, "test");
    assertThat(tracker.getReservedBytes()).isGreaterThanOrEqualTo(tracker.getUsedBytes() - QueryHeapTracker.UNRESERVED_BYTES);

    tracker.release(tracker.getUsedBytes() - 100);
    assertThat(tracker.getReservedBytes()).as("back within the allowance: nothing stays reserved").isZero();
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
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
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
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
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
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
    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
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
    assertThat(limit.getChargedBytes()).as("a refused charge is not counted, as the tracker does not count it").isZero();
    limit.release();
    assertThat(tracker.getUsedBytes()).isEqualTo(QueryHeapTracker.UNRESERVED_BYTES);

    hog.close();
    tracker.close();
  }

  @Test
  void aRefusedOperationLeavesTheShareOfItsSiblingsAlone() {
    setBudgetAboveBaseline(4);
    final BasicCommandContext context = new BasicCommandContext();
    final QueryHeapTracker tracker = context.getQueryHeapTracker();

    // B, a sibling operation of the same query, holds a share of the tracker that is still in use
    final OperationHeapLimit b = OperationHeapLimit.of(context, "ORDER BY");
    b.charge(QueryHeapTracker.UNRESERVED_BYTES + MB);
    final long siblingShare = tracker.getUsedBytes();
    assertThat(siblingShare).isEqualTo(QueryHeapTracker.UNRESERVED_BYTES + MB);
    final long reservedWithSibling = QueryHeapBudget.getReservedBytes();

    // A charges some, then is refused, then releases on the way out, as every step does on a failure
    final QueryHeapTracker others = holdAllBut(64 * 1024);
    final OperationHeapLimit a = OperationHeapLimit.of(context, "GROUP BY");
    a.charge(OperationHeapLimit.FORWARD_BYTES);
    assertThatThrownBy(() -> {
      for (int i = 0; i < 100; i++)
        a.charge(OperationHeapLimit.FORWARD_BYTES);
    }).isInstanceOf(QueryHeapBudgetExceededException.class);
    a.release();

    assertThat(tracker.getUsedBytes()).as("the tracker still counts all of B").isEqualTo(siblingShare);
    others.close();
    assertThat(QueryHeapBudget.getReservedBytes()).as("B's reservation is still held").isGreaterThanOrEqualTo(reservedWithSibling);

    b.release();
    assertThat(tracker.getUsedBytes()).isZero();
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
  }

  @Test
  void aTopKHeapChargesTheRowsItKeeps() {
    final OperationHeapLimit limit = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY");
    final String small = "s";
    final String large = "l".repeat(10_000);
    limit.add(1, small);
    final long withSmall = limit.getChargedBytes();

    // A better row replacing the worst one is charged for what it takes more, or gives back what it takes less
    limit.replace(small, large);
    assertThat(limit.getChargedBytes()).isEqualTo(withSmall - HeapEstimator.estimate(small) + HeapEstimator.estimate(large));
    limit.replace(large, small);
    assertThat(limit.getChargedBytes()).isEqualTo(withSmall);

    // Re-estimating the rows a top-N sort kept charges them exactly, not the average of the rows it saw
    for (int i = 0; i < 200; i++)
      limit.add(i + 2, small);
    limit.rechargeAll(List.of(large, large));
    assertThat(limit.getChargedBytes()).isEqualTo(2 * (HeapEstimator.estimate(large) + HeapEstimator.REFERENCE_BYTES));
    limit.release();
  }

  @Test
  void aRidBitmapReportsWhatItsWordsTake() {
    final RidSet rids = new RidSet();
    rids.add(new RID(0, 1));
    assertThat(rids.getAllocatedBytes()).as("the first block").isEqualTo(4096L * Long.BYTES);

    // ONE RID FAR AWAY GROWS THE BITMAP OF ITS BUCKET UP TO ITS POSITION
    rids.add(new RID(0, 1L << 24));
    assertThat(rids.getAllocatedBytes()).isGreaterThanOrEqualTo((1L << 24) / 64 * Long.BYTES);
    rids.add(new RID(1, 1));
    final long twoBuckets = rids.getAllocatedBytes();
    rids.add(new RID(1, 2));
    assertThat(rids.getAllocatedBytes()).as("a RID in an allocated word takes nothing more").isEqualTo(twoBuckets);

    rids.clear();
    assertThat(rids.getAllocatedBytes()).isZero();
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

  // ---- #9979: every growing buffer is under both heap limits ----

  /**
   * Issue #9979 (#9830: #9792, #9793): every buffer that grows with the input is under both heap limits, the per-operation
   * element cap ({@code arcadedb.queryMaxHeapElementsAllowedPerOp}) and the heap budget all queries share
   * ({@code arcadedb.queryMaxHeapRAM}). A LET subquery, the MATCH prefetch and the SQL aggregates that keep every value were
   * invisible to the cap. The visited set of a TRAVERSE, a couple of primitives per record, is charged to the budget alone.
   */
  @Nested
  class HeapLimitCoverage extends TestHelper {
    private static final int  RECORDS = 4_000;
    private static final long CAP     = 100L;

    /** Every statement below holds the whole of D, or far more of it than the cap, in one buffer. */
    private static final List<String> SQL_OVER_CAP = List.of(//
        // the controls: the steps that already charged
        "SELECT k FROM D ORDER BY k",//
        "SELECT DISTINCT k FROM D",//
        "SELECT count(DISTINCT k) FROM D",//
        // #9792
        "SELECT $x.size() AS s FROM D LET $x = (SELECT k FROM D) LIMIT 1",//
        "SELECT $x.size() AS s FROM D LET $x = (SELECT k FROM D WHERE k >= $parent.$current.k) WHERE k = 0",//
        // #10080: the same subquery written inline, and with a modifier that moves the LET to the expression step
        "SELECT (SELECT k FROM D).size() AS s FROM D LIMIT 1",//
        "SELECT $x AS s FROM D LET $x = (SELECT k FROM D).size() LIMIT 1",//
        // #9793
        "SELECT list(k) FROM D",//
        "SELECT set(k) FROM D",//
        "SELECT unionAll(k) FROM D",//
        // intersect() holds at most its first value: two rows carrying the same 200 values
        "SELECT intersect(big) FROM D WHERE k < 2",//
        "SELECT symmetricDifference(k) FROM D",//
        "SELECT map(m) FROM D",//
        "SELECT percentile(k, 0.5) FROM D",//
        "SELECT median(k) FROM D",//
        "SELECT mode(k) FROM D",//
        "SELECT ts.percentile(k, 0.5) FROM D",//
        "SELECT ts.rank(k, t) FROM D",//
        "SELECT ts.movingAvg(k, 3) FROM D",//
        "SELECT ts.lead(k, 1, t) FROM D",//
        "SELECT ts.lag(k, 1, t) FROM D",//
        "SELECT ts.rate(k, t) FROM D",//
        "SELECT ts.interpolate(k, 'prev', t) FROM D",//
        "SELECT g, list(k) FROM D GROUP BY g",//
        "SELECT g, percentile(k, 0.5) FROM D GROUP BY g");

    private static final List<String> CYPHER_OVER_CAP = List.of(//
        // the controls
        "MATCH (d:D) RETURN collect(d.k)",//
        "MATCH (d:D) RETURN collect(DISTINCT d.k)",//
        // #9793
        "MATCH (d:D) RETURN percentileCont(d.k, 0.5)",//
        "MATCH (d:D) RETURN percentileDisc(d.k, 0.5)",//
        "MATCH (d:D) RETURN count(DISTINCT d.k)",//
        // the same SQL aggregates, reached from openCypher
        "MATCH (d:D) RETURN sql.list(d.k)",//
        "MATCH (d:D) RETURN d.g AS g, sql.list(d.k) AS l",//
        "MATCH (d:D) RETURN sql.mode(d.k)");

    /** The aggregates the statements above cover; a newly registered one that keeps every value must be added there. */
    private static final Set<String> COVERED_AGGREGATES = Set.of("list", "set", "unionall", "intersect", "symmetricdifference", "map",
        "percentile", "median", "mode", "ts.percentile", "ts.rank", "ts.movingavg", "ts.lead", "ts.lag", "ts.rate", "ts.interpolate");

    @AfterEach
    void resetBudget() {
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
    }

    /** Regression test for issue #9979. */
    @Test
    void everyBufferFailsPastTheElementCap() {
      createData(false);
      setCap(CAP);
      // every statement that slipped past the cap, all of them at once rather than the first one
      final List<String> notCapped = new ArrayList<>();
      for (final String sql : SQL_OVER_CAP)
        if (!failsOnTheCap("sql", sql))
          notCapped.add(sql);
      for (final String cypher : CYPHER_OVER_CAP)
        if (!failsOnTheCap("opencypher", cypher))
          notCapped.add(cypher);
      assertThat(notCapped).isEmpty();
    }

    private boolean failsOnTheCap(final String language, final String query) {
      try {
        drain(language, query);
        return false;
      } catch (final HeapLimitExceededException e) {
        assertThat(e.getMessage()).as(query).contains(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
        return true;
      }
    }

    /**
     * Regression test for issue #9979.
     * <p>
     * The same statements, with room to spare: what failed above was the cap, not the statement.
     */
    @Test
    void everyStatementRunsBelowTheElementCap() {
      createData(false);
      setCap(100_000L);
      for (final String sql : SQL_OVER_CAP)
        assertThat(drain("sql", sql)).as(sql).isGreaterThan(0);
      for (final String cypher : CYPHER_OVER_CAP)
        assertThat(drain("opencypher", cypher)).as(cypher).isGreaterThan(0);

      assertThat(single("SELECT $x.size() AS s FROM D LET $x = (SELECT k FROM D) LIMIT 1").<Integer>getProperty("s")).isEqualTo(RECORDS);
      assertThat(single("SELECT count(*) AS c FROM (TRAVERSE out('R') FROM (SELECT FROM D WHERE k = 0) WHILE $depth < 5000)")
          .<Number>getProperty("c").intValue()).isEqualTo(RECORDS);
      assertThat(single("SELECT list(k) AS l FROM D").<List<?>>getProperty("l")).hasSize(RECORDS);
      // one bridge per group, each counting its own values from zero
      try (final ResultSet rs = database.query("opencypher", "MATCH (d:D) RETURN d.g AS g, sql.list(d.k) AS l")) {
        int groups = 0;
        while (rs.hasNext()) {
          assertThat(rs.next().<List<?>>getProperty("l")).hasSize(RECORDS / 2);
          ++groups;
        }
        assertThat(groups).isEqualTo(2);
      }
    }

    /**
     * Regression test for issue #9979.
     * <p>
     * The cap counts what a function holds, not what it is given: a set() of two distinct values over 4000 rows is fine.
     */
    @Test
    void theCapCountsWhatAFunctionHoldsNotWhatItIsGiven() {
      createData(false);
      setCap(CAP);
      assertThat(single("SELECT set(g) AS s FROM D").<Set<?>>getProperty("s")).hasSize(2);
      assertThat(single("SELECT mode(g) AS m FROM D").<List<?>>getProperty("m")).hasSize(2);
      assertThat(drain("opencypher", "MATCH (d:D) RETURN sql.set(d.g)")).isEqualTo(1);
      // difference() keeps nothing between rows: it answers each row on its own arguments
      assertThat(drain("sql", "SELECT difference(ks, [0]) FROM D")).isEqualTo(RECORDS);
    }

    /**
     * Regression test for issue #9979.
     * <p>
     * With the shared budget disabled, the element cap is all there is, and it still fails the query.
     */
    @Test
    void theCapFailsTheQueryWithTheBudgetDisabled() {
      createData(false);
      setCap(CAP);
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(0L);
      for (final String sql : List.of("SELECT list(k) FROM D", "SELECT percentile(k, 0.5) FROM D",
          "SELECT $x.size() AS s FROM D LET $x = (SELECT k FROM D) LIMIT 1"))
        assertThatThrownBy(() -> drain("sql", sql)).as(sql).isInstanceOf(HeapLimitExceededException.class);
    }

    /**
     * Regression test for issue #9979.
     * <p>
     * The bytes of a LET subquery reach the budget all queries share, and go back to it when the query ends.
     */
    @Test
    void aLetSubqueryIsChargedToTheSharedBudgetAndReleased() {
      createData(true);
      setCap(-1L);
      // The counter is read while the LET is open: a query another test left would move it by more than the LET holds when it is
      // collected meanwhile, and would answer the refusals below in place of the statements under test
      QueryHeapBudgetTestSupport.awaitNoReservationLeft();
      final long baseline = QueryHeapBudget.getReservedBytes();

      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
      for (final String sql : List.of("SELECT u FROM D ORDER BY u",//
          "SELECT $x.size() AS s FROM D LET $x = (SELECT u FROM D) LIMIT 1",//
          "SELECT $x.size() AS s FROM D LET $x = (SELECT u FROM D WHERE k >= $parent.$current.k) WHERE k = 0"))
        assertThatThrownBy(() -> drain("sql", sql)).as(sql).hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
      assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);

      // with room, the global LET holds its ~4MB for as long as the query runs, and gives it back when it is closed
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(256L);
      try (final ResultSet rs = database.query("sql", "SELECT $x.size() AS s FROM D LET $x = (SELECT u FROM D)")) {
        assertThat(rs.next().<Integer>getProperty("s")).isEqualTo(RECORDS);
        assertThat(QueryHeapBudget.getReservedBytes()).isGreaterThan(baseline + RECORDS * 1_000L);
      }
      assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
    }

    /**
     * Regression test for issue #9979.
     * <p>
     * The visited set of a TRAVERSE is charged to the shared budget, not capped in number: a traversal reaching more records
     * than the element cap runs, one that would take the budget past its limit fails, and gives back what it took.
     */
    @Test
    void aTraverseVisitedSetIsChargedToTheBudgetNotCapped() {
      createData(false);
      setCap(CAP);
      for (final String strategy : List.of("DEPTH_FIRST", "BREADTH_FIRST"))
        assertThat(single("SELECT count(*) AS c FROM (TRAVERSE out('R') FROM (SELECT FROM D WHERE k = 0) WHILE $depth < 5000 STRATEGY "
            + strategy + ")").<Number>getProperty("c").intValue()).as(strategy).isEqualTo(RECORDS);

      // a hub with 80,000 leaves: about 2MB of visited entries breadth first, 11MB depth first, past a 1MB budget
      final int leaves = 80_000;
      database.command("sql", "CREATE VERTEX TYPE Hub");
      database.command("sql", "CREATE VERTEX TYPE Leaf");
      database.command("sql", "CREATE EDGE TYPE Spoke");
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("Hub").save();
        for (int i = 0; i < leaves; i++)
          hub.newEdge("Spoke", database.newVertex("Leaf").save());
      });
      final long baseline = QueryHeapBudget.getReservedBytes();
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
      for (final String strategy : List.of("DEPTH_FIRST", "BREADTH_FIRST")) {
        assertThatThrownBy(() -> drain("sql", "SELECT count(*) AS c FROM (TRAVERSE out('Spoke') FROM Hub STRATEGY " + strategy + ")"))
            .as(strategy).hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
        assertThat(QueryHeapBudget.getReservedBytes()).as(strategy).isLessThanOrEqualTo(baseline);
      }

      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(256L);
      for (final String strategy : List.of("DEPTH_FIRST", "BREADTH_FIRST"))
        assertThat(single("SELECT count(*) AS c FROM (TRAVERSE out('Spoke') FROM Hub STRATEGY " + strategy + ")")
            .<Number>getProperty("c").intValue()).as(strategy).isEqualTo(leaves + 1);
    }

    /**
     * Regression test for issue #9979.
     * <p>
     * The prefetch a MATCH runs on a small root holds that root's records for the whole statement.
     */
    @Test
    void theMatchPrefetchFailsPastTheElementCap() {
      createData(false);
      // a root estimated below the planner's prefetch threshold of 100 records
      database.command("sql", "CREATE DOCUMENT TYPE P");
      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.newDocument("P").set("k", i).save();
      });
      final String match = "MATCH {type: P, as: p}, {type: D, as: d, where: (k = 0)} RETURN count(*) AS c";
      try (final ResultSet rs = database.query("sql", "EXPLAIN " + match)) {
        assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("PREFETCH");
      }
      setCap(10L);
      assertThatThrownBy(() -> drain("sql", match)).isInstanceOf(HeapLimitExceededException.class).hasMessageContaining("MATCH prefetch of p");
      setCap(100L);
      assertThat(single(match).<Number>getProperty("c").intValue()).isEqualTo(50);
    }

    /**
     * Regression test for issue #9979.
     * <p>
     * Every registered SQL aggregate that keeps every value is one the statements of this test exercise.
     */
    @Test
    void everyBufferingAggregateIsCovered() {
      final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
      final Set<String> buffering = new TreeSet<>();
      // the cap counts what such a function holds: one that does not say counts every value it is given, which over-counts
      // a function that keeps distinct values only
      final Set<String> notReportingHeld = new TreeSet<>();
      for (final String name : factory.getFunctionNames())
        if (factory.getFunctionInstance(name) instanceof SQLAggregatedFunction function && function.holdsEveryValue()) {
          buffering.add(name.toLowerCase(Locale.ENGLISH));
          try {
            if (function.getClass().getMethod("getHeldValues").getDeclaringClass() == SQLAggregatedFunction.class)
              notReportingHeld.add(name);
          } catch (final NoSuchMethodException e) {
            throw new AssertionError(e);
          }
        }
      assertThat(COVERED_AGGREGATES).containsAll(buffering);
      assertThat(notReportingHeld).isEmpty();
    }

    private void createData(final boolean withPayload) {
      database.command("sql", "CREATE VERTEX TYPE D");
      database.command("sql", "CREATE EDGE TYPE R");
      database.transaction(() -> {
        MutableVertex previous = null;
        for (int i = 0; i < RECORDS; i++) {
          final MutableVertex v = database.newVertex("D").set("k", i).set("g", i % 2).set("t", 1_000L * i).set("ks", List.of(i, i + 1))
              .set("m", Map.of("key" + i, i));
          if (i < 2)
            v.set("big", IntStream.range(0, 200).boxed().toList());
          if (withPayload)
            v.set("u", String.valueOf(i).repeat(1_000 / String.valueOf(i).length() + 1));
          v.save();
          if (previous != null)
            previous.newEdge("R", v);
          previous = v;
        }
      });
    }

    private void setCap(final long cap) {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, cap);
    }

    private int drain(final String language, final String query) {
      int rows = 0;
      try (final ResultSet rs = database.query(language, query)) {
        while (rs.hasNext()) {
          rs.next();
          ++rows;
        }
      }
      return rows;
    }

    private Result single(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        assertThat(rs.hasNext()).as(sql).isTrue();
        return rs.next();
      }
    }
  }

  // ---- #10080: an inline subquery is under the heap limits ----

  /**
   * Issue #10080: {@code OperationHeapLimit.materialize()} is "the one way a step materialises a sub-plan, so none can hold one on the
   * heap without the limits seeing it", and was applied to every STEP that materialises one and to none of the EXPRESSIONS that do.
   * A subquery written inline - in a projection, a function argument, a SET value, a {@code CREATE EDGE ... FROM} clause - ran the
   * hand-rolled drain loop {@code materialize()} was created to retire, so the identical subquery over the identical data was refused
   * as a LET and accepted inline, and a single modifier after a LET subquery moved the planner from the covered step to the
   * uncovered one. The claim {@code materialize()} makes - no expression, condition, statement or function drains a sub-plan into a
   * list by hand - is held by {@code SubPlanDrainedByHandCensusTest} (census module).
   */
  @Nested
  class ExpressionSubqueryHeapLimit extends TestHelper {
    private static final int  OUTER_ROWS = 3_000;
    private static final int  INNER_ROWS = 4_000;
    private static final long CAP        = 100L;

    /** Every statement holds the whole of InnerT (or V) in one sub-plan, in a position that is not hoisted to a LET. */
    private static final List<String> OVER_CAP = List.of(//
        // the controls: the LET twin that was already covered, and a hoisted position
        "SELECT $x.size() AS s FROM OuterT LET $x = (SELECT k, pad FROM InnerT) WHERE k = 0",//
        "SELECT FROM OuterT WHERE k IN (SELECT k FROM InnerT)",//
        // the expression positions
        "SELECT (SELECT k, pad FROM InnerT).size() AS s FROM OuterT WHERE k = 0",//
        "SELECT coalesce((SELECT k, pad FROM InnerT), 0) AS s FROM OuterT WHERE k = 0",//
        "UPDATE OuterT SET n = (SELECT k, pad FROM InnerT).size() WHERE k = 0",//
        // one modifier after the subquery moves the LET from LetQueryStep to LetExpressionStep
        "SELECT $x AS s FROM OuterT LET $x = (SELECT k, pad FROM InnerT).size() WHERE k = 0",//
        "SELECT $x AS s FROM OuterT LET $x = (SELECT k, pad FROM InnerT)[0] WHERE k = 0",//
        // the endpoints of CREATE EDGE
        "CREATE EDGE E FROM (SELECT FROM V) TO (SELECT FROM V WHERE k = 0)");

    @AfterEach
    void resetSettings() {
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
    }

    /** Regression test for issue #10080. */
    @Test
    void everySubqueryPositionFailsPastTheElementCap() {
      createData();
      setCap(CAP);

      // every statement that slipped past the cap, all of them at once rather than the first one
      final List<String> notCapped = new ArrayList<>();
      for (final String sql : OVER_CAP)
        if (!failsOnTheCap(sql))
          notCapped.add(sql);
      assertThat(notCapped).isEmpty();
    }

    /**
     * Regression test for issue #10080.
     * <p>
     * The same statements, with room to spare: what failed above was the cap, not the statement.
     */
    @Test
    void theSameStatementsRunBelowTheCap() {
      createData();
      setCap(100_000L);

      for (final String sql : OVER_CAP)
        assertThat(drain(sql)).as(sql).isGreaterThan(0);

      assertThat(single("SELECT (SELECT k, pad FROM InnerT).size() AS s FROM OuterT WHERE k = 0").<Integer>getProperty("s")).isEqualTo(
          INNER_ROWS);
      assertThat(single("SELECT $x AS s FROM OuterT LET $x = (SELECT k, pad FROM InnerT).size() WHERE k = 0").<Integer>getProperty(
          "s")).isEqualTo(INNER_ROWS);
    }

    /** Regression test for issue #10080. */
    @Test
    void theFailureNamesTheConstructAndTheSetting() {
      createData();
      setCap(CAP);

      assertThatThrownBy(() -> drain("SELECT (SELECT k, pad FROM InnerT).size() AS s FROM OuterT WHERE k = 0")).isInstanceOf(
          HeapLimitExceededException.class).hasMessageContaining("subquery").hasMessageContaining(
          GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    }

    /**
     * Regression test for issue #10080.
     * <p>
     * The bytes of an inline subquery reach the budget all queries share, and go back to it when the query ends.
     */
    @Test
    void anInlineSubqueryIsChargedToTheSharedBudgetAndReleased() {
      createData();
      setCap(-1L);
      final long baseline = QueryHeapBudget.getReservedBytes();

      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
      for (final String sql : List.of("SELECT (SELECT k, pad FROM InnerT).size() AS s FROM OuterT WHERE k = 0",
          "SELECT $x AS s FROM OuterT LET $x = (SELECT k, pad FROM InnerT).size() WHERE k = 0"))
        assertThatThrownBy(() -> drain(sql)).as(sql).hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
      assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);

      // with room, the same statement runs, and gives everything back
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(256L);
      assertThat(single("SELECT (SELECT k, pad FROM InnerT).size() AS s FROM OuterT WHERE k = 0").<Integer>getProperty("s")).isEqualTo(
          INNER_ROWS);
      assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
    }

    /**
     * Regression test for issue #10080.
     * <p>
     * A subquery re-evaluated for every outer row must give back what it took before the next row: 3,000 evaluations of ~500 bytes
     * are 1.5MB through a budget of 1MB, and only a charge that accumulated would fail it.
     */
    @Test
    void aSubqueryEvaluatedPerRowDoesNotAccumulateItsCharge() {
      createData();
      setCap(-1L);
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);

      assertThat(drain("SELECT (SELECT pad FROM InnerT WHERE k = 1).size() AS s FROM OuterT")).isEqualTo(OUTER_ROWS);
    }

    /** Regression test for issue #10080. */
    @Test
    void aUserDefinedSqlFunctionIsUnderTheCapToo() {
      createData();
      final SQLFunctionLibraryDefinition library = new SQLFunctionLibraryDefinition(database, "lib10080");
      library.registerFunction(new SQLFunctionDefinition(database, "all", "SELECT k FROM InnerT"));
      database.getSchema().registerFunctionLibrary(library);

      setCap(CAP);
      assertThatThrownBy(() -> database.getSchema().getFunction("lib10080", "all").execute()).isInstanceOf(
          HeapLimitExceededException.class).hasMessageContaining("SQL function 'all'").hasMessageContaining(
          GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());

      setCap(100_000L);
      assertThat((List<?>) database.getSchema().getFunction("lib10080", "all").execute()).hasSize(INNER_ROWS);
    }

    /**
     * Regression test for issue #10080.
     * <p>
     * What the function answers is unchanged: nothing for no row, the bare value for one, the list of values for many.
     */
    @Test
    @SuppressWarnings("unchecked")
    void aUserDefinedSqlFunctionAnswersWhatItAlwaysDid() {
      createData();
      final SQLFunctionLibraryDefinition library = new SQLFunctionLibraryDefinition(database, "lib10080b");
      library.registerFunction(new SQLFunctionDefinition(database, "none", "SELECT k FROM InnerT WHERE k < 0"));
      library.registerFunction(new SQLFunctionDefinition(database, "one", "SELECT k FROM InnerT WHERE k = 7"));
      library.registerFunction(new SQLFunctionDefinition(database, "many", "SELECT k FROM InnerT WHERE k < 3 ORDER BY k"));
      database.getSchema().registerFunctionLibrary(library);

      assertThat(database.getSchema().getFunction("lib10080b", "none").execute()).isNull();
      assertThat(database.getSchema().getFunction("lib10080b", "one").execute()).isEqualTo(7);
      assertThat((List<Object>) database.getSchema().getFunction("lib10080b", "many").execute()).containsExactly(0, 1, 2);
    }

    private void createData() {
      database.command("sql", "CREATE VERTEX TYPE OuterT");
      database.command("sql", "CREATE DOCUMENT TYPE InnerT");
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE E");
      final String pad = "x".repeat(500);
      database.transaction(() -> {
        for (int i = 0; i < OUTER_ROWS; i++)
          database.newVertex("OuterT").set("k", i).save();
        for (int i = 0; i < INNER_ROWS; i++) {
          database.newDocument("InnerT").set("k", i).set("pad", pad).save();
          database.newVertex("V").set("k", i).save();
        }
      });
    }

    private void setCap(final long cap) {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, cap);
    }

    private boolean failsOnTheCap(final String sql) {
      try {
        drain(sql);
        return false;
      } catch (final HeapLimitExceededException e) {
        assertThat(e.getMessage()).as(sql).contains(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
        return true;
      }
    }

    private int drain(final String sql) {
      final int[] rows = new int[1];
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", sql)) {
          while (rs.hasNext()) {
            rs.next();
            ++rows[0];
          }
        }
      });
      return rows[0];
    }

    private Result single(final String sql) {
      final Result[] result = new Result[1];
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", sql)) {
          assertThat(rs.hasNext()).as(sql).isTrue();
          result[0] = rs.next();
        }
      });
      return result[0];
    }
  }
}
