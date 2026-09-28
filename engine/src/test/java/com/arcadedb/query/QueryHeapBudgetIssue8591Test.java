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
package com.arcadedb.query;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.QueryHeapBudgetExceededException;
import com.arcadedb.query.opencypher.executor.operators.RowBuffer;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapTracker;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #8591: {@code arcadedb.queryMaxHeapElementsAllowedPerOp} bounds what one operation of one query holds, which
 * does not stop many large queries from exhausting the heap together (55 concurrent requests held 64 GB in the incident
 * behind #8583). The in-memory buffers of every query - SQL and OpenCypher sorts, distincts, groups, collect(), join
 * buffers - now reserve their estimated size from one budget the whole JVM shares, {@code arcadedb.queryMaxHeapRAM}:
 * <ul>
 *   <li>a query that would take the reservations past it is refused with a transient
 *   {@link QueryHeapBudgetExceededException}, and runs once the others released what they held;</li>
 *   <li>a buffer gives its reservation back as soon as it is served, when its query closes, and - for a cursor the
 *   caller abandoned - once the garbage collector finds it, so a finished query never shrinks the budget.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class QueryHeapBudgetIssue8591Test extends TestHelper {
  private static final int    ROWS   = 20_000;
  private static final long   MB     = 1024 * 1024;
  private static final String FILLER = "x".repeat(80);
  private static final int    BIG_PAYLOAD = 200 * 1024;

  private static final String[] SQL_BUFFERS = {                                     //
      "SELECT FROM Doc ORDER BY name",                                              //
      "SELECT name FROM Doc ORDER BY name DESC",                                    //
      "SELECT DISTINCT name FROM Doc",                                              //
      "SELECT name, count(*) AS c FROM Doc GROUP BY name" };

  // AGGREGATES THAT KEEP EVERY VALUE THEY AGGREGATE: ONE GROUP, WHICH WITHOUT THEM WOULD HOLD A FEW HUNDRED BYTES
  private static final String[] SQL_GATHERING_AGGREGATES = {                        //
      "SELECT list(name) AS names FROM Doc",                                        //
      "SELECT set(name) AS names FROM Doc",                                         //
      "SELECT percentile(id, 0.5) AS p FROM Doc",                                   //
      "SELECT mode(name) AS m FROM Doc" };

  private static final String[] CYPHER_BUFFERS = {                                  //
      "MATCH (p:P) RETURN p.name AS name ORDER BY name",                            //
      "MATCH (p:P) RETURN DISTINCT p.name AS name",                                 //
      "MATCH (p:P) WITH DISTINCT p.name AS name RETURN name",                       //
      "MATCH (p:P) RETURN p.name AS name UNION MATCH (q:Q) RETURN q.name AS name" };

  // THE ROWS OF AN AGGREGATION CARRY WHAT IT GATHERED - A collect() LIST - DOWNSTREAM: HELD UNTIL THE QUERY CLOSES
  private static final String[] CYPHER_AGGREGATIONS = {                             //
      "MATCH (p:P) RETURN p.name AS name, count(*) AS c",                           //
      "MATCH (p:P) RETURN p.id % 100 AS g, collect(p.name) AS names",               //
      "MATCH (p:P) RETURN collect(p.name) AS names",                                //
      "MATCH (p:P) RETURN count(DISTINCT p.name) AS names" };

  private static final String[] JOIN_BUFFERS = {                                    //
      "MATCH (p:P), (q:Q) WHERE p.id < 5 RETURN p.id AS p, q.id AS q",              //
      "MATCH (p:P), (q:Q) WHERE p.id = q.id RETURN p.id AS p, q.id AS q" };

  /**
   * What the budget held reserved when the test started. Another test's result set the garbage collector reclaims
   * during this one can only lower the shared counter, so "nothing left behind" is asserted as at most this.
   */
  private long baseline;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc", 4);
    database.getSchema().createVertexType("P");
    database.getSchema().createVertexType("Q");
    database.getSchema().createVertexType("Big");
    database.transaction(() -> {
      // MANY SMALL ROWS, THEN A FEW LARGE ONES A TOP-N SORT BY SIZE KEEPS
      for (int i = 0; i < 2_000; i++)
        database.newVertex("Big").set("size", i, "payload", "x").save();
      for (int i = 0; i < 10; i++)
        database.newVertex("Big").set("size", 1_000_000 + i, "payload", "y".repeat(BIG_PAYLOAD)).save();
    });
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++) {
        final String name = "name-%06d-%s".formatted(i, FILLER);
        database.newDocument("Doc").set("id", i, "name", name).save();
        database.newVertex("P").set("id", i, "name", name).save();
        database.newVertex("Q").set("id", i, "name", name).save();
      }
    });
    // SMALL UNITS, SO THE SCAN OF THE FIXTURE CAN RUN IN THE WORKERS OF A PARALLEL SCAN
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    baseline = settledReservedBytes();
  }

  @Test
  void sqlBuffersAreRefusedWhileOtherQueriesHoldTheBudget() {
    for (final String query : SQL_BUFFERS)
      assertRefusedWhileOthersHoldTheBudget("sql", query);
    for (final String query : SQL_GATHERING_AGGREGATES)
      assertRefusedWhileOthersHoldTheBudget("sql", query);
  }

  @Test
  void cypherBuffersAreRefusedWhileOtherQueriesHoldTheBudget() {
    for (final String query : CYPHER_BUFFERS)
      assertRefusedWhileOthersHoldTheBudget("opencypher", query);
    for (final String query : CYPHER_AGGREGATIONS)
      assertRefusedWhileOthersHoldTheBudget("opencypher", query);
  }

  @Test
  void cypherJoinBuffersAreRefusedWhileOtherQueriesHoldTheBudget() {
    // The right side of a Cartesian product - Q, the second pattern - and the build side of a hash join
    for (final String query : JOIN_BUFFERS)
      assertRefusedWhileOthersHoldTheBudget("opencypher", query);
  }

  @Test
  void aParallelGroupByChargesTheGroupsOfItsWorkers() {
    final String query = "SELECT name, count(*) AS c FROM Doc GROUP BY name";
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("CALCULATE AGGREGATE PROJECTIONS (parallel");
    }
    assertRefusedWhileOthersHoldTheBudget("sql", query);

    assertThat(countRows("sql", query)).isEqualTo(ROWS);
    assertThat(QueryHeapBudget.getReservedBytes()).as("the workers' groups were given back").isLessThanOrEqualTo(baseline);
  }

  @Test
  void aParallelGroupByWithALimitHoldsOnlyTheGroupsItReturns() {
    final String all = "SELECT name, count(*) AS c FROM Doc GROUP BY name";
    final String one = all + " LIMIT 1";
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + one)) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("CALCULATE AGGREGATE PROJECTIONS (parallel");
    }

    final long allGroups;
    try (final ResultSet rs = database.query("sql", all)) {
      rs.next();
      allGroups = QueryHeapBudget.getReservedBytes() - baseline;
    }
    assertThat(allGroups).as("every group is held while the rows are served").isGreaterThan(MB);

    // The groups past the LIMIT are gone once the merged ones are listed: their charges must not stay behind them
    try (final ResultSet rs = database.query("sql", one)) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(QueryHeapBudget.getReservedBytes() - baseline).as("only the returned group is held").isLessThan(allGroups / 4);
      rs.next();
    }
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
  }

  @Test
  void buffersGiveTheirHeapBackOnceEveryRowIsServed() {
    for (final String query : SQL_BUFFERS)
      assertHeldWhileServedAndGivenBackAtTheEnd("sql", query);
    for (final String query : CYPHER_BUFFERS)
      assertHeldWhileServedAndGivenBackAtTheEnd("opencypher", query);
  }

  @Test
  void aggregationsGiveTheirHeapBackWhenTheQueryCloses() {
    for (final String query : CYPHER_AGGREGATIONS) {
      final ResultSet rs = database.query("opencypher", query);
      while (rs.hasNext())
        rs.next();
      assertThat(QueryHeapBudget.getReservedBytes()).as("what the aggregation gathered is still held: " + query)
          .isGreaterThan(baseline);
      rs.close();
      assertThat(QueryHeapBudget.getReservedBytes()).as(query).isLessThanOrEqualTo(baseline);
    }
  }

  @Test
  void joinBuffersGiveTheirHeapBackWhenTheJoinEnds() {
    for (final String query : JOIN_BUFFERS)
      assertHeldWhileServedAndGivenBackAtTheEnd("opencypher", query);
  }

  @Test
  void aQueryClosedBeforeItsEndGivesItsHeapBack() {
    for (final String query : SQL_BUFFERS)
      assertGivenBackOnClose("sql", query);
    for (final String query : CYPHER_BUFFERS)
      assertGivenBackOnClose("opencypher", query);
    for (final String query : CYPHER_AGGREGATIONS)
      assertGivenBackOnClose("opencypher", query);
    for (final String query : JOIN_BUFFERS)
      assertGivenBackOnClose("opencypher", query);
  }

  @Test
  void anAbandonedCursorGivesItsHeapBackOnceCollected() {
    readOneRowAndAbandon("sql", "SELECT FROM Doc ORDER BY name");
    readOneRowAndAbandon("opencypher", "MATCH (p:P) RETURN p.name AS name ORDER BY name");
    assertThat(QueryHeapBudget.getReservedBytes()).isGreaterThan(baseline);

    await().atMost(Duration.ofSeconds(30)).until(() -> {
      System.gc();
      return QueryHeapBudget.getReservedBytes() <= baseline;
    });
  }

  @Test
  void aWriteQueryGivesItsHeapBackWhenItReturns() {
    database.transaction(() -> {
      database.command("opencypher", "MATCH (p:P) WITH collect(p) AS ps UNWIND ps AS p SET p.touched = true").close();
      database.command("opencypher", "MATCH (p:P) WITH p ORDER BY p.name DESC SET p.sorted = true").close();
    });
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
    assertThat(countRows("opencypher", "MATCH (p:P) WHERE p.touched AND p.sorted RETURN p")).isEqualTo(ROWS);
  }

  @Test
  void aWriteQueryRefusedHeapRollsBack() {
    final QueryHeapTracker others = holdAllBut(MB / 2);
    try {
      assertThatThrownBy(() -> database.transaction(
          () -> database.command("opencypher", "MATCH (p:P) WITH p ORDER BY p.name SET p.sorted = true").close()))
          .isInstanceOf(QueryHeapBudgetExceededException.class);
    } finally {
      others.close();
    }
    assertThat(countRows("opencypher", "MATCH (p:P) WHERE p.sorted RETURN p")).isZero();
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
  }

  @Test
  void anEagerForeachHoldsItsRowsUnderTheBudget() {
    // A FOREACH followed by a read holds every input row until its last write is applied (issue #6922)
    final String query = "MATCH (p:P) FOREACH (x IN [1] | SET p.f = x) WITH p CALL meta.stats() YIELD value AS stats "
        + "RETURN count(*) AS c";
    final QueryHeapTracker others = holdAllBut(MB / 2);
    try {
      assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", query).close()))
          .isInstanceOf(QueryHeapBudgetExceededException.class);
    } finally {
      others.close();
    }
    assertThat(countRows("opencypher", "MATCH (p:P) WHERE p.f = 1 RETURN p")).as("rolled back").isZero();

    database.transaction(() -> database.command("opencypher", query).close());
    assertThat(countRows("opencypher", "MATCH (p:P) WHERE p.f = 1 RETURN p")).isEqualTo(ROWS);
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
  }

  @Test
  void aCompactingJoinBufferAdjustsOnlyItsOwnShareOfTheOperation() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);
    final OperationHeapLimit limit = OperationHeapLimit.of(context, "hash join");
    // THE HASH TABLE OVER THE ROWS, CHARGED TO THE SAME OPERATION AS THE BUFFER
    limit.charge(MB);

    final RowBuffer buffer = new RowBuffer(database, limit, 5);
    try (final ResultSet rs = database.query("opencypher", "MATCH (q:Q) RETURN q LIMIT 10")) {
      while (rs.hasNext()) {
        final ResultInternal row = new ResultInternal();
        row.setProperty("q", rs.next().getProperty("q"));
        buffer.add(row);
      }
    }
    assertThat(buffer.isCompact()).isTrue();
    assertThat(limit.getChargedBytes()).as("the compaction kept the table's charge").isEqualTo(MB + 10L * 12);

    buffer.clear();
    assertThat(limit.getChargedBytes()).as("the buffer gave back its own share only").isEqualTo(MB);
    limit.release();
  }

  @Test
  void aTopNSortChargesTheRowsItKeepsNotAnAverageOfTheOnesItSaw() {
    // The 10 rows kept take 2MB; a share of the charge proportional to their number, or the average row size of the
    // 2010 rows seen, would count a fraction of it
    final String query = "SELECT FROM Big ORDER BY size DESC LIMIT 10";
    try (final ResultSet rs = database.query("sql", query)) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(QueryHeapBudget.getReservedBytes() - baseline).isGreaterThanOrEqualTo(10L * BIG_PAYLOAD - QueryHeapTracker.UNRESERVED_BYTES);
      long rows = 0;
      while (rs.hasNext()) {
        assertThat(rs.next().<Integer>getProperty("size")).isGreaterThanOrEqualTo(1_000_000);
        ++rows;
      }
      assertThat(rows).isEqualTo(10);
    }
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
  }

  @Test
  void concurrentQueriesCannotHoldMoreThanTheBudget() throws Exception {
    final String query = "SELECT FROM Doc ORDER BY name";
    final long single = measureReservation("sql", query);
    assertThat(single).as("the query buffers past the unreserved allowance").isGreaterThan(MB);

    // ROOM FOR TWO OF THEM AT ONCE, NOT FOR SIX
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue((baseline + 5 * single / 2) / MB + 1);

    final int threads = 6;
    final CountDownLatch holding = new CountDownLatch(threads);
    final AtomicInteger completed = new AtomicInteger();
    final AtomicInteger refused = new AtomicInteger();
    final ConcurrentLinkedQueue<Throwable> unexpected = new ConcurrentLinkedQueue<>();
    final Thread[] workers = new Thread[threads];
    for (int t = 0; t < threads; t++) {
      workers[t] = new Thread(() -> {
        boolean counted = false;
        try (final ResultSet rs = database.query("sql", query)) {
          // THE FIRST ROW SORTS THE WHOLE BUFFER: EVERY QUERY THAT GOT ITS SHARE HOLDS IT UNTIL ALL OF THEM TRIED
          rs.hasNext();
          holding.countDown();
          counted = true;
          holding.await(30, TimeUnit.SECONDS);
          long rows = 0;
          while (rs.hasNext()) {
            rs.next();
            ++rows;
          }
          if (rows == ROWS)
            completed.incrementAndGet();
        } catch (final QueryHeapBudgetExceededException e) {
          refused.incrementAndGet();
        } catch (final Throwable e) {
          unexpected.add(e);
        } finally {
          if (!counted)
            holding.countDown();
        }
      });
      workers[t].start();
    }
    for (final Thread worker : workers) {
      worker.join(60_000);
      assertThat(worker.isAlive()).as("the worker finished").isFalse();
    }

    assertThat(unexpected).isEmpty();
    assertThat(completed.get() + refused.get()).isEqualTo(threads);
    assertThat(completed.get()).isBetween(1, 2);
    assertThat(refused.get()).isGreaterThanOrEqualTo(threads - 2);
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
  }

  private void assertRefusedWhileOthersHoldTheBudget(final String language, final String query) {
    final QueryHeapTracker others = holdAllBut(MB / 2);
    try {
      assertThatThrownBy(() -> countRows(language, query))
          .as(query)
          .isInstanceOf(QueryHeapBudgetExceededException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
      assertThat(QueryHeapBudget.getReservedBytes()).as("the refused query holds nothing: " + query)
          .isLessThanOrEqualTo(baseline + others.getReservedBytes());
    } finally {
      others.close();
    }
    assertThat(countRows(language, query)).as("served once the others released: " + query).isPositive();
    assertThat(QueryHeapBudget.getReservedBytes()).as(query).isLessThanOrEqualTo(baseline);
  }

  private void assertHeldWhileServedAndGivenBackAtTheEnd(final String language, final String query) {
    try (final ResultSet rs = database.query(language, query)) {
      // A SORT HOLDS ITS BUFFER FROM THE FIRST ROW, A DISTINCT OR A PRODUCT FILLS IT AS THE ROWS GO
      long mostReserved = 0L;
      while (rs.hasNext()) {
        rs.next();
        mostReserved = Math.max(mostReserved, QueryHeapBudget.getReservedBytes());
      }
      assertThat(mostReserved).as("the buffer was charged while its rows were served: " + query).isGreaterThan(baseline);
      assertThat(QueryHeapBudget.getReservedBytes()).as("every row was served, the result set is still open: " + query)
          .isLessThanOrEqualTo(baseline);
    }
  }

  private void assertGivenBackOnClose(final String language, final String query) {
    final ResultSet rs = database.query(language, query);
    while (QueryHeapBudget.getReservedBytes() <= baseline) {
      assertThat(rs.hasNext()).as("the buffer is charged before the last row: " + query).isTrue();
      rs.next();
    }
    rs.close();
    assertThat(QueryHeapBudget.getReservedBytes()).as(query).isLessThanOrEqualTo(baseline);
  }

  private void readOneRowAndAbandon(final String language, final String query) {
    final ResultSet rs = database.query(language, query);
    rs.next();
  }

  /** What one run of {@code query} holds reserved once its buffer is filled. */
  private long measureReservation(final String language, final String query) {
    try (final ResultSet rs = database.query(language, query)) {
      rs.hasNext();
      return QueryHeapBudget.getReservedBytes() - baseline;
    }
  }

  /**
   * A tracker standing for the other running queries, holding all the budget but {@code freeBytes}. It shrinks the
   * JVM-wide {@code QUERY_MAX_HEAP_RAM}, which {@code TestHelper.afterTest()} puts back with every other setting.
   */
  private QueryHeapTracker holdAllBut(final long freeBytes) {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(baseline / MB + 16);
    final QueryHeapTracker others = new QueryHeapTracker();
    others.charge(QueryHeapTracker.UNRESERVED_BYTES + QueryHeapBudget.getLimitBytes() - QueryHeapBudget.getReservedBytes() - freeBytes,
        "other queries");
    return others;
  }

  private long countRows(final String language, final String query) {
    long rows = 0;
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
    }
    return rows;
  }

  /** The reservations once the ones held by trackers nobody references anymore were given back. */
  private static long settledReservedBytes() {
    long current = QueryHeapBudget.getReservedBytes();
    for (int i = 0; i < 20 && current > 0; i++) {
      final long previous = current;
      System.gc();
      try {
        Thread.sleep(20);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      current = QueryHeapBudget.getReservedBytes();
      if (current == previous && i > 3)
        break;
    }
    return current;
  }
}
