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
package com.arcadedb.graph;

import com.arcadedb.Profiler;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10100: when the census of a list cannot count it, every lightweight edge of that list walks the list itself,
 * which is the per-vertex quadratic cost the census exists to remove. That fallback was reported by one WARNING per JVM,
 * taken by the first failure anywhere (any database, any list) and followed by FINE lines for the rest of the process,
 * with no counter: after the first one, a persistent failure was invisible.
 * <p>
 * Its sibling {@code ContractionHierarchy.onEdgeTypesMismatch} counts the same kind of fallback and logs a bounded number
 * of distinct causes. The census now does too: a counter per database, read from {@code Database.getStats()} and the
 * profiler, and one WARNING per (database, list, direction), at most {@code 64} of them per database.
 * <p>
 * A census is made to fail the way {@code LightEdgeCensusTest} does it: its owner is a record that does not exist.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10100LightEdgeCensusFailureCountedTest extends TestHelper {
  private static final String STAT = "lightEdgeCensusFallbacks";
  private static final int    MAX_REPORTED_LISTS = 64;

  private Logger          previousLogger;
  private CapturingLogger captured;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
  }

  @BeforeEach
  void installCapturingLogger() {
    previousLogger = LogManager.instance().getLogger();
    captured = new CapturingLogger();
    LogManager.instance().setLogger(captured);
  }

  @AfterEach
  void restoreLogger() {
    LogManager.instance().setLogger(previousLogger);
  }

  @Test
  void everyCensusThatCannotCountItsListIsCounted() {
    final long before = fallbacks(database);
    final long profilerBefore = profilerTotal();

    fail(database, missingOwner(database, 1), Vertex.DIRECTION.OUT);
    fail(database, missingOwner(database, 2), Vertex.DIRECTION.OUT);
    fail(database, missingOwner(database, 2), Vertex.DIRECTION.IN);

    assertThat(fallbacks(database)).as("the number is the failure, not the log line").isEqualTo(before + 3);
    assertThat(profilerTotal()).as("and the profiler exports the same number").isEqualTo(profilerBefore + 3);
  }

  @Test
  void aCensusThatSucceedsOrIsNeverAskedTwiceCountsNothing() {
    final long before = fallbacks(database);

    // ASKED ONCE: THE CENSUS DECLINES WITHOUT COUNTING THE LIST, SO IT CANNOT FAIL
    final LightEdgeCensus askedOnce = new LightEdgeCensus((DatabaseInternal) database, missingOwner(database, 3),
        Vertex.DIRECTION.OUT);
    assertThat(askedOnce.resolve(null, null, 0, 0)).isFalse();

    assertThat(fallbacks(database)).isEqualTo(before);
    assertThat(captured.censusRecords).isEmpty();
  }

  @Test
  void theFirstFailureOfEachListIsAWarningAndItsRepeatsAreNot() {
    final RID first = missingOwner(database, 10);
    final RID second = missingOwner(database, 11);

    fail(database, first, Vertex.DIRECTION.OUT);
    // ANOTHER ITERATION OVER THE SAME LIST BUILDS ANOTHER CENSUS: SAME LIST, SO NOT A SECOND WARNING
    fail(database, first, Vertex.DIRECTION.OUT);
    // ANOTHER LIST OF ANOTHER VERTEX, AND THE OTHER DIRECTION OF THE SAME VERTEX, EACH GET THEIR OWN LINE
    fail(database, second, Vertex.DIRECTION.OUT);
    fail(database, first, Vertex.DIRECTION.IN);

    assertThat(captured.censusRecords(Level.WARNING)).hasSize(3);
    assertThat(captured.censusRecords(Level.FINE)).hasSize(1);
  }

  @Test
  void aFailureInAnotherDatabaseIsNotSilencedByTheFirstOne() throws Exception {
    final RID owner = missingOwner(database, 20);
    fail(database, owner, Vertex.DIRECTION.OUT);
    assertThat(captured.censusRecords(Level.WARNING)).hasSize(1);

    executeInNewDatabase("Issue10100OtherDatabase", other -> {
      other.command("sql", "CREATE VERTEX TYPE P");
      final long before = fallbacks(other);

      fail(other, missingOwner(other, 20), Vertex.DIRECTION.OUT);

      assertThat(fallbacks(other)).isEqualTo(before + 1);
      assertThat(captured.censusRecords(Level.WARNING)).as("one WARNING per database, not one per JVM").hasSize(2);
    });
  }

  @Test
  void theNumberOfReportedListsIsBoundedButTheCounterIsNot() {
    final long before = fallbacks(database);
    final int lists = MAX_REPORTED_LISTS + 36;

    for (int i = 0; i < lists; i++)
      fail(database, missingOwner(database, 1_000 + i), Vertex.DIRECTION.OUT);

    assertThat(fallbacks(database)).isEqualTo(before + lists);
    assertThat(captured.censusRecords(Level.WARNING)).hasSize(MAX_REPORTED_LISTS);
    assertThat(captured.censusRecords(Level.FINE)).hasSize(lists - MAX_REPORTED_LISTS);
  }

  @Test
  void theBudgetOfReportedListsIsExactWhenManyThreadsFail() throws Exception {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final long before = fallbacks(database);
    final int threads = 8;
    final int listsPerThread = 50;
    final List<RID> owners = new ArrayList<>();
    for (int i = 0; i < threads * listsPerThread; i++)
      owners.add(missingOwner(database, 5_000 + i));

    final AtomicInteger reported = new AtomicInteger();
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final int first = t * listsPerThread;
        futures.add(pool.submit(() -> {
          for (int i = first; i < first + listsPerThread; i++)
            if (local.countLightEdgeCensusFallback(owners.get(i), Vertex.DIRECTION.OUT))
              reported.incrementAndGet();
        }));
      }
      for (final Future<?> future : futures)
        future.get(60, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }

    assertThat(reported.get()).as("a check-then-add would overshoot the budget when threads race on the last slots")
        .isEqualTo(MAX_REPORTED_LISTS);
    assertThat(fallbacks(database)).isEqualTo(before + threads * listsPerThread);
  }

  /** Two questions: the census declines the first and builds on the second, and the build fails on a missing owner. */
  private static void fail(final Database db, final RID owner, final Vertex.DIRECTION direction) {
    final LightEdgeCensus census = new LightEdgeCensus((DatabaseInternal) db, owner, direction);
    // THE EDGE IS NEVER LOOKED AT BEFORE THE BUILD ANSWERS, AND A FAILED BUILD ANSWERS "DECLINED"
    assertThat(census.resolve(null, null, 0, 0)).isFalse();
    assertThat(census.resolve(null, null, 0, 0)).isFalse();
    assertThat(census.hasFailed()).isTrue();
  }

  private static RID missingOwner(final Database db, final long position) {
    return db.newRID(db.getSchema().getType("P").getBuckets(false).getFirst().getFileId(), 1_000_000L + position);
  }

  private static long fallbacks(final Database db) {
    return ((Number) ((DatabaseInternal) db).getStats().get(STAT)).longValue();
  }

  private static long profilerTotal() {
    return Profiler.INSTANCE.toJSON().getJSONObject(STAT).getLong("count", -1L);
  }

  /** Accepts every level and keeps the level and the format string of the census' records. */
  private static final class CapturingLogger implements Logger {
    final List<String> censusRecords = new CopyOnWriteArrayList<>();

    List<String> censusRecords(final Level level) {
      return censusRecords.stream().filter(r -> r.startsWith(level + ":")).toList();
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
        final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
        final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
        final Object arg15, final Object arg16, final Object arg17) {
      capture(requester, level, message);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(requester, level, message);
    }

    private void capture(final Object requester, final Level level, final String message) {
      if (requester instanceof LightEdgeCensus && message != null && message.startsWith("Cannot count the lightweight edges"))
        censusRecords.add(level + ":" + message);
    }

    @Override
    public void flush() {
      // NOTHING TO FLUSH
    }
  }
}
