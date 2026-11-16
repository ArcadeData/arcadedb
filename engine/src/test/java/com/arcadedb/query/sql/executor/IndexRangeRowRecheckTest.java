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
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11790: a SQL range lookup on an index loads each record when it reaches the record's entry. A commit that moved the record
 * to a key outside the range after the lookup started must not make the statement return it: a point lookup already checks the
 * record it loads against its key (#9902), a range lookup served it as loaded.
 */
class IndexRangeRowRecheckTest extends TestHelper {
  private static final int RECORDS = 5_000;

  @Test
  void aRangeLookupDoesNotReturnARecordACommitMovedOutOfTheRange() throws Exception {
    database.transaction(() -> {
      final var type = database.getSchema().createDocumentType("T");
      type.createProperty("v", Type.INTEGER);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "v");
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("T").set("k", i).set("v", i % 10).save();
    });

    final List<Object> values = new ArrayList<>();
    try (final ResultSet rows = database.query("sql", "SELECT k, v FROM T WHERE v >= 3 AND v < 5")) {
      values.add(rows.next().getProperty("v"));

      // Another transaction moves every record of the range out of it, while the statement is half way through its entries
      final FutureTask<Void> move = new FutureTask<>(() -> {
        database.transaction(() -> database.command("sql", "UPDATE T SET v = 100 WHERE v >= 3 AND v < 5"));
        return null;
      });
      new Thread(move).start();
      move.get(60, TimeUnit.SECONDS);

      while (rows.hasNext())
        values.add(rows.next().getProperty("v"));
    }

    assertThat(values).as("every row a statement returns satisfies its WHERE").allMatch(v -> v instanceof Integer i && i >= 3 && i < 5);
  }

  /**
   * The same stale entries fed a DELETE with a range WHERE: it loaded the record a commit had moved out of the range and deleted
   * it, with no conflict to stop it since it deleted the version it had just read.
   */
  @Test
  void aRangeDeleteDoesNotDeleteARecordACommitMovedOutOfTheRange() throws Exception {
    database.transaction(() -> {
      final var type = database.getSchema().createDocumentType("T");
      type.createProperty("v", Type.INTEGER);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "v");
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("T").set("k", i).set("v", i % 10).save();
    });

    // On the first record the DELETE removes, another transaction moves every OTHER record of the range out of it and commits (the
    // record being deleted is left alone: a concurrent change to it is the version conflict of another issue, #11733)
    final boolean[] moved = { false };
    final BeforeRecordDeleteListener moveOnce = record -> {
      if (!moved[0]) {
        moved[0] = true;
        final FutureTask<Void> move = new FutureTask<>(() -> {
          database.transaction(() -> database.command("sql", "UPDATE T SET v = 100 WHERE v >= 3 AND v < 5 AND @rid <> ?",
              record.getIdentity()));
          return null;
        });
        new Thread(move).start();
        try {
          move.get(60, TimeUnit.SECONDS);
        } catch (final Exception e) {
          throw new IllegalStateException(e);
        }
      }
      return true;
    };
    database.getEvents().registerListener(moveOnce);
    try {
      database.transaction(() -> database.command("sql", "DELETE FROM T WHERE v >= 3 AND v < 5"));
    } finally {
      database.getEvents().unregisterListener(moveOnce);
    }

    try (final ResultSet rows = database.query("sql", "SELECT count(*) AS c FROM T WHERE v = 100")) {
      assertThat(rows.next().<Number>getProperty("c").longValue())
          .as("the records moved out of the range before the DELETE reached them are not deleted").isEqualTo(RECORDS / 5 - 1);
    }
  }

  /**
   * A range sorted on another property is served in physical order: the step reads every RID of the range first, then loads the
   * records. A commit landing between the two must not make the load serve a record it moved out of the range.
   */
  @Test
  void aPhysicalOrderLoadDoesNotReturnARecordACommitMovedOutOfTheRange() throws Exception {
    createFixture(RECORDS, 10, 0);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    try {
      final RangeRun run = runWithAMoveAtTheFirstLoad();
      assertThat(run.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.PHYSICAL_ORDER);
      assertRowsInRange(run, RECORDS / 5);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  /** The same for a range too large to buffer at once, loaded chunk by chunk. */
  @Test
  void aChunkedPhysicalOrderLoadDoesNotReturnARecordACommitMovedOutOfTheRange() throws Exception {
    createFixture(RECORDS, 10, 0);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    PhysicalOrderRidFetcher.maxBufferedRids = 100;
    try {
      final RangeRun run = runWithAMoveAtTheFirstLoad();
      assertThat(run.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.PHYSICAL_ORDER_CHUNKED);
      assertRowsInRange(run, RECORDS / 5);
    } finally {
      PhysicalOrderRidFetcher.maxBufferedRids = 1 << 20;
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  /**
   * The same when the workers of a parallel scan load the slices of the range, one chunk or several. A worker reads a page of
   * records at once, so the records are large enough to spread a slice over several pages, which the worker that met the first
   * record reads after the commit, and the range is selective enough to be loaded in physical order on many workers.
   */
  @Test
  void aParallelPhysicalOrderLoadDoesNotReturnARecordACommitMovedOutOfTheRange() throws Exception {
    final int pagesPerUnit = database.getConfiguration().getValueAsInteger(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 1);
    try {
      for (final int bufferCap : new int[] { 1 << 20, 64 }) {
        PhysicalOrderRidFetcher.maxBufferedRids = bufferCap;
        createFixture(20_000, 200, 1_000);
        final RangeRun run = runWithAMoveAtTheFirstLoad();
        assertThat(run.plan()).as("buffer of %d RIDs", bufferCap).contains("loaded in parallel");
        assertRowsInRange(run, 200);
        database.getSchema().dropType("T");
      }
    } finally {
      PhysicalOrderRidFetcher.maxBufferedRids = 1 << 20;
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, pagesPerUnit);
    }
  }

  private record RangeRun(List<Object> values, GetValueFromIndexEntryStep.Strategy strategy, String plan) {
  }

  /**
   * Type T with an index on {@code v}: {@code records} documents, {@code v} cycling through {@code 0..modulo-1}, each padded with
   * {@code padding} characters. The range {@code v >= 3 AND v < 5} holds two records of every {@code modulo}.
   */
  private void createFixture(final int records, final int modulo, final int padding) {
    final String pad = "x".repeat(padding);
    database.transaction(() -> {
      final var type = database.getSchema().createDocumentType("T");
      type.createProperty("v", Type.INTEGER);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "v");
      for (int i = 0; i < records; i++)
        database.newDocument("T").set("k", i).set("v", i % modulo).set("pad", pad).save();
    });
  }

  /**
   * Runs a range query sorted on another property, the shape served in physical order. When it loads its first record, another
   * transaction moves every other record of the range out of it and commits.
   */
  private RangeRun runWithAMoveAtTheFirstLoad() {
    final AtomicBoolean moved = new AtomicBoolean();
    final AfterRecordReadListener moveOnce = record -> {
      if (moved.compareAndSet(false, true)) {
        final FutureTask<Void> move = new FutureTask<>(() -> {
          database.transaction(() -> database.command("sql", "UPDATE T SET v = 100 WHERE v >= 3 AND v < 5 AND @rid <> ?",
              record.getIdentity()));
          return null;
        });
        new Thread(move).start();
        try {
          move.get(60, TimeUnit.SECONDS);
        } catch (final Exception e) {
          throw new IllegalStateException(e);
        }
      }
      return record;
    };
    final List<Object> values = new ArrayList<>();
    database.getEvents().registerListener(moveOnce);
    try (final ResultSet rows = database.query("sql", "SELECT k, v FROM T WHERE v >= 3 AND v < 5 ORDER BY k")) {
      while (rows.hasNext())
        values.add(rows.next().getProperty("v"));
      final ExecutionPlan plan = rows.getExecutionPlan().get();
      GetValueFromIndexEntryStep step = null;
      for (final ExecutionStep candidate : plan.getSteps())
        if (candidate instanceof GetValueFromIndexEntryStep found)
          step = found;
      assertThat(step).as(plan.prettyPrint(0, 2)).isNotNull();
      return new RangeRun(values, step.getStrategy(), plan.prettyPrint(0, 2));
    } finally {
      database.getEvents().unregisterListener(moveOnce);
      assertThat(moved.get()).as("the commit landed while the range was loaded").isTrue();
    }
  }

  private static void assertRowsInRange(final RangeRun run, final int rangeRows) {
    assertThat(run.values()).as("a record a commit moved out of the range is not returned").hasSizeLessThan(rangeRows);
    assertThat(run.values()).as("every row a statement returns satisfies its WHERE")
        .allMatch(v -> v instanceof Integer i && i >= 3 && i < 5);
  }
}
