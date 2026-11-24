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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageVersionReservations;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12470: on a replicated leader, the page-version ledger reserves the header page of a shard
 * (page 0 of its mutable bucket) from the moment the leader validates an entry of another node until it applies it, which
 * takes a Raft round trip. A compaction commits that page twice, in Phase 0 (sets the compaction flag) and in Phase 4c
 * (clears it with the mutable pages), and every check of it found the reservation: Phase 0 retried three times back to
 * back and gave up with "Compaction failed in phase 0 after retries", Phase 4c did not retry at all and threw away the
 * whole compaction ("Compaction failed in phase 4c"), sealed blocks included.
 * <p>
 * The reservation is simulated through {@link PageVersionReservations}, the hook the replication layer installs, so the
 * engine can be driven without a cluster: the header page stays reserved for a number of consultations, then is released,
 * which is what the apply of the entry does. Counting consultations instead of milliseconds keeps the test off the clock.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesCompactionReservedHeaderPageTest extends TestHelper {
  private static final String TYPE = "Issue12470Ts";
  private static final int    ROWS = 300;

  /** Far more than the three back-to-back attempts of Phase 0 can consume, and well under the wait bound at its poll rate. */
  private static final int RESERVED_CONSULTATIONS = 15;

  @AfterEach
  void clearHooks() {
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = null;
    ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).setPageVersionReservations(null);
  }

  // #12470
  @Test
  void phase0WaitsForAnEntryInFlightOnTheHeaderPageInsteadOfRetryingAtOnce() throws Exception {
    final TimeSeriesShard shard = createTypeWithSamples();
    final AtomicInteger consultations = new AtomicInteger();
    reserveHeaderPage(shard, new AtomicBoolean(true), consultations);

    engine().compactAll();

    assertThat(consultations.get()).as("the header page was consulted until the entry was applied").isGreaterThan(RESERVED_CONSULTATIONS);
    assertCompacted(shard);
  }

  // #12470
  @Test
  void phase4cWaitsForAnEntryInFlightOnTheHeaderPageInsteadOfThrowingTheCompactionAway() throws Exception {
    final TimeSeriesShard shard = createTypeWithSamples();
    final AtomicBoolean reserved = new AtomicBoolean();
    final AtomicInteger consultations = new AtomicInteger();
    reserveHeaderPage(shard, reserved, consultations);
    // Phase 0 has committed by the time this runs: an entry of another node is validated now, before the swap and the clear
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = () -> reserved.set(true);

    engine().compactAll();

    assertThat(consultations.get()).as("the header page was consulted until the entry was applied").isGreaterThan(RESERVED_CONSULTATIONS);
    assertCompacted(shard);
  }

  // #12470: the delete of samples commits the same page, and ran its three attempts back to back as well
  @Test
  void aDeleteOfSamplesWaitsForAnEntryInFlightOnTheHeaderPage() throws Exception {
    final TimeSeriesShard shard = createTypeWithSamples();
    final AtomicInteger consultations = new AtomicInteger();
    reserveHeaderPage(shard, new AtomicBoolean(true), consultations);

    final long deleted = engine().deleteSamples(1_700_000_000_000L, 1_700_000_000_000L + 9, null);

    assertThat(deleted).isEqualTo(10L);
    assertThat(consultations.get()).as("the header page was consulted until the entry was applied").isGreaterThan(RESERVED_CONSULTATIONS);
    assertThat(countRows()).isEqualTo(ROWS - 10);
  }

  // #12470
  @Test
  @Tag("slow")
  void aCompactionPassStillGivesUpOnAnEntryThatIsNeverApplied() throws Exception {
    final TimeSeriesShard shard = createTypeWithSamples();
    final AtomicBoolean reserved = new AtomicBoolean(true);
    final AtomicInteger consultations = new AtomicInteger();
    // Never released: a dropped request that nothing confirms is swept by the ledger only after a long while
    reserveHeaderPage(shard, reserved, consultations, Integer.MAX_VALUE);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> engine().compactAll()).isInstanceOf(IOException.class).hasMessageContaining("phase 0");
    stopwatch.assertGaveUpWithin(60_000, "a bounded wait for an entry that never lands from one that waits for ever");

    // Nothing was published and nothing was lost: the flag is not left set and every sample is still there once
    assertThat(shard.getMutableBucket().isCompactionInProgress()).isFalse();
    assertThat(shard.getSealedStore().getBlockCount()).isZero();
    assertThat(countRows()).isEqualTo(ROWS);

    // The entry lands: the next pass publishes
    reserved.set(false);
    engine().compactAll();
    assertCompacted(shard);
  }

  private TimeSeriesShard createTypeWithSamples() {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    // The scheduler compacts a new type after a few seconds: it would empty the mutable bucket under the test
    database.getSchema().getEmbedded().getTimeSeriesMaintenanceScheduler().cancel(TYPE);
    for (int i = 0; i < ROWS; i++) {
      final long ts = 1_700_000_000_000L + i;
      final double v = i;
      database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET ts = ?, host = ?, v = ?", ts, "h0", v));
    }
    assertThat(countRows()).isEqualTo(ROWS);
    return engine().getShard(0);
  }

  /**
   * Reserves the header page of the shard's mutable bucket above its local version while {@code reserved} is set, for
   * {@link #RESERVED_CONSULTATIONS} consultations, as the ledger of a leader does between the validation of an entry of
   * another node and its apply.
   */
  private void reserveHeaderPage(final TimeSeriesShard shard, final AtomicBoolean reserved, final AtomicInteger consultations) {
    reserveHeaderPage(shard, reserved, consultations, RESERVED_CONSULTATIONS);
  }

  private void reserveHeaderPage(final TimeSeriesShard shard, final AtomicBoolean reserved, final AtomicInteger consultations,
      final int heldFor) {
    final PageId headerPage = new PageId(database, shard.getMutableBucket().getFileId(), 0);
    ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).setPageVersionReservations(pageId -> {
      if (!reserved.get() || !pageId.equals(headerPage))
        return -1;
      return consultations.incrementAndGet() <= heldFor ? Integer.MAX_VALUE : -1;
    });
  }

  private void assertCompacted(final TimeSeriesShard shard) throws IOException {
    assertThat(shard.getSealedStore().getBlockCount()).as("the samples were sealed").isGreaterThan(0);
    database.begin();
    try {
      assertThat(shard.getMutableBucket().getDataPageCount()).as("the mutable bucket was cleared").isZero();
      assertThat(shard.getMutableBucket().isCompactionInProgress()).as("the compaction flag was reset").isFalse();
    } finally {
      database.rollback();
    }
    assertThat(countRows()).as("every sample is there once").isEqualTo(ROWS);
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType(TYPE)).getEngine();
  }

  private long countRows() {
    // count(), not count(*): the rows are counted, not a stored counter
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + TYPE)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
