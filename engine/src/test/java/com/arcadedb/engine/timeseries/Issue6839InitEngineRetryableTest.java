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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Issue #6839: {@code initEngine()} has to be retryable, because since #6356 it is the ONLY way a type registered
 * without a storage engine ever gets one - the HA sealed-blob repair path drives exactly this call.
 * <p>
 * What stood in the way was a cleanup that looked like hygiene. Three places closed the shard's mutable bucket
 * when something else failed to open: {@code TimeSeriesShard}'s sealed-store construction, its crash-recovery
 * block, and {@code TimeSeriesEngine}'s unwind of the shards it had already built. The bucket is registered with
 * the schema on every branch that reaches them, so the schema owns closing it and none of those closes ever
 * prevented a leak - but {@code PaginatedComponentFile.close()} sets {@code open=false} and its own lazy reopen
 * then refuses ("was closed on purpose") for the life of the process. So the failed attempt destroyed the thing
 * the next attempt needs, and the retry came back reporting {@code isEngineAvailable() == true} over a component
 * that throws on every page it touches. Harmless while a failed {@code initEngine()} meant the type vanished from
 * the schema; the whole obstacle to recovery once #6356 made it stay.
 * <p>
 * This class pins the CRASH-RECOVERY door, the one a repair is most likely to meet: a {@code .ts.sealed} is most
 * often damaged by a crash during compaction, which is precisely when {@code isCompactionInProgress()} is true on
 * reopen, so the retry that installs a good sealed file runs straight into this block. The sibling doors are
 * pinned by {@code Issue6839TsSealedBlobRecoveryTest} (sealed-store construction, and the multi-shard unwind).
 * <p>
 * The failure is injected with a BROKEN SYMLINK at the {@code .ts.sealed.tmp} path {@code truncateToBlockCount}
 * writes through: {@code File.exists()} follows the link and answers false, so {@code TimeSeriesSealedStore}'s
 * constructor skips its own stale-tmp cleanup and opens normally, and then
 * {@code new RandomAccessFile(tempPath, "rw")} inside the rewrite resolves the link to a directory that is not
 * there and throws. Deterministic, and it needs no test hook in production code. A plain directory does not work
 * for this: the constructor deletes an empty one before the shard ever reaches crash recovery, and throws on a
 * non-empty one at construction, which is the OTHER door and is already covered.
 * <p>
 * Note also that a watermark merely disagreeing with the block count does NOT throw - {@code truncateToBlockCount}
 * returns early when the target is at or above the directory size - so the watermark below is deliberately BELOW
 * the block count, to get past that guard and reach the rewrite.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6839InitEngineRetryableTest extends TestHelper {

  private static final String TYPE_NAME = "Cpu";
  private static final int    ROWS      = 3_000;

  /** One of the three tests deliberately leaves a type with no engine behind. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Test
  void initEngineRecoversAfterCrashRecoveryFailedOnAPreviousAttempt() throws Exception {
    createTypeAndCompact();

    // A crash mid-compaction, in the state it leaves behind: the flag set, and a watermark BELOW the sealed
    // store's block count so recovery has real truncation work to do rather than returning early.
    final long blocks = engineOf().getShard(0).getSealedStore().getBlockCount();
    assertThat(blocks).isGreaterThan(0L);
    database.transaction(() -> {
      try {
        engineOf().getShard(0).getMutableBucket().setCompactionInProgress(true);
        engineOf().getShard(0).getMutableBucket().setCompactionWatermark(blocks - 1);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });

    final Path blocker = new File(getDatabasePath(), TYPE_NAME + "_shard_0.ts.sealed.tmp").toPath();
    database.close();
    try {
      createBrokenSymlink(blocker);
    } finally {
      database = factory.open();
    }

    final LocalTimeSeriesType broken = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    assertThat(broken.isEngineAvailable()).as("crash recovery must have failed, or this test proves nothing")
        .isFalse();

    // The repair: whatever was blocking the rewrite is gone. This stands in for the HA path installing a good
    // sealed file - what matters here is only that the SECOND initEngine() has a reason to succeed.
    Files.delete(blocker);

    broken.initEngine();

    assertThat(broken.isEngineAvailable()).as("the retry must be able to succeed").isTrue();
    assertThat(broken.getEngineUnavailableReason()).isNull();

    // The point of the whole exercise: the mutable bucket the failed attempt used to close is still usable. The
    // flag above flips to true either way - only touching the file distinguishes a real recovery from a component
    // that will throw on the first page it reads. Asserted as a DELTA rather than a total, because the recovery
    // this test provoked legitimately truncates the sealed store to the watermark and how many samples that
    // discards is beside the point: what has to hold is that the bucket still reads and still takes writes.
    final long afterRecovery = countSamples();
    appendRows(broken.getEngine(), ROWS, 100);
    assertThat(countSamples()).isEqualTo(afterRecovery + 100L);
  }

  /** With nothing blocking it, the same crash-recovery state simply recovers on the first open, as it always did. */
  @Test
  void anUnobstructedCrashRecoveryStillRunsOnTheFirstOpen() throws Exception {
    createTypeAndCompact();

    final long blocks = engineOf().getShard(0).getSealedStore().getBlockCount();
    database.transaction(() -> {
      try {
        engineOf().getShard(0).getMutableBucket().setCompactionInProgress(true);
        engineOf().getShard(0).getMutableBucket().setCompactionWatermark(blocks - 1);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });

    database.close();
    database = factory.open();

    final LocalTimeSeriesType reopened = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    assertThat(reopened.isEngineAvailable()).isTrue();
    assertThat(reopened.getEngine().getShard(0).getMutableBucket().isCompactionInProgress()).isFalse();
    assertThat(reopened.getEngine().getShard(0).getSealedStore().getBlockCount()).isEqualTo(blocks - 1);
  }

  /** A second {@code initEngine()} on a healthy type is a no-op, not a second engine over the same files. */
  @Test
  void initEngineOnAHealthyTypeIsANoOp() throws Exception {
    createTypeAndCompact();

    final LocalTimeSeriesType healthy = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    final TimeSeriesEngine before = healthy.getEngine();

    healthy.initEngine();

    assertThat(healthy.getEngine()).isSameAs(before);
    assertThat(countSamples()).isEqualTo(ROWS);
  }

  // ---- Helpers ----

  /**
   * Skips the test rather than failing it where symbolic links cannot be created (Windows without the privilege).
   * The production fix is platform-independent; only this injection is not.
   */
  private static void createBrokenSymlink(final Path link) {
    try {
      Files.createSymbolicLink(link, link.resolveSibling("no-such-directory-6839").resolve("target"));
    } catch (final IOException | UnsupportedOperationException e) {
      assumeTrue(false, "symbolic links are not available here: " + e.getMessage());
    }
    assertThat(Files.exists(link)).as("a BROKEN link, so the sealed store's stale-tmp cleanup skips it").isFalse();
  }

  private void createTypeAndCompact() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME
        + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = engineOf();
    appendRows(engine, 0, ROWS);
    engine.compactAll();
  }

  private TimeSeriesEngine engineOf() {
    return ((LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME)).getEngine();
  }

  private void appendRows(final TimeSeriesEngine engine, final int from, final int rows) throws IOException {
    final long[] timestamps = new long[rows];
    final Object[][] columns = new Object[2][rows];
    for (int i = 0; i < rows; i++) {
      timestamps[i] = 1_700_000_000_000L + (from + i) * 1_000L;
      columns[0][i] = "host_" + ((from + i) % 7);
      columns[1][i] = (double) (from + i);
    }
    engine.appendBatch(timestamps, columns);
  }

  private long countSamples() {
    try (final ResultSet resultSet = database.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME)) {
      return resultSet.next().<Number>getProperty("cnt").longValue();
    }
  }
}
