/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4832.
 * <p>
 * {@link SnapshotInstaller#atomicSwap} moves files entry-by-entry, so the on-disk database directory is
 * transiently a mix of old-removed and new-installed files. The {@code setSnapshotInstallInProgress} flag
 * deflects HTTP clients with a 503 during that window, but the engine-internal open paths
 * (reconcile / apply / health-monitor threads calling {@link ArcadeDBServer#getDatabase}) do not consult that
 * flag, and the install deregisters the database before swapping - so a concurrent open could otherwise
 * re-register the database from the half-swapped directory and observe a partially-populated (or stale) view.
 * <p>
 * The fix runs the whole close -&gt; swap -&gt; reopen sequence ({@link SnapshotInstaller#swapAndReopen}) while
 * holding the server's database-registry lock ({@link ArcadeDBServer#getDatabasesLock()}), the same monitor
 * every open/create/register path takes. This test pins the contract deterministically: it pauses the install
 * <i>inside</i> the critical section (right after the live database has been closed and deregistered) and proves
 * that a concurrent {@link ArcadeDBServer#getDatabase} <b>blocks</b> on the registry lock instead of re-opening
 * the database mid-swap. Once the install completes, the concurrent open returns the fully installed snapshot,
 * never the stale or partial view.
 * <p>
 * Without the fix the concurrent open is not serialised: it runs during the pause, re-registers the database
 * from the about-to-be-swapped files, and returns immediately (so {@code waitForBlocked} fails).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotInstallSwapLockTest {

  private static final String DB_NAME        = "snap4832";
  private static final String PASSWORD       = "DefaultPasswordForTests";
  private static final int    LIVE_COUNT     = 20;
  private static final int    SNAPSHOT_COUNT = 35;

  @Test
  @Timeout(90)
  void concurrentOpenBlocksDuringSwap(@TempDir final Path root) throws Exception {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_4832");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.HA_ENABLED, false);

    final ArcadeDBServer server = new ArcadeDBServer(config);
    server.start();

    final CountDownLatch insideSwap = new CountDownLatch(1);
    final CountDownLatch releaseSwap = new CountDownLatch(1);
    try {
      // The live database the server is serving: LIVE_COUNT vertices.
      final ServerDatabase live = server.getOrCreateDatabase(DB_NAME);
      live.transaction(() -> {
        live.getSchema().createVertexType("Node");
        for (int i = 0; i < LIVE_COUNT; i++)
          live.newVertex("Node").set("v", i).save();
      });
      final Path dbPath = Path.of(((DatabaseInternal) live).getDatabasePath());

      // Stage a complete, loadable snapshot under .snapshot-new with a DIFFERENT record count so a successful
      // swap is observable (SNAPSHOT_COUNT, not LIVE_COUNT). Closed before the swap so its files can be moved.
      final Path snapshotNew = dbPath.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
      try (final Database snap = new DatabaseFactory(snapshotNew.toString()).create()) {
        snap.transaction(() -> {
          snap.getSchema().createVertexType("Node");
          for (int i = 0; i < SNAPSHOT_COUNT; i++)
            snap.newVertex("Node").set("v", i).save();
        });
      }
      Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

      final Path snapshotBackup = dbPath.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
      final Path pendingMarker = dbPath.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
      Files.writeString(pendingMarker, "");

      // Pause the install inside the registry-locked critical section, after the live DB is closed/deregistered.
      SnapshotInstaller.swapBarrierForTesting = () -> {
        insideSwap.countDown();
        try {
          releaseSwap.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      };

      // Thread A: run the real swap; it parks at the barrier holding the registry lock (with the fix).
      final AtomicBoolean swapDone = new AtomicBoolean(false);
      final AtomicReference<Throwable> swapError = new AtomicReference<>();
      final Thread swapper = new Thread(() -> {
        try {
          SnapshotInstaller.swapAndReopen(DB_NAME, dbPath, snapshotNew, snapshotBackup, pendingMarker, server);
          swapDone.set(true);
        } catch (final Throwable t) {
          swapError.set(t);
        }
      }, "snapshot-swapper");
      swapper.start();

      // Wait until the install is parked inside the critical section.
      assertThat(insideSwap.await(15, TimeUnit.SECONDS)).as("install reached the in-swap barrier").isTrue();
      assertThat(server.existsDatabase(DB_NAME)).as("live DB closed/deregistered inside the swap").isFalse();

      // Thread B: a concurrent open, as an HA-internal thread would do. With the fix it MUST block on the
      // registry lock the parked install holds; without the fix it would re-open the about-to-be-swapped files.
      final AtomicLong observedCount = new AtomicLong(-1);
      final AtomicReference<Throwable> openError = new AtomicReference<>();
      final Thread opener = new Thread(() -> {
        try {
          observedCount.set(server.getDatabase(DB_NAME).countType("Node", true));
        } catch (final Throwable t) {
          openError.set(t);
        }
      }, "concurrent-opener");
      opener.start();

      assertThat(waitForBlocked(opener, 5_000))
          .as("concurrent open blocks on the registry lock while the swap is in progress").isTrue();
      assertThat(observedCount.get()).as("concurrent open has not returned a mid-swap view").isEqualTo(-1L);
      assertThat(swapDone.get()).as("swap still parked inside the critical section").isFalse();

      // Let the install finish; the concurrent open now proceeds and must see the installed snapshot.
      releaseSwap.countDown();
      swapper.join(30_000);
      opener.join(30_000);

      assertThat(swapError.get()).as("swap completed without error").isNull();
      assertThat(openError.get()).as("concurrent open completed without error").isNull();
      assertThat(swapDone.get()).as("swap finished after the barrier was released").isTrue();
      assertThat(observedCount.get())
          .as("concurrent open observed the fully installed snapshot, never a stale/partial count")
          .isEqualTo(SNAPSHOT_COUNT);

      // Post-conditions: new snapshot installed; markers and backup cleaned up.
      assertThat(snapshotNew).doesNotExist();
      assertThat(snapshotBackup).doesNotExist();
      assertThat(pendingMarker).doesNotExist();
      assertThat(server.existsDatabase(DB_NAME)).isTrue();
      assertThat(server.getDatabase(DB_NAME).countType("Node", true)).isEqualTo(SNAPSHOT_COUNT);
    } finally {
      SnapshotInstaller.swapBarrierForTesting = null;
      // Ensure no thread stays parked if an assertion failed before the release.
      releaseSwap.countDown();
      // Drop the database created by the test, then stop the server.
      try {
        if (server.existsDatabase(DB_NAME))
          ((DatabaseInternal) server.getDatabase(DB_NAME)).getEmbedded().drop();
      } catch (final Exception ignore) {
        // best-effort cleanup; the @TempDir is removed regardless
      }
      server.stop();
    }
  }

  private static boolean waitForBlocked(final Thread t, final long timeoutMs) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (t.getState() == Thread.State.BLOCKED)
        return true;
      Thread.sleep(20);
    }
    return t.getState() == Thread.State.BLOCKED;
  }
}
