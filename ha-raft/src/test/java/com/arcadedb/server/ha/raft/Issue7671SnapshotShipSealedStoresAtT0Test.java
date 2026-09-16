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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7671: the HA snapshot ship pairs a {@code schema.json} captured at the window's t0 with a
 * {@code .ts.sealed} set it listed off the filesystem at some later moment, and #7456 removed the database read
 * lock that used to make those one observation.
 * <p>
 * The rationale #7456 recorded - "since #6114 the window carries those files itself [...] so there is nothing left
 * for the lock to protect" - accounted for {@code configuration.json} and {@code schema.json} only. A TimeSeries
 * sealed store is not a {@code FileManager} file, so it is in neither the window's file list nor the deferred
 * deletion that keeps a DROPPED page file readable until the window closes, and the only remaining exclusion on
 * that path is {@code TimeSeriesCompactionPause}, which excludes a compaction and not schema DDL. A
 * {@code DROP TYPE} landing mid-ship therefore deleted the shards while the t0 {@code schema.json} still declared
 * the type, and {@code addFileToZip}'s {@code exists()} guard dropped them from the archive silently - the #4831
 * completeness manifest certifying an archive that was genuinely incomplete, and the follower installing the
 * #6356 / #6839 "type whose sealed store fails to load" state by construction.
 * <p>
 * Two halves are asserted here, because the fix has two: the sealed set is now listed WITH the window under the
 * read lock, so it is the t0 set; and a store from that set that has gone away by the time it is read fails the
 * ship instead of being skipped.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7671">issue #7671</a>
 * @see SnapshotHttpHandler#streamThroughPointInTimeImage
 * @see SnapshotHttpHandler#addSealedStoresToZip
 */
class Issue7671SnapshotShipSealedStoresAtT0Test {
  private static final String DATABASE_PATH = "target/databases/snapshot-ship-sealed-t0-7671";
  private static final String TYPE          = "Reading";
  private static final long   BASE_TS       = 1_700_000_000_000L;
  private static final int    SAMPLES       = 20_000;

  /**
   * A wait that is EXPECTED to expire: it IS the assertion that the capture is parked on the write lock. A stall
   * can only make it more true, so no wall-clock bound is being asserted.
   */
  private static final long BLOCKED_PROBE_MS = 2_000L;

  /**
   * Budget for a thread to get where it is going once nothing is in its way. Generous on purpose: a wider bound
   * cannot turn a passing run red, it only decides how long a genuinely broken test takes to say so.
   */
  private static final long PROGRESS_WAIT_MS = 60_000L;

  @BeforeEach
  @AfterEach
  void clean() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  /**
   * The headline claim, and the one that fails against the code this issue was filed on: the window and the sealed
   * listing are ONE observation, so no schema change can land between them.
   * <p>
   * Driven by holding the database WRITE lock - the lock every {@code recordFileChanges} DDL waits for - and
   * asserting the capture does not complete while it is held. Before the fix the window path took no database lock
   * at all, so the capture sailed straight past a held write lock and the short wait below returned {@code true}.
   */
  @Test
  void theWindowAndTheSealedListingAreCapturedUnderOneReadLock() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabaseWithSealedStore()) {
      final DatabaseInternal db = (DatabaseInternal) database;

      final CountDownLatch writeLockHeld = new CountDownLatch(1);
      final CountDownLatch releaseWriteLock = new CountDownLatch(1);
      final CountDownLatch captured = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      final Thread ddl = new Thread(() -> db.executeInWriteLock(() -> {
        writeLockHeld.countDown();
        assertThat(releaseWriteLock.await(PROGRESS_WAIT_MS, TimeUnit.MILLISECONDS)).isTrue();
        return null;
      }), "issue7671-write-lock");
      ddl.setDaemon(true);
      ddl.start();

      final Thread ship = new Thread(() -> {
        try {
          SnapshotHttpHandler.streamThroughPointInTimeImage(db, database.getName(), null,
              (image, pause) -> captured.countDown());
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
          captured.countDown();
        }
      }, "issue7671-ship");
      ship.setDaemon(true);

      assertThat(writeLockHeld.await(PROGRESS_WAIT_MS, TimeUnit.MILLISECONDS))
          .as("the fixture must actually hold the write lock, or the assertion below would be vacuous").isTrue();
      ship.start();

      assertThat(captured.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS))
          .as("the capture must wait for the schema write lock: a DDL statement landing between the window's t0 "
              + "and the sealed-store listing is exactly the pairing this lock exists to make atomic")
          .isFalse();

      releaseWriteLock.countDown();
      assertThat(captured.await(PROGRESS_WAIT_MS, TimeUnit.MILLISECONDS))
          .as("and it must proceed as soon as the write lock is free").isTrue();

      ship.join(PROGRESS_WAIT_MS);
      ddl.join(PROGRESS_WAIT_MS);
      assertThat(failure.get()).isNull();
    }
  }

  /**
   * The lock is over the CAPTURE and not over the transfer, which is the availability property #7456 bought and
   * this fix must not give back. A {@code DROP TYPE} issued from inside the streamer - i.e. while the ship would be
   * writing bytes to the follower - has to complete, not queue behind a lock held for the download's duration.
   */
  @Test
  void theTransferItselfStillHoldsNoDatabaseLock() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabaseWithSealedStore()) {
      final DatabaseInternal db = (DatabaseInternal) database;

      final CountDownLatch streaming = new CountDownLatch(1);
      final CountDownLatch ddlDone = new CountDownLatch(1);
      final AtomicReference<Exception> ddlFailure = new AtomicReference<>();

      final Thread ddl = new Thread(() -> {
        try {
          assertThat(streaming.await(PROGRESS_WAIT_MS, TimeUnit.MILLISECONDS)).isTrue();
          database.command("sql", "DROP TYPE " + TYPE);
        } catch (final Exception e) {
          ddlFailure.compareAndSet(null, e);
        } finally {
          ddlDone.countDown();
        }
      }, "issue7671-drop-during-transfer");
      ddl.setDaemon(true);
      ddl.start();

      final AtomicReference<Boolean> ddlCompletedInsideTheTransfer = new AtomicReference<>(false);
      SnapshotHttpHandler.streamThroughPointInTimeImage(db, database.getName(), null, (image, pause) -> {
        streaming.countDown();
        try {
          ddlCompletedInsideTheTransfer.set(ddlDone.await(PROGRESS_WAIT_MS, TimeUnit.MILLISECONDS));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      ddl.join(PROGRESS_WAIT_MS);
      assertThat(ddlFailure.get()).isNull();
      assertThat(ddlCompletedInsideTheTransfer.get())
          .as("DDL must run alongside the snapshot ship: the read lock covers the capture, not the download")
          .isTrue();
    }
  }

  /**
   * The reported repro, end to end: a {@code DROP TYPE} of a TIMESERIES type lands after t0, so the sealed shards
   * the t0 {@code schema.json} still declares are gone by the time the ship reads them.
   * <p>
   * The assertion is that the archive FAILS rather than quietly omitting them. Skipping is what made the bug
   * silent: the manifest of #4831 records what was added, so a file never added is never missed, and the follower
   * extracts a database whose schema declares a TIMESERIES type with no sealed store.
   */
  @Test
  void aTypeDroppedAfterT0FailsTheShipInsteadOfSilentlyShippingItsSchemaWithoutItsData() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabaseWithSealedStore()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final List<File> sealedAtT0 = captureSealedSetAtT0(db, database.getName());
      assertThat(sealedAtT0).as("the fixture must actually have sealed something, or this proves nothing").isNotEmpty();

      database.command("sql", "DROP TYPE " + TYPE);
      assertThat(TimeSeriesSealedStore.listSealedFiles(new File(db.getDatabasePath())))
          .as("the drop must actually have deleted the sealed shards, or the assertion below proves nothing")
          .isEmpty();

      final List<SnapshotManager.ManifestEntry> manifest = new ArrayList<>();
      assertThatThrownBy(() -> archiveSealedStores(sealedAtT0, manifest))
          .as("a sealed store the archive's own schema declares must not be skipped")
          .hasMessageContaining(sealedAtT0.get(0).getName())
          .hasMessageContaining("went away after the snapshot's point in time");

      assertThat(manifest)
          .as("and nothing may be recorded for it, so the completeness manifest cannot certify the hole")
          .noneMatch(entry -> entry.name().equals(sealedAtT0.get(0).getName()));
    }
  }

  /**
   * The same hole reached without any DDL at all: a repair of a type whose engine never loaded takes no shard lock
   * (#7475) and an operator can always move a file. The ship's answer has to be the same, because what makes it
   * wrong is the archive contradicting its own {@code schema.json}, not how the file came to be missing.
   */
  @Test
  void aSealedStoreRemovedOutOfBandAfterT0AlsoFailsTheShip() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabaseWithSealedStore()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final List<File> sealedAtT0 = captureSealedSetAtT0(db, database.getName());
      assertThat(sealedAtT0).isNotEmpty();

      assertThat(sealedAtT0.get(0).delete()).isTrue();

      assertThatThrownBy(() -> archiveSealedStores(sealedAtT0, new ArrayList<>()))
          .hasMessageContaining(sealedAtT0.get(0).getName());
    }
  }

  /**
   * The undisturbed path, which is what keeps the three assertions above from passing because the ship refuses
   * everything: every sealed store captured at t0 is archived, with one manifest entry each.
   */
  @Test
  void theSealedStoresCapturedAtT0AreArchivedWithAManifestEntryEach() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabaseWithSealedStore()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final List<File> sealedAtT0 = captureSealedSetAtT0(db, database.getName());
      assertThat(sealedAtT0).isNotEmpty();

      final List<SnapshotManager.ManifestEntry> manifest = new ArrayList<>();
      archiveSealedStores(sealedAtT0, manifest);

      assertThat(manifest.stream().map(SnapshotManager.ManifestEntry::name))
          .containsExactlyInAnyOrderElementsOf(sealedAtT0.stream().map(File::getName).toList());
      for (final SnapshotManager.ManifestEntry entry : manifest)
        assertThat(entry.size()).as("%s must be archived whole", entry.name()).isGreaterThan(0L);
    }
  }

  /**
   * The size advertised to the follower before the first body byte (issue #7037) has to describe the bytes the ship
   * is about to send, and on the window path those are the t0 sealed set's - not a fresh listing's. Asserted as an
   * exact total so a sealed store silently dropped from either side of the comparison shows up.
   */
  @Test
  void theAnnouncedSizeCountsTheSealedSetTheShipWillStream() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabaseWithSealedStore()) {
      final DatabaseInternal db = (DatabaseInternal) database;

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        final List<File> sealedFiles = List.of(TimeSeriesSealedStore.listSealedFiles(new File(db.getDatabasePath())));
        assertThat(sealedFiles).isNotEmpty();

        final long sealedBytes = sealedFiles.stream().mapToLong(File::length).sum();
        final long windowBytes = snapshot.getFiles().stream().mapToLong(PageSnapshot.SnapshotFile::size).sum();
        final long configBytes = snapshot.getConfigurationFiles().stream()
            .mapToLong(PageSnapshot.SnapshotConfigFile::size).sum();

        assertThat(SnapshotHttpHandler.estimateUncompressedBytes(db, snapshot, sealedFiles))
            .as("the announced size must count the sealed stores this archive carries, plus the 8-byte marker")
            .isEqualTo(windowBytes + configBytes + sealedBytes + Long.BYTES);
      }
    }
  }

  /**
   * Opens a window exactly as the ship does, takes the sealed set it captured, and closes the window again - so the
   * assertions above are about the set the ship WOULD have streamed rather than about a listing the test made up.
   */
  private static List<File> captureSealedSetAtT0(final DatabaseInternal db, final String databaseName) {
    final AtomicReference<List<File>> captured = new AtomicReference<>();
    SnapshotHttpHandler.streamThroughPointInTimeImage(db, databaseName, null, (image, pause) -> {
      assertThat(image).as("the fixture must have taken the window path").isNotNull();
      captured.set(image.sealedFiles());
    });
    assertThat(captured.get()).isNotNull();
    return captured.get();
  }

  private static void archiveSealedStores(final List<File> sealedFiles,
      final List<SnapshotManager.ManifestEntry> manifest) throws Exception {
    try (final ZipOutputStream zipOut = new ZipOutputStream(new ByteArrayOutputStream())) {
      SnapshotHttpHandler.addSealedStoresToZip(zipOut, sealedFiles, manifest);
    }
  }

  private Database createDatabaseWithSealedStore() throws Exception {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.command("sql",
        "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

    final long[] timestamps = new long[SAMPLES];
    final Object[] hosts = new Object[SAMPLES];
    final Object[] values = new Object[SAMPLES];
    for (int i = 0; i < SAMPLES; i++) {
      timestamps[i] = BASE_TS + i * 1_000L;
      hosts[i] = "host_" + (i % 4);
      values[i] = (double) i;
    }
    final var engine = ((LocalTimeSeriesType) database.getSchema().getType(TYPE)).getEngine();
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
    engine.compactAll();

    ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
    return database;
  }
}
