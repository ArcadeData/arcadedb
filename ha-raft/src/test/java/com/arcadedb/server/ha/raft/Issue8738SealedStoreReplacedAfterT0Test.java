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
import com.arcadedb.engine.timeseries.ListedSealedStore;
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8738: the HA verify and the snapshot ship list the TimeSeries sealed stores at their window's t0 and read
 * them later by PATH. A TimeSeries type dropped and recreated under the same name in between leaves the listed path
 * naming the NEW {@code .ts.sealed}, which read cleanly and was reported as covered (verify) or shipped as the t0 store
 * (snapshot). Each listed store now carries the identity its file had at t0, and a read that finds a different file
 * is reported as not covered, or fails the ship.
 *
 * @see ListedSealedStore
 */
class Issue8738SealedStoreReplacedAfterT0Test {
  private static final String DATABASE_PATH = "target/databases/sealed-store-replaced-after-t0-8738";
  private static final String TS_TYPE       = "Reading";
  private static final String TS_DDL        =
      "CREATE TIMESERIES TYPE " + TS_TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1";
  private static final long   BASE_TS       = 1_700_000_000_000L;
  private static final int    SAMPLES       = 5_000;
  /** Budget for something expected to happen. Generous: a wider bound cannot turn a passing run red. */
  private static final long   WAIT_MS       = 30_000L;

  private final PostVerifyDatabaseHandler handler = new PostVerifyDatabaseHandler(null, null);

  @BeforeEach
  void clean() {
    PostVerifyDatabaseHandler.whileChecksummingForTesting = null;
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @AfterEach
  void tearDown() {
    handler.close();
    clean();
  }

  // ------------------------------------------------------------------------------------------------- VERIFY

  /**
   * The reported case on the verify: the type is dropped and recreated under the same name while the verify CRCs its
   * window. The listed path then names the recreated store, which must not be reported as covered.
   */
  @Test
  void verifyReportsATypeDroppedAndRecreatedAfterT0AsNotCovered() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final Set<String> sealedAtT0 = sealedFileNames(db);
      assertThat(sealedAtT0).as("the fixture must have sealed something, or this proves nothing").isNotEmpty();

      final AtomicReference<Set<String>> sealedAfterRecreate = new AtomicReference<>();
      final AtomicReference<Throwable> ddlFailure = new AtomicReference<>();
      final AtomicBoolean ddlCompleted = new AtomicBoolean();
      PostVerifyDatabaseHandler.whileChecksummingForTesting = snapshot -> {
        // ON ANOTHER THREAD, SO A REGRESSION THAT PUTS THE READ LOCK BACK FAILS THIS TEST INSTEAD OF HANGING IT
        final Thread ddl = new Thread(() -> {
          try {
            database.command("sql", "DROP TYPE " + TS_TYPE);
            database.command("sql", TS_DDL);
            sealedAfterRecreate.set(sealedFileNames(db));
          } catch (final Throwable t) {
            ddlFailure.compareAndSet(null, t);
          }
        }, "issue8738-drop-recreate");
        ddl.setDaemon(true);
        ddl.start();
        ddlCompleted.set(joined(ddl, WAIT_MS));
      };

      final JSONObject checksums = new JSONObject();
      final boolean covered = handler.computeLocalChecksums(db, checksums, new JSONArray());

      assertThat(ddlFailure.get()).isNull();
      assertThat(ddlCompleted.get()).as("the drop and recreate must have run inside the verify").isTrue();
      assertThat(sealedAfterRecreate.get())
          .as("the recreated type must own a sealed store under a name listed at t0, or this proves nothing")
          .containsAnyElementsOf(sealedAtT0);

      assertThat(covered)
          .as("a sealed store whose path names a different file than at t0 must make the answer report incomplete "
              + "coverage, not a silent 'covered'")
          .isFalse();
      assertThat(checksums.keySet())
          .as("and the recreated store's checksum must not be passed off as the t0 one")
          .doesNotContainAnyElementsOf(sealedAtT0);
    }
  }

  /**
   * The identity, not the size or the content, is what is compared: a byte-identical copy with the same size and the
   * same last-modified time, renamed over the listed path, is still a different file than the one listed.
   */
  @Test
  void verifyReportsASameSizeSameTimestampReplacementAsNotCovered() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final File[] sealed = TimeSeriesSealedStore.listSealedFiles(new File(db.getDatabasePath()));
      assertThat(sealed).isNotEmpty();

      final AtomicReference<IOException> replaceFailure = new AtomicReference<>();
      PostVerifyDatabaseHandler.whileChecksummingForTesting = snapshot -> {
        try {
          replaceWithIdenticalCopy(sealed[0]);
        } catch (final IOException e) {
          replaceFailure.set(e);
        }
      };

      final JSONObject checksums = new JSONObject();
      final boolean covered = handler.computeLocalChecksums(db, checksums, new JSONArray());

      assertThat(replaceFailure.get()).isNull();
      assertThat(covered).isFalse();
      assertThat(checksums.keySet()).doesNotContain(sealed[0].getName());
    }
  }

  /** The counterweight: with nothing replaced, the same verify still reports full coverage of the same stores. */
  @Test
  void verifyOfAnUndisturbedDatabaseStillCoversEverySealedStore() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final Set<String> sealedAtT0 = sealedFileNames(db);
      assertThat(sealedAtT0).isNotEmpty();

      final JSONObject checksums = new JSONObject();
      assertThat(handler.computeLocalChecksums(db, checksums, new JSONArray())).isTrue();
      assertThat(checksums.keySet()).containsAll(sealedAtT0);
    }
  }

  // ------------------------------------------------------------------------------------------------- SNAPSHOT SHIP

  /**
   * The reported case on the snapshot ship: the t0 {@code schema.json} and page files go out with the sealed store of
   * a type recreated after t0. That archive describes no state the leader ever had, so the ship must fail (and the
   * follower retry) rather than certify it.
   */
  @Test
  void shipFailsWhenATypeWasDroppedAndRecreatedAfterT0() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final List<ListedSealedStore> sealedAtT0 = captureSealedSetAtT0(db, database.getName());
      assertThat(sealedAtT0).isNotEmpty();

      database.command("sql", "DROP TYPE " + TS_TYPE);
      database.command("sql", TS_DDL);
      assertThat(sealedAtT0.get(0).file())
          .as("the recreated type must own a sealed store under the listed name, or this proves nothing").exists();

      final List<SnapshotManager.ManifestEntry> manifest = new ArrayList<>();
      assertThatThrownBy(() -> archiveSealedStores(sealedAtT0, manifest))
          .isInstanceOf(ListedSealedStore.ChangedException.class)
          .hasMessageContaining(sealedAtT0.get(0).name())
          .hasMessageContaining("not the file that was listed");

      assertThat(manifest)
          .as("nothing may be recorded for it, so the completeness manifest cannot certify it")
          .noneMatch(entry -> entry.name().equals(sealedAtT0.get(0).name()));
    }
  }

  /** The ship's twin of the identity-only case above. */
  @Test
  void shipFailsOnASameSizeSameTimestampReplacement() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final List<ListedSealedStore> sealedAtT0 = captureSealedSetAtT0(db, database.getName());
      assertThat(sealedAtT0).isNotEmpty();

      replaceWithIdenticalCopy(sealedAtT0.get(0).file());

      assertThatThrownBy(() -> archiveSealedStores(sealedAtT0, new ArrayList<>()))
          .isInstanceOf(ListedSealedStore.ChangedException.class)
          .hasMessageContaining(sealedAtT0.get(0).name());
    }
  }

  /**
   * The size announced to the follower's space check (#7037) is the t0 size, which is the only size a sealed entry can
   * ship with now that a store whose size moved fails the ship.
   */
  @Test
  void theAnnouncedSizeIsTheT0SizeOfTheSealedStores() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        final List<ListedSealedStore> sealedAtT0 = ListedSealedStore.listOrNull(new File(db.getDatabasePath()));
        assertThat(sealedAtT0).isNotEmpty();
        final long t0SealedBytes = sealedAtT0.stream().mapToLong(store -> store.file().length()).sum();
        final long withoutSealed = SnapshotHttpHandler.estimateUncompressedBytes(db, snapshot, List.of());

        database.command("sql", "DROP TYPE " + TS_TYPE);
        database.command("sql", TS_DDL);
        assertThat(sealedAtT0.stream().mapToLong(store -> store.file().length()).sum())
            .as("the recreated store must differ in size, or this proves nothing").isNotEqualTo(t0SealedBytes);

        assertThat(SnapshotHttpHandler.estimateUncompressedBytes(db, snapshot, sealedAtT0))
            .isEqualTo(withoutSealed + t0SealedBytes);
      }
    }
  }

  // ------------------------------------------------------------------------------------------------- IDENTITY

  /**
   * A store whose identity could not be captured when it was listed is never trusted unchecked: its read is refused,
   * rather than the missing identity reading as "nothing to compare, so it matches".
   */
  @Test
  void aStoreListedWithoutAnIdentityIsRefused() throws Exception {
    try (final Database database = createDatabase()) {
      final File[] sealed = TimeSeriesSealedStore.listSealedFiles(new File(((DatabaseInternal) database).getDatabasePath()));
      assertThat(sealed).isNotEmpty();

      final ListedSealedStore unidentified = new ListedSealedStore(sealed[0], null);
      assertThatThrownBy(unidentified::open).isInstanceOf(ListedSealedStore.ChangedException.class);
      assertThatThrownBy(() -> unidentified.verifyUnchanged(-1L)).isInstanceOf(ListedSealedStore.ChangedException.class);
    }
  }

  /** A read that consumed a different byte count than the t0 size is refused even when the attributes match. */
  @Test
  void aReadOfADifferentLengthThanAtT0IsRefused() throws Exception {
    try (final Database database = createDatabase()) {
      final File[] sealed = TimeSeriesSealedStore.listSealedFiles(new File(((DatabaseInternal) database).getDatabasePath()));
      final ListedSealedStore listed = ListedSealedStore.capture(sealed[0]);

      listed.verifyUnchanged(listed.size());
      assertThatThrownBy(() -> listed.verifyUnchanged(listed.size() - 1))
          .isInstanceOf(ListedSealedStore.ChangedException.class);
    }
  }

  // ------------------------------------------------------------------------------------------------- HELPERS

  /**
   * Replaces {@code file} with a byte-identical copy carrying the same last-modified time, through an atomic rename:
   * the same size, the same content, the same mtime, a different file.
   */
  private static void replaceWithIdenticalCopy(final File file) throws IOException {
    final FileTime lastModified = Files.getLastModifiedTime(file.toPath());
    final File copy = new File(file.getParentFile(), file.getName() + ".copy-8738");
    Files.copy(file.toPath(), copy.toPath(), StandardCopyOption.REPLACE_EXISTING);
    Files.setLastModifiedTime(copy.toPath(), lastModified);
    Files.move(copy.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }

  private static List<ListedSealedStore> captureSealedSetAtT0(final DatabaseInternal db, final String databaseName) {
    final AtomicReference<List<ListedSealedStore>> captured = new AtomicReference<>();
    SnapshotHttpHandler.streamThroughPointInTimeImage(db, databaseName, null, (image, pause) -> {
      assertThat(image).as("the fixture must have taken the window path").isNotNull();
      captured.set(image.sealedFiles());
    });
    assertThat(captured.get()).isNotNull();
    return captured.get();
  }

  private static void archiveSealedStores(final List<ListedSealedStore> sealedFiles,
      final List<SnapshotManager.ManifestEntry> manifest) throws Exception {
    try (final ZipOutputStream zipOut = new ZipOutputStream(new ByteArrayOutputStream())) {
      SnapshotHttpHandler.addSealedStoresToZip(zipOut, sealedFiles, manifest);
    }
  }

  private static boolean joined(final Thread thread, final long millis) {
    try {
      thread.join(millis);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return !thread.isAlive();
  }

  private static Set<String> sealedFileNames(final DatabaseInternal db) {
    final Set<String> names = new HashSet<>();
    for (final File file : TimeSeriesSealedStore.listSealedFiles(new File(db.getDatabasePath())))
      names.add(file.getName());
    return names;
  }

  private Database createDatabase() throws Exception {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    try {
      database.command("sql", TS_DDL);
      final long[] timestamps = new long[SAMPLES];
      final Object[] hosts = new Object[SAMPLES];
      final Object[] values = new Object[SAMPLES];
      for (int i = 0; i < SAMPLES; i++) {
        timestamps[i] = BASE_TS + i * 1_000L;
        hosts[i] = "host_" + (i % 4);
        values[i] = (double) i;
      }
      final var engine = ((LocalTimeSeriesType) database.getSchema().getType(TS_TYPE)).getEngine();
      engine.appendBatch(timestamps, new Object[][] { hosts, values });
      engine.compactAll();

      ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
      return database;
    } catch (final Throwable t) {
      database.close();
      throw t;
    }
  }
}
