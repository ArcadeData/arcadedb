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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7456: the HA snapshot ship archives the configuration the WINDOW carries, and holds no database read lock
 * while it streams.
 * <p>
 * #6114 gave {@link PageSnapshot} the two configuration files as bytes captured inside the t0 barrier and used them
 * to remove {@code database.executeInReadLock(...)} from the full backup. The snapshot ship has the same shape and
 * was left out of that PR. Until this change a follower resyncing from a multi-GB leader blocked that leader's DDL
 * for the whole transfer - the longest such window in the product.
 * <p>
 * The frozen-files fallback is deliberately NOT migrated, and two of the tests here pin that: with no t0 to capture
 * the configuration at, the live files have to be read off the filesystem and the lock is the only thing keeping
 * them in step with the pages the flush suspension is freezing.
 *
 * @see com.arcadedb.engine.PageSnapshot#getConfigurationFiles()
 */
class Issue7456SnapshotShipConfigurationFromWindowTest {
  private static final String DATABASE_PATH = "target/databases/snapshot-ship-configuration-7456";
  private static final String TYPE          = "Doc";
  private static final int    RECORDS       = 2_000;
  /** Created AFTER t0 in every test: the schema entry that tells a window-sourced archive from a filesystem one. */
  private static final String AFTER_T0_TYPE = "CreatedAfterT0";

  @BeforeEach
  @AfterEach
  void clean() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  /**
   * The headline claim. A type created while the window is open must NOT appear in the {@code schema.json} the ship
   * archives: an archive whose schema names a bucket its pages do not contain is precisely what the removed read
   * lock used to be preventing, and the window carries the matching pair itself.
   */
  @Test
  void theWindowPathArchivesTheConfigurationCapturedAtT0() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        // DDL AFTER t0 - which the removed read lock used to make impossible for the transfer's duration
        database.getSchema().createDocumentType(AFTER_T0_TYPE);

        final Map<String, byte[]> archived = archiveConfiguration(db, snapshot);

        assertThat(archived).containsKey(LocalSchema.SCHEMA_FILE_NAME);
        assertThat(typeNamesOf(archived.get(LocalSchema.SCHEMA_FILE_NAME)))
            .as("the ship must archive the schema as of t0, not the one the DDL left on disk afterwards")
            .contains(TYPE)
            .doesNotContain(AFTER_T0_TYPE);
      }
    }
  }

  /**
   * The same call on the fallback path keeps reading the live files, so the type created before the call IS there.
   * This is the regression guard on the branch that was deliberately left alone: a "simplification" that routed both
   * paths through the window would silently make the frozen-files archive read a configuration nothing pins.
   */
  @Test
  void theFallbackPathStillReadsTheConfigurationOffTheFilesystem() throws Exception {
    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;

      database.getSchema().createDocumentType(AFTER_T0_TYPE);

      final Map<String, byte[]> archived = archiveConfiguration(db, null);

      assertThat(archived).containsKey(LocalSchema.SCHEMA_FILE_NAME);
      assertThat(typeNamesOf(archived.get(LocalSchema.SCHEMA_FILE_NAME)))
          .as("with no window there is no t0, so the fallback reads what is on disk right now")
          .contains(TYPE, AFTER_T0_TYPE);
    }
  }

  /**
   * The size advertised to the follower before the first body byte (issue #7037) has to describe the SAME bytes the
   * ship is about to send. Sizing the configuration off the filesystem while streaming the window's copy would
   * announce a figure for an archive nobody sends - here the two differ by construction, because the DDL inside the
   * window grows {@code schema.json} on disk.
   */
  @Test
  void theAnnouncedSizeCountsTheWindowsConfigurationBytes() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final File schemaFile = ((LocalSchema) database.getSchema()).getConfigurationFile();

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        final long schemaBytesAtT0 = configurationSize(snapshot, LocalSchema.SCHEMA_FILE_NAME);

        // GROW schema.json ON DISK WHILE THE WINDOW IS OPEN, ENOUGH THAT THE TWO FIGURES CANNOT COINCIDE
        for (int i = 0; i < 40; i++)
          database.getSchema().createDocumentType(AFTER_T0_TYPE + i);

        final long schemaBytesOnDisk = schemaFile.length();
        assertThat(schemaBytesOnDisk)
            .as("the fixture must actually move the on-disk size, or the assertion below proves nothing")
            .isGreaterThan(schemaBytesAtT0);

        final long announced = SnapshotHttpHandler.estimateUncompressedBytes(db, snapshot);
        final long windowBytes = snapshot.getFiles().stream().mapToLong(PageSnapshot.SnapshotFile::size).sum();
        final long configBytes = snapshot.getConfigurationFiles().stream()
            .mapToLong(PageSnapshot.SnapshotConfigFile::size).sum();

        // NO SEALED STORES IN THIS FIXTURE (no TimeSeries type), so the total is exactly pages + configuration +
        // the 8-byte last-tx-id marker
        assertThat(announced)
            .as("the announced size must be the window's own bytes, not the filesystem's")
            .isEqualTo(windowBytes + configBytes + Long.BYTES);
      }
    }
  }

  /**
   * The availability half of the change: a DDL statement completes WHILE the ship is streaming. The assertion is
   * logical rather than a stopwatch - a {@code CREATE TYPE} that returns before the transfer body is released cannot
   * have been queued behind a lock the transfer holds for its whole duration.
   */
  @Test
  void theWindowPathHoldsNoDatabaseReadLock() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;

      final CountDownLatch streaming = new CountDownLatch(1);
      final CountDownLatch ddlDone = new CountDownLatch(1);
      final AtomicBoolean sawWindow = new AtomicBoolean();
      final AtomicReference<Exception> ddlFailure = new AtomicReference<>();

      final Thread ddl = new Thread(() -> {
        try {
          assertThat(streaming.await(60, TimeUnit.SECONDS)).isTrue();
          database.getSchema().createDocumentType(AFTER_T0_TYPE);
        } catch (final Exception e) {
          ddlFailure.compareAndSet(null, e);
        } finally {
          ddlDone.countDown();
        }
      }, "issue7456-ddl");
      ddl.setDaemon(true);
      ddl.start();

      final AtomicBoolean ddlCompletedInsideTheTransfer = new AtomicBoolean();
      SnapshotHttpHandler.streamThroughPointInTimeImage(db, database.getName(), null, (snapshot, pause) -> {
        sawWindow.set(snapshot != null);
        streaming.countDown();
        try {
          ddlCompletedInsideTheTransfer.set(ddlDone.await(30, TimeUnit.SECONDS));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      ddl.join(60_000);
      assertThat(ddlFailure.get()).isNull();
      assertThat(sawWindow.get()).as("the fixture must have taken the window path").isTrue();
      assertThat(ddlCompletedInsideTheTransfer.get())
          .as("DDL must run alongside the snapshot ship, not queue behind a lock held for the whole transfer")
          .isTrue();
      assertThat(database.getSchema().existsType(AFTER_T0_TYPE)).isTrue();
    }
  }

  /**
   * The mirror image, and the reason the two branches are not one: with the window disabled the ship falls back to
   * freezing the live files, and there the read lock IS still taken - a concurrent DDL blocks until the transfer
   * releases it. Asserting the DDL does NOT complete within a short wait is a timeout that is expected to expire,
   * so no wall-clock bound is being asserted.
   */
  @Test
  void theFallbackPathStillHoldsTheDatabaseReadLock() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(false);

    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;

      final CountDownLatch streaming = new CountDownLatch(1);
      final CountDownLatch ddlDone = new CountDownLatch(1);
      final AtomicBoolean sawWindow = new AtomicBoolean(true);
      final AtomicReference<Exception> ddlFailure = new AtomicReference<>();

      final Thread ddl = new Thread(() -> {
        try {
          assertThat(streaming.await(60, TimeUnit.SECONDS)).isTrue();
          database.getSchema().createDocumentType(AFTER_T0_TYPE);
        } catch (final Exception e) {
          ddlFailure.compareAndSet(null, e);
        } finally {
          ddlDone.countDown();
        }
      }, "issue7456-ddl-fallback");
      ddl.setDaemon(true);
      ddl.start();

      final AtomicBoolean ddlCompletedInsideTheTransfer = new AtomicBoolean();
      SnapshotHttpHandler.streamThroughPointInTimeImage(db, database.getName(), null, (snapshot, pause) -> {
        sawWindow.set(snapshot != null);
        streaming.countDown();
        try {
          ddlCompletedInsideTheTransfer.set(ddlDone.await(2, TimeUnit.SECONDS));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      ddl.join(60_000);
      assertThat(ddlFailure.get()).isNull();
      assertThat(sawWindow.get()).as("the fixture must have taken the frozen-files path").isFalse();
      assertThat(ddlCompletedInsideTheTransfer.get())
          .as("the fallback still pins the configuration with the read lock, so the DDL waits for the transfer")
          .isFalse();
    }
  }

  /**
   * The behaviour change the review asked to see pinned rather than only argued: on the WINDOW path a symlinked
   * {@code schema.json} is now shipped, because the window holds bytes {@code Files.readAllBytes} read through the
   * link at t0, while {@code SnapshotHttpHandler.addFileToZip}'s symlink refusal survives on the fallback branch
   * only.
   * <p>
   * Shipping it is the intended outcome. The refusal exists to keep an archive entry from carrying content read
   * from an attacker-chosen path; here the entry name is one of two fixed ones and the bytes are the leader's own
   * live schema, which is what the ship exists to transfer - whereas dropping the entry hands the follower a
   * database with no schema at all. {@code FullBackupFormat} has behaved this way on its window path since #6114.
   */
  @Test
  void theWindowPathShipsASymlinkedSchemaThatTheFallbackStillRefuses() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;

      final Path schemaPath = ((LocalSchema) database.getSchema()).getConfigurationFile().toPath();
      // ABSOLUTE, because a symlink's target is resolved relative to the LINK's own directory: a relative one
      // here points inside the database directory and the link is simply broken, which the window reports as an
      // absent file and this test would then "pass" on the fallback assertion alone
      final Path outsideTarget = new File(DATABASE_PATH).getAbsoluteFile().toPath().getParent()
          .resolve("snapshot-ship-7456-schema-outside.json");
      Files.deleteIfExists(outsideTarget);
      Files.move(schemaPath, outsideTarget);
      Files.createSymbolicLink(schemaPath, outsideTarget);
      assertThat(Files.isSymbolicLink(schemaPath)).as("the fixture must really have made it a symlink").isTrue();
      assertThat(Files.exists(schemaPath)).as("the symlink must resolve, or the window sees an absent file").isTrue();

      try {
        // FALLBACK: the refusal still applies, so the archive carries configuration.json and nothing else
        assertThat(archiveConfiguration(db, null))
            .as("the frozen-files path must keep refusing a symlinked configuration file")
            .doesNotContainKey(LocalSchema.SCHEMA_FILE_NAME);

        // WINDOW: the bytes were read through the link at t0, so the follower gets a usable schema
        try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
          final Map<String, byte[]> archived = archiveConfiguration(db, snapshot);
          assertThat(archived)
              .as("the window path ships the linked-to bytes rather than dropping the schema entirely")
              .containsKey(LocalSchema.SCHEMA_FILE_NAME);
          assertThat(typeNamesOf(archived.get(LocalSchema.SCHEMA_FILE_NAME))).contains(TYPE);
        }
      } finally {
        Files.deleteIfExists(schemaPath);
        Files.move(outsideTarget, schemaPath);
      }
    }
  }

  // ------------------------------------------------------------------------------------------------- HELPERS

  /** Runs the handler's configuration-archiving step into a ZIP in memory and returns the entries it produced. */
  private static Map<String, byte[]> archiveConfiguration(final DatabaseInternal db, final PageSnapshot snapshot)
      throws Exception {
    final List<SnapshotManager.ManifestEntry> manifest = new ArrayList<>();
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final ZipOutputStream zipOut = new ZipOutputStream(bytes)) {
      SnapshotHttpHandler.addConfigurationToZip(zipOut, db, snapshot, manifest);
      zipOut.finish();
    }

    final Map<String, byte[]> entries = new HashMap<>();
    try (final ZipInputStream zipIn = new ZipInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      for (ZipEntry entry = zipIn.getNextEntry(); entry != null; entry = zipIn.getNextEntry())
        entries.put(entry.getName(), zipIn.readAllBytes());
    }

    // The manifest the follower verifies against must describe exactly the entries that were written (issue #4831)
    assertThat(manifest).extracting(SnapshotManager.ManifestEntry::name)
        .containsExactlyInAnyOrderElementsOf(entries.keySet());
    for (final SnapshotManager.ManifestEntry entry : manifest)
      assertThat(entry.size()).isEqualTo(entries.get(entry.name()).length);

    assertThat(entries).containsKey(LocalDatabase.CONFIGURATION_FILE_NAME);
    return entries;
  }

  private static long configurationSize(final PageSnapshot snapshot, final String fileName) {
    return snapshot.getConfigurationFiles().stream().filter(c -> c.fileName().equals(fileName))
        .mapToLong(PageSnapshot.SnapshotConfigFile::size).sum();
  }

  private static Iterable<String> typeNamesOf(final byte[] schemaJson) {
    return new JSONObject(new String(schemaJson, StandardCharsets.UTF_8)).getJSONObject("types", new JSONObject())
        .keySet();
  }

  private Database createDatabase() throws Exception {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.getSchema().createDocumentType(TYPE);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("id", i).set("payload", "x".repeat(200)).save();
    });
    ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
    // configuration.json only exists once a setting has been persisted: the ship's archive must carry it, and the
    // helper asserts it is there, so make sure there is one
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 1);
    ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).saveConfiguration();
    return database;
  }
}
