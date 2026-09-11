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
package com.arcadedb.integration.backup;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6114: the full backup no longer holds the database read lock for its duration, so DDL runs alongside it,
 * and the archive still carries a configuration that matches the pages it contains.
 * <p>
 * #6075 removed the flush suspension, which stopped the backup throttling ordinary WRITERS. What it left behind was
 * {@code database.executeInReadLock(...)} wrapped around the whole backup, which blocks DDL - {@code CREATE TYPE},
 * {@code DROP TYPE}, {@code CREATE INDEX} - for as long as the backup runs. The lock existed only to keep
 * {@code configuration.json} and {@code schema.json} consistent with the page files, which the window now does
 * itself by capturing them at t0.
 * <p>
 * Tagged {@code slow}: both tests deliberately throttle the backup so the DDL has a window to run inside, which is
 * the only way to observe the property at all.
 *
 * @see com.arcadedb.engine.PageSnapshot#getConfigurationFiles()
 */
@Tag("slow")
class Issue6114LockFreeBackupIT {
  private static final String DATABASE_PATH = "target/databases/backup-lock-free";
  private static final String RESTORED_PATH = "target/databases/backup-lock-free-restored";
  private static final String BACKUP_FILE   = "target/backup-lock-free.zip";
  private static final String TYPE          = "Doc";
  private static final int    RECORDS       = 20_000;
  /** Throttled so the backup outlives enough DDL statements for the assertions to mean something. */
  private static final int    MAX_MB_PER_SECOND = 4;

  @BeforeEach
  @AfterEach
  void clean() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
  }

  /**
   * The headline claim: a schema change completes WHILE the backup is still streaming. The assertion is logical,
   * not a stopwatch - a DDL statement that returns before the backup has finished cannot have been queued behind a
   * lock the backup holds for its whole duration.
   */
  @Test
  void ddlCompletesWhileTheBackupIsStillRunning() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabase()) {
      final AtomicBoolean backupRunning = new AtomicBoolean(false);
      final AtomicInteger ddlDuringBackup = new AtomicInteger();
      final AtomicBoolean running = new AtomicBoolean(true);
      final AtomicReference<Exception> ddlFailure = new AtomicReference<>();
      final CountDownLatch ddlWarmedUp = new CountDownLatch(1);

      final Thread ddl = new Thread(() -> {
        int sequence = 0;
        while (running.get()) {
          try {
            final boolean duringBackup = backupRunning.get();
            database.getSchema().createDocumentType("Ddl" + sequence++);
            if (duringBackup && backupRunning.get())
              ddlDuringBackup.incrementAndGet();
            ddlWarmedUp.countDown();
            Thread.sleep(5);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          } catch (final Exception e) {
            ddlFailure.compareAndSet(null, e);
            return;
          }
        }
      }, "issue6114-ddl");
      ddl.setDaemon(true);
      ddl.start();

      try {
        assertThat(ddlWarmedUp.await(60, TimeUnit.SECONDS)).isTrue();

        backupRunning.set(true);
        try {
          new Backup(database, BACKUP_FILE).setVerboseLevel(0).setMaxMBPerSecond(MAX_MB_PER_SECOND).backupDatabase();
        } finally {
          backupRunning.set(false);
        }

        running.set(false);
        ddl.join(60_000);
        assertThat(ddlFailure.get()).isNull();

        assertThat(ddlDuringBackup.get())
            .as("DDL must run alongside the backup, not queue behind a lock the backup holds for its duration")
            .isGreaterThan(0);
      } finally {
        running.set(false);
        ddl.join(60_000);
      }
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The correctness half of the same change: with {@code CREATE TYPE} / {@code DROP TYPE} running for the whole
   * backup, the archive still restores to a database that opens and passes {@code CHECK DATABASE}, and its
   * {@code schema.json} names no type whose bucket files the archive does not contain.
   * <p>
   * This is a REGRESSION GUARD over the whole pipeline, not the discriminating proof of the point-in-time property:
   * the gap it hunts - a configuration read after t0 naming a bucket created after t0, which the window does not
   * carry - is microseconds wide, so a DDL loop cannot be relied on to land inside it. Verified: with the fix
   * reverted to "drop the lock, keep reading the configuration off the filesystem" this test still passes. What
   * pins the property deterministically is
   * {@code Issue6114SnapshotConfigurationCaptureTest.theWindowCarriesBothConfigurationFilesAsOfT0}, which moves the
   * live schema on while the window is open and asserts the window does not follow. What this test defends is that
   * the two ends are actually wired together, and that the churn the removed lock now permits does not break the
   * archive.
   */
  @Test
  void theArchiveIsConsistentUnderConcurrentDdl() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabase()) {
      final AtomicBoolean running = new AtomicBoolean(true);
      final AtomicReference<Exception> ddlFailure = new AtomicReference<>();
      final CountDownLatch ddlWarmedUp = new CountDownLatch(1);
      final AtomicInteger created = new AtomicInteger();

      final Thread ddl = new Thread(() -> {
        int sequence = 0;
        while (running.get()) {
          try {
            final String name = "Churn" + sequence++;
            database.getSchema().createDocumentType(name);
            created.incrementAndGet();
            ddlWarmedUp.countDown();
            // DROP EVERY OTHER ONE, SO THE DEFERRED FILE-DROP PATH (#6075 challenge C2) IS EXERCISED TOO
            if (sequence % 2 == 0)
              database.getSchema().dropType(name);
          } catch (final Exception e) {
            ddlFailure.compareAndSet(null, e);
            return;
          }
        }
      }, "issue6114-ddl-churn");
      ddl.setDaemon(true);
      ddl.start();

      try {
        assertThat(ddlWarmedUp.await(60, TimeUnit.SECONDS)).isTrue();

        new Backup(database, BACKUP_FILE).setVerboseLevel(0).setMaxMBPerSecond(MAX_MB_PER_SECOND).backupDatabase();

        running.set(false);
        ddl.join(60_000);
        assertThat(ddlFailure.get()).isNull();
        assertThat(created.get()).as("the DDL must really have churned the schema during the backup").isGreaterThan(5);
      } finally {
        running.set(false);
        ddl.join(60_000);
      }

      assertArchiveSchemaNamesNoMissingFile();

      new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

      try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
        assertThat(restored.command("sql", "check database").nextIfAvailable().<Long>getProperty("totalErrors")).isZero();
        assertThat(restored.countType(TYPE, false)).isEqualTo(RECORDS);
        // EVERY TYPE THE RESTORED SCHEMA NAMES RESOLVES ITS BUCKETS: A SCHEMA THAT OUTRAN THE PAGE IMAGE WOULD NOT
        for (final DocumentType type : restored.getSchema().getTypes())
          assertThat(type.getBuckets(false)).as("type '%s' must have its buckets", type.getName()).isNotEmpty();
      }
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * Reads the archive's own {@code schema.json} entry and checks that every bucket and index file it names is also
   * an entry of the same archive. This is the property in its purest form, before a restore gets a chance to be
   * forgiving about it.
   */
  private void assertArchiveSchemaNamesNoMissingFile() throws Exception {
    try (final ZipFile zip = new ZipFile(new File(BACKUP_FILE))) {
      final ZipEntry schemaEntry = zip.getEntry(LocalSchema.SCHEMA_FILE_NAME);
      assertThat(schemaEntry).as("the archive must carry a schema.json").isNotNull();

      final List<String> entries = new ArrayList<>();
      zip.stream().forEach(e -> entries.add(e.getName()));

      final JSONObject schema;
      try (final InputStream in = zip.getInputStream(schemaEntry)) {
        schema = new JSONObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }

      final JSONObject types = schema.getJSONObject("types", new JSONObject());
      final List<String> missing = new ArrayList<>();
      for (final String typeName : types.keySet()) {
        final JSONObject type = types.getJSONObject(typeName);
        for (final String bucket : type.getJSONArray("buckets", new JSONArray()).toListOfStrings()) {
          // A BUCKET FILE IS NAMED "<bucket>.<fileId>.<pageSize>.bucket", so the bucket name plus a dot is the prefix
          final String prefix = bucket + ".";
          if (entries.stream().noneMatch(name -> name.startsWith(prefix)))
            missing.add(typeName + " -> " + bucket);
        }
      }

      assertThat(missing)
          .as("schema.json in the archive must not name a bucket whose file the archive does not contain")
          .isEmpty();
    }
  }

  private Database createDatabase() {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.getSchema().createDocumentType(TYPE);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("id", i).set("payload", "x".repeat(500)).save();
    });
    ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
    return database;
  }
}
