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
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.MaintenanceCoordinator;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A {@code BACKUP DATABASE} statement that was asked to stop ends at the next chunk it reads, whatever its write rate cap,
 * whichever archive writer it uses and whether or not the caller has a transaction open: the partial archive is removed and
 * the maintenance slot and the progress entry are released on the way out.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class BackupStatementStopRequestTest extends TestHelper {
  private static final String BACKUP_DIRECTORY = "target/backups/stop-request";

  private final RunningQueryRegistry registry = new RunningQueryRegistry();
  private       String               savedBackupDirectory;

  @BeforeEach
  void prepare() {
    FileUtils.deleteRecursively(Path.of(BACKUP_DIRECTORY).toFile());
    savedBackupDirectory = GlobalConfiguration.SERVER_BACKUP_DIRECTORY.getValueAsString();
    GlobalConfiguration.SERVER_BACKUP_DIRECTORY.setValue(BACKUP_DIRECTORY);

    database.getSchema().createDocumentType("Doc").createProperty("payload", Type.STRING);
    database.transaction(() -> {
      for (int i = 0; i < 200; i++)
        database.newDocument("Doc").set("payload", "payload-" + i).save();
    });
  }

  @AfterEach
  void cleanup() {
    GlobalConfiguration.SERVER_BACKUP_DIRECTORY.setValue(savedBackupDirectory);
    FileUtils.deleteRecursively(Path.of(BACKUP_DIRECTORY).toFile());
  }

  @Test
  void aStatementStoppedBeforeItStartsWritesNoArchiveWithTheSingleThreadedWriter() {
    assertStoppedStatementLeavesNothing("backup database file://stopped.zip with compressionThreads = 0");
  }

  @Test
  void aStatementStoppedBeforeItStartsWritesNoArchiveWithTheParallelWriter() {
    assertStoppedStatementLeavesNothing("backup database file://stopped.zip with compressionThreads = 2");
    assertThat(compressorThreads()).as("the compression pool of the stopped backup is gone").isZero();
  }

  @Test
  void aStatementStoppedBeforeItStartsWritesNoArchiveWhenThrottled() {
    assertStoppedStatementLeavesNothing("backup database file://stopped.zip with maxMBPerSecond = 1");
  }

  @Test
  void aStoppedStatementReleasesTheMaintenanceSlot() {
    final HeldSlots coordinator = new HeldSlots();
    ((DatabaseInternal) database).setWrapper(MaintenanceCoordinator.WRAPPER_NAME, coordinator);
    try {
      assertStoppedStatementLeavesNothing("backup database file://stopped.zip");

      assertThat(coordinator.begun.get()).as("the statement took the slot").isEqualTo(1);
      assertThat(coordinator.ended.get()).as("and gave it back").isEqualTo(1);
    } finally {
      ((DatabaseInternal) database).setWrapper(MaintenanceCoordinator.WRAPPER_NAME, null);
    }

    // THE NEXT BACKUP OF THE SAME NAME RUNS: NOTHING IS LEFT BEHIND THAT WOULD REFUSE IT
    try (final ResultSet resultSet = database.command("sql", "backup database file://stopped.zip")) {
      assertThat(resultSet.next().<String>getProperty("result")).isEqualTo("OK");
    }
    assertThat(archive("stopped.zip")).exists();
  }

  @Test
  void aStoppedStatementKeepsTheCallersTransaction() {
    database.begin();
    try {
      database.newDocument("Doc").set("payload", "pending").save();

      try (final RunningQuery query = registry.register(database.getName(), "admin", "test", null, null,
          RunningQuery.Client.NONE)) {
        query.terminate("root");
        assertThatThrownBy(() -> database.command("sql", "backup database file://stopped.zip").close()).isInstanceOf(
            QueryTerminatedException.class);
      }

      assertThat(database.isTransactionActive()).as("the transaction of the caller is as it was").isTrue();
      assertThat(archive("stopped.zip")).doesNotExist();
      assertThat(archive("stopped.zip.partial")).doesNotExist();
    } finally {
      database.rollback();
    }
  }

  @Test
  void aStatementThatWasNotStoppedStillWritesItsArchive() {
    try (final RunningQuery ignored = registry.register(database.getName(), "admin", "test", null, null,
        RunningQuery.Client.NONE)) {
      try (final ResultSet resultSet = database.command("sql", "backup database file://kept.zip with compressionThreads = 2")) {
        assertThat(resultSet.next().<String>getProperty("result")).isEqualTo("OK");
      }
    }
    assertThat(archive("kept.zip")).exists();
    assertThat(archive("kept.zip").length()).isGreaterThan(0);
  }

  // A PLAIN HANG DETECTOR, NOT A LATENCY BOUND: THE STOP IS ASKED FOR WITH A RATE CAP THAT KEEPS THE BACKUP BUSY FOR HALF A MINUTE
  @Test
  @Timeout(value = 300, unit = TimeUnit.SECONDS)
  void aThrottledStatementStopsWhileItIsRunning() throws Exception {
    // ABOUT 24 MB OF PAGES: AT 1 MB/S THE BACKUP IS FAR FROM DONE WHEN THE STOP ARRIVES
    final String block = "x".repeat(80_000);
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newDocument("Doc").set("payload", block + i).save();
    });

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    try (final RunningQuery query = registry.register(database.getName(), "admin", "test", null, null,
        RunningQuery.Client.NONE)) {
      final Thread stopper = new Thread(() -> {
        // THE ARCHIVE IS CLAIMED BY ITS PARTIAL NAME BEFORE THE FIRST BYTE IS READ
        final File partial = archive("running.zip.partial");
        final long giveUp = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        try {
          while (!partial.exists() && System.nanoTime() < giveUp)
            Thread.sleep(5);
          // LET THE BACKUP SETTLE INTO ITS CAPPED READ LOOP BEFORE ASKING IT TO STOP
          Thread.sleep(300);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        query.terminate("root");
      }, "stopper");
      stopper.start();

      assertThatThrownBy(
          () -> database.command("sql", "backup database file://running.zip with maxMBPerSecond = 1, compressionThreads = 0")
              .close()).isInstanceOf(QueryTerminatedException.class);
      stopper.join();
    }

    watch.assertGaveUpWithin(12_000, "a backup that stops when asked from one that runs to the end of its capped rate");
    assertThat(archive("running.zip")).doesNotExist();
    assertThat(archive("running.zip.partial")).doesNotExist();
    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).isEmpty();
  }

  private void assertStoppedStatementLeavesNothing(final String statement) {
    try (final RunningQuery query = registry.register(database.getName(), "admin", "test", null, null,
        RunningQuery.Client.NONE)) {
      query.terminate("root");

      assertThatThrownBy(() -> database.command("sql", statement).close()).isInstanceOf(QueryTerminatedException.class)
          .hasMessageContaining("BACKUP DATABASE");
    }

    assertThat(archive("stopped.zip")).as("no archive under the final name").doesNotExist();
    assertThat(archive("stopped.zip.partial")).as("no partial archive").doesNotExist();
    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).as("no progress entry").isEmpty();
  }

  private File archive(final String name) {
    return new File(BACKUP_DIRECTORY + File.separator + database.getName() + File.separator + name);
  }

  private static long compressorThreads() {
    return Thread.getAllStackTraces().keySet().stream().filter(thread -> thread.isAlive() && thread.getName().startsWith(
        "arcadedb-backup-compressor")).count();
  }

  /** The admission policy a server binds, reduced to counting what the statement asks of it. */
  private static final class HeldSlots implements MaintenanceCoordinator {
    final AtomicInteger begun = new AtomicInteger();
    final AtomicInteger ended = new AtomicInteger();

    @Override
    public Operation begin(final String databaseName, final Operation operation) {
      begun.incrementAndGet();
      return null;
    }

    @Override
    public void end(final String databaseName, final Operation operation) {
      ended.incrementAndGet();
    }
  }
}
