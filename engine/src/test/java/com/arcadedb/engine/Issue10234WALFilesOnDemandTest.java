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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10234: every open database kept a {@code java.util.Timer} thread of its own for the WAL
 * housekeeping, and one open WAL file per core from the moment it opened, so a server holding thousands of databases held
 * thousands of threads and tens of thousands of file descriptors before any request. The housekeeping now runs on one
 * scheduler shared by every database, a WAL file is opened by the first commit that needs it, and the housekeeping
 * releases a WAL file nobody wrote for {@link GlobalConfiguration#TX_WAL_IDLE_TIMEOUT}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10234WALFilesOnDemandTest extends TestHelper {

  @Test
  void aDatabaseThatIsOnlyReadHoldsNoWALFile() {
    database.getSchema().createDocumentType("Doc").createProperty("name", Type.STRING);
    database.transaction(() -> database.newDocument("Doc").set("name", "a").save());

    reopenDatabase();
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    assertThat(transactionManager.getOpenWALFileCountForTesting()).isZero();
    assertThat(walFiles()).isEmpty();

    assertThat(database.query("sql", "SELECT FROM Doc").stream().count()).isEqualTo(1);
    assertThat(transactionManager.getOpenWALFileCountForTesting()).isZero();

    // THE FIRST COMMIT OPENS ONE, ON THE SLOT OF ITS THREAD ONLY
    database.transaction(() -> database.newDocument("Doc").set("name", "b").save());
    assertThat(transactionManager.getOpenWALFileCountForTesting()).isEqualTo(1);
    assertThat(walFiles()).hasSize(1);
  }

  @Test
  void anIdleWALFileIsReleasedAndAFreshOneOpenedByTheNextCommit() {
    database.getSchema().createDocumentType("Doc").createProperty("name", Type.STRING);
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    // THE PASSES ARE DRIVEN BY HAND
    transactionManager.cancelHousekeepingForTesting();
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_IDLE_TIMEOUT, 1L);

    database.transaction(() -> database.newDocument("Doc").set("name", "first").save());
    assertThat(transactionManager.getOpenWALFileCountForTesting()).isEqualTo(1);
    final List<File> before = walFiles();

    // RELEASED ONLY ONCE ITS PAGES REACHED THE DATA FILES, AND DROPPED BY THE SAME PASS AFTER THE FSYNC
    ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
    waitForIdleTimeout();
    transactionManager.runWALHousekeeping();
    assertThat(transactionManager.getOpenWALFileCountForTesting()).isZero();
    assertThat(walFiles()).isEmpty();
    assertThat(before.getFirst()).doesNotExist();

    database.transaction(() -> database.newDocument("Doc").set("name", "second").save());
    assertThat(transactionManager.getOpenWALFileCountForTesting()).isEqualTo(1);
    assertThat(walFiles()).hasSize(1).doesNotContainAnyElementsOf(before);

    reopenDatabase();
    assertThat(database.query("sql", "SELECT FROM Doc").stream().count()).isEqualTo(2);
  }

  @Test
  void commitsRacingTheReleaseOfIdleWALFilesLoseNothing() throws Exception {
    database.getSchema().createDocumentType("Doc").createProperty("n", Type.INTEGER);
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    transactionManager.cancelHousekeepingForTesting();
    // EVERY FILE IS IDLE AS SOON AS ITS PAGES ARE FLUSHED: THE PASSES BELOW RELEASE FILES WHILE COMMITS PICK THEM
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_IDLE_TIMEOUT, 1L);

    final int threads = 4;
    final int perThread = 300;
    final AtomicBoolean writing = new AtomicBoolean(true);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread housekeeping = new Thread(() -> {
      while (writing.get())
        try {
          transactionManager.runWALHousekeeping();
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
    }, "issue10234-housekeeping");

    final List<Thread> writers = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      final int base = t * perThread;
      writers.add(new Thread(() -> {
        try {
          for (int i = 0; i < perThread; i++) {
            final int n = base + i;
            database.transaction(() -> database.newDocument("Doc").set("n", n).save());
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }, "issue10234-writer-" + t));
    }

    housekeeping.start();
    writers.forEach(Thread::start);
    for (final Thread writer : writers)
      writer.join();
    writing.set(false);
    housekeeping.join();

    assertThat(failure.get()).isNull();
    assertThat(database.countType("Doc", false)).isEqualTo(threads * perThread);

    // AND WHAT THE RACING COMMITS WROTE IS ON DISK AFTER A REOPEN
    reopenDatabase();
    assertThat(database.query("sql", "SELECT count(*) AS c FROM Doc").next().<Long>getProperty("c")).isEqualTo(
        (long) threads * perThread);
  }

  /**
   * The #7479 check moved from the open to the first commit that opens a WAL file: a name another live instance already
   * holds must fail that commit loudly and fence the database, never retry it into another file.
   */
  @Test
  void aWALFileOpenedOnDemandThatAnotherInstanceHoldsFencesTheDatabase() throws Exception {
    database.getSchema().createDocumentType("Doc").createProperty("name", Type.STRING);
    reopenDatabase();
    final LocalDatabase db = (LocalDatabase) database;
    final TransactionManager transactionManager = db.getTransactionManager();
    transactionManager.cancelHousekeepingForTesting();
    assertThat(transactionManager.getOpenWALFileCountForTesting()).isZero();

    final String collidingPath = db.getDatabasePath() + File.separator + "txlog_" + transactionManager.getLogFileCounterForTesting()
        + ".wal";
    // STANDS IN FOR ANOTHER LIVE INSTANCE THAT CLAIMED THE NAME THE COMMIT IS ABOUT TO OPEN: A WALFile LOCKS ITS FILE FOR LIFE
    final WALFile anotherInstanceWAL = new WALFile(collidingPath);
    try {
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Doc").set("name", "x").save()))
          .hasMessageContaining("already open by another process or database instance");
      assertThat(db.isFencedForRecovery()).isTrue();
    } finally {
      anotherInstanceWAL.close();
      new File(collidingPath).delete();
    }

    database.close();
  }

  @Test
  void anActiveWALFileIsKept() {
    database.getSchema().createDocumentType("Doc").createProperty("name", Type.STRING);
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    transactionManager.cancelHousekeepingForTesting();

    // THE DEFAULT TIMEOUT, A MINUTE: A FILE WRITTEN A MOMENT AGO STAYS
    database.transaction(() -> database.newDocument("Doc").set("name", "a").save());
    ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
    transactionManager.runWALHousekeeping();
    assertThat(transactionManager.getOpenWALFileCountForTesting()).isEqualTo(1);

    // AND A TIMEOUT OF 0 NEVER RELEASES ONE
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_IDLE_TIMEOUT, 0L);
    waitForIdleTimeout();
    transactionManager.runWALHousekeeping();
    assertThat(transactionManager.getOpenWALFileCountForTesting()).isEqualTo(1);
  }

  @Test
  void databasesShareTheHousekeepingThreads() {
    final List<Database> databases = new ArrayList<>();
    final List<String> paths = new ArrayList<>();
    try {
      for (int i = 0; i < 6; i++) {
        final String path = getDatabasePath() + "_shared_" + i;
        FileUtils.deleteRecursively(new File(path));
        paths.add(path);
        databases.add(new DatabaseFactory(path).create());
      }

      for (final Database db : databases) {
        assertThat(((DatabaseInternal) db).getTransactionManager().isHousekeepingScheduled()).isTrue();
        // NO TIMER THREAD OF ITS OWN ANY MORE
        assertThat(threadsNamed("ArcadeDB TransactionManager " + db.getName())).isZero();
      }
      assertThat(threadsNamed("ArcadeDB WAL Housekeeping-")).isLessThanOrEqualTo(2);
    } finally {
      for (final Database db : databases) {
        final TransactionManager transactionManager = ((DatabaseInternal) db).getTransactionManager();
        db.drop();
        assertThat(transactionManager.isHousekeepingScheduled()).isFalse();
      }
      for (final String path : paths)
        FileUtils.deleteRecursively(new File(path));
    }
  }

  private static long threadsNamed(final String prefix) {
    return Thread.getAllStackTraces().keySet().stream().filter(t -> t.getName().startsWith(prefix)).count();
  }

  private static void waitForIdleTimeout() {
    // A FEW MILLISECONDS PAST A 1 MS TIMEOUT: A LOWER BOUND THE STALL OF A LOADED JVM ONLY MAKES MORE TRUE
    try {
      Thread.sleep(20);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private List<File> walFiles() {
    final File[] files = new File(getDatabasePath()).listFiles((dir, name) -> name.endsWith(".wal"));
    return files == null ? List.of() : List.of(files);
  }
}
