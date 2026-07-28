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
package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.async.NewRecordCallback;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5418: engine background threads must never outlive the JVM's ability to exit, nor the database they
 * belong to.
 * <p>
 * Two independent guarantees are asserted here:
 * <ol>
 *   <li>every thread the engine starts on behalf of an open database is a DAEMON thread, so an embedder that
 *   leaks a {@link Database} handle (a crashed test, an exception path skipping close) can still exit:
 *   {@code DestroyJavaVM} waits for non-daemon threads only;</li>
 *   <li>the JVM shutdown hook installed by {@link DatabaseFactory} closes whatever is still open, so the
 *   daemonization above never turns into data loss - the pages reach the disk during shutdown.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DatabaseLifecycleBackgroundThreadsTest {
  private static final String DB_PATH            = "./target/databases/issue-5418-lifecycle";
  private static final String TIMER_THREAD_PREFIX = "VectorIndex-InactivityTimer-";

  @AfterEach
  public void cleanUp() {
    final Database db = DatabaseFactory.getActiveDatabaseInstance(DB_PATH);
    if (db != null && db.isOpen())
      db.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  public void engineBackgroundThreadsAreDaemon() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database database = factory.create();
      try {
        database.command("sql", "CREATE DOCUMENT TYPE Doc");

        // TOUCH THE ASYNC API TOO, SO ITS WORKER THREADS ARE STARTED AND CAN BE CHECKED
        database.async().setParallelLevel(2);
        database.async().createRecord(database.newDocument("Doc").set("id", 0), (NewRecordCallback) null, null);
        database.async().waitCompletion(30_000);

        // AND THE SCRIPTING ENGINE, WHOSE USER-CODE POOL IS ANOTHER SET OF WORKERS SPAWNED ON DEMAND
        database.command("js", "3 + 5").close();

        final List<Thread> nonDaemon = new ArrayList<>();
        for (final Thread t : liveThreads()) {
          if (t.isDaemon())
            continue;
          final String name = t.getName();
          if (name.startsWith("ArcadeDB") || name.startsWith("AsyncExecutor-") || name.startsWith("arcadedb-"))
            nonDaemon.add(t);
        }

        assertThat(nonDaemon)
            .as("An engine background thread that is not a daemon keeps the embedder's JVM alive forever when a Database handle is leaked (issue #5418)")
            .isEmpty();
      } finally {
        database.drop();
      }
    }
  }

  @Test
  public void shutdownHookClosesLeakedDatabases() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    final Database database = factory.create();
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.begin();
    database.newDocument("Doc").set("id", 1).save();
    database.commit();

    // LEAKED ON PURPOSE: NO close() HERE. THE JVM SHUTDOWN HOOK MUST TAKE CARE OF IT
    assertThat(DatabaseFactory.getActiveDatabaseInstances()).contains(database);

    DatabaseFactory.closeActiveDatabaseInstances();

    assertThat(database.isOpen()).as("The shutdown hook must close the leaked database").isFalse();
    assertThat(DatabaseFactory.getActiveDatabaseInstances()).doesNotContain(database);

    // AND THE CLOSE MUST BE A GRACEFUL ONE: THE COMMITTED RECORD IS READABLE AGAIN AFTER REOPENING
    try (final DatabaseFactory reopenFactory = new DatabaseFactory(DB_PATH)) {
      final Database reopened = reopenFactory.open(ComponentFile.MODE.READ_WRITE);
      try {
        assertThat(reopened.countType("Doc", true)).isEqualTo(1);
      } finally {
        reopened.drop();
      }
    }
  }

  @Test
  public void vectorIndexInactivityTimerIsCancelledOnDatabaseClose() {
    withArmedVectorInactivityTimer((database, timerThreadName) -> {
      database.close();

      assertThat(awaitTimerThreadDeath(timerThreadName))
          .as("Database.close() must cancel the inactivity rebuild timers of its vector indexes, otherwise they fire on a closed database (issue #5418)")
          .isEmpty();
    });
  }

  @Test
  public void vectorIndexInactivityTimerIsCancelledOnDatabaseDrop() {
    withArmedVectorInactivityTimer((database, timerThreadName) -> {
      database.drop();

      assertThat(awaitTimerThreadDeath(timerThreadName))
          .as("Database.drop() must cancel the inactivity rebuild timers of its vector indexes, otherwise they fire on a database whose files no longer exist (issue #5418)")
          .isEmpty();
    });
  }

  /**
   * Creates a database with an LSM_VECTOR index holding a handful of buffered mutations - too few to reach the
   * rebuild threshold, so the inactivity timer is armed and, with a 60s window, still armed (not yet fired) when
   * the callback disposes of the database. The callback receives the name of THIS index's timer thread, so a
   * timer leaked by an unrelated test class running in the same JVM cannot influence the outcome.
   */
  private void withArmedVectorInactivityTimer(final BiConsumer<Database, String> callback) {
    final int timeoutMs = GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.getValueAsInteger();
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(60_000);
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database database = factory.create();
      try {
        database.command("sql", "CREATE DOCUMENT TYPE Doc");
        database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA {dimensions: 4, similarity: 'COSINE'}");

        database.begin();
        for (int i = 0; i < 3; i++)
          database.newDocument("Doc").set("id", i).set("vector", new float[] { i, i + 1f, i + 2f, i + 3f }).save();
        database.commit();

        final List<Thread> armed = inactivityTimerThreads(TIMER_THREAD_PREFIX);
        assertThat(armed).as("The vector index inactivity rebuild timer must be armed after a few buffered mutations")
            .hasSize(1);

        callback.accept(database, armed.getFirst().getName());
      } finally {
        if (database.isOpen())
          database.close();
        GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(timeoutMs);
      }
    }
  }

  private static List<Thread> inactivityTimerThreads(final String namePrefix) {
    final List<Thread> result = new ArrayList<>();
    for (final Thread t : liveThreads())
      if (t.isAlive() && t.getName().startsWith(namePrefix))
        result.add(t);
    return result;
  }

  /** A cancelled {@link java.util.Timer} thread exits asynchronously: give it a bounded window to die. */
  private static List<Thread> awaitTimerThreadDeath(final String threadName) {
    List<Thread> alive = inactivityTimerThreads(threadName);
    for (int retry = 0; retry < 100 && !alive.isEmpty(); retry++) {
      try {
        Thread.sleep(20);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      alive = inactivityTimerThreads(threadName);
    }
    return alive;
  }

  private static Set<Thread> liveThreads() {
    return Thread.getAllStackTraces().keySet();
  }
}
