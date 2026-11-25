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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12251: on the leader, the callbacks a commit owes (the after-commit ones, and the after-completion ones, such as the refresh
 * of a continuous aggregate or of a materialized view) ran while the commit still held the database read lock and was still counted
 * at the recording-session gate (#12098). A callback is work of its own, with a transaction and a commit of its own, and that commit
 * waits for any schema session in progress. The session of another client waits for the commits past the gate and then for the write
 * lock: waiting for the very commit that waited for it, until the quorum timeout. Every TIMESERIES insert into a type with a
 * continuous aggregate ends this way (its samples commit in a transaction of their own, and the statement's transaction owes the
 * aggregate a refresh), and with several writers every insert took 10 to 60 s.
 * <p>
 * The callbacks now run once the commit has left the gate and every lock, whichever arm of the commit concluded it: a schema change
 * made while they run is not held off by them. The quorum timeout is raised above the 20 s the callbacks wait for the schema change,
 * so that a session that gives up on the gate by timing out cannot pass as one that did not wait for it.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12251CommitCallbacksOutsideLocksTest extends BaseRaftHATest {
  private static final long CALLBACK_WAIT_MS = 20_000L;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 3 * CALLBACK_WAIT_MS);
  }

  /** A callback that makes a schema change on a thread of its own, as the late-sample record of a concurrent insert does. */
  private static final class SchemaChangeFromCallback {
    private final Database                   db;
    private final String                     typeName;
    private final AtomicBoolean              callbackRan              = new AtomicBoolean();
    private final AtomicBoolean              endedWhileTheCallbackRan = new AtomicBoolean();
    private final AtomicReference<Throwable> failure                  = new AtomicReference<>();
    private volatile Thread                  schemaChange;

    private SchemaChangeFromCallback(final Database db, final String typeName) {
      this.db = db;
      this.typeName = typeName;
    }

    /** The callback: starts the schema change and waits for it, as long as a hang detector allows. */
    private Runnable callback() {
      return () -> {
        start();
        awaitEndInCallback();
      };
    }

    private void start() {
      callbackRan.set(true);
      schemaChange = new Thread(() -> {
        try {
          db.command("sql", "CREATE DOCUMENT TYPE " + typeName).close();
        } catch (final Throwable t) {
          failure.set(t);
        }
      }, "schema-change-" + typeName);
      schemaChange.start();
    }

    private void awaitEndInCallback() {
      join(CALLBACK_WAIT_MS);
      endedWhileTheCallbackRan.set(!schemaChange.isAlive());
    }

    private void join(final long millis) {
      try {
        schemaChange.join(millis);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    private void assertCompletedWhileTheCallbackRan(final String commit) {
      try {
        assertThat(callbackRan.get()).as("the " + commit + " ran its callback").isTrue();
        assertThat(endedWhileTheCallbackRan.get())
            .as("the schema change completed while the callback of the " + commit + " was running").isTrue();
        assertThat(failure.get()).isNull();
        assertThat(db.getSchema().existsType(typeName)).isTrue();
      } finally {
        // a hang detector: the thread is released by the quorum timeout at the latest
        if (schemaChange != null)
          join(2 * CALLBACK_WAIT_MS);
      }
    }
  }

  private Database leaderDatabase() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    return getServerDatabase(leaderIndex, getDatabaseName());
  }

  @Test
  void aSchemaChangeMadeWhileTheCallbacksOfAReadOnlyCommitRunIsNotHeldOffByThem() {
    final Database leaderDb = leaderDatabase();
    final SchemaChangeFromCallback probe = new SchemaChangeFromCallback(leaderDb, "Issue12251");

    // nothing written: the commit takes the read-only arm, as the transaction of a TIMESERIES insert does
    leaderDb.transaction(() -> ((DatabaseInternal) leaderDb).getTransaction()
        .addAfterCompletionCallbackIfAbsent("issue12251", probe.callback()));

    probe.assertCompletedWhileTheCallbackRan("read-only commit");
  }

  /** The after-commit callbacks (the ones a materialized view registers) run outside the locks too. */
  @Test
  void aSchemaChangeMadeWhileTheAfterCommitCallbackOfAReadOnlyCommitRunsIsNotHeldOffByIt() {
    final Database leaderDb = leaderDatabase();
    final SchemaChangeFromCallback probe = new SchemaChangeFromCallback(leaderDb, "Issue12251AfterCommit");

    leaderDb.transaction(() -> ((DatabaseInternal) leaderDb).getTransaction().addAfterCommitCallback(probe.callback()));

    probe.assertCompletedWhileTheCallbackRan("read-only commit");
  }

  /**
   * The commit that writes concludes under the read lock too (its tail takes it again to publish), and is counted at the gate until
   * it ends: the callbacks of a transaction with writes, a materialized view refresh over what it wrote, owe the same.
   */
  @Test
  void aSchemaChangeMadeWhileTheCallbacksOfACommitThatWritesRunIsNotHeldOffByThem() {
    final Database leaderDb = leaderDatabase();
    leaderDb.command("sql", "CREATE DOCUMENT TYPE Issue12251Written").close();
    final SchemaChangeFromCallback afterCompletion = new SchemaChangeFromCallback(leaderDb, "Issue12251WrittenCompletion");
    final SchemaChangeFromCallback afterCommit = new SchemaChangeFromCallback(leaderDb, "Issue12251WrittenAfterCommit");

    leaderDb.transaction(() -> {
      leaderDb.newDocument("Issue12251Written").set("origin", "writer").save();
      ((DatabaseInternal) leaderDb).getTransaction().addAfterCommitCallback(afterCommit.callback());
      ((DatabaseInternal) leaderDb).getTransaction().addAfterCompletionCallbackIfAbsent("issue12251-written", afterCompletion.callback());
    });

    afterCommit.assertCompletedWhileTheCallbackRan("commit that writes");
    afterCompletion.assertCompletedWhileTheCallbackRan("commit that writes");
    assertThat(countOn(leaderDb, "Issue12251Written", "writer")).isEqualTo(1L);
  }

  /**
   * What a rollback owes (#10687: the refresh of an aggregate over samples a rolled back transaction wrote beside itself) is run
   * outside the database read lock the rollback takes, for the same reason.
   */
  @Test
  void aSchemaChangeMadeWhileTheCallbacksOfARollbackRunIsNotHeldOffByThem() {
    final Database leaderDb = leaderDatabase();
    final SchemaChangeFromCallback probe = new SchemaChangeFromCallback(leaderDb, "Issue12251RolledBack");

    leaderDb.begin();
    ((DatabaseInternal) leaderDb).getTransaction().addAfterCompletionCallbackIfAbsent("issue12251-rollback", probe.callback());
    leaderDb.rollback();

    probe.assertCompletedWhileTheCallbackRan("rollback");
  }

  /**
   * The rollback of every nested level, which the HTTP handler runs for a request that ends with a transaction open, owes the same
   * after the one read lock it takes for all of them: the callbacks of each level run in the order the levels were rolled back, the
   * innermost first, and no level is left deferring what it owes.
   */
  @Test
  void aSchemaChangeMadeWhileTheCallbacksOfARollbackOfAllNestedLevelsRunIsNotHeldOffByThem() {
    final Database leaderDb = leaderDatabase();
    final SchemaChangeFromCallback outer = new SchemaChangeFromCallback(leaderDb, "Issue12251OuterLevel");
    final SchemaChangeFromCallback inner = new SchemaChangeFromCallback(leaderDb, "Issue12251InnerLevel");
    final List<String> order = Collections.synchronizedList(new ArrayList<>());

    leaderDb.begin();
    ((DatabaseInternal) leaderDb).getTransaction().addAfterCompletionCallbackIfAbsent("issue12251-outer", () -> {
      order.add("outer");
      outer.callback().run();
    });
    leaderDb.begin();
    ((DatabaseInternal) leaderDb).getTransaction().addAfterCompletionCallbackIfAbsent("issue12251-inner", () -> {
      order.add("inner");
      inner.callback().run();
    });
    leaderDb.rollbackAllNested();

    inner.assertCompletedWhileTheCallbackRan("rollback of the inner level");
    outer.assertCompletedWhileTheCallbackRan("rollback of the outer level");
    assertThat(order).containsExactly("inner", "outer");
    assertThat(leaderDb.isTransactionActive()).isFalse();
    final TransactionContext left = ((DatabaseInternal) leaderDb).getTransactionIfExists();
    assertThat(left == null || !left.isDeferringCallbacks()).as("nothing is left deferring").isTrue();
  }

  /**
   * The cycle itself, as the refresh of a continuous aggregate closes it: the callback of a commit starts a schema session, and once
   * the session has claimed the file recording it commits a transaction of its own, which waits for that session. The session must
   * not be waiting for the commit that runs the callback, or neither ends before the quorum timeout.
   */
  @Test
  void aCommitMadeByACallbackWhileASchemaSessionIsOpenDoesNotWaitForTheCommitThatRanIt() {
    final Database leaderDb = leaderDatabase();
    leaderDb.command("sql", "CREATE DOCUMENT TYPE Issue12251Refresh").close();
    final SchemaChangeFromCallback probe = new SchemaChangeFromCallback(leaderDb, "Issue12251Session");
    final AtomicBoolean refreshCommitted = new AtomicBoolean();
    final AtomicReference<Throwable> refreshFailure = new AtomicReference<>();

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    leaderDb.transaction(() -> ((DatabaseInternal) leaderDb).getTransaction().addAfterCompletionCallbackIfAbsent("issue12251-cycle", () -> {
      probe.start();
      // the session has claimed the file recording, or ended: either way the commit below meets it as the refresh of an aggregate does
      final long deadline = System.currentTimeMillis() + CALLBACK_WAIT_MS;
      while (probe.schemaChange.isAlive() && ((DatabaseInternal) leaderDb).getFileManager().getRecordedChanges() == null
          && System.currentTimeMillis() < deadline)
        LockSupport.parkNanos(1_000_000L);
      try {
        leaderDb.transaction(() -> leaderDb.newDocument("Issue12251Refresh").set("origin", "refresh").save());
        refreshCommitted.set(true);
      } catch (final Throwable t) {
        refreshFailure.set(t);
      }
      probe.awaitEndInCallback();
    }));

    probe.assertCompletedWhileTheCallbackRan("commit that ran the refresh");
    assertThat(refreshFailure.get()).isNull();
    assertThat(refreshCommitted.get()).isTrue();
    stopwatch.assertGaveUpWithin(CALLBACK_WAIT_MS,
        "the refresh commit and the schema session waited for each other, which ends only at the quorum timeout (60 s here)");
    assertThat(countOn(leaderDb, "Issue12251Refresh", "refresh")).isEqualTo(1L);
  }

  /**
   * A nested commit runs its callbacks while its own context is still on top of the stack: popped before them, the enclosing
   * transaction was on top, a callback's {@code database.transaction()} joined it, and the rollback of the enclosing transaction took
   * back what the callback had committed. Both arms, the one with nothing to write and the one that writes.
   */
  @Test
  void theCallbackOfANestedReadOnlyCommitDoesNotJoinTheEnclosingTransaction() {
    assertTheCallbackOfANestedCommitDoesNotJoinTheEnclosingTransaction(false);
  }

  @Test
  void theCallbackOfANestedCommitThatWritesDoesNotJoinTheEnclosingTransaction() {
    assertTheCallbackOfANestedCommitDoesNotJoinTheEnclosingTransaction(true);
  }

  private void assertTheCallbackOfANestedCommitDoesNotJoinTheEnclosingTransaction(final boolean nestedWrites) {
    final Database leaderDb = leaderDatabase();
    leaderDb.command("sql", "CREATE DOCUMENT TYPE Issue12251Nested").close();

    final AtomicReference<Boolean> enclosingActiveInCallback = new AtomicReference<>();
    leaderDb.begin();
    try {
      leaderDb.newDocument("Issue12251Nested").set("origin", "enclosing").save();

      leaderDb.begin();
      if (nestedWrites)
        leaderDb.newDocument("Issue12251Nested").set("origin", "nested").save();
      ((DatabaseInternal) leaderDb).getTransaction().addAfterCompletionCallbackIfAbsent("issue12251-nested", () -> {
        enclosingActiveInCallback.set(leaderDb.isTransactionActive());
        leaderDb.transaction(() -> leaderDb.newDocument("Issue12251Nested").set("origin", "callback").save());
      });
      leaderDb.commit();
    } finally {
      leaderDb.rollback();
    }

    assertThat(enclosingActiveInCallback.get()).as("the callback ran with no transaction of the caller's on top").isFalse();
    assertThat(countOn(leaderDb, "Issue12251Nested", "callback")).as("what the callback committed survives the enclosing rollback")
        .isEqualTo(1L);
    assertThat(countOn(leaderDb, "Issue12251Nested", "enclosing")).isZero();
    assertThat(countOn(leaderDb, "Issue12251Nested", "nested")).as("the nested commit, a commit of its own").isEqualTo(nestedWrites ? 1L : 0L);
  }

  private static long countOn(final Database db, final String typeName, final String origin) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM " + typeName + " WHERE origin = ?", origin)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
