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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8383: the #8282 fix routed only {@code ServerDatabase.commit()} through the database's CURRENT wrapper. Every
 * other method of a handle resolved before the HA wrap (or built around a wrapper a plugin restart has since
 * replaced) still ran on the instance it captured - most importantly {@code command()}, where
 * {@code RaftReplicatedDatabase} decides that a write on a follower is forwarded to the leader instead of executed
 * locally. A stale handle never reached that decision.
 * <p>
 * The wrapper is a {@link Proxy} installed through {@code LocalDatabase.setWrappedDatabaseInstance()}, the call
 * {@code RaftReplicatedDatabase}'s constructor makes. It counts every method it is asked for, by name, and runs it on the
 * real database: enough to tell "went through the wrapper" from "went around it".
 */
class Issue8383StaleServerDatabaseRoutingTest extends TestHelper {

  private static final String TYPE = "Issue8383Doc";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE).createProperty("name", Type.STRING);
    // A server opens its databases with auto-transaction on, which is what lets a bare INSERT run on a handle.
    database.setAutoTransaction(true);
  }

  @AfterEach
  void removeWrapper() {
    if (database != null && database.isOpen())
      local().setWrappedDatabaseInstance(local());
  }

  private LocalDatabase local() {
    return (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
  }

  private DatabaseInternal installWrapper(final Map<String, AtomicInteger> calls) {
    final LocalDatabase real = local();
    final DatabaseInternal wrapper = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          calls.computeIfAbsent(method.getName(), k -> new AtomicInteger()).incrementAndGet();
          try {
            return method.invoke(real, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
    real.setWrappedDatabaseInstance(wrapper);
    return wrapper;
  }

  private static int count(final Map<String, AtomicInteger> calls, final String method) {
    final AtomicInteger c = calls.get(method);
    return c == null ? 0 : c.get();
  }

  private static void drain(final ResultSet rs) {
    try (rs) {
      while (rs.hasNext())
        rs.next();
    }
  }

  @Test
  void everyCommandOverloadOnAStaleHandleReachesTheWrapper() {
    final ServerDatabase staleHandle = new ServerDatabase(null, local());
    final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
    installWrapper(calls);

    final ContextConfiguration cfg = new ContextConfiguration();
    drain(staleHandle.command("sql", "INSERT INTO " + TYPE + " SET name = 'a'"));
    drain(staleHandle.command("sql", "INSERT INTO " + TYPE + " SET name = ?", "b"));
    drain(staleHandle.command("sql", "INSERT INTO " + TYPE + " SET name = :n", Map.of("n", "c")));
    drain(staleHandle.command("sql", "INSERT INTO " + TYPE + " SET name = ?", cfg, "d"));
    drain(staleHandle.command("sql", "INSERT INTO " + TYPE + " SET name = :n", cfg, Map.<String, Object>of("n", "e")));

    // The leader forward of RaftReplicatedDatabase lives in command(): a handle that never calls it never forwards.
    assertThat(count(calls, "command")).as("each command on the stale handle must reach the current wrapper, once")
        .isEqualTo(5);
    assertThat(database.countType(TYPE, true)).isEqualTo(5L);
  }

  @Test
  void everyQueryAndExecuteOverloadOnAStaleHandleReachesTheWrapper() {
    database.transaction(() -> database.newDocument(TYPE).set("name", "q").save());
    final ServerDatabase staleHandle = new ServerDatabase(null, local());
    final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
    installWrapper(calls);

    drain(staleHandle.query("sql", "SELECT FROM " + TYPE));
    drain(staleHandle.query("sql", "SELECT FROM " + TYPE + " WHERE name = ?", "q"));
    drain(staleHandle.query("sql", "SELECT FROM " + TYPE + " WHERE name = :n", Map.of("n", "q")));
    // query() on the wrapper is where a follower applies the read-consistency barrier.
    assertThat(count(calls, "query")).isEqualTo(3);

    drain(staleHandle.execute("sql", "SELECT FROM " + TYPE + ";", Map.of()));
    drain(staleHandle.execute("sql", "SELECT FROM " + TYPE + ";"));
    assertThat(count(calls, "execute")).isEqualTo(2);
  }

  @Test
  void transactionBoundariesOnAStaleHandleReachTheWrapper() {
    final ServerDatabase staleHandle = new ServerDatabase(null, local());
    final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
    installWrapper(calls);

    // Counted one boundary at a time: transaction() below also calls begin()/commit()/rollback() on the wrapper from
    // INSIDE LocalDatabase, which would make an aggregate count pass without the handle ever reaching it.
    staleHandle.begin();
    assertThat(count(calls, "begin")).as("begin() gates a client out while the directory is replaced").isEqualTo(1);
    staleHandle.newDocument(TYPE).set("name", "rolled-back").save();
    staleHandle.rollback();
    assertThat(count(calls, "rollback")).isEqualTo(1);

    staleHandle.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    assertThat(count(calls, "begin")).isEqualTo(2);
    staleHandle.rollbackAllNested();
    assertThat(count(calls, "rollbackAllNested")).isEqualTo(1);

    staleHandle.transaction(() -> staleHandle.newDocument(TYPE).set("name", "tx-1").save());
    staleHandle.transaction(() -> staleHandle.newDocument(TYPE).set("name", "tx-2").save(), false);
    staleHandle.transaction(() -> staleHandle.newDocument(TYPE).set("name", "tx-3").save(), false, 1);
    staleHandle.transaction(() -> staleHandle.newDocument(TYPE).set("name", "tx-4").save(), false, 1, null, null);

    assertThat(count(calls, "transaction")).isEqualTo(4);
    assertThat(database.countType(TYPE, true)).isEqualTo(4L);
  }

  @Test
  void readsOnAStaleHandleReachTheWrapper() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(TYPE).set("name", "r").save().getIdentity());
    final ServerDatabase staleHandle = new ServerDatabase(null, local());
    final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
    installWrapper(calls);

    // The HA wrapper refuses these while the database directory is being replaced (issue #8363).
    assertThat(staleHandle.countType(TYPE, true)).isEqualTo(1L);
    assertThat(staleHandle.existsRecord(rid[0])).isTrue();
    assertThat(staleHandle.lookupByRID(rid[0], true)).isNotNull();
    assertThat(staleHandle.iterateType(TYPE, true).hasNext()).isTrue();
    staleHandle.scanType(TYPE, true, doc -> true);

    assertThat(count(calls, "countType")).isEqualTo(1);
    assertThat(count(calls, "existsRecord")).isEqualTo(1);
    assertThat(count(calls, "lookupByRID")).isEqualTo(1);
    assertThat(count(calls, "iterateType")).isEqualTo(1);
    assertThat(count(calls, "scanType")).isEqualTo(1);
  }

  @Test
  void aHandleBuiltAroundAReplacedWrapperCommandsThroughTheCurrentOne() {
    // rewrapDatabases() on a plugin restart installs a NEW wrapper on the same LocalDatabase; the old one holds the
    // Raft server the restart discarded, so its isLeader() and its forward must not be the ones consulted.
    final Map<String, AtomicInteger> oldCalls = new ConcurrentHashMap<>();
    final ServerDatabase handleOnOldWrapper = new ServerDatabase(null, installWrapper(oldCalls));
    final Map<String, AtomicInteger> newCalls = new ConcurrentHashMap<>();
    installWrapper(newCalls);

    drain(handleOnOldWrapper.command("sql", "INSERT INTO " + TYPE + " SET name = 'rewrapped'"));
    drain(handleOnOldWrapper.query("sql", "SELECT FROM " + TYPE));

    assertThat(count(newCalls, "command")).as("the command must reach the wrapper installed last").isEqualTo(1);
    assertThat(count(newCalls, "query")).isEqualTo(1);
    assertThat(count(oldCalls, "command")).as("and not the one it replaced").isZero();
    assertThat(count(oldCalls, "query")).isZero();
    assertThat(database.countType(TYPE, true)).isEqualTo(1L);
  }

  @Test
  void theProfilingPathOfAStaleHandleReachesTheWrapperToo() {
    final ServerQueryProfiler profiler = mock(ServerQueryProfiler.class);
    when(profiler.isRecording()).thenReturn(true);
    final ArcadeDBServer server = FakeArcadeDBServer.create().queryProfiler(profiler);

    final ServerDatabase staleHandle = new ServerDatabase(server, local());
    final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
    installWrapper(calls);

    drain(staleHandle.command("sql", "INSERT INTO " + TYPE + " SET name = 'profiled'"));
    drain(staleHandle.command("sql", "INSERT INTO " + TYPE + " SET name = :n", Map.of("n", "profiled-2")));
    drain(staleHandle.query("sql", "SELECT FROM " + TYPE));

    assertThat(count(calls, "command")).isEqualTo(2);
    assertThat(count(calls, "query")).isEqualTo(1);
    assertThat(database.countType(TYPE, true)).isEqualTo(2L);
  }

  @Test
  void aTransactionStraddlingAReWrapIsStillOneTransaction() {
    // current() is resolved per call, so begin() goes through the wrapper installed then and commit() through the one
    // installed since. Both delegate to the same embedded LocalDatabase, whose thread context holds the transaction.
    final Map<String, AtomicInteger> oldCalls = new ConcurrentHashMap<>();
    final ServerDatabase handle = new ServerDatabase(null, installWrapper(oldCalls));

    handle.begin();
    handle.newDocument(TYPE).set("name", "before-rewrap").save();

    final Map<String, AtomicInteger> newCalls = new ConcurrentHashMap<>();
    installWrapper(newCalls);

    handle.newDocument(TYPE).set("name", "after-rewrap").save();
    assertThat(handle.isTransactionActive()).as("the re-wrap must not lose the open transaction").isTrue();
    handle.commit();

    assertThat(count(oldCalls, "begin")).isEqualTo(1);
    assertThat(count(oldCalls, "commit")).isZero();
    assertThat(count(newCalls, "commit")).as("the commit goes through the wrapper installed last").isEqualTo(1);
    assertThat(handle.isTransactionActive()).isFalse();
    assertThat(database.countType(TYPE, true)).as("both writes committed together").isEqualTo(2L);
  }

  @Test
  void theHAHooksReachTheWrapperInsteadOfTheirStandaloneDefaults() throws Exception {
    // Not a staleness case: ServerDatabase did not override these at all, so even a handle built around the current
    // wrapper answered isLeader() == true on a follower while delegating isReplicated().
    final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
    final ServerDatabase freshHandle = new ServerDatabase(null, installWrapper(calls));

    assertThat(freshHandle.isLeader()).isTrue();
    assertThat(freshHandle.runWithCompactionReplication(() -> true)).isTrue();
    freshHandle.recordTimeSeriesSealedChange("t", 0, "f", new byte[0]);
    freshHandle.countRecordsRead(1);

    assertThat(count(calls, "isLeader")).isEqualTo(1);
    assertThat(count(calls, "runWithCompactionReplication")).isEqualTo(1);
    assertThat(count(calls, "recordTimeSeriesSealedChange")).isEqualTo(1);
    assertThat(count(calls, "countRecordsRead")).isEqualTo(1);

    // And a stale handle follows the current wrapper for them too.
    final ServerDatabase staleHandle = new ServerDatabase(null, local());
    final Map<String, AtomicInteger> newCalls = new ConcurrentHashMap<>();
    installWrapper(newCalls);
    staleHandle.isLeader();
    assertThat(count(newCalls, "isLeader")).isEqualTo(1);
  }

  @Test
  void aHandleOnAnUnwrappedDatabaseRunsLocally() {
    final ServerDatabase handle = new ServerDatabase(null, local());

    drain(handle.command("sql", "INSERT INTO " + TYPE + " SET name = 'standalone'"));
    handle.transaction(() -> handle.newDocument(TYPE).set("name", "standalone-2").save());

    assertThat(handle.countType(TYPE, true)).isEqualTo(2L);
    try (final ResultSet rs = handle.query("sql", "SELECT count(*) AS c FROM " + TYPE)) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2L);
    }
  }
}
