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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.database.RID;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #11281: a database quarantined because its own copy is torn (a crash recovery that did not
 * replay every transaction, #11250; a unique index with no header page, #11258) kept answering client reads from the
 * torn files while its resync was pending. The Jepsen bank workload read {@code {nil 1165}} instead of five accounts
 * totalling 5000 from such a node.
 * <p>
 * {@code RaftReplicatedDatabase} now refuses a client request on such a database with a {@link NeedRetryException},
 * like it does while the directory is being replaced, and still lets the engine's own threads through.
 */
class Issue11281QuarantinedDatabaseRefusesClientsTest {

  private static final String DB_DIR  = "./target/databases";
  private static final String DB_NAME = "test-11281-quarantine-gate";
  private static final String DB_PATH = DB_DIR + "/" + DB_NAME;

  private LocalDatabase                        localDb;
  private RaftReplicatedDatabase               replicated;
  private final AtomicReference<DivergenceCause> cause = new AtomicReference<>();
  private RID                                  seedRid;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DB_DIR + "/.raft"));
    localDb = (LocalDatabase) new DatabaseFactory(DB_PATH).create();
    localDb.getSchema().createDocumentType("Seed");
    localDb.transaction(() -> seedRid = localDb.newDocument("Seed").set("k", 1).save().getIdentity());

    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DB_DIR);
    server.returns("getConfiguration", config);
    final FakeArcadeStateMachine stateMachine = new FakeArcadeStateMachine() {
      @Override
      DivergenceCause quarantineCause(final String dbName) {
        return DB_NAME.equals(dbName) ? cause.get() : null;
      }
    };
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(true);
    raft.stateMachine(stateMachine);
    replicated = new RaftReplicatedDatabase(server, localDb, raft);
    localDb.setAutoTransaction(true);
  }

  @AfterEach
  void tearDown() {
    ProtocolContext.clear();
    if (localDb != null && localDb.isOpen()) {
      if (localDb.isTransactionActive())
        localDb.rollbackAllNested();
      localDb.close();
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DB_DIR + "/.raft"));
  }

  private Map<String, Executable> clientEntryPoints() {
    final Map<String, Executable> calls = new LinkedHashMap<>();
    calls.put("query(sql)", () -> close(replicated.query("sql", "select from Seed")));
    calls.put("query(sql, varargs)", () -> close(replicated.query("sql", "select from Seed where k = ?", 1)));
    calls.put("query(sql, map)", () -> close(replicated.query("sql", "select from Seed where k = :k", Map.of("k", 1))));
    calls.put("command(sql)", () -> close(replicated.command("sql", "select from Seed")));
    calls.put("begin()", () -> replicated.begin());
    calls.put("lookupByRID", () -> replicated.lookupByRID(seedRid, true));
    calls.put("existsRecord", () -> replicated.existsRecord(seedRid));
    calls.put("countType", () -> replicated.countType("Seed", true));
    calls.put("iterateType", () -> replicated.iterateType("Seed", true));
    calls.put("scanType", () -> replicated.scanType("Seed", true, doc -> true));
    calls.put("async()", () -> replicated.async());
    return calls;
  }

  private static void close(final ResultSet resultSet) {
    while (resultSet.hasNext())
      resultSet.next();
    resultSet.close();
  }

  private void assertEveryClientEntryPointIsRefused(final String protocol) {
    ProtocolContext.set(protocol);
    try {
      clientEntryPoints().forEach((name, call) -> assertThatThrownBy(call::execute)
          .as("a %s client's %s on a quarantined database", protocol, name)
          .isInstanceOf(NeedRetryException.class)
          .hasMessageContaining(DB_NAME)
          .hasMessageContaining("quarantined"));
    } finally {
      ProtocolContext.clear();
    }
  }

  private void assertEveryClientEntryPointIsServed(final String protocol) {
    ProtocolContext.set(protocol);
    try {
      clientEntryPoints().forEach((name, call) -> {
        assertThatNoException().as("%s's %s", protocol, name).isThrownBy(call::execute);
        if (localDb.isTransactionActive())
          localDb.rollback();
      });
    } finally {
      ProtocolContext.clear();
    }
  }

  @Test
  void aClientIsServedWhenNothingIsQuarantined() {
    assertEveryClientEntryPointIsServed("http");
  }

  @Test
  void everyClientEntryPointIsRefusedAfterAnIncompleteCrashRecovery() {
    cause.set(DivergenceCause.LOCAL_RECOVERY_INCOMPLETE);
    for (final String protocol : new String[] { "http", "bolt", "postgres", "grpc", "mongo", "redis", "ws" })
      assertEveryClientEntryPointIsRefused(protocol);
  }

  @Test
  void everyClientEntryPointIsRefusedWhenAUniqueIndexCouldNotBeLoaded() {
    cause.set(DivergenceCause.UNAVAILABLE_UNIQUE_INDEX);
    assertEveryClientEntryPointIsRefused("http");
  }

  @Test
  void theServiceResumesOnceTheQuarantineLifts() {
    cause.set(DivergenceCause.LOCAL_RECOVERY_INCOMPLETE);
    assertEveryClientEntryPointIsRefused("http");
    cause.set(null);
    assertEveryClientEntryPointIsServed("http");
  }

  /** The apply thread, the resync and the health monitor read through the same wrapper: refusing them is a divergence. */
  @Test
  void theEngineIsNeverRefused() {
    cause.set(DivergenceCause.LOCAL_RECOVERY_INCOMPLETE);
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
    assertEveryClientEntryPointIsServed(ProtocolContext.INTERNAL);
  }

  /** A replication-gap quarantine keeps its documented behaviour: the copy is merely behind, not torn. */
  @Test
  void aDatabaseQuarantinedForAReplicationGapStillServes() {
    cause.set(DivergenceCause.WAL_VERSION_GAP);
    assertEveryClientEntryPointIsServed("http");
  }

  /** The census: the gate names exactly the causes that say the local files are torn, and every cause is decided. */
  @Test
  void everyCauseIsClassified() {
    assertThat(DivergenceCause.values()).as("the scan still reaches DivergenceCause").isNotEmpty();
    for (final DivergenceCause c : DivergenceCause.values())
      assertThat(c.isLocalCopyUntrustworthy()).as(c.name()).isEqualTo(
          c == DivergenceCause.LOCAL_RECOVERY_INCOMPLETE || c == DivergenceCause.UNAVAILABLE_UNIQUE_INDEX);
  }
}
