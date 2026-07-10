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
import com.arcadedb.database.BootstrapFingerprint;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4824.
 * <p>
 * {@link ArcadeStateMachine} multiplexes every database onto one Raft group but used to persist a
 * single global {@code .raft/applied-index} scalar. The replay-skip in
 * {@link ArcadeStateMachine#applyBootstrapFingerprintEntry} ("this database's bootstrap baseline was
 * already applied in a prior session, skip verification") consulted that GLOBAL value even though the
 * decision is per-database. A co-located database that advanced the global index past this database's
 * {@code BOOTSTRAP_FINGERPRINT_ENTRY} index would silently suppress the verification of a database
 * that was never bootstrapped - the "value that mixes databases" defect.
 * <p>
 * The fix makes applied-index tracking per-database-aware: the skip now consults this database's own
 * applied index and only skips when there is positive per-database evidence.
 * <p>
 * The tests drive a real (unstarted) {@link ArcadeDBServer} with real {@link LocalDatabase}
 * collaborators registered into its registry - no mocking framework. To observe whether the
 * verification path was reached, a tiny {@link CountingServer} subclass tallies the single-argument
 * {@code getDatabase} calls that {@code applyBootstrapFingerprintEntry} makes only after it decides
 * NOT to skip.
 */
class ArcadeStateMachineAppliedIndexPerDatabaseTest {

  private static final String DB_A     = "db-a";
  private static final String DB_OTHER = "db-other";

  @TempDir
  private Path          serverDir;
  private CountingServer server;
  private LocalDatabase  localDbA;
  private LocalDatabase  localDbOther;
  private String         dbAPath;
  private String         dbOtherPath;

  /**
   * A real {@link ArcadeDBServer} that records how many times the single-argument
   * {@link #getDatabase(String)} - the call {@code applyBootstrapFingerprintEntry} reaches only on the
   * non-skip (verification) path - has been invoked. Everything else delegates to the real server.
   */
  private static final class CountingServer extends ArcadeDBServer {
    final AtomicInteger getDatabaseInvocations = new AtomicInteger();

    CountingServer(final ContextConfiguration configuration) {
      super(configuration);
    }

    @Override
    public ServerDatabase getDatabase(final String databaseName) {
      getDatabaseInvocations.incrementAndGet();
      return super.getDatabase(databaseName);
    }
  }

  @BeforeEach
  void setUp() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    server = new CountingServer(config);

    dbAPath = serverDir.resolve(DB_A).toString();
    localDbA = (LocalDatabase) new DatabaseFactory(dbAPath).create();
    server.registerDatabase(DB_A, localDbA);
  }

  @AfterEach
  void tearDown() {
    if (localDbA != null && localDbA.isOpen())
      localDbA.close();
    if (localDbOther != null && localDbOther.isOpen())
      localDbOther.close();
    if (dbAPath != null)
      FileUtils.deleteRecursively(new File(dbAPath));
    if (dbOtherPath != null)
      FileUtils.deleteRecursively(new File(dbOtherPath));
  }

  private ArcadeStateMachine newStateMachine() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    return sm;
  }

  /**
   * Registers a real second database so {@code getDatabaseNames()} reports it. Used by the
   * full-install test, the only one that needs DB_OTHER actually present on the node (the others use
   * the name purely as an applied-index map key).
   */
  private void registerSecondDatabase() {
    dbOtherPath = serverDir.resolve(DB_OTHER).toString();
    localDbOther = (LocalDatabase) new DatabaseFactory(dbOtherPath).create();
    server.registerDatabase(DB_OTHER, localDbOther);
  }

  /**
   * Regression: another database advancing the GLOBAL applied index past this database's bootstrap
   * entry index must NOT skip this database's bootstrap verification. Before the fix the skip fired
   * (global 100 &gt;= 50) and {@code applyBootstrapFingerprintEntry} returned before ever consulting
   * the local database, so {@code server.getDatabase(DB_A)} was never called.
   */
  @Test
  void bootstrapSkipNotTriggeredByAnotherDatabasesAppliedIndex() throws Exception {
    final ArcadeStateMachine sm = newStateMachine();

    // A co-located database advanced the shared Raft log far ahead of db-a's bootstrap entry.
    sm.writePersistedAppliedIndex(100L, DB_OTHER);

    // Build a MATCHING bootstrap entry for db-a so the verification path returns cleanly once reached
    // (no snapshot install). The discriminator is purely "was the local database consulted?".
    final String realFingerprint = BootstrapFingerprint.compute(new File(dbAPath));
    final long realLastTxId = localDbA.getLastTransactionId();
    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_A, realFingerprint, realLastTxId);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    final int callsBefore = server.getDatabaseInvocations.get();

    // db-a's own bootstrap entry sits at index 50, below the global 100.
    sm.applyBootstrapFingerprintEntry(decoded, 50L);

    // Verification must have run for db-a (its local state was read), proving the global index of a
    // different database did not suppress it.
    assertThat(server.getDatabaseInvocations.get())
        .as("verification consulted the local database despite a higher global applied index")
        .isGreaterThan(callsBefore);

    // The baseline is recorded regardless of the skip decision.
    assertThat(sm.getBootstrapBaseline(DB_A)).isNotNull();
    assertThat(sm.getBootstrapBaseline(DB_A).fingerprint()).isEqualTo(realFingerprint);
  }

  /**
   * A database's OWN per-database applied index at or beyond the bootstrap entry index still skips its
   * verification (the legitimate replay-skip on restart is preserved). When skipped,
   * {@code applyBootstrapFingerprintEntry} returns before consulting the local database.
   */
  @Test
  void bootstrapSkipHonorsPerDatabaseAppliedIndex() throws Exception {
    final ArcadeStateMachine sm = newStateMachine();

    // Positive per-database evidence that db-a was already applied past its bootstrap entry index.
    sm.writePersistedAppliedIndex(50L, DB_A);

    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_A, "0".repeat(64), Long.MAX_VALUE);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    final int callsBefore = server.getDatabaseInvocations.get();

    sm.applyBootstrapFingerprintEntry(decoded, 50L);

    // Skipped: the local database was never consulted.
    assertThat(server.getDatabaseInvocations.get())
        .as("skip fired on positive per-database evidence; the local database is never consulted")
        .isEqualTo(callsBefore);
    // Baseline is still recorded before the skip check.
    assertThat(sm.getBootstrapBaseline(DB_A)).isNotNull();
  }

  /**
   * A legacy plain-number {@code .raft/applied-index} file (written by an older version) is honoured
   * as the GLOBAL value but yields NO per-database evidence: a per-database read returns -1, so it can
   * never falsely skip a per-database decision.
   */
  @Test
  void legacyPlainNumberFileReadAsGlobalNotPerDatabase() throws Exception {
    final ArcadeStateMachine sm = newStateMachine();

    final Path raftDir = serverDir.resolve(".raft");
    Files.createDirectories(raftDir);
    Files.writeString(raftDir.resolve("applied-index"), "77");

    assertThat(sm.readPersistedAppliedIndex())
        .as("legacy plain-number file is honoured as the global applied index")
        .isEqualTo(77L);
    assertThat(sm.readPersistedAppliedIndex(DB_A))
        .as("legacy file carries no per-database evidence")
        .isEqualTo(-1L);
  }

  /**
   * Per-database and global values round-trip through the persisted file and are recovered by a fresh
   * state machine instance (simulating a process restart).
   */
  @Test
  void perDatabaseValuesRoundTripAcrossRestart() {
    final ArcadeStateMachine writer = newStateMachine();
    writer.writePersistedAppliedIndex(10L, DB_A);
    writer.writePersistedAppliedIndex(20L, DB_OTHER);

    final ArcadeStateMachine reopened = newStateMachine();
    assertThat(reopened.readPersistedAppliedIndex(DB_A)).isEqualTo(10L);
    assertThat(reopened.readPersistedAppliedIndex(DB_OTHER)).isEqualTo(20L);
    assertThat(reopened.readPersistedAppliedIndex())
        .as("global tracks the last applied index across all databases")
        .isEqualTo(20L);
    assertThat(reopened.readPersistedAppliedIndex("never-seen")).isEqualTo(-1L);
  }

  /**
   * The on-disk file is the new JSON document ({@code {"global": n, "db": {...}}}), not the legacy
   * plain number. The round-trip test exercises the same read/write code on both ends, so a format
   * regression that stayed internally consistent would still pass there; this asserts the actual bytes
   * so the persisted shape is pinned independently of the reader.
   */
  @Test
  void persistedFileIsJsonDocumentOnDisk() throws Exception {
    final ArcadeStateMachine sm = newStateMachine();
    sm.writePersistedAppliedIndex(10L, DB_A);
    sm.writePersistedAppliedIndex(20L, DB_OTHER);

    final String content = Files.readString(serverDir.resolve(".raft").resolve("applied-index")).trim();
    assertThat(content).as("the persisted file is a JSON document, not a legacy plain number").startsWith("{");

    final JSONObject json = new JSONObject(content);
    assertThat(json.getLong("global", -1)).isEqualTo(20L);
    final JSONObject perDb = json.getJSONObject("db", new JSONObject());
    assertThat(perDb.getLong(DB_A, -1)).isEqualTo(10L);
    assertThat(perDb.getLong(DB_OTHER, -1)).isEqualTo(20L);
  }

  /**
   * A corrupt/unparseable applied-index file degrades to {@code -1} for both the global and any
   * per-database read rather than throwing. This coupling is relied upon by {@code reinitialize()},
   * whose snapshot-gap check ({@code persistedApplied >= 0 && ...}) then evaluates false and suppresses
   * the "snapshot ahead, download from leader" path - matching the pre-change behaviour.
   */
  @Test
  void corruptFileDegradesToMinusOne() throws Exception {
    final Path raftDir = serverDir.resolve(".raft");
    Files.createDirectories(raftDir);
    // Looks like the new JSON document (leading '{') but is truncated, so JSON parsing fails.
    Files.writeString(raftDir.resolve("applied-index"), "{\"global\": 42, \"db\": {\"db-a\":");

    final ArcadeStateMachine sm = newStateMachine();
    assertThat(sm.readPersistedAppliedIndex())
        .as("a corrupt file degrades the global value to -1 instead of throwing")
        .isEqualTo(-1L);
    assertThat(sm.readPersistedAppliedIndex(DB_A))
        .as("a corrupt file yields no per-database evidence")
        .isEqualTo(-1L);
  }

  /**
   * A full state-machine snapshot install records the snapshot index for EVERY present database, so a
   * subsequent bootstrap entry for any of them is correctly recognised as already applied. This locks
   * in the documented {@code writePersistedAppliedIndexForAllDatabases} behaviour and exercises the
   * {@code getDatabaseNames()} path.
   */
  @Test
  void fullInstallRecordsSnapshotIndexForEveryPresentDatabase() {
    registerSecondDatabase();

    final ArcadeStateMachine sm = newStateMachine();
    sm.writePersistedAppliedIndexForAllDatabases(500L);

    assertThat(sm.readPersistedAppliedIndex()).isEqualTo(500L);
    assertThat(sm.readPersistedAppliedIndex(DB_A)).isEqualTo(500L);
    assertThat(sm.readPersistedAppliedIndex(DB_OTHER)).isEqualTo(500L);
  }

  /**
   * Dropping a database advances the global position and evicts that database's per-database entry in
   * a single write, so the map does not retain a stale entry for a database that no longer exists.
   */
  @Test
  void dropDatabaseEvictsPerDatabaseEntryAndAdvancesGlobal() {
    final ArcadeStateMachine sm = newStateMachine();

    sm.writePersistedAppliedIndex(40L, DB_A);
    sm.writePersistedAppliedIndex(41L, DB_OTHER);
    assertThat(sm.readPersistedAppliedIndex(DB_A)).isEqualTo(40L);

    sm.writePersistedAppliedIndexDroppingDatabase(99L, DB_A);

    assertThat(sm.readPersistedAppliedIndex(DB_A))
        .as("the dropped database's per-database entry is evicted")
        .isEqualTo(-1L);
    assertThat(sm.readPersistedAppliedIndex(DB_OTHER))
        .as("a co-located database's entry is untouched")
        .isEqualTo(41L);
    assertThat(sm.readPersistedAppliedIndex())
        .as("the global position still advances on the drop entry")
        .isEqualTo(99L);
  }

  /**
   * A write that targets one database must preserve the other databases' entries already on disk: the
   * write loads the existing map before mutating (load-before-mutate), so an unrelated database's
   * persisted index is not clobbered when the file is rewritten.
   */
  @Test
  void writePreservesOtherDatabasesEntriesOnDisk() {
    // First instance persists entries for both databases.
    final ArcadeStateMachine writer = newStateMachine();
    writer.writePersistedAppliedIndex(10L, DB_A);
    writer.writePersistedAppliedIndex(20L, DB_OTHER);

    // A fresh instance (no in-memory state) writes a NEW index for db-a only; db-other must survive.
    final ArcadeStateMachine reopened = newStateMachine();
    reopened.writePersistedAppliedIndex(30L, DB_A);

    assertThat(reopened.readPersistedAppliedIndex(DB_A)).isEqualTo(30L);
    assertThat(reopened.readPersistedAppliedIndex(DB_OTHER))
        .as("an unrelated database's persisted entry survives a single-database rewrite")
        .isEqualTo(20L);

    // And it is durable: a third instance reading from disk still sees both.
    final ArcadeStateMachine third = newStateMachine();
    assertThat(third.readPersistedAppliedIndex(DB_A)).isEqualTo(30L);
    assertThat(third.readPersistedAppliedIndex(DB_OTHER)).isEqualTo(20L);
  }
}
