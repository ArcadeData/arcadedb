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
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5100.
 * <p>
 * {@link ArcadeStateMachine#applyBootstrapFingerprintEntry} records the committed bootstrap baseline
 * into an in-memory map that {@link ArcadeStateMachine#getBootstrapBaseline} reads. After a restart
 * with persistent Raft storage, the {@code BOOTSTRAP_FINGERPRINT_ENTRY} sits below the Ratis snapshot
 * index and is never replayed, so the in-memory map came back empty and the durable baseline in the
 * Raft log became invisible. The fix persists the baselines next to the applied-index bookkeeping and
 * reloads them lazily, so a fresh state machine instance (simulating a process restart) recovers them.
 * <p>
 * The tests drive a real (unstarted) {@link ArcadeDBServer} with a real {@link LocalDatabase} - no
 * mocking framework - and mirror {@code ArcadeStateMachineAppliedIndexPerDatabaseTest}.
 */
class ArcadeStateMachineBootstrapBaselinePersistenceTest {

  private static final String DB_A     = "db-a";
  private static final String DB_OTHER = "db-other";

  @TempDir
  private Path           serverDir;
  private ArcadeDBServer server;
  private LocalDatabase  localDbA;
  private String         dbAPath;

  @BeforeEach
  void setUp() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    server = new ArcadeDBServer(config);

    dbAPath = serverDir.resolve(DB_A).toString();
    localDbA = (LocalDatabase) new DatabaseFactory(dbAPath).create();
    server.registerDatabase(DB_A, localDbA);
  }

  @AfterEach
  void tearDown() {
    if (localDbA != null && localDbA.isOpen())
      localDbA.close();
    if (dbAPath != null)
      FileUtils.deleteRecursively(new File(dbAPath));
  }

  private ArcadeStateMachine newStateMachine() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    return sm;
  }

  private ByteString encodeBaseline(final String dbName, final String fingerprint, final long lastTxId) {
    return RaftLogEntryCodec.encodeBootstrapFingerprintEntry(dbName, fingerprint, lastTxId);
  }

  /**
   * The committed baseline survives a restart: a fresh state machine instance (no in-memory state)
   * recovers the fingerprint and lastTxId from disk even though the bootstrap entry is never replayed.
   */
  @Test
  void baselineSurvivesRestart() throws Exception {
    final ArcadeStateMachine writer = newStateMachine();

    final String fingerprint = BootstrapFingerprint.compute(new File(dbAPath));
    final long lastTxId = localDbA.getLastTransactionId();
    writer.applyBootstrapFingerprintEntry(RaftLogEntryCodec.decode(encodeBaseline(DB_A, fingerprint, lastTxId)), 50L);
    assertThat(writer.getBootstrapBaseline(DB_A)).isNotNull();

    // Simulate a process restart: a brand-new state machine that must recover the baseline from disk,
    // because the bootstrap entry sits below the Ratis snapshot index and is never replayed.
    final ArcadeStateMachine reopened = newStateMachine();
    final ArcadeStateMachine.BootstrapBaseline recovered = reopened.getBootstrapBaseline(DB_A);
    assertThat(recovered).as("baseline must be recovered from disk after restart").isNotNull();
    assertThat(recovered.fingerprint()).isEqualTo(fingerprint);
    assertThat(recovered.lastTxId()).isEqualTo(lastTxId);
  }

  /**
   * With no persisted file, a fresh state machine reports no baseline (null) rather than throwing.
   */
  @Test
  void noPersistedFileReturnsNull() {
    final ArcadeStateMachine sm = newStateMachine();
    assertThat(sm.getBootstrapBaseline(DB_A)).isNull();
  }

  /**
   * The on-disk baselines file is a JSON document keyed by database name, pinning the persisted shape
   * independently of the reader.
   */
  @Test
  void persistedFileIsJsonDocumentOnDisk() throws Exception {
    final ArcadeStateMachine sm = newStateMachine();

    final String fingerprint = BootstrapFingerprint.compute(new File(dbAPath));
    final long lastTxId = localDbA.getLastTransactionId();
    sm.applyBootstrapFingerprintEntry(RaftLogEntryCodec.decode(encodeBaseline(DB_A, fingerprint, lastTxId)), 50L);

    final String content = Files.readString(serverDir.resolve(".raft").resolve("bootstrap-baselines")).trim();
    assertThat(content).as("the persisted file is a JSON document").startsWith("{");

    final JSONObject json = new JSONObject(content);
    final JSONObject dbA = json.getJSONObject(DB_A);
    assertThat(dbA.getString("fingerprint")).isEqualTo(fingerprint);
    assertThat(dbA.getLong("lastTxId", -1)).isEqualTo(lastTxId);
  }

  /**
   * A baseline write for one database preserves other databases' baselines already on disk: the write
   * loads the existing file before rewriting it (load-before-mutate), so an unrelated database's
   * persisted baseline is not clobbered.
   */
  @Test
  void writePreservesOtherDatabasesBaselinesOnDisk() throws Exception {
    // A late joiner with no local copy still records the baseline (recorded before the presence check),
    // so DB_OTHER can be persisted purely by name without creating the database.
    final ArcadeStateMachine writer = newStateMachine();
    final String fingerprintA = BootstrapFingerprint.compute(new File(dbAPath));
    final long lastTxIdA = localDbA.getLastTransactionId();
    writer.applyBootstrapFingerprintEntry(RaftLogEntryCodec.decode(encodeBaseline(DB_A, fingerprintA, lastTxIdA)), 50L);
    writer.applyBootstrapFingerprintEntry(RaftLogEntryCodec.decode(encodeBaseline(DB_OTHER, "a".repeat(64), 7L)), 51L);

    // A fresh instance (no in-memory state) records a NEW baseline for db-a only; db-other must survive.
    final ArcadeStateMachine reopened = newStateMachine();
    reopened.applyBootstrapFingerprintEntry(RaftLogEntryCodec.decode(encodeBaseline(DB_A, fingerprintA, lastTxIdA)), 60L);

    // A third instance reading from disk still sees both baselines.
    final ArcadeStateMachine third = newStateMachine();
    assertThat(third.getBootstrapBaseline(DB_A)).isNotNull();
    assertThat(third.getBootstrapBaseline(DB_OTHER))
        .as("an unrelated database's persisted baseline survives a single-database rewrite")
        .isNotNull();
    assertThat(third.getBootstrapBaseline(DB_OTHER).fingerprint()).isEqualTo("a".repeat(64));
    assertThat(third.getBootstrapBaseline(DB_OTHER).lastTxId()).isEqualTo(7L);
  }
}
