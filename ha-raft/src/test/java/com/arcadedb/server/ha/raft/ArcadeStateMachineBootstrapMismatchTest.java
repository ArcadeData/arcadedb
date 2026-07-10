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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the error-recovery path in
 * {@link ArcadeStateMachine#applyBootstrapFingerprintEntry}.
 * <p>
 * When a bootstrap fingerprint mismatch is detected and the subsequent snapshot download from the
 * leader fails (e.g. the leader is transiently unreachable during a cluster restart), the method
 * must NOT propagate the exception. {@code applyBootstrapFingerprintEntry} runs on the Raft
 * StateMachineUpdater thread, so a propagated exception reaches {@code applyTransaction}'s
 * {@code catch (Throwable)}, trips {@code haltedAfterCriticalError}, and fires an emergency
 * {@code server.stop()} - leaving the database closed and every client request failing with
 * {@code DatabaseIsClosedException} (the customer-reported failure mode).
 * <p>
 * The combined fix has two layers: {@link SnapshotInstaller#install} now downloads before touching
 * the live files (so a failed download leaves the database open), and the catch block here keeps the
 * server up and schedules an async retry once a leader is reachable.
 */
class ArcadeStateMachineBootstrapMismatchTest {

  private static final String DB_PATH = "./target/databases/test-bootstrap-mismatch";
  private static final String DB_NAME = "test-bootstrap-mismatch";

  private LocalDatabase localDb;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    localDb = (LocalDatabase) new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (localDb != null && localDb.isOpen())
      localDb.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /**
   * A snapshot-download failure during bootstrap-mismatch recovery must not propagate (which would
   * halt the state machine and shut the server down) and must leave the database open and usable.
   */
  @Test
  void bootstrapMismatchDownloadFailureKeepsDatabaseOpen() throws Exception {
    // 0 retries so SnapshotInstaller fails immediately without exponential-backoff sleep.
    // The leader address is null because raftHAServer is null in this unit test.
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases");
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

    // Track database registration: should NOT be removed, since the download fails before the swap.
    final AtomicBoolean dbRegistered = new AtomicBoolean(true);
    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getConfiguration()).thenReturn(config);
    when(mockServer.existsDatabase(DB_NAME)).thenAnswer(inv -> dbRegistered.get());

    // The server arg is null: the only path exercised here is getEmbedded().close() (via
    // closeLocalDatabaseIfOpen), which dereferences the wrapped localDb, not the server, so null is safe.
    final ServerDatabase serverDb = new ServerDatabase(null, localDb);
    when(mockServer.getDatabase(DB_NAME)).thenReturn(serverDb);
    doAnswer(inv -> {
      dbRegistered.set(false);
      return null;
    }).when(mockServer).removeDatabase(DB_NAME);

    // raftHAServer stays null so the leader-address suppliers inside installFromLeaderForBootstrap()
    // return null. SnapshotInstaller exhausts its (zero) retries, throws IOException during the
    // download phase - before the live files are touched - which is wrapped as RuntimeException.
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(mockServer);

    // Chosen fingerprint is all-zero hex, which never matches a real database on disk.
    // chosenLastTxId = Long.MAX_VALUE ensures localLastTxId < chosenLastTxId so the "late newer
    // joiner" guard is not triggered; only the fingerprint mismatch matters.
    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(
        DB_NAME, "0".repeat(64), Long.MAX_VALUE);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    // Must not throw. Before the fix this RuntimeException propagated to applyTransaction, which
    // halted the state machine and triggered emergency server shutdown.
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(decoded, 1L));

    // The database must remain open and registered (download-before-close leaves it intact).
    assertThat(localDb.isOpen()).as("Database stays open after a failed bootstrap install").isTrue();
    assertThat(dbRegistered.get()).as("Database stays registered after a failed bootstrap install").isTrue();
  }

  /**
   * When the bootstrap fingerprint matches the local database (fingerprint and lastTxId both equal
   * the committed baseline), the method takes the fast-path return without triggering any snapshot
   * install or touching the database registration.
   */
  @Test
  void bootstrapFingerprintMatchSkipsInstall() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases");

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getConfiguration()).thenReturn(config);
    when(mockServer.existsDatabase(DB_NAME)).thenReturn(true);

    // The server arg is null: the only path exercised here is getEmbedded().close() (via
    // closeLocalDatabaseIfOpen), which dereferences the wrapped localDb, not the server, so null is safe.
    final ServerDatabase serverDb = new ServerDatabase(null, localDb);
    when(mockServer.getDatabase(DB_NAME)).thenReturn(serverDb);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(mockServer);

    // Compute the real local fingerprint and lastTxId so the entry matches exactly.
    final String realFingerprint = BootstrapFingerprint.compute(new File(DB_PATH));
    final long realLastTxId = localDb.getLastTransactionId();

    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(
        DB_NAME, realFingerprint, realLastTxId);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(decoded, 1L));

    // Baseline must be recorded even on a match.
    final ArcadeStateMachine.BootstrapBaseline baseline = sm.getBootstrapBaseline(DB_NAME);
    assertThat(baseline).isNotNull();
    assertThat(baseline.fingerprint()).isEqualTo(realFingerprint);
    assertThat(baseline.lastTxId()).isEqualTo(realLastTxId);

    // No snapshot install: removeDatabase must never be called.
    verify(mockServer, never()).removeDatabase(DB_NAME);
  }
}
