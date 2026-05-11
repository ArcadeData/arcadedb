/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression IT for issue #4147 late-newer-joiner refusal: a peer whose local {@code lastTxId} is
 * strictly greater than the cluster's committed bootstrap baseline must NOT silently overwrite
 * its own data. The guard in {@code ArcadeStateMachine.applyBootstrapFingerprintEntry} emits a
 * SEVERE and returns without calling {@code installFromLeaderForBootstrap}.
 * <p>
 * The fixture engineers the scenario by:
 * <ol>
 *   <li>Letting the cluster form and bootstrap normally (canonical low {@code lastTxId} on every
 *       peer; baseline = e.g. 7).</li>
 *   <li>Stopping server 2, wiping its Raft storage so Ratis treats it as a fresh peer on rejoin,
 *       and overwriting its {@code last-tx-id.bin} with a much higher value
 *       (1_000_000).</li>
 *   <li>Restarting server 2. Its state machine replays the Raft log via Ratis AppendEntries
 *       (gap is a handful of entries, well below {@code snapshotThreshold}), and when
 *       {@code BOOTSTRAP_FINGERPRINT_ENTRY} is applied the local {@code lastTxId=1_000_000} is
 *       greater than the entry's {@code chosenLastTxId=7} - the SEVERE-and-return branch
 *       fires.</li>
 * </ol>
 * The assertion checks that server 2's bootstrap baseline is the cluster's committed value (the
 * entry was applied; the baseline map was populated <b>above</b> the refusal check) rather than
 * server 2's higher pre-staged value, and that server 2's local on-disk {@code last-tx-id.bin}
 * was preserved (proving {@code installFromLeaderForBootstrap} did NOT run).
 */
class RaftBootstrapLateNewerJoinerIT extends BaseRaftHATest {

  private static final long LATE_JOINER_PRE_STAGED_LAST_TX_ID = 1_000_000L;
  private static final int  LATE_JOINER_INDEX                 = 2;

  @Override
  protected boolean persistentRaftStorage() {
    // We want server 2's Raft storage WIPED on its restart so the leader re-ships entries via
    // AppendEntries rather than treating server 2 as a still-caught-up peer.
    return false;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void lateNewerJoinerAdoptsClusterBaselineAndPreservesLocalLastTxId() throws IOException {
    final String dbName = getDatabaseName();

    // Phase 1: wait for the initial bootstrap baseline to commit and apply on every peer.
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(250, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            final RaftHAPlugin plugin = getRaftPlugin(i);
            assertThat(plugin).as("server %d Raft plugin", i).isNotNull();
            assertThat(plugin.getRaftHAServer().getStateMachine().getBootstrapBaseline(dbName))
                .as("server %d baseline", i).isNotNull();
          }
        });

    final var clusterBaseline = getRaftPlugin(0).getRaftHAServer()
        .getStateMachine().getBootstrapBaseline(dbName);
    final long clusterBaselineLastTxId = clusterBaseline.lastTxId();
    assertThat(clusterBaselineLastTxId)
        .as("test premise: cluster baseline must be lower than the late joiner's pre-staged value")
        .isLessThan(LATE_JOINER_PRE_STAGED_LAST_TX_ID);

    // Capture server 2's database path BEFORE stop so we can perturb its last-tx-id.bin.
    final ServerDatabase preStopDb = (ServerDatabase) getServerDatabase(LATE_JOINER_INDEX, dbName);
    assertThat(preStopDb).isNotNull();
    final LocalDatabase preStopLocal = (LocalDatabase) preStopDb.getWrappedDatabaseInstance().getEmbedded();
    final File databasePath = new File(preStopLocal.getDatabasePath());
    final File lastTxIdFile = new File(databasePath, "last-tx-id.bin");

    // Phase 2: stop server 2 cleanly so its TransactionManager flushes last-tx-id.bin, then
    // overwrite that file with the high value.
    LogManager.instance().log(this, Level.INFO, "TEST: stopping server %d for late-joiner setup", LATE_JOINER_INDEX);
    getServer(LATE_JOINER_INDEX).stop();
    // Let the OS release the gRPC port and the Raft storage handle.
    try {
      Thread.sleep(2_000);
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      return;
    }

    // Wipe server 2's Raft storage so Ratis re-ships log entries on rejoin (we want the apply
    // path, not InstallSnapshot, to be the carrier of BOOTSTRAP_FINGERPRINT_ENTRY).
    final String rootPath = GlobalConfiguration.SERVER_ROOT_PATH.getValueAsString();
    final File raftStorage = new File(rootPath + File.separator + "raft-storage-" + peerIdForIndex(LATE_JOINER_INDEX));
    FileUtils.deleteRecursively(raftStorage);

    try (final DataOutputStream out = new DataOutputStream(new FileOutputStream(lastTxIdFile))) {
      out.writeLong(LATE_JOINER_PRE_STAGED_LAST_TX_ID);
    }

    // Phase 3: restart server 2. State machine replays Raft log via AppendEntries.
    LogManager.instance().log(this, Level.INFO,
        "TEST: restarting server %d with pre-staged last-tx-id.bin=%d", LATE_JOINER_INDEX,
        LATE_JOINER_PRE_STAGED_LAST_TX_ID);
    getServer(LATE_JOINER_INDEX).start();

    // Phase 4: wait for server 2 to re-apply the BOOTSTRAP_FINGERPRINT_ENTRY. The baseline is
    // set inside applyBootstrapFingerprintEntry BEFORE the SEVERE-refusal check, so getBootstrapBaseline
    // returns non-null once the entry has been applied (even on the refusal branch).
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          final RaftHAPlugin plugin = getRaftPlugin(LATE_JOINER_INDEX);
          assertThat(plugin).isNotNull();
          assertThat(plugin.getRaftHAServer().getStateMachine().getBootstrapBaseline(dbName)).isNotNull();
        });

    // Assert 1: late joiner adopted the cluster's baseline (NOT its own higher pre-staged value).
    final var lateJoinerBaseline = getRaftPlugin(LATE_JOINER_INDEX).getRaftHAServer()
        .getStateMachine().getBootstrapBaseline(dbName);
    assertThat(lateJoinerBaseline.lastTxId())
        .as("late joiner's baseline must be the cluster's value, not its own pre-staged")
        .isEqualTo(clusterBaselineLastTxId);
    assertThat(lateJoinerBaseline.lastTxId())
        .as("late joiner's baseline must be strictly less than its pre-staged lastTxId")
        .isLessThan(LATE_JOINER_PRE_STAGED_LAST_TX_ID);

    // Assert 2: the SEVERE-refusal branch did NOT call installFromLeaderForBootstrap, so server
    // 2's last-tx-id.bin still contains the pre-staged 1_000_000. If the snapshot reinstall had
    // run, the file would have been wiped (it's not in the snapshot ZIP) and the engine would
    // have written its current in-memory value (close to 7).
    assertThat(lastTxIdFile.isFile())
        .as("server 2's last-tx-id.bin must still exist (no snapshot reinstall happened)")
        .isTrue();
    final long onDiskLastTxId;
    try (final java.io.DataInputStream in = new java.io.DataInputStream(new java.io.FileInputStream(lastTxIdFile))) {
      onDiskLastTxId = in.readLong();
    }
    assertThat(onDiskLastTxId)
        .as("server 2's local last-tx-id.bin must be preserved at the high pre-staged value")
        .isEqualTo(LATE_JOINER_PRE_STAGED_LAST_TX_ID);
  }
}
