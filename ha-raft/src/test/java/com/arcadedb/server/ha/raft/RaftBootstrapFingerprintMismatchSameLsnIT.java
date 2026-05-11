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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression IT for issue #4147 mismatch path: when peers report the same {@code lastTxId} but
 * one peer's fingerprint diverges, every peer must converge on a single committed baseline and
 * the mismatched peer must reinstall the leader-shipped full snapshot.
 * <p>
 * The fixture appends a trailing newline to {@code configuration.json} on server 1 before
 * startup. {@code configuration.json} is in the
 * {@link com.arcadedb.database.BootstrapFingerprint} surface AND is shipped in the snapshot ZIP
 * (see {@code SnapshotHttpHandler.addFileToZip}), so it is a clean perturbation: divergence
 * before the cluster forms; reconverged byte content after the install.
 * <p>
 * The assertion does NOT compare {@link com.arcadedb.database.BootstrapFingerprint#compute} values
 * post-bootstrap. The fingerprint surface is documented as cold-start-only - bucket and index
 * files drift legitimately at runtime (page allocation, compaction order) so the same logical
 * state produces different fingerprints across peers. Instead, the test reads
 * {@code configuration.json} bytes directly on every peer and asserts they converge to a single
 * value (canonical if the source was a non-perturbed peer; perturbed if server 1 itself was the
 * source). Both outcomes prove the cluster converged after the mismatch was detected and the
 * snapshot reinstall completed.
 */
class RaftBootstrapFingerprintMismatchSameLsnIT extends BaseRaftHATest {

  private static final int MISMATCH_PEER_INDEX = 1;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    super.onBeforeStarting(server);
    final String name = server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int idx = Integer.parseInt(name.substring(name.lastIndexOf('_') + 1));
    if (idx != MISMATCH_PEER_INDEX)
      return;
    final String dbDir = server.getConfiguration()
        .getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY)
        + File.separator + getDatabaseName();
    final File configFile = new File(dbDir, "configuration.json");
    if (!configFile.isFile())
      return;
    try (final FileWriter w = new FileWriter(configFile, true)) {
      w.write("\n");
    } catch (final IOException e) {
      throw new RuntimeException("Failed to perturb configuration.json on server " + idx, e);
    }
  }

  @Test
  void mismatchedPeerConvergesOnCommittedBaseline() {
    final String dbName = getDatabaseName();

    // Phase 1: bootstrap baseline committed on every peer.
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

    // Every peer agrees on the same committed baseline (single Raft entry, deterministic apply).
    final var ref = getRaftPlugin(0).getRaftHAServer().getStateMachine().getBootstrapBaseline(dbName);
    for (int i = 1; i < getServerCount(); i++) {
      final var b = getRaftPlugin(i).getRaftHAServer().getStateMachine().getBootstrapBaseline(dbName);
      assertThat(b.fingerprint()).as("server %d baseline fingerprint agreement", i).isEqualTo(ref.fingerprint());
      assertThat(b.lastTxId()).as("server %d baseline lastTxId agreement", i).isEqualTo(ref.lastTxId());
    }

    // Phase 2: every peer's configuration.json bytes converge to a single value. For the
    // mismatched peer, this requires installFromLeaderForBootstrap to have completed the snapshot
    // swap. configuration.json is byte-stable post-open (the engine only reads it during normal
    // open() flow; it's only rewritten on database create or on schema-modifying transactions,
    // neither of which happens during this test), so byte-equality across peers is a reliable
    // signal of cluster convergence.
    Awaitility.await()
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          final byte[] reference = readConfigJsonBytes(0, dbName);
          for (int i = 1; i < getServerCount(); i++) {
            final byte[] peerBytes = readConfigJsonBytes(i, dbName);
            assertThat(peerBytes)
                .as("server %d configuration.json bytes must converge to server 0's after mismatch reinstall", i)
                .isEqualTo(reference);
          }
        });
  }

  private byte[] readConfigJsonBytes(final int serverIndex, final String dbName) throws IOException {
    final ServerDatabase serverDb = (ServerDatabase) getServerDatabase(serverIndex, dbName);
    if (serverDb == null)
      throw new IOException("Database not yet available on server " + serverIndex);
    final var embedded = serverDb.getWrappedDatabaseInstance().getEmbedded();
    if (!(embedded instanceof LocalDatabase localDb))
      throw new IOException("Database not yet a LocalDatabase on server " + serverIndex);
    return Files.readAllBytes(new File(localDb.getDatabasePath(), "configuration.json").toPath());
  }
}
