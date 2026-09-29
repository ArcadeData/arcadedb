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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.DatabaseNotAvailableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Regression tests for issue #8605, server side. A node holding a closed copy marked unverified by #8589 that is elected
 * leader used to reopen it unasked, as the cluster's copy, and drop the mark - even when the previous leader held a
 * newer copy closed. {@link ArcadeDBServer#getDatabase} now asks {@link HAServerPlugin#refuseToReopenUnverifiedClosedCopy}
 * first, on the leader, and reopens only when the peers verified the copy.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8605LeaderReopenGateTest {

  private static final String MARKED   = "db8605";
  private static final String UNMARKED = "db8605plain";
  private static final String PASSWORD = "DefaultPasswordForTests";

  @TempDir
  Path root;

  private ArcadeDBServer server;

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.setHA(null);
      for (final String name : new String[] { MARKED, UNMARKED })
        try {
          if (server.existsDatabase(name))
            ((DatabaseInternal) server.getDatabase(name)).getEmbedded().close();
        } catch (final Exception ignore) {
          // best-effort cleanup; the @TempDir is removed regardless
        }
      server.stop();
    }
  }

  /** The issue as reported: a peer holds a newer copy, so the leader does not reopen its own, and keeps the mark. */
  @Test
  void aLeaderWhosePeersHoldANewerCopyDoesNotReopenIt() throws IOException {
    createDatabaseOnDisk(MARKED, true);
    server = startServer();
    final HAServerPlugin ha = leader("a newer copy is held by peer-1 (applied index 42)");
    server.setHA(ha);

    assertThatThrownBy(() -> server.getDatabase(MARKED))
        .isInstanceOf(DatabaseNotAvailableException.class)
        .hasMessageContaining("a newer copy is held by peer-1")
        .hasMessageContaining(ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE);
    assertThat(server.existsDatabase(MARKED)).as("the copy is not registered").isFalse();
    assertThat(Files.exists(marker(MARKED))).as("and keeps its mark").isTrue();
    verify(ha, times(1)).refuseToReopenUnverifiedClosedCopy(MARKED);
  }

  /** Every request asks again, so a copy the peers verify later is reopened then. */
  @Test
  void aLeaderReopensTheCopyOnceItsPeersVerifyIt() throws IOException {
    createDatabaseOnDisk(MARKED, true);
    server = startServer();
    final HAServerPlugin ha = leader(null);
    server.setHA(ha);

    assertThat(server.getDatabase(MARKED).isOpen()).isTrue();
    assertThat(Files.exists(marker(MARKED))).as("the verified copy is the cluster's: its mark goes").isFalse();
    verify(ha, times(1)).refuseToReopenUnverifiedClosedCopy(MARKED);
  }

  /** An ordinary closed copy costs the leader no round trip to its peers. */
  @Test
  void aLeaderDoesNotAskAboutAnUnmarkedCopy() throws IOException {
    createDatabaseOnDisk(UNMARKED, false);
    server = startServer();
    final HAServerPlugin ha = leader("must not be asked");
    server.setHA(ha);
    server.removeDatabase(UNMARKED);

    assertThat(server.getDatabase(UNMARKED).isOpen()).isTrue();
    verify(ha, never()).refuseToReopenUnverifiedClosedCopy(anyString());
  }

  /**
   * The snapshot installer reopens while holding the registry lock, so it never waits on a round trip to the peers: on
   * the leader a marked copy it restored stays closed, as it does on a follower.
   */
  @Test
  void theInstallersReopenOnTheLeaderRefusesWithoutAskingThePeers() throws IOException {
    createDatabaseOnDisk(MARKED, true);
    server = startServer();
    final HAServerPlugin ha = leader(null);
    server.setHA(ha);

    assertThatThrownBy(() -> server.reopenDatabaseUnderSnapshotRecovery(MARKED))
        .isInstanceOf(DatabaseNotAvailableException.class)
        .hasMessageContaining("has not compared it");
    assertThat(server.existsDatabase(MARKED)).isFalse();
    assertThat(Files.exists(marker(MARKED))).isTrue();
    verify(ha, never()).refuseToReopenUnverifiedClosedCopy(anyString());
  }

  /** A follower refuses as #8589 made it, without asking anyone. */
  @Test
  void aFollowerDoesNotAskItsPeers() throws IOException {
    createDatabaseOnDisk(MARKED, true);
    server = startServer();
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.isLeader()).thenReturn(false);
    server.setHA(ha);

    assertThatThrownBy(() -> server.getDatabase(MARKED)).isInstanceOf(DatabaseNotAvailableException.class);
    verify(ha, never()).refuseToReopenUnverifiedClosedCopy(anyString());
  }

  /** An HA implementation that cannot compare the copies does not guess: the default refuses. */
  @Test
  void theDefaultOfAPluginThatCannotCompareRefuses() throws IOException {
    createDatabaseOnDisk(MARKED, true);
    server = startServer();
    final HAServerPlugin ha = mock(HAServerPlugin.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    when(ha.isLeader()).thenReturn(true);
    server.setHA(ha);

    assertThatThrownBy(() -> server.getDatabase(MARKED))
        .isInstanceOf(DatabaseNotAvailableException.class)
        .hasMessageContaining("cannot compare");
    assertThat(Files.exists(marker(MARKED))).isTrue();
  }

  // ------------------------------------------------------------------------------------------------------------

  private static HAServerPlugin leader(final String refusal) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.isLeader()).thenReturn(true);
    when(ha.refuseToReopenUnverifiedClosedCopy(anyString())).thenReturn(refusal);
    return ha;
  }

  private Path marker(final String name) {
    return root.resolve("databases").resolve(name).resolve(ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE);
  }

  private void createDatabaseOnDisk(final String name, final boolean marked) throws IOException {
    final Path dir = root.resolve("databases").resolve(name);
    try (final Database db = new DatabaseFactory(dir.toString()).create()) {
      db.transaction(() -> db.getSchema().createVertexType("Node"));
    }
    if (marked)
      Files.writeString(dir.resolve(ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE), "");
  }

  private ArcadeDBServer startServer() throws IOException {
    Files.createDirectories(root.resolve("databases"));
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_8605");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(StaticBaseServerTest.allocateFreePorts(1)[0]));
    // No HA plugin lives in this module: HA is REQUESTED and the test registers a mock as the plugin.
    config.setValue(GlobalConfiguration.HA_ENABLED, true);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

    final ArcadeDBServer started = new ArcadeDBServer(config);
    started.start();
    return started;
  }
}
