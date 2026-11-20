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
import org.junit.jupiter.api.Nested;
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
 * The reopen gate of a database directory carrying an unverified closed copy left by an HA resync, on a follower and on a
 * node elected leader.
 */
class UnverifiedClosedCopyReopenGateTest {
  // Regression: #8589 getDatabase refuses to reopen a closed copy marked unverified
  /**
   * Regression tests for issue #8589, server side: {@link ArcadeDBServer#getDatabase} is the single reopen point, and a
   * database directory carrying {@link ArcadeDBServer#UNVERIFIED_CLOSED_COPY_FILE} - a closed copy the last HA resync
   * could not verify because the leader did not hold it - is not reopened on a follower, by a request or by the boot
   * scan. The leader reopens it and drops the mark; a server without HA ignores it.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class FollowerReopenGate {
    private static final String MARKED   = "db8589";
    private static final String UNMARKED = "db8589plain";
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
              ((DatabaseInternal) server.getDatabaseForOperator(name)).getEmbedded().close();
          } catch (final Exception ignore) {
            // best-effort cleanup; the @TempDir is removed regardless
          }
        server.stop();
      }
    }

    /** The boot scan skips the marked copy instead of opening it, and does not fail the boot for it. */
    @Test
    void theBootScanDoesNotOpenAMarkedCopyWhenHAIsRequested() throws IOException {
      createDatabaseOnDisk(MARKED, true);
      createDatabaseOnDisk(UNMARKED, false);

      server = startServer(true, null);

      assertThat(server.existsDatabase(UNMARKED)).as("an unmarked copy loads as before").isTrue();
      assertThat(server.existsDatabase(MARKED)).as("the marked copy is not opened").isFalse();
      assertThat(Files.exists(marker(MARKED))).as("and its mark is kept").isTrue();
    }

    /** The issue as reported: the next request that names it on a follower is refused, not served unclamped. */
    @Test
    void aFollowerRefusesToReopenAMarkedCopy() throws IOException {
      createDatabaseOnDisk(MARKED, true);
      server = startServer(true, null);
      server.setHA(ha(false));

      assertThatThrownBy(() -> server.getDatabaseForOperator(MARKED))
          .isInstanceOf(DatabaseNotAvailableException.class)
          .hasMessageContaining("could not verify");
      assertThat(server.existsDatabase(MARKED)).isFalse();
      assertThat(Files.exists(marker(MARKED))).isTrue();
    }

    /** Before the HA plugin registers, the role is unknown, which is read as a follower. */
    @Test
    void aServerWhoseHAPluginHasNotRegisteredRefusesToo() throws IOException {
      createDatabaseOnDisk(MARKED, true);
      server = startServer(true, null);

      assertThatThrownBy(() -> server.getDatabaseForOperator(MARKED)).isInstanceOf(DatabaseNotAvailableException.class);
    }

    /**
     * The leader's copy is the cluster's: it reopens and the mark goes, so a later step-down does not refuse it - once its
     * peers have said none of them holds a newer copy (issue #8605, {@code UnverifiedClosedCopyReopenGateTest.LeaderReopenGate}).
     */
    @Test
    void theLeaderReopensAMarkedCopyAndDropsTheMark() throws IOException {
      createDatabaseOnDisk(MARKED, true);
      server = startServer(true, null);
      final HAServerPlugin leader = ha(true);
      when(leader.refuseToReopenUnverifiedClosedCopy(MARKED)).thenReturn(null);
      server.setHA(leader);

      assertThat(server.getDatabaseForOperator(MARKED).isOpen()).isTrue();
      assertThat(Files.exists(marker(MARKED))).isFalse();
    }

    /** Without HA nothing is replicated and nothing is unverified: the mark is ignored, and kept. */
    @Test
    void aServerWithoutHAIgnoresTheMark() throws IOException {
      createDatabaseOnDisk(MARKED, true);
      server = startServer(false, null);

      assertThat(server.existsDatabase(MARKED)).isTrue();
      assertThat(Files.exists(marker(MARKED))).isTrue();
    }

    /** A default database with a marked copy is neither opened nor recreated over it. */
    @Test
    void aMarkedDefaultDatabaseIsNeitherOpenedNorRecreated() throws IOException {
      createDatabaseOnDisk(MARKED, true);
      server = startServer(true, MARKED + "[root]");

      assertThat(server.existsDatabase(MARKED)).isFalse();
      assertThat(Files.exists(marker(MARKED))).isTrue();
      try (final Database db = new DatabaseFactory(root.resolve("databases").resolve(MARKED).toString()).open()) {
        assertThat(db.getSchema().existsType("Node")).as("the copy on disk is the original, not a new one").isTrue();
      }
    }

    // ------------------------------------------------------------------------------------------------------------

    private static HAServerPlugin ha(final boolean leader) {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.isLeader()).thenReturn(leader);
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

    private ArcadeDBServer startServer(final boolean haRequested, final String defaultDatabases) throws IOException {
      Files.createDirectories(root.resolve("databases"));
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_8589");
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(StaticBaseServerTest.allocateFreePorts(1)[0]));
      // No HA plugin lives in this module: HA is REQUESTED and nothing registers, which is the boot scan's window.
      config.setValue(GlobalConfiguration.HA_ENABLED, haRequested);
      if (defaultDatabases != null)
        config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES, defaultDatabases);
      config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

      final ArcadeDBServer started = new ArcadeDBServer(config);
      started.start();
      return started;
    }
  }

  // Regression: #8605 a node elected leader does not reopen an unverified closed copy unasked
  /**
   * Regression tests for issue #8605, server side. A node holding a closed copy marked unverified by #8589 that is elected
   * leader used to reopen it unasked, as the cluster's copy, and drop the mark - even when the previous leader held a
   * newer copy closed. {@link ArcadeDBServer#getDatabase} now asks {@link HAServerPlugin#refuseToReopenUnverifiedClosedCopy}
   * first, on the leader, and reopens only when the peers verified the copy.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class LeaderReopenGate {
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
              ((DatabaseInternal) server.getDatabaseForOperator(name)).getEmbedded().close();
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

      assertThatThrownBy(() -> server.getDatabaseForOperator(MARKED))
          .isInstanceOf(DatabaseNotAvailableException.class)
          .hasMessageContaining("did not confirm")
          // the way out is the audited override of issue #8641, not deleting the marker by hand
          .hasMessageContaining("POST /api/v1/cluster/accept-copy/" + MARKED)
          // the reason names peers and failed-call text: it goes to the log and the cluster alert, not to the client
          .hasMessageNotContaining("peer-1");
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

      assertThat(server.getDatabaseForOperator(MARKED).isOpen()).isTrue();
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

      assertThat(server.getDatabaseForOperator(UNMARKED).isOpen()).isTrue();
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

      assertThatThrownBy(() -> server.getDatabaseForOperator(MARKED)).isInstanceOf(DatabaseNotAvailableException.class);
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

      assertThat(ha.refuseToReopenUnverifiedClosedCopy(MARKED)).contains("cannot compare");
      assertThatThrownBy(() -> server.getDatabaseForOperator(MARKED))
          .isInstanceOf(DatabaseNotAvailableException.class)
          .hasMessageContaining("did not confirm");
      assertThat(Files.exists(marker(MARKED))).isTrue();
    }

    /**
     * The predicate the boot scan and the default-database pass skip on: a node that already leads at the second pass
     * does not open a marked copy there - that open would ask every peer from the boot thread - and leaves it to the next
     * request, which goes through the check.
     */
    @Test
    void theBootScansSkipAMarkedCopyOnANodeThatAlreadyLeads() throws IOException {
      createDatabaseOnDisk(MARKED, true);
      createDatabaseOnDisk(UNMARKED, false);
      server = startServer();
      final Path databases = root.resolve("databases");

      server.setHA(leader(null));
      assertThat(server.leaderHoldsUnverifiedClosedCopy(databases.resolve(MARKED).toFile())).isTrue();
      assertThat(server.leaderHoldsUnverifiedClosedCopy(databases.resolve(UNMARKED).toFile())).isFalse();

      final HAServerPlugin follower = mock(HAServerPlugin.class);
      when(follower.isLeader()).thenReturn(false);
      server.setHA(follower);
      assertThat(server.leaderHoldsUnverifiedClosedCopy(databases.resolve(MARKED).toFile()))
          .as("a follower skips it through the #8589 predicate instead").isFalse();
      assertThat(server.refusesUnverifiedClosedCopy(databases.resolve(MARKED).toFile())).isTrue();
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
}
