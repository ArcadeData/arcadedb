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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8589, server side: {@link ArcadeDBServer#getDatabase} is the single reopen point, and a
 * database directory carrying {@link ArcadeDBServer#UNVERIFIED_CLOSED_COPY_FILE} - a closed copy the last HA resync
 * could not verify because the leader did not hold it - is not reopened on a follower, by a request or by the boot
 * scan. The leader reopens it and drops the mark; a server without HA ignores it.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8589UnverifiedClosedCopyReopenGateTest {

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
            ((DatabaseInternal) server.getDatabase(name)).getEmbedded().close();
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

    assertThatThrownBy(() -> server.getDatabase(MARKED))
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

    assertThatThrownBy(() -> server.getDatabase(MARKED)).isInstanceOf(DatabaseNotAvailableException.class);
  }

  /** The leader's copy is the cluster's: it reopens and the mark goes, so a later step-down does not refuse it. */
  @Test
  void theLeaderReopensAMarkedCopyAndDropsTheMark() throws IOException {
    createDatabaseOnDisk(MARKED, true);
    server = startServer(true, null);
    server.setHA(ha(true));

    assertThat(server.getDatabase(MARKED).isOpen()).isTrue();
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
