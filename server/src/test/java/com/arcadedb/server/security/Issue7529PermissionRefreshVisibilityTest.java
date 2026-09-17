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
package com.arcadedb.server.security;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.FileManager;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7529: a node has to be able to SAY whether the replicated group changes it received have been enforced
 * here, not merely received.
 * <p>
 * Issue #7510 closed the lag; what it left open was that the hand-off logged only on its two failure paths and
 * counted nothing, so the only evidence the fast path existed was the absence of a WARNING. A sweep that silently
 * stopped running looked, from every surface an operator can poll, exactly like a healthy node: {@code GET
 * /server/groups} reports the new document either way, and the {@code 200} from the node that served the change
 * says nothing about the others.
 * <p>
 * The assertions below are on the counters rather than on the enforcement - {@code Issue7510ReplicatedGroupRefreshTest}
 * owns that - because what is under test here is whether the counters tell the truth about what happened.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7529PermissionRefreshVisibilityTest {

  private static final String CONFIG_PATH        = "target/test-security-7529-visibility";
  private static final String DATABASE           = "graph";
  private static final String GROUP              = "editors";
  private static final int    RELOAD_EVERY_MS    = 600_000;
  private static final long   REFRESH_TIMEOUT_MS = 10_000;

  private ServerSecurity security;
  private ServerDatabase database;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    assertThat(dir.mkdirs()).isTrue();

    database = mockDatabase(DATABASE);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getDatabaseNames()).thenReturn(Set.of(DATABASE));
    when(server.getDatabase(DATABASE)).thenReturn(database);

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, RELOAD_EVERY_MS);

    security = new ServerSecurity(server, configuration, CONFIG_PATH);
    when(server.getSecurity()).thenReturn(security);
  }

  @AfterEach
  void tearDown() {
    if (security != null)
      security.stopService();
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  /** A fresh node has done nothing, and says so with zeros rather than with an absent section. */
  @Test
  void aNodeThatHasReceivedNothingReportsZeros() {
    final PermissionRefreshMetrics.Snapshot stats = security.getPermissionRefreshStats();

    assertThat(stats.entriesApplied()).isZero();
    assertThat(stats.sweepsCompleted()).isZero();
    assertThat(stats.sweepsFailed()).isZero();
    assertThat(stats.lastEntryAppliedAt()).isZero();
    assertThat(stats.lastSweepAt()).isZero();
  }

  /**
   * The headline pair. An applied entry and the sweep it triggers are both counted, and the two timestamps are
   * set - which is what lets a dashboard alert on the first rising while the second does not.
   */
  @Test
  void anAppliedGroupEntryIsCountedAndSoIsTheSweepItTriggers() {
    final long before = System.currentTimeMillis();

    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));

    assertThat(security.getPermissionRefreshStats().entriesApplied()).isEqualTo(1);
    assertThat(security.getPermissionRefreshStats().lastEntryAppliedAt()).isGreaterThanOrEqualTo(before);

    assertThat(await(() -> security.getPermissionRefreshStats().sweepsCompleted() >= 1))
        .as("the sweep the applied entry scheduled is counted when it finishes").isTrue();

    final PermissionRefreshMetrics.Snapshot stats = security.getPermissionRefreshStats();
    assertThat(stats.databasesRefreshed()).isGreaterThanOrEqualTo(1);
    assertThat(stats.databaseRefreshFailures()).isZero();
    assertThat(stats.sweepsFailed()).isZero();
    assertThat(stats.lastSweepAt()).isGreaterThanOrEqualTo(before);
  }

  /** Every hand-off is counted whether or not the worker took it, so the two numbers stay comparable. */
  @Test
  void everyHandOffToTheRefreshWorkerIsCounted() {
    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));
    security.applyReplicatedGroups(documentGranting(new JSONArray()));

    final PermissionRefreshMetrics.Snapshot stats = security.getPermissionRefreshStats();
    assertThat(stats.entriesApplied()).isEqualTo(2);
    assertThat(stats.refreshesRequested()).isEqualTo(2);
    // A coalesced hand-off is a subset of the requested ones, never a separate population: asserting the
    // relation rather than an exact count is what keeps this from depending on the worker's scheduling.
    assertThat(stats.refreshesCoalesced()).isBetween(0L, stats.refreshesRequested());
  }

  /**
   * The direction the issue is about. A database the sweep cannot re-derive is counted, and the sweep still
   * completes - which is the distinction an operator needs: {@code databaseRefreshFailures} rising while
   * {@code sweepsFailed} stays at 0 is one database, not a node that has stopped converging.
   */
  @Test
  void aDatabaseTheSweepCannotRefreshIsCountedWithoutFailingTheSweep() {
    final String path = CONFIG_PATH + "-mixed";
    final File dir = new File(path);
    FileUtils.deleteRecursively(dir);
    assertThat(dir.mkdirs()).isTrue();

    // Built before the stubbing: mockDatabase() stubs a mock of its own, and Mockito refuses a when() opened
    // inside another when()'s argument list.
    final ServerDatabase healthy = mockDatabase(DATABASE);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getDatabaseNames()).thenReturn(new LinkedHashSet<>(List.of("dropped-under-the-sweep", DATABASE)));
    when(server.getDatabase("dropped-under-the-sweep"))
        .thenThrow(new DatabaseNotAvailableException("Database 'dropped-under-the-sweep' is not available"));
    when(server.getDatabase(DATABASE)).thenReturn(healthy);

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, RELOAD_EVERY_MS);

    final ServerSecurity mixed = new ServerSecurity(server, configuration, path);
    when(server.getSecurity()).thenReturn(mixed);
    try {
      mixed.refreshAllDatabasePermissions();

      final PermissionRefreshMetrics.Snapshot stats = mixed.getPermissionRefreshStats();
      assertThat(stats.sweepsCompleted()).isEqualTo(1);
      assertThat(stats.sweepsFailed()).as("one refused database is not a failed sweep").isZero();
      assertThat(stats.databaseRefreshFailures()).isEqualTo(1);
      assertThat(stats.databasesRefreshed()).as("the database after the refused one was still refreshed")
          .isEqualTo(1);
    } finally {
      mixed.stopService();
      FileUtils.deleteRecursively(dir);
    }
  }

  /**
   * A sweep run by the {@code server-groups.json} watcher counts exactly like one run by a replicated apply.
   * Counting it in the worker instead would make {@code sweepsCompleted} mean different things on a peer and on
   * the node that served the change, which is the comparison the number exists for.
   */
  @Test
  void aSweepFromAnySourceIsCounted() {
    security.refreshAllDatabasePermissions();

    final PermissionRefreshMetrics.Snapshot stats = security.getPermissionRefreshStats();
    assertThat(stats.sweepsCompleted()).isEqualTo(1);
    assertThat(stats.entriesApplied()).as("no replicated entry was involved").isZero();
  }

  /** The snapshot is a value: holding one must not make it track later activity. */
  @Test
  void aSnapshotDoesNotMoveUnderTheReader() {
    final PermissionRefreshMetrics.Snapshot taken = security.getPermissionRefreshStats();

    security.applyReplicatedGroups(documentGranting(new JSONArray()));

    assertThat(taken.entriesApplied()).isZero();
    assertThat(security.getPermissionRefreshStats().entriesApplied()).isEqualTo(1);
  }

  private static boolean await(final BooleanSupplier condition) {
    final long deadline = System.currentTimeMillis() + REFRESH_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean())
        return true;
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return condition.getAsBoolean();
  }

  private static JSONObject groupWith(final JSONArray databaseAccess) {
    return new JSONObject()
        .put("access", databaseAccess)
        .put("resultSetLimit", -1L)
        .put("readTimeout", -1L)
        .put("types", new JSONObject().put("*", new JSONObject().put("access",
            new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord"))));
  }

  private static String documentGranting(final JSONArray databaseAccess) {
    return new JSONObject()
        .put("version", ServerSecurity.LATEST_VERSION)
        .put("databases", new JSONObject().put(DATABASE,
            new JSONObject().put("groups", new JSONObject().put(GROUP, groupWith(databaseAccess)))))
        .toString();
  }

  private static ServerDatabase mockDatabase(final String name) {
    final FileManager fileManager = mock(FileManager.class);
    when(fileManager.getFiles()).thenReturn(List.of());

    final Schema schema = mock(Schema.class);
    when(schema.getTypes()).thenReturn(List.of());

    final ServerDatabase db = mock(ServerDatabase.class);
    when(db.getName()).thenReturn(name);
    when(db.getFileManager()).thenReturn(fileManager);
    when(db.getSchema()).thenReturn(schema);
    return db;
  }
}
