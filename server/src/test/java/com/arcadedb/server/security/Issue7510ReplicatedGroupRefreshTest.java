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
import com.arcadedb.security.SecurityDatabaseUser.DATABASE_ACCESS;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7510: a group document that arrives over HA replication must reach the peer's <b>cached</b> permissions
 * without waiting for the {@code arcadedb.server.reloadEvery} file-watcher tick.
 * <p>
 * The peer half of #7373 installed the document and stopped there - correctly, because
 * {@link ServerSecurity#applyReplicatedGroups} runs on the Raft state-machine apply thread and
 * {@link ServerSecurity#updateSchema} walks every open database. Convergence was then left to
 * {@code SecurityGroupFileRepository}'s watcher, i.e. up to one reload interval (5 s by default) during which a
 * principal already connected to that peer kept the permission the operator had just narrowed.
 * <p>
 * Every assertion below waits for an <i>outcome</i> rather than for the executor, so the test says nothing about
 * how the refresh is scheduled: it fails by timing out if the peer converges only on the watcher tick, which the
 * watcher cannot deliver here - {@link #RELOAD_EVERY_MS} is far longer than {@link #REFRESH_TIMEOUT_MS}.
 */
class Issue7510ReplicatedGroupRefreshTest {

  private static final String CONFIG_PATH        = "target/test-security-7510-refresh";
  private static final String DATABASE           = "graph";
  private static final String GROUP              = "editors";
  /** Long enough that a pass can only come from the replicated-apply refresh, never from the file watcher. */
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

    database = mockDatabase();

    // A mocked server that reports the database as open, which is what makes this a peer with something to
    // refresh: ServerSecurity resolves the principal's permissions through server.getSecurity(), and the refresh
    // walks server.getDatabaseNames() / getDatabase(name). getHA() answers null, so this node is not itself
    // submitting Raft entries - exactly the peer applying someone else's entry.
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

  /**
   * Row 1 of the coverage table - the direction that matters. A narrowed permission must stop being granted on
   * the peer without the reload tick.
   */
  @Test
  void aReplicatedRevocationReachesTheCachedPrincipalWithoutTheReloadTick() {
    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));

    final ServerSecurityUser alice = createAlice();
    final ServerSecurityDatabaseUser cached = alice.getDatabaseUser(database);
    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
        .as("the grant is in force on the peer before the revocation").isTrue();

    security.applyReplicatedGroups(documentGranting(new JSONArray()));

    assertThat(security.getDatabaseGroupsConfiguration(DATABASE).getJSONObject(GROUP).getJSONArray("access"))
        .as("the document itself is revoked immediately, as it always was").isEmpty();
    assertThat(await(() -> !cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)))
        .as("the principal already connected to the peer is denied without waiting for the reload tick").isTrue();
  }

  /** Row 2 - the grant direction, so the test cannot pass by denying everything. */
  @Test
  void aReplicatedGrantReachesTheCachedPrincipalWithoutTheReloadTick() {
    security.applyReplicatedGroups(documentGranting(new JSONArray()));

    final ServerSecurityUser alice = createAlice();
    final ServerSecurityDatabaseUser cached = alice.getDatabaseUser(database);
    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
        .as("the peer starts with no grant").isFalse();

    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));

    assertThat(await(() -> cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)))
        .as("the widened grant reaches the cached principal without waiting for the reload tick").isTrue();
  }

  /**
   * Row 3 - the peer could not write {@code server-groups.json}, so the apply reports the durability failure. The
   * document is nonetheless in force in memory from that moment (see {@code SecurityGroupFileRepository#applyReplicated}),
   * so the refresh has to have been scheduled before the throw, not after it.
   */
  @Test
  void aRefreshIsStillScheduledWhenTheReplicatedWriteFails() throws Exception {
    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));

    final ServerSecurityUser alice = createAlice();
    final ServerSecurityDatabaseUser cached = alice.getDatabaseUser(database);
    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isTrue();

    // Make the write fail deterministically, on every platform and for root too: replace the configuration
    // DIRECTORY with a regular file of the same name. SecurityGroupFileRepository.persist() then calls
    // Files.createTempFile() with that file as the parent directory, which cannot succeed whatever the
    // permissions say - where chmod 0555 is merely usually enough, and is not enough at all for a privileged
    // process, which would let this test pass without ever reaching the branch it is named after.
    final File blocker = new File(CONFIG_PATH);
    FileUtils.deleteRecursively(blocker);
    try {
      assertThat(blocker.createNewFile()).isTrue();

      // Asserted, not tolerated: the durability half MUST have failed, or the enforcement assertion below would
      // be the ordinary success path wearing this test's name.
      assertThatThrownBy(() -> security.applyReplicatedGroups(documentGranting(new JSONArray())))
          .isInstanceOf(ReplicatedSecurityConfigPersistenceException.class);

      assertThat(await(() -> !cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)))
          .as("the revocation is enforced on the peer even though persisting it failed").isTrue();
    } finally {
      blocker.delete();
    }
  }

  /**
   * The refresh's read-and-publish must be one critical section. Several threads refresh independently - the group
   * file's watcher, {@code ServerControlPlane} on an admin request, {@code LocalDatabase.open()}, and the worker
   * this issue added - and an unsynchronised read let the slower of two publish the OLDER document last, writing a
   * widened grant back over a narrowed one (CWE-863).
   * <p>
   * Asserted as mutual exclusion rather than by trying to lose a race on purpose: the overlap is what the lock
   * forbids, and a lost update is only its consequence. The override sleeps inside the configuration read, so
   * without the lock several sweeps are inside it at once and the peak count exceeds one.
   */
  @Test
  void concurrentRefreshesNeverOverlapTheirReadAndPublish() throws Exception {
    final String path = CONFIG_PATH + "-exclusion";
    final File dir = new File(path);
    FileUtils.deleteRecursively(dir);
    assertThat(dir.mkdirs()).isTrue();

    final AtomicInteger inside = new AtomicInteger();
    final AtomicInteger peak = new AtomicInteger();

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getDatabaseNames()).thenReturn(Set.of(DATABASE));
    when(server.getDatabase(DATABASE)).thenReturn(database);

    final ServerSecurity counting = new ServerSecurity(server, new ContextConfiguration(), path) {
      @Override
      protected JSONObject getDatabaseGroupsConfiguration(final String databaseName) {
        peak.accumulateAndGet(inside.incrementAndGet(), Math::max);
        try {
          Thread.sleep(50);
          return super.getDatabaseGroupsConfiguration(databaseName);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        } finally {
          inside.decrementAndGet();
        }
      }
    };
    when(server.getSecurity()).thenReturn(counting);

    final int threads = 4;
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    try {
      for (int i = 0; i < threads; i++)
        new Thread(() -> {
          try {
            start.await();
            counting.updateSchema(database);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        }, "refresh-" + i).start();

      start.countDown();
      assertThat(done.await(30, TimeUnit.SECONDS)).as("every refresh finished").isTrue();
      assertThat(peak.get())
          .as("no two refreshes were ever inside the read-and-publish section together").isEqualTo(1);
    } finally {
      counting.stopService();
      FileUtils.deleteRecursively(dir);
    }
  }

  /**
   * Row 7 - the body the group file's reload watcher has always run is now shared with the replicated path, so
   * assert it directly: it must walk every database the server has open.
   */
  @Test
  void theReloadWatcherBodyRefreshesEveryOpenDatabase() {
    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));

    final ServerSecurityUser alice = createAlice();
    final ServerSecurityDatabaseUser cached = alice.getDatabaseUser(database);
    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isTrue();

    security.saveGroup(DATABASE, GROUP, groupWith(new JSONArray()));
    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
        .as("a local save does not refresh the caches by itself - ServerControlPlane does that").isTrue();

    security.refreshAllDatabasePermissions();

    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
        .as("the shared refresh body re-derives every open database from the current document").isFalse();
  }

  /**
   * The sweep is guarded per database, not once around the loop: one database that cannot be handed over - dropped
   * under the iteration, or still carrying the interrupted-snapshot marker {@code ArcadeDBServer.getDatabase()}
   * refuses with {@code DatabaseNotAvailableException} - must cost only its own refresh. Aborting would leave
   * every database after it in the iteration waiting for the reload tick, i.e. reintroduce this very issue for
   * them.
   */
  @Test
  void oneUnavailableDatabaseDoesNotStopTheOthersFromBeingRefreshed() {
    final String healthyPath = CONFIG_PATH + "-mixed";
    final File dir = new File(healthyPath);
    FileUtils.deleteRecursively(dir);
    assertThat(dir.mkdirs()).isTrue();

    final ServerDatabase healthy = mockDatabase();
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    // A LinkedHashSet so the broken name is iterated FIRST: with the guard around the loop instead of inside it,
    // the healthy database that follows would never be reached.
    when(server.getDatabaseNames()).thenReturn(new LinkedHashSet<>(List.of("dropped-under-the-sweep", DATABASE)));
    when(server.getDatabase("dropped-under-the-sweep"))
        .thenThrow(new DatabaseNotAvailableException("Database 'dropped-under-the-sweep' is not available"));
    when(server.getDatabase(DATABASE)).thenReturn(healthy);

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, RELOAD_EVERY_MS);

    final ServerSecurity mixed = new ServerSecurity(server, configuration, healthyPath);
    when(server.getSecurity()).thenReturn(mixed);
    try {
      mixed.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));

      final ServerSecurityUser alice = mixed.createUser(new JSONObject()
          .put("name", "alice")
          .put("password", mixed.encodePassword("alice-password"))
          .put("databases", new JSONObject().put(DATABASE, new JSONArray().put(GROUP))));
      final ServerSecurityDatabaseUser cached = alice.getDatabaseUser(healthy);
      assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isTrue();

      mixed.applyReplicatedGroups(documentGranting(new JSONArray()));

      assertThat(await(() -> !cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)))
          .as("the database after the unavailable one is still refreshed").isTrue();
    } finally {
      mixed.stopService();
      FileUtils.deleteRecursively(dir);
    }
  }

  private ServerSecurityUser createAlice() {
    return security.createUser(new JSONObject()
        .put("name", "alice")
        .put("password", security.encodePassword("alice-password"))
        .put("databases", new JSONObject().put(DATABASE, new JSONArray().put(GROUP))));
  }

  /**
   * Waits for an outcome rather than for a thread: the refresh is asynchronous by design, and a test that
   * asserted on the executor would pin the mechanism instead of the guarantee.
   */
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

  /** A whole group document, in the shape a {@code SECURITY_GROUPS_ENTRY} carries. */
  private static String documentGranting(final JSONArray databaseAccess) {
    return new JSONObject()
        .put("version", ServerSecurity.LATEST_VERSION)
        .put("databases", new JSONObject().put(DATABASE,
            new JSONObject().put("groups", new JSONObject().put(GROUP, groupWith(databaseAccess)))))
        .toString();
  }

  private static ServerDatabase mockDatabase() {
    final FileManager fileManager = mock(FileManager.class);
    when(fileManager.getFiles()).thenReturn(List.of());

    final Schema schema = mock(Schema.class);
    when(schema.getTypes()).thenReturn(List.of());

    final ServerDatabase db = mock(ServerDatabase.class);
    when(db.getName()).thenReturn(DATABASE);
    when(db.getFileManager()).thenReturn(fileManager);
    when(db.getSchema()).thenReturn(schema);
    return db;
  }
}
