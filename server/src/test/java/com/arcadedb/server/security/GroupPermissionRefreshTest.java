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
package com.arcadedb.server.security;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.FileManager;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser.DATABASE_ACCESS;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.ServedDatabases;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.DedicatedThreadPool.PoolStats;
import com.arcadedb.utility.FileUtils;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/**
 * Refresh of the cached group permissions of a principal: the database-level grants and quotas, a replicated
 * group document reaching a peer, and the counters and pool statistics of the refresh worker.
 */
class GroupPermissionRefreshTest {
  /**
   * Unit regression test for issue #6806. {@code ServerSecurityDatabaseUser} keeps two independent permission
   * maps refreshed by two different methods, and the server's only refresh path called just one of them
   * ({@code updateFileAccess}), leaving the database-level grants read by {@code requestAccessOnDatabase} -
   * {@code updateSchema}, {@code updateSecurity}, {@code updateDatabaseSettings} - plus the
   * {@code resultSetLimit}/{@code readTimeout} quotas frozen at the values they had when the user first
   * touched the database. A revoked grant kept working until restart.
   * <p>
   * Drives {@link ServerSecurityUser#refreshDatabaseConfiguration} directly, which is what
   * {@code ServerSecurity.updateSchema} now calls for every user;
   * {@code LiveSecurityChangeIT.GroupPermissionRefresh} covers the same fix end to end over HTTP.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DatabaseConfigurationRefresh {
    /** Reading the group file starts its watcher; every security a test builds is stopped after it. */
    private static final List<ServerSecurity> SECURITIES = new ArrayList<>();

    @AfterEach
    void stopSecurities() {
      SECURITIES.forEach(ServerSecurity::stopService);
      SECURITIES.clear();
    }


    private static final String DATABASE = "issue6806db";
    private static final String GROUP    = "editor";

    @Test
    void revokedDatabaseGrantIsVisibleOnTheCachedDatabaseUser() {
      final DatabaseInternal database = mockDatabase();
      final ServerSecurityUser alice = newUser(database, groups(new JSONArray().put("updateSchema"), -1L));

      final ServerSecurityDatabaseUser dbUser = alice.getDatabaseUser(database);
      assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isTrue();

      alice.refreshDatabaseConfiguration(database, groups(new JSONArray(), -1L));

      // The SAME cached instance must now deny: getDatabaseUser() returns it forever (databaseCache is only
      // cleared by refreshDatabaseNames(), which nothing in src/main calls), so a stale map is permanent.
      assertThat(alice.getDatabaseUser(database)).isSameAs(dbUser);
      assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse();
    }

    @Test
    void refreshingOnlyTheFileAccessMapLeavesTheDatabaseGrantStale() {
      // Pins the mechanism of the bug, so a future refactor cannot quietly go back to it: updateFileAccess()
      // rebuilds the per-type/per-bucket maps and NOTHING else, so calling it alone - which is what
      // ServerSecurity.updateSchema() used to do - leaves databaseAccessMap exactly as it was.
      final DatabaseInternal database = mockDatabase();
      final ServerSecurityUser alice = newUser(database, groups(new JSONArray().put("updateSchema"), -1L));

      final ServerSecurityDatabaseUser dbUser = alice.getDatabaseUser(database);
      assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isTrue();

      dbUser.updateFileAccess(database, groups(new JSONArray(), -1L));
      assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
          .as("half a refresh leaves the revoked grant in effect - the defect of #6806").isTrue();

      dbUser.updateDatabaseConfiguration(groups(new JSONArray(), -1L));
      assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse();
    }

    @Test
    void newlyGrantedDatabaseGrantIsVisibleOnTheCachedDatabaseUser() {
      // The gap was symmetric: a grant added after the user first touched the database was ignored too.
      final DatabaseInternal database = mockDatabase();
      final ServerSecurityUser alice = newUser(database, groups(new JSONArray(), -1L));

      final ServerSecurityDatabaseUser dbUser = alice.getDatabaseUser(database);
      assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SECURITY)).isFalse();

      alice.refreshDatabaseConfiguration(database, groups(new JSONArray().put("updateSecurity"), -1L));

      assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SECURITY)).isTrue();
    }

    @Test
    void aRefreshNeverPublishesHalfOfTheNewConfiguration() throws Exception {
      // The authorization checks are unsynchronized by design - they run on every record access - so the two
      // halves of the permission state have to be published as one value. When they were two independent
      // volatile writes, a reader could see the new database-level grants crossed with the old per-type map.
      // A reader spinning across many refreshes must only ever observe one configuration or the other.
      final DatabaseInternal database = mockDatabase();
      final ServerSecurityUser alice = newUser(database, groups(new JSONArray().put("updateSchema"), 7L));
      final ServerSecurityDatabaseUser dbUser = alice.getDatabaseUser(database);

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicReference<String> mixed = new AtomicReference<>();
      final Thread reader = new Thread(() -> {
        while (!stop.get()) {
          // ONE read of the published state - two successive calls to the public checks are two reads and may
          // legitimately straddle a refresh, which is exactly why the state is one value. In this
          // configuration the halves move together: updateSchema is granted exactly when the quota is 7, so a
          // snapshot carrying one without the other could only come from a half-published refresh.
          final ServerSecurityDatabaseUser.AccessSnapshot snapshot = dbUser.currentAccess();
          final boolean granted = snapshot.databaseAccess()[DATABASE_ACCESS.UPDATE_SCHEMA.ordinal()];
          final long limit = snapshot.resultSetLimit();
          if (granted != (limit == 7L))
            mixed.compareAndSet(null, "updateSchema=" + granted + " resultSetLimit=" + limit);
        }
      });
      reader.start();
      try {
        for (int i = 0; i < 2_000; i++)
          alice.refreshDatabaseConfiguration(database,
              i % 2 == 0 ? groups(new JSONArray(), -1L) : groups(new JSONArray().put("updateSchema"), 7L));
      } finally {
        stop.set(true);
        reader.join(30_000);
      }

      assertThat(mixed.get()).as("observed a half-published refresh").isNull();
    }

    @Test
    void groupQuotasAreRefreshedToo() {
      final DatabaseInternal database = mockDatabase();
      final ServerSecurityUser alice = newUser(database, groups(new JSONArray(), -1L));

      final ServerSecurityDatabaseUser dbUser = alice.getDatabaseUser(database);
      assertThat(dbUser.getResultSetLimit()).isEqualTo(-1L);

      alice.refreshDatabaseConfiguration(database, groups(new JSONArray(), 7L));

      assertThat(dbUser.getResultSetLimit()).isEqualTo(7L);
    }

    @Test
    void aSyntheticPrincipalIsNeverWidenedByTheGroupFile() {
      // An API-token principal carries its own group definition. The refresh must keep using it, exactly as
      // registerDatabaseUser() does, so the groups of server-groups.json can never grant it more than the
      // token's own permissions.
      final DatabaseInternal database = mockDatabase();
      final ServerSecurityUser token = newUser(database, groups(new JSONArray(), -1L))
          .withSyntheticGroupConfig(groups(new JSONArray(), -1L));

      final ServerSecurityDatabaseUser dbUser = token.getDatabaseUser(database);
      assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse();

      token.refreshDatabaseConfiguration(database, groups(new JSONArray().put("updateSchema"), -1L));

      assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse();
    }

    @Test
    void refreshingAUserThatNeverTouchedTheDatabaseIsANoOp() {
      final DatabaseInternal database = mockDatabase();
      // The server's live configuration (what registerDatabaseUser will read) grants nothing.
      final ServerSecurityUser alice = newUser(database, groups(new JSONArray(), -1L));

      // Nothing is cached for this pair, so the refresh must do nothing at all rather than materialise an
      // entry - ServerSecurity.updateSchema() sweeps EVERY server user on every schema change, and building a
      // ServerSecurityDatabaseUser for each of them would be both wasteful and a way to inject a permission
      // set that never came from the live configuration.
      alice.refreshDatabaseConfiguration(database, groups(new JSONArray().put("updateSchema"), -1L));

      // First access builds the entry from the live configuration, which still grants nothing.
      assertThat(alice.getDatabaseUser(database).requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse();
    }

    private static JSONObject groups(final JSONArray databaseAccess, final long resultSetLimit) {
      return new JSONObject().put(GROUP, new JSONObject()
          .put("access", databaseAccess)
          .put("resultSetLimit", resultSetLimit)
          .put("readTimeout", -1L)
          .put("types", new JSONObject().put("*", new JSONObject().put("access",
              new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord")))));
    }

    private static ServerSecurityUser newUser(final DatabaseInternal database, final JSONObject groupConfiguration) {
      // A real security reading the group configuration from its file, as on a node
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      final Path configPath = Path.of(server.getConfigPath());
      try {
        Files.createDirectories(configPath);
        Files.writeString(configPath.resolve(SecurityGroupFileRepository.FILE_NAME), new JSONObject()
            .put("version", ServerSecurity.LATEST_VERSION)
            .put("databases", new JSONObject().put(DATABASE, new JSONObject().put("groups", groupConfiguration))).toString());
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
      final ServerSecurity security = new ServerSecurity(server, new ContextConfiguration(), configPath.toString());
      SECURITIES.add(security);
      server.security(security);

      final JSONObject userConfiguration = new JSONObject()
          .put("name", "alice")
          .put("password", "irrelevant")
          .put("databases", new JSONObject().put(DATABASE, new JSONArray().put(GROUP)));

      return new ServerSecurityUser(server, userConfiguration);
    }

    private static DatabaseInternal mockDatabase() {
      final FileManager fileManager = mock(FileManager.class);
      when(fileManager.getFiles()).thenReturn(List.of());

      final Schema schema = mock(Schema.class);
      when(schema.getTypes()).thenReturn(List.of());

      final DatabaseInternal database = mock(DatabaseInternal.class);
      when(database.getName()).thenReturn(DATABASE);
      when(database.getFileManager()).thenReturn(fileManager);
      when(database.getSchema()).thenReturn(schema);
      return database;
    }
  }

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
  @Nested
  class ReplicatedGroupRefresh {
    @RegisterExtension
    static final ServedDatabases SERVED = new ServedDatabases();

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

      database = SERVED.open(DATABASE);

      // A mocked server that reports the database as open, which is what makes this a peer with something to
      // refresh: ServerSecurity resolves the principal's permissions through server.getSecurity(), and the refresh
      // walks server.getDatabaseNames() / getDatabase(name). getHA() answers null, so this node is not itself
      // submitting Raft entries - exactly the peer applying someone else's entry.
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.databaseNames(DATABASE);
      server.on("getDatabaseForOperator", args -> Objects.equals(args[0], DATABASE) ? database : null);

      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, RELOAD_EVERY_MS);

      security = new ServerSecurity(server, configuration, CONFIG_PATH);
      server.security(security);
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

      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.databaseNames(DATABASE);
      server.on("getDatabaseForOperator", args -> Objects.equals(args[0], DATABASE) ? database : null);

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
      server.security(counting);

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

      final ServerDatabase healthy = SERVED.open(DATABASE);
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      // A LinkedHashSet so the broken name is iterated FIRST: with the guard around the loop instead of inside it,
      // the healthy database that follows would never be reached.
      server.returns("getDatabaseNames", new LinkedHashSet<>(List.of("dropped-under-the-sweep", DATABASE)));
      server.on("getDatabaseForOperator", args -> {
        if (Objects.equals(args[0], "dropped-under-the-sweep"))
          throw new DatabaseNotAvailableException("Database 'dropped-under-the-sweep' is not available");
        return Objects.equals(args[0], DATABASE) ? healthy : null;
      });

      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, RELOAD_EVERY_MS);

      final ServerSecurity mixed = new ServerSecurity(server, configuration, healthyPath);
      server.security(mixed);
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

  }

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
   * The assertions below are on the counters rather than on the enforcement - {@code GroupPermissionRefreshTest.ReplicatedGroupRefresh}
   * owns that - because what is under test here is whether the counters tell the truth about what happened.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RefreshVisibility {
    @RegisterExtension
    static final ServedDatabases SERVED = new ServedDatabases();

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

      database = SERVED.open(DATABASE);

      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.databaseNames(DATABASE);
      server.on("getDatabaseForOperator", args -> Objects.equals(args[0], DATABASE) ? database : null);

      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, RELOAD_EVERY_MS);

      security = new ServerSecurity(server, configuration, CONFIG_PATH);
      server.security(security);
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

      final ServerDatabase healthy = SERVED.open(DATABASE);

      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.returns("getDatabaseNames", new LinkedHashSet<>(List.of("dropped-under-the-sweep", DATABASE)));
      server.on("getDatabaseForOperator", args -> {
        if (Objects.equals(args[0], "dropped-under-the-sweep"))
          throw new DatabaseNotAvailableException("Database 'dropped-under-the-sweep' is not available");
        return Objects.equals(args[0], DATABASE) ? healthy : null;
      });

      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, RELOAD_EVERY_MS);

      final ServerSecurity mixed = new ServerSecurity(server, configuration, path);
      server.security(mixed);
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

  }

  /**
   * Issue #7856: the permission-refresh worker's load - the numbers behind the {@code pool=security_refresh}
   * executor row - must describe the worker as it is: a refresh running, one queued in the single slot, and the
   * hand-offs coalesced behind it counted.
   * <p>
   * The sweep is held inside the worker on a latch so the busy state is observed rather than raced: the first
   * refresh is running, the second fills the one slot, and the third has nowhere to go.
   */
  @Nested
  class RefreshPoolStats {
    @RegisterExtension
    static final ServedDatabases SERVED = new ServedDatabases();

    private static final String CONFIG_PATH     = "target/test-security-7856-pool-stats";
    private static final String DATABASE        = "graph";
    private static final String WORKER_NAME     = "arcadedb-security-permissions-refresh";
    private static final long   TIMEOUT_MS      = 10_000;

    private final CountDownLatch sweepStarted = new CountDownLatch(1);
    private final CountDownLatch releaseSweep = new CountDownLatch(1);

    private ServerSecurity security;

    @BeforeEach
    void setUp() {
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
      final File dir = new File(CONFIG_PATH);
      if (dir.exists())
        FileUtils.deleteRecursively(dir);
      assertThat(dir.mkdirs()).isTrue();

      final ServerDatabase database = SERVED.open(DATABASE);
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.on("getDatabaseNames", args -> {
        // Only the worker is held: any other caller of the name list must not be parked by the test.
        if (WORKER_NAME.equals(Thread.currentThread().getName())) {
          sweepStarted.countDown();
          try {
            releaseSweep.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        return Set.of(DATABASE);
      });
      server.on("getDatabaseForOperator", args -> Objects.equals(args[0], DATABASE) ? database : null);

      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, 600_000);

      security = new ServerSecurity(server, configuration, CONFIG_PATH);
      server.security(security);
    }

    @AfterEach
    void tearDown() {
      releaseSweep.countDown();
      if (security != null)
        security.stopService();
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
      FileUtils.deleteRecursively(new File(CONFIG_PATH));
    }

    @Test
    void anIdleWorkerReportsNoThreadAndItsOneFreeSlot() {
      final PoolStats stats = security.getPermissionsRefreshPoolStats();

      assertThat(stats.poolSize()).as("core 0: an idle server carries no refresh thread").isZero();
      assertThat(stats.activeThreads()).isZero();
      assertThat(stats.queueDepth()).isZero();
      assertThat(stats.queueCapacityRemaining()).as("the single queue slot").isEqualTo(1);
      assertThat(stats.callerRunFallbacks()).as("the pool drops, it never runs on the submitter").isZero();
    }

    @Test
    void aBusyWorkerShowsItsQueuedRefreshAndTheCoalescedOneBehindIt() throws InterruptedException {
      security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));
      assertThat(sweepStarted.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).as("the first refresh must reach the worker")
          .isTrue();

      security.applyReplicatedGroups(documentGranting(new JSONArray()));
      security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));

      final PoolStats busy = security.getPermissionsRefreshPoolStats();
      assertThat(busy.poolSize()).isEqualTo(1);
      assertThat(busy.activeThreads()).isEqualTo(1);
      assertThat(busy.queueDepth()).as("the second refresh waits in the one slot").isEqualTo(1);
      assertThat(busy.queueCapacityRemaining()).isZero();
      assertThat(security.getPermissionRefreshStats().refreshesCoalesced())
          .as("the third found the slot taken and was coalesced into the queued one").isEqualTo(1);

      releaseSweep.countDown();

      assertThat(await(() -> security.getPermissionsRefreshPoolStats().completedTasks() == 2))
          .as("the running and the queued refresh both finish on the worker").isTrue();
      assertThat(security.getPermissionsRefreshPoolStats().queueDepth()).isZero();
    }

    private static boolean await(final BooleanSupplier condition) {
      final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
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

    private static String documentGranting(final JSONArray databaseAccess) {
      final JSONObject group = new JSONObject()
          .put("access", databaseAccess)
          .put("resultSetLimit", -1L)
          .put("readTimeout", -1L)
          .put("types", new JSONObject().put("*", new JSONObject().put("access",
              new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord"))));
      return new JSONObject()
          .put("version", ServerSecurity.LATEST_VERSION)
          .put("databases", new JSONObject().put(DATABASE,
              new JSONObject().put("groups", new JSONObject().put("editors", group))))
          .toString();
    }

  }
}
