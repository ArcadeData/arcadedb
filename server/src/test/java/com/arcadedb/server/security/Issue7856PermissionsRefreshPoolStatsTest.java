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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.ServedDatabases;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.DedicatedThreadPool.PoolStats;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7856: the permission-refresh worker's load - the numbers behind the {@code pool=security_refresh}
 * executor row - must describe the worker as it is: a refresh running, one queued in the single slot, and the
 * hand-offs coalesced behind it counted.
 * <p>
 * The sweep is held inside the worker on a latch so the busy state is observed rather than raced: the first
 * refresh is running, the second fills the one slot, and the third has nowhere to go.
 */
class Issue7856PermissionsRefreshPoolStatsTest {
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
    server.on("getDatabase", args -> Objects.equals(args[0], DATABASE) ? database : null);

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
