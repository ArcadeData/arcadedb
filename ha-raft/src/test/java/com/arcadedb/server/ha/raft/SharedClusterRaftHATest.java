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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Opt-in variant of {@link BaseRaftHATest} that boots the cluster ONCE per class instead of once per test. A cluster
 * boot costs about 12 s, so a class of N small tests pays it once rather than N times.
 * <p>
 * How it works: the class runs with {@link TestInstance.Lifecycle#PER_CLASS}, {@link #startSharedCluster()}
 * ({@code @BeforeAll}) and {@link #stopSharedCluster()} ({@code @AfterAll}) run the inherited per-test setup and
 * teardown, and {@link #beginTest()} / {@link #endTest()} are overridden WITHOUT {@code @BeforeEach} / {@code @AfterEach},
 * which is what stops JUnit from running them per test (JUnit runs an overriding method as a lifecycle method only when
 * the override carries the annotation itself). Both overrides are final, so a subclass cannot put the per-test
 * lifecycle back by re-annotating one. Per-class setup goes in an override of {@link #onSharedClusterStarted()}, NOT in
 * a bare {@code @BeforeAll}: a reboot recreates the database and loses whatever a {@code @BeforeAll} built, while the
 * hook runs again after every reboot. Per-test setup goes in a {@code @BeforeEach} method of its own. An override of
 * {@link #startSharedCluster()} or {@link #stopSharedCluster()} must carry {@code @BeforeAll} / {@code @AfterAll} again,
 * for the same reason as above.
 * <p>
 * Before every test {@link #ensureClusterIsHealthy()} checks, without sleeping when the cluster is healthy, that every
 * server is started, exactly one is leader, every replica has caught up with the leader, the test database exists on
 * every server and is not quarantined. Otherwise it reboots the whole cluster from scratch (stop,
 * {@code deleteDatabaseFolders()}, start) and logs why. That is a safety net, not a licence: a reboot costs what this
 * class exists to save. The check is not a proof of health: "caught up" is inferred from the applied index alone, so a
 * partitioned replica that has already caught up passes, and a test must therefore restore what it changed itself
 * rather than rely on the check to notice.
 * <p>
 * Rules for a test of a subclass, because the state it leaves is the next test's starting state:
 * <ul>
 *   <li>name every type, property, index, user, group and setting it creates with {@link #uniqueName(String)}, so two
 *       tests never collide on a name and none depends on a name being absent;</li>
 *   <li>do not assert on totals of the shared database (record counts of the fixture types, the type list): an earlier
 *       test may have added to them;</li>
 *   <li>a test that stops, isolates or restarts a node, transfers leadership or changes a server or global setting
 *       must leave the cluster healthy (restart what it stopped, restore what it changed) before it returns;</li>
 *   <li>the database comparison of {@code endTest()} runs once, after the last test, so a per-test divergence check
 *       has to be asserted in the test itself ({@code assertClusterConsistency()}).</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class SharedClusterRaftHATest extends BaseRaftHATest {
  private final AtomicInteger nameCounter = new AtomicInteger();

  @BeforeAll
  protected void startSharedCluster() {
    super.beginTest();
    onSharedClusterStarted();
  }

  /**
   * Per-class setup of a subclass: the schema, data, users or settings every test of the class relies on. Runs once the
   * shared cluster is up, AND again after every reboot {@link #ensureClusterIsHealthy()} performs, because a reboot
   * recreates the database from scratch. Setup done in a bare {@code @BeforeAll} of the subclass runs only once and is
   * lost by a reboot, so the remaining tests of the class would fail for a reason that points at them instead of at the
   * reboot. No-op by default.
   */
  protected void onSharedClusterStarted() {
  }

  @AfterAll
  protected void stopSharedCluster() {
    super.endTest();
  }

  /**
   * Not a per-test lifecycle method here (no {@code @BeforeEach}): {@link #startSharedCluster()} calls it once per class.
   */
  @Override
  public final void beginTest() {
    super.beginTest();
  }

  /**
   * Not a per-test lifecycle method here (no {@code @AfterEach}): {@link #stopSharedCluster()} calls it once per class.
   */
  @Override
  public final void endTest() {
    super.endTest();
  }

  @Override
  protected final boolean isClusterSharedAcrossTests() {
    return true;
  }

  /**
   * Gives the next test a healthy cluster: free when the one the previous test left is healthy, a full reboot when it
   * is not.
   */
  @BeforeEach
  void ensureClusterIsHealthy() {
    final String degraded = clusterDegradation();
    if (degraded == null)
      return;

    LogManager.instance().log(this, Level.WARNING, "TEST: shared cluster of %s is degraded (%s), rebooting it before the next test",
        getClass().getSimpleName(), degraded);
    stopEveryServer();
    // Issue #6297: the folders are deleted BEFORE resetAll(), as BaseGraphServerTest.endTest() does, because their paths
    // are resolved from the live configuration and a reset first would delete the wrong ones.
    deleteDatabaseFolders();
    GlobalConfiguration.resetAll();
    GlobalConfiguration.TEST.setValue(false);
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);
    super.beginTest();
    onSharedClusterStarted();
  }

  /**
   * Stops every started server of the degraded cluster, each in its own try, so one that fails to stop cannot leave the
   * next one running into the reboot.
   */
  private void stopEveryServer() {
    final ArcadeDBServer[] servers = getServers();
    if (servers == null)
      return;
    for (int i = servers.length - 1; i > -1; --i)
      try {
        if (servers[i] != null && servers[i].isStarted())
          servers[i].stop();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "TEST: could not stop server %d of the degraded shared cluster", e, i);
      }
  }

  /**
   * {@code prefix + "_" + n}, with {@code n} never repeated within the class, for the types, users and settings a test
   * creates on the shared cluster.
   */
  protected String uniqueName(final String prefix) {
    return prefix + "_" + nameCounter.incrementAndGet();
  }

  /**
   * Why the cluster cannot serve the next test, or {@code null} when it can. Each check answers at once on a healthy
   * cluster: {@link #findLeaderIndex()} and {@link #awaitReplicationIsCompleted(int)} only wait while the answer is
   * "not yet", which is a cluster a test left mid-election or mid-catch-up.
   */
  private String clusterDegradation() {
    final ArcadeDBServer[] servers = getServers();
    if (servers == null || servers.length != getServerCount())
      return "no cluster";
    for (int i = 0; i < servers.length; i++)
      if (servers[i] == null || !servers[i].isStarted())
        return "server " + i + " is down";

    if (findLeaderIndex() < 0)
      return "no leader";
    int leaders = 0;
    int leader = -1;
    for (int i = 0; i < servers.length; i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin == null || plugin.getRaftHAServer() == null)
        return "server " + i + " has no Raft plugin";
      if (plugin.isLeader()) {
        leaders++;
        leader = i;
      }
    }
    if (leaders != 1)
      return leaders + " leaders";

    for (int i = 0; i < servers.length; i++) {
      if (i != leader && !awaitReplicationIsCompleted(i))
        return "replica " + i + " did not catch up with leader " + leader;
      if (isCreateDatabases() && !servers[i].existsDatabase(getDatabaseName()))
        return "database missing on server " + i;
      if (isDatabaseQuarantined(i, getDatabaseName()))
        return "database quarantined on server " + i;
    }
    return null;
  }
}
