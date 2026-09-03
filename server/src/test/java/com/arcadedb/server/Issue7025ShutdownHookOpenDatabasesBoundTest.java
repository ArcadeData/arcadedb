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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.server.ArcadeDBServer.STATUS;
import com.arcadedb.server.ArcadeDBServer.ShutdownHookBound;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7025: the shutdown hook applied its fixed 2000ms STARTING bound to a server whose
 * databases were already open - they open at the top of {@code startInternal()}, long before the status turns ONLINE -
 * so a shutdown signal arriving during the HTTP/plugin/HA bring-up gave up on the lifecycle lock after two seconds and
 * left every open database to a WAL replay on the next start.
 * <p>
 * The bound now follows the thing it protects: no database open, short fixed wait; databases open, the configured
 * {@code arcadedb.server.shutdownTimeout}; lock holder parked in {@code System.exit()} (the #5418 deadlock shape), no
 * wait at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7025ShutdownHookOpenDatabasesBoundTest extends StaticBaseServerTest {
  private static final String DATABASE_DIRECTORY = "./target/databases";
  private static final String DATABASE           = "issue7025";

  /**
   * How long the plugin below holds {@code start()} once the hook is waiting for the lock: longer than the fixed
   * 2000ms bound the hook used to apply, so the old behaviour - giving up and leaving the databases open - has time
   * to happen if it ever comes back. Not a latency assertion: nothing measures elapsed time here.
   */
  private static final long START_HOLD_MS = ArcadeDBServer.SHUTDOWN_HOOK_STARTING_TIMEOUT_MS + 1_000;

  @Test
  void theBoundFollowsTheOpenDatabasesNotTheStatus() {
    assertThat(ArcadeDBServer.chooseShutdownHookBound(STATUS.STARTING, false, false))
        .as("STARTING with nothing open: nothing to flush, short fixed wait").isEqualTo(ShutdownHookBound.STARTING_NO_DATABASES);
    assertThat(ArcadeDBServer.chooseShutdownHookBound(STATUS.STARTING, true, false))
        .as("STARTING with databases open: the configured timeout, exactly as when ONLINE").isEqualTo(ShutdownHookBound.CONFIGURED);
    for (final STATUS status : new STATUS[] { STATUS.OFFLINE, STATUS.ONLINE, STATUS.SHUTTING_DOWN })
      for (final boolean open : new boolean[] { false, true })
        assertThat(ArcadeDBServer.chooseShutdownHookBound(status, open, false)).as(status + " open=" + open)
            .isEqualTo(ShutdownHookBound.CONFIGURED);
    // THE #5418 SHAPE WINS OVER EVERYTHING: THE HOLDER CAN NEVER RELEASE THE LOCK, SO NO WAIT CAN HELP
    assertThat(ArcadeDBServer.chooseShutdownHookBound(STATUS.STARTING, true, true)).isEqualTo(ShutdownHookBound.HOLDER_EXITING);
    assertThat(ArcadeDBServer.chooseShutdownHookBound(STATUS.ONLINE, true, true)).isEqualTo(ShutdownHookBound.HOLDER_EXITING);
  }

  @Test
  void aThreadParkedInSystemExitIsRecognisedFromItsStack() {
    assertThat(ArcadeDBServer.isRunningShutdownHooks(new StackTraceElement[] {
        new StackTraceElement("java.lang.Object", "wait", "Object.java", 1),
        new StackTraceElement("java.lang.Thread", "join", "Thread.java", 1),
        new StackTraceElement("java.lang.ApplicationShutdownHooks", "runHooks", "ApplicationShutdownHooks.java", 1),
        new StackTraceElement("java.lang.Shutdown", "runHooks", "Shutdown.java", 1),
        new StackTraceElement("java.lang.Shutdown", "exit", "Shutdown.java", 1),
        new StackTraceElement("java.lang.Runtime", "exit", "Runtime.java", 1),
        new StackTraceElement("java.lang.System", "exit", "System.java", 1),
        new StackTraceElement("org.apache.ratis.util.ExitUtils", "terminate", "ExitUtils.java", 1),
        new StackTraceElement(ArcadeDBServer.class.getName(), "startInternal", "ArcadeDBServer.java", 1) })).isTrue();

    assertThat(ArcadeDBServer.isRunningShutdownHooks(new StackTraceElement[] {
        new StackTraceElement("java.net.ServerSocket", "bind", "ServerSocket.java", 1),
        new StackTraceElement(ArcadeDBServer.class.getName(), "startInternal", "ArcadeDBServer.java", 1),
        new StackTraceElement(ArcadeDBServer.class.getName(), "start", "ArcadeDBServer.java", 1) }))
        .as("a start() that is merely progressing must be waited for").isFalse();
    assertThat(ArcadeDBServer.isRunningShutdownHooks(new StackTraceElement[0])).isFalse();
  }

  @Test
  void theWarningNamesTheBoundThatWasApplied() {
    final String starting = ArcadeDBServer.shutdownHookLockTimeoutWarning(2_000, STATUS.STARTING,
        ShutdownHookBound.STARTING_NO_DATABASES);
    assertThat(starting).contains("no database open yet").contains("fixed 2000ms bound")
        .doesNotContain("Raise arcadedb.server.shutdownTimeout");

    final String configured = ArcadeDBServer.shutdownHookLockTimeoutWarning(60_000, STATUS.STARTING, ShutdownHookBound.CONFIGURED);
    assertThat(configured).as("with databases open the setting IS the bound, even while STARTING")
        .contains("Raise arcadedb.server.shutdownTimeout").doesNotContain("fixed");

    final String exiting = ArcadeDBServer.shutdownHookLockTimeoutWarning(0, STATUS.STARTING, ShutdownHookBound.HOLDER_EXITING);
    assertThat(exiting).contains("inside System.exit()").contains("did not wait")
        .doesNotContain("Raise arcadedb.server.shutdownTimeout");
  }

  /**
   * The end-to-end shape of the report: a shutdown signal arriving while the server is still STARTING but has its
   * databases open. The plugin holds {@code start()} for longer than the old fixed bound with the hook already waiting
   * on the lock; the hook must outlast that hold, acquire the lock once {@code start()} releases it and perform the
   * full graceful stop - which is what closes the database cleanly.
   */
  @Test
  @Tag("slow")
  @Timeout(120)
  void aHookArrivingWhileStartingWithOpenDatabasesWaitsForTheLockAndStopsCleanly() throws Exception {
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(DATABASE_DIRECTORY);
    final File databaseDir = new File(DATABASE_DIRECTORY, DATABASE);
    try (final Database db = new DatabaseFactory(databaseDir.getPath()).create()) {
      db.getSchema().createDocumentType("Doc");
    }
    assertThat(new File(databaseDir, "database.lck")).as("a cleanly closed database leaves no lock marker").doesNotExist();

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_0");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DATABASE_DIRECTORY);
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.SERVER_SHUTDOWN_TIMEOUT, 60_000L);
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, "hook-during-start:" + HookDuringStartPlugin.class.getName());

    HookDuringStartPlugin.reset();
    final ArcadeDBServer server = new ArcadeDBServer(config);
    try {
      server.start();

      final Thread hook = HookDuringStartPlugin.hook.get();
      assertThat(hook).as("the plugin must have fired the hook while start() was in progress").isNotNull();
      hook.join(60_000);
      assertThat(hook.isAlive()).as("the hook must return once start() releases the lock").isFalse();
      assertThat(HookDuringStartPlugin.hookFailure.get()).isNull();

      assertThat(HookDuringStartPlugin.statusSeenByTheHook.get())
          .as("the shape under test: the hook ran while the server was still STARTING").isEqualTo(STATUS.STARTING);
      assertThat(HookDuringStartPlugin.databasesSeenByTheHook.get())
          .as("...with the databases already open").contains(DATABASE);

      // THE OLD 2000ms BOUND EXPIRED DURING THE HOLD AND THE HOOK RETURNED WITHOUT STOPPING: THE SERVER WOULD STILL
      // BE ONLINE HERE AND THE DATABASE STILL OPEN. THE FIX WAITS THE CONFIGURED BOUND, SO THE HOOK STOPPED IT.
      assertThat(server.getStatus()).as("the hook performed the graceful stop").isEqualTo(STATUS.OFFLINE);
      assertThat(server.getDatabaseNames()).isEmpty();
      assertThat(new File(databaseDir, "database.lck")).as("the database was closed cleanly, so no WAL replay next time")
          .doesNotExist();
    } finally {
      if (server.getStatus() != STATUS.OFFLINE)
        server.stop();
    }
  }

  /**
   * Fires {@code stopFromShutdownHook()} from another thread while {@code start()} is inside the plugin phase - i.e.
   * after {@code loadDatabases()}, with the status still STARTING - then holds {@code start()} for
   * {@link #START_HOLD_MS} once the hook is provably waiting. Discovered through the {@code ServiceLoader} registration
   * in {@code META-INF/services/com.arcadedb.server.ServerPlugin} of the test resources and installed only when
   * {@code SERVER_PLUGINS} names it, so it needs the public no-argument constructor.
   */
  public static class HookDuringStartPlugin implements ServerPlugin {
    static final AtomicReference<Thread>              hook                   = new AtomicReference<>();
    static final AtomicReference<Throwable>           hookFailure            = new AtomicReference<>();
    static final AtomicReference<STATUS>              statusSeenByTheHook    = new AtomicReference<>();
    static final AtomicReference<Set<String>>          databasesSeenByTheHook = new AtomicReference<>();

    private ArcadeDBServer server;

    static void reset() {
      hook.set(null);
      hookFailure.set(null);
      statusSeenByTheHook.set(null);
      databasesSeenByTheHook.set(null);
    }

    @Override
    public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
      this.server = arcadeDBServer;
    }

    @Override
    public void startService() {
      final CountDownLatch hookStarted = new CountDownLatch(1);
      final Thread thread = new Thread(() -> {
        try {
          statusSeenByTheHook.set(server.getStatus());
          // A COPY: getDatabaseNames() IS A LIVE VIEW THAT THE STOP BELOW EMPTIES
          databasesSeenByTheHook.set(new HashSet<>(server.getDatabaseNames()));
          hookStarted.countDown();
          server.stopFromShutdownHook();
        } catch (final Throwable t) {
          hookFailure.set(t);
        }
      }, "issue7025-shutdown-hook");
      thread.setDaemon(true);
      hook.set(thread);
      thread.start();

      try {
        if (!hookStarted.await(30, TimeUnit.SECONDS))
          hookFailure.compareAndSet(null, new IllegalStateException("the hook thread never reached the lifecycle lock"));
        // THE HOOK IS NOW WAITING FOR THE LIFECYCLE LOCK THIS start() HOLDS: OUTLAST THE OLD FIXED BOUND
        Thread.sleep(START_HOLD_MS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void stopService() {
    }

    @Override
    public PluginInstallationPriority getInstallationPriority() {
      return PluginInstallationPriority.BEFORE_HTTP_ON;
    }
  }
}
