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
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.ServedDatabases;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10707: a security file watcher's tick is not stopped by {@code ReloadableSecurityArtifact.stop()}, which cancels the
 * {@code Timer} and leaves a tick that has begun to run. A groups-file tick that was already sweeping the server's databases
 * when the server stopped reached {@code ArcadeDBServer.getDatabase} after the databases were closed, and that opens the
 * directory again and takes its file lock: the next server on the same directory then fails on an
 * {@code OverlappingFileLockException}.
 * <ul>
 *   <li>{@code ServerSecurity.stopService()} waits, briefly, for a sweep the groups watcher is running;</li>
 *   <li>a tick that was reading its file when the store stopped does not go on to {@code onReloaded()}.</li>
 * </ul>
 */
class Issue10707SecurityWatcherStopTest {
  @RegisterExtension
  static final ServedDatabases SERVED = new ServedDatabases();

  private static final String CONFIG_PATH     = "target/test-security-10707";
  private static final String DATABASE        = "graph";
  private static final int    RELOAD_EVERY_MS = 20;

  private ServerSecurity security;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
    assertThat(new File(CONFIG_PATH).mkdirs()).isTrue();
  }

  @AfterEach
  void tearDown() {
    if (security != null)
      security.stopService();
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  /**
   * The sweep is held inside {@code getDatabase}, on the watcher's thread, while the service stops. {@code stopService} is
   * the call after which the server closes its databases, so it must not return before the sweep has: otherwise the sweep's
   * next {@code getDatabase} meets a closed server.
   */
  @Test
  void stopServiceWaitsForTheGroupsSweepTheWatcherIsRunning() throws Exception {
    final ServerDatabase database = SERVED.open(DATABASE);
    final CountDownLatch sweeping = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean sweepEnded = new AtomicBoolean();

    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.databaseNames(DATABASE);
    server.on("getDatabase", args -> {
      // Only the groups watcher's thread is held; its name carries the file it watches
      if (Thread.currentThread().getName().contains(SecurityGroupFileRepository.FILE_NAME)) {
        sweeping.countDown();
        try {
          release.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        sweepEnded.set(true);
      }
      return database;
    });

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY, RELOAD_EVERY_MS);
    security = new ServerSecurity(server, configuration, CONFIG_PATH);
    server.security(security);
    security.startService();

    // A stamp strictly newer than the baseline is a change to the watcher; the content is read and published as it is
    final File groups = new File(CONFIG_PATH, SecurityGroupFileRepository.FILE_NAME);
    assertThat(groups).exists();
    assertThat(groups.setLastModified(System.currentTimeMillis() + 60_000L)).isTrue();
    assertThat(sweeping.await(10, TimeUnit.SECONDS)).as("the watcher began a sweep").isTrue();

    final AtomicBoolean sweepHadEnded = new AtomicBoolean();
    final CompletableFuture<Void> stopping = CompletableFuture.runAsync(() -> {
      security.stopService();
      sweepHadEnded.set(sweepEnded.get());
    });
    try {
      assertThatThrownBy(() -> stopping.get(500, TimeUnit.MILLISECONDS))
          .as("stopService() returned while the watcher's sweep was still running").isInstanceOf(TimeoutException.class);
    } finally {
      release.countDown();
    }
    stopping.get(30, TimeUnit.SECONDS);

    assertThat(sweepHadEnded.get()).as("the sweep had ended when stopService() returned").isTrue();
  }

  /**
   * A tick that began before the store stopped and was reading its file when it did. It has reloaded by the time it could
   * sweep, but a stopped store must not: {@code onReloaded()} is where the groups store reaches the server's databases.
   */
  @Test
  void aTickThatWasReadingWhenTheStoreStoppedDoesNotRunOnReloaded() throws Exception {
    final StubStore store = new StubStore();
    store.startWatching(RELOAD_EVERY_MS);
    assertThat(store.inTick.await(10, TimeUnit.SECONDS)).as("a tick began").isTrue();

    store.stop();
    store.releaseTick.countDown();

    final Thread tick = store.tickThread.get();
    // Timer.cancel() ends the timer thread once the task it is running returns: joining it is waiting for the tick
    tick.join(TimeUnit.SECONDS.toMillis(10));
    assertThat(tick.isAlive()).as("the tick ended").isFalse();
    assertThat(store.reloaded.get()).as("onReloaded() runs of a stopped store").isZero();
  }

  /** A store over a file that is always seen as changed, whose first tick is held inside {@code fileChanged()}. */
  private static final class StubStore extends ReloadableSecurityArtifact<String> {
    final CountDownLatch         inTick      = new CountDownLatch(1);
    final CountDownLatch         releaseTick = new CountDownLatch(1);
    final AtomicReference<Thread> tickThread  = new AtomicReference<>();
    final AtomicInteger          reloaded    = new AtomicInteger();

    StubStore() throws IOException {
      super(new Object(), new Object(), writtenFile("store.json"), new File(CONFIG_PATH, "store-error.json"),
          "arcadedb-test-10707-watcher", "Test store");
    }

    private static File writtenFile(final String name) throws IOException {
      final File file = new File(CONFIG_PATH, name);
      java.nio.file.Files.writeString(file.toPath(), "stored");
      return file;
    }

    @Override
    protected boolean fileChanged() {
      tickThread.compareAndSet(null, Thread.currentThread());
      inTick.countDown();
      try {
        releaseTick.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return true;
    }

    @Override
    protected String read() throws IOException {
      return java.nio.file.Files.readString(this.file.toPath());
    }

    @Override
    protected String fallback() {
      return "default";
    }

    @Override
    protected boolean publish(final String value, final boolean isFallback) {
      return true;
    }

    @Override
    protected boolean hasInForce() {
      return true;
    }

    @Override
    protected String keeping() {
      return "keeping what is in force";
    }

    @Override
    protected void onReloaded() {
      reloaded.incrementAndGet();
    }
  }
}
