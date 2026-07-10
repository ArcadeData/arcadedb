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
import com.arcadedb.engine.ComponentFile;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5031: the server database registry serialized every request on a single
 * JVM-wide monitor, the register/remove registry mutations bypassed that monitor, and the HA-related
 * fields were published to concurrent readers without {@code volatile}.
 *
 * <ul>
 *   <li>Defect 1: {@code getDatabase} must serve an already-open database via a lock-free fast path,
 *   so a lookup does not block while another thread holds {@code databasesLock} (e.g. across the HA
 *   snapshot installer's close-swap-reopen sequence).</li>
 *   <li>Defect 2: {@code registerDatabase} / {@code removeDatabase} must acquire {@code databasesLock}
 *   so they cannot race {@code getDatabase} into two instances over the same directory.</li>
 *   <li>Defect 3: {@code haServer} / {@code databaseWrapper} (and, for symmetry, {@code security} /
 *   {@code httpServer}) must be {@code volatile} so readers on the Undertow worker threads observe the
 *   values written by the HA plugin after the HTTP server already started serving requests.</li>
 * </ul>
 */
@Tag("slow")
class Issue5031ServerRegistryConcurrencyIT {
  private ArcadeDBServer server;

  @TempDir
  Path tempDir;

  @BeforeEach
  void setup() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, tempDir.toString());
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    configuration.setValue(GlobalConfiguration.HA_ENABLED, false);
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "DevTestPassword123!");
    configuration.setValue(GlobalConfiguration.TEST, true);

    server = new ArcadeDBServer(configuration);
    server.start();
    server.createDatabase("registry", ComponentFile.MODE.READ_WRITE);
  }

  @AfterEach
  void teardown() {
    if (server != null && server.isStarted())
      server.stop();
  }

  /**
   * Defect 1: an already-open database must be returned via the lock-free fast path. We hold
   * {@code databasesLock} in a background thread (simulating the HA snapshot installer or a long
   * create/open) and assert that a concurrent lookup for the already-open database still completes
   * promptly instead of blocking on the monitor.
   */
  @Test
  void openDatabaseLookupDoesNotBlockOnRegistryLock() throws Exception {
    final CountDownLatch lockHeld = new CountDownLatch(1);
    final CountDownLatch releaseLock = new CountDownLatch(1);

    final Thread holder = new Thread(() -> {
      synchronized (server.getDatabasesLock()) {
        lockHeld.countDown();
        try {
          releaseLock.await(10, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }, "registry-lock-holder");
    holder.start();

    assertThat(lockHeld.await(5, TimeUnit.SECONDS)).isTrue();

    final AtomicReference<ServerDatabase> looked = new AtomicReference<>();
    final Thread lookup = new Thread(() -> looked.set(server.getDatabase("registry", false, true)), "registry-lookup");
    lookup.start();
    lookup.join(TimeUnit.SECONDS.toMillis(3));

    final boolean returnedWhileLocked = !lookup.isAlive();
    releaseLock.countDown();
    holder.join(TimeUnit.SECONDS.toMillis(5));
    lookup.join(TimeUnit.SECONDS.toMillis(5));

    assertThat(returnedWhileLocked)
        .as("getDatabase on an already-open database must not block while databasesLock is held")
        .isTrue();
    assertThat(looked.get()).isNotNull();
    assertThat(looked.get().isOpen()).isTrue();
  }

  /**
   * Defect 1 (startup safety): the lock-free fast path must be inactive while the server is not yet
   * {@code ONLINE}. During startup the HA plugin re-wraps the open databases under {@code databasesLock};
   * a lookup in that window must fall through to the locked slow path (and block) instead of handing out
   * the pre-wrap instance. We simulate the startup window by flipping {@code status} back to {@code STARTING}
   * and holding {@code databasesLock}: the lookup must block until the lock is released.
   */
  @Test
  void fastPathIsDisabledWhileServerNotOnline() throws Exception {
    final Field statusField = ArcadeDBServer.class.getDeclaredField("status");
    statusField.setAccessible(true);
    final Object online = statusField.get(server);

    final CountDownLatch lockHeld = new CountDownLatch(1);
    final CountDownLatch releaseLock = new CountDownLatch(1);
    try {
      statusField.set(server, ArcadeDBServer.STATUS.STARTING);

      final Thread holder = new Thread(() -> {
        synchronized (server.getDatabasesLock()) {
          lockHeld.countDown();
          try {
            releaseLock.await(10, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }, "registry-lock-holder");
      holder.start();
      assertThat(lockHeld.await(5, TimeUnit.SECONDS)).isTrue();

      final AtomicReference<ServerDatabase> looked = new AtomicReference<>();
      final CountDownLatch lookupReached = new CountDownLatch(1);
      final Thread lookup = new Thread(() -> {
        lookupReached.countDown();
        looked.set(server.getDatabase("registry", false, true));
      }, "registry-lookup");
      lookup.start();
      // Ensure the lookup thread actually reached the getDatabase call before judging whether it blocked,
      // so a merely-unscheduled thread cannot make the test pass without exercising the gate.
      assertThat(lookupReached.await(5, TimeUnit.SECONDS)).isTrue();
      lookup.join(TimeUnit.SECONDS.toMillis(1));

      final boolean returnedWhileLocked = !lookup.isAlive();
      releaseLock.countDown();
      holder.join(TimeUnit.SECONDS.toMillis(5));
      lookup.join(TimeUnit.SECONDS.toMillis(5));

      assertThat(returnedWhileLocked)
          .as("while the server is not ONLINE the lookup must take the locked slow path and block")
          .isFalse();
      assertThat(looked.get()).isNotNull();
    } finally {
      statusField.set(server, online);
    }
  }

  /**
   * Defect 1 (correctness): concurrent lookups for the same open database all observe the identical
   * registered instance - the fast path never creates a second wrapper.
   */
  @Test
  void concurrentLookupsReturnSameInstance() throws Exception {
    final int threads = 32;
    final CyclicBarrier barrier = new CyclicBarrier(threads);
    final Map<ServerDatabase, Boolean> seen = Collections.synchronizedMap(new IdentityHashMap<>());
    final Thread[] pool = new Thread[threads];

    for (int i = 0; i < threads; i++) {
      pool[i] = new Thread(() -> {
        try {
          barrier.await(5, TimeUnit.SECONDS);
          for (int j = 0; j < 100; j++)
            seen.put(server.getDatabase("registry", false, true), Boolean.TRUE);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
      pool[i].start();
    }
    for (final Thread t : pool)
      t.join(TimeUnit.SECONDS.toMillis(15));

    assertThat(seen).hasSize(1);
  }

  /**
   * Defect 2: {@code removeDatabase} must acquire {@code databasesLock}. While the lock is held by
   * another thread, a concurrent {@code removeDatabase} must block until the lock is released.
   */
  @Test
  void removeDatabaseAcquiresRegistryLock() throws Exception {
    final CountDownLatch lockHeld = new CountDownLatch(1);
    final CountDownLatch releaseLock = new CountDownLatch(1);
    final CountDownLatch removeStarted = new CountDownLatch(1);
    final CountDownLatch removeFinished = new CountDownLatch(1);

    final Thread holder = new Thread(() -> {
      synchronized (server.getDatabasesLock()) {
        lockHeld.countDown();
        try {
          releaseLock.await(10, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }, "registry-lock-holder");
    holder.start();

    assertThat(lockHeld.await(5, TimeUnit.SECONDS)).isTrue();

    final Thread remover = new Thread(() -> {
      removeStarted.countDown();
      server.removeDatabase("registry");
      removeFinished.countDown();
    }, "registry-remover");
    remover.start();

    assertThat(removeStarted.await(5, TimeUnit.SECONDS)).isTrue();

    // With the lock held, removeDatabase must NOT complete.
    final boolean finishedWhileLocked = removeFinished.await(1, TimeUnit.SECONDS);
    releaseLock.countDown();

    final boolean finishedAfterRelease = removeFinished.await(5, TimeUnit.SECONDS);
    holder.join(TimeUnit.SECONDS.toMillis(5));
    remover.join(TimeUnit.SECONDS.toMillis(5));

    assertThat(finishedWhileLocked)
        .as("removeDatabase must block while databasesLock is held (it must acquire the lock)")
        .isFalse();
    assertThat(finishedAfterRelease)
        .as("removeDatabase must complete once databasesLock is released")
        .isTrue();
    assertThat(server.existsDatabase("registry")).isFalse();
  }

  /**
   * Defect 3: the fields mutated after the HTTP server has started serving requests must be
   * {@code volatile} so concurrent readers on the Undertow worker threads observe the writes.
   */
  @Test
  void haRelatedFieldsAreVolatile() {
    assertFieldVolatile("haServer");
    assertFieldVolatile("databaseWrapper");
    assertFieldVolatile("security");
    assertFieldVolatile("httpServer");
  }

  private static void assertFieldVolatile(final String fieldName) {
    final Field field = findField(fieldName);
    assertThat(field).as("field '%s' must exist on ArcadeDBServer", fieldName).isNotNull();
    assertThat(Modifier.isVolatile(field.getModifiers()))
        .as("field '%s' must be declared volatile so post-startup writes are visible to worker threads", fieldName)
        .isTrue();
  }

  private static Field findField(final String fieldName) {
    for (final Field f : ArcadeDBServer.class.getDeclaredFields())
      if (f.getName().equals(fieldName))
        return f;
    return null;
  }
}
