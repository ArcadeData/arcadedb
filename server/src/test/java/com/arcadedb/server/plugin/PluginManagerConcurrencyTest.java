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
package com.arcadedb.server.plugin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Concurrency regression for issue #5033: {@link PluginManager#getPluginNames()} must return an
 * immutable snapshot, not a live {@code keySet()} view. A live view throws
 * {@code ConcurrentModificationException} if a late {@code registerPlugin} mutates the backing map
 * while a caller iterates the returned names.
 */
class PluginManagerConcurrencyTest {

  @TempDir
  Path tempDir;

  private ArcadeDBServer server;
  private PluginManager  manager;

  private static class NoopPlugin implements ServerPlugin {
    @Override
    public String getName() {
      return "noop";
    }

    @Override
    public void startService() {
      // NO-OP
    }
  }

  @BeforeEach
  void setup() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, tempDir.toString());
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());

    server = new ArcadeDBServer(configuration);
    manager = new PluginManager(server, configuration);
  }

  @AfterEach
  void teardown() {
    if (server != null && server.isStarted())
      server.stop();
  }

  @Test
  void getPluginNamesReturnsSnapshotNotLiveView() {
    manager.registerPlugin("plugin-a", new NoopPlugin());
    manager.registerPlugin("plugin-a2", new NoopPlugin());

    final Set<String> names = manager.getPluginNames();

    // Deterministically modify the backing map WHILE iterating the returned names. With the old live
    // keySet() view the second next() detects the structural change and throws
    // ConcurrentModificationException; with a snapshot copy the iterator is unaffected.
    final Throwable thrown = catchThrowable(() -> {
      final Iterator<String> it = names.iterator();
      it.next();                                        // start iterating (captures modCount on live view)
      manager.registerPlugin("plugin-b", new NoopPlugin()); // structural modification mid-iteration
      while (it.hasNext())
        it.next();
    });

    assertThat(thrown).as("iterating the returned names during a concurrent registration must not throw").isNull();
    // The snapshot froze at the two entries present when getPluginNames() was called.
    assertThat(names).containsExactlyInAnyOrder("plugin-a", "plugin-a2");
  }

  @Test
  void getPluginNamesIterationSurvivesConcurrentRegistration() throws Exception {
    // A registrar continuously mutates the map while an iterator repeatedly reads and walks the names.
    // Kept modest so the test stays fast and lightweight; the deterministic test above is the primary gate.
    final int registrations = 20_000;
    final AtomicBoolean registrarDone = new AtomicBoolean(false);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final CountDownLatch started = new CountDownLatch(1);

    final Thread registrar = new Thread(() -> {
      started.countDown();
      for (int i = 0; i < registrations; i++)
        manager.registerPlugin("plugin-" + i, new NoopPlugin());
      registrarDone.set(true);
    }, "plugin-registrar");

    final Thread iterator = new Thread(() -> {
      try {
        started.await();
        while (!registrarDone.get()) {
          final Set<String> names = manager.getPluginNames();
          for (final String name : names)
            assertThat(name).isNotNull();
        }
      } catch (final Throwable e) {
        failure.compareAndSet(null, e);
      }
    }, "plugin-iterator");

    registrar.start();
    iterator.start();

    registrar.join(TimeUnit.SECONDS.toMillis(60));
    iterator.join(TimeUnit.SECONDS.toMillis(60));

    assertThat(failure.get()).as("no ConcurrentModificationException from getPluginNames()").isNull();
  }
}
