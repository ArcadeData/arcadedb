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

import com.arcadedb.database.Database;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the ConcurrentModificationException observed when multiple threads share a
 * single {@link RemoteDatabase} and race on the first schema load. Before the fix in
 * {@link com.arcadedb.remote.RemoteSchema#reload}, two threads could both see {@code types == null},
 * both enter the unsynchronized init, and one of them would throw
 * {@link java.util.ConcurrentModificationException} from {@code HashMap.computeIfAbsent}. This
 * test hammers that code path with a cold start latch so all threads race into the first query
 * simultaneously.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("IntegrationTest")
class RemoteSchemaConcurrentInitIT extends BaseGraphServerTest {

  private static final int THREADS        = 20;
  private static final int CALLS_PER_THREAD = 10;

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  protected void populateDatabase() {
    final Database database = getDatabases()[0];
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      final VertexType v = schema.buildVertexType().withName("V").withTotalBuckets(3).create();
      v.createProperty("id", Integer.class);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
    });
  }

  @Test
  void concurrentSchemaLoadIsSafe() throws Exception {
    try (final RemoteDatabase shared = new RemoteDatabase("localhost", 2480, getDatabaseName(),
        "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      shared.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
      shared.setTimeout(30_000);

      final CountDownLatch ready = new CountDownLatch(THREADS);
      final CountDownLatch go = new CountDownLatch(1);
      final AtomicInteger errors = new AtomicInteger(0);
      final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
      final Future<?>[] futs = new Future<?>[THREADS];

      try {
        for (int t = 0; t < THREADS; t++) {
          futs[t] = pool.submit(() -> {
            ready.countDown();
            try {
              go.await();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
            for (int i = 0; i < CALLS_PER_THREAD; i++) {
              try {
                // getSchema().existsType triggers checkSchemaIsLoaded -> reload; the race is
                // inside reload's HashMap init. Interleaving with a command that returns vertex
                // records exercises the RemoteImmutableVertex path that hit the CME in the wild.
                shared.getSchema().existsType("V");
                shared.command("sql", "CREATE VERTEX V SET id = ?",
                    Thread.currentThread().getId() * 1000L + i).close();
              } catch (final Throwable e) {
                errors.incrementAndGet();
                throw e;
              }
            }
          });
        }

        ready.await(30, TimeUnit.SECONDS);
        go.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(60, TimeUnit.SECONDS))
            .as("all threads finished within 60s").isTrue();

        for (final Future<?> f : futs)
          f.get();
      } finally {
        pool.shutdownNow();
      }

      assertThat(errors.get())
          .as("no ConcurrentModificationException or other error from shared RemoteDatabase init")
          .isZero();
    }
  }
}
