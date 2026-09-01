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
package com.arcadedb.function.polyglot;

import com.arcadedb.TestHelper;
import com.arcadedb.function.FunctionDefinition;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test guarding the fix for #7006 against reintroducing concurrent access to the shared GraalVM
 * {@code Context}. An earlier version of the fix replaced the old (broken) {@code synchronized(polyglotEngine)}
 * with a {@link ReentrantReadWriteLock}, which correctly stopped {@code reloadEngine()} from closing the engine
 * under an in-flight call, but let multiple callers hold the read lock and invoke
 * {@code callback.execute(polyglotEngine)} concurrently - something GraalVM's JS context does not support without
 * explicit multi-threaded access configuration (the same class of problem already fixed for
 * {@code PolyglotQueryEngine} under issue #6759). The final fix uses a plain mutex instead, so every caller of
 * {@link PolyglotFunctionLibraryDefinition#execute} stays fully serialized.
 * <p>
 * This test increments a JS-global counter with a non-atomic read-modify-write from many threads. If calls into
 * the shared context were ever allowed to run concurrently, the lost-update race would make the final count come
 * out lower than the number of calls with overwhelming probability; under full serialization it is always exact.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7006ConcurrentCallSerializationTest extends TestHelper {

  @Test
  void concurrentCallersOfTheSameFunctionAreSerialized() throws Exception {
    database.command("sql", """
        DEFINE FUNCTION issue7006.bump "globalThis.counter7006 = (globalThis.counter7006 || 0) + 1; return globalThis.counter7006;" LANGUAGE js;
        """);
    final FunctionDefinition function = database.getSchema().getFunction("issue7006", "bump");

    final int threadCount     = 8;
    final int callsPerThread  = 200;
    final ExecutorService     pool  = Executors.newFixedThreadPool(threadCount);
    final CountDownLatch      ready = new CountDownLatch(threadCount);
    final CountDownLatch      go    = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final AtomicInteger       completed = new AtomicInteger();

    try {
      for (int t = 0; t < threadCount; t++) {
        pool.submit(() -> {
          try {
            ready.countDown();
            go.await(10, TimeUnit.SECONDS);
            for (int i = 0; i < callsPerThread; i++)
              function.execute();
            completed.incrementAndGet();
          } catch (final Throwable e) {
            error.set(e);
          }
        });
      }

      assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
      go.countDown();

      pool.shutdown();
      assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

      assertThat(error.get()).isNull();
      assertThat(completed.get()).isEqualTo(threadCount);

      final Number finalCount = (Number) function.execute();
      assertThat(finalCount.intValue()).isEqualTo(threadCount * callsPerThread + 1);
    } finally {
      database.getSchema().unregisterFunctionLibrary("issue7006");
    }
  }
}
