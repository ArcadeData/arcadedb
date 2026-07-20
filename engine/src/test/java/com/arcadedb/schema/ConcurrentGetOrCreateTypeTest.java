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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * getOrCreate*Type() must be atomic: concurrent callers racing on the same type name either create it
 * once or observe the fully built type, never a half-populated one. Without serialization the racing
 * callers each see a bucket count below the target and both try to associate the same bucket, which
 * fails with "the bucket is already associated to the type".
 */
class ConcurrentGetOrCreateTypeTest extends TestHelper {

  private static final int THREADS    = 8;
  private static final int ITERATIONS = 20;

  @Test
  void concurrentGetOrCreateVertexTypeIsAtomic() throws Exception {
    for (int iteration = 0; iteration < ITERATIONS; ++iteration)
      runRace("ConcurrentVertex" + iteration, true);
  }

  @Test
  void concurrentGetOrCreateDocumentTypeIsAtomic() throws Exception {
    for (int iteration = 0; iteration < ITERATIONS; ++iteration)
      runRace("ConcurrentDocument" + iteration, false);
  }

  private void runRace(final String typeName, final boolean vertex) throws Exception {
    final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(THREADS);
    final List<Thread> threads = new java.util.ArrayList<>(THREADS);

    for (int i = 0; i < THREADS; ++i) {
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          if (vertex)
            database.getSchema().getOrCreateVertexType(typeName);
          else
            database.getSchema().getOrCreateDocumentType(typeName);
        } catch (final Throwable t) {
          errors.add(t);
        } finally {
          done.countDown();
        }
      });
      threads.add(thread);
      thread.start();
    }

    start.countDown();
    assertThat(done.await(30, TimeUnit.SECONDS)).as("all racing threads completed").isTrue();
    for (final Thread thread : threads)
      thread.join();

    assertThat(errors).as("no thread failed racing on getOrCreate of '%s'", typeName).isEmpty();

    final DocumentType type = database.getSchema().getType(typeName);
    assertThat(type).isNotNull();
    // Buckets must be associated exactly once, no duplicates from the losing racers.
    assertThat(type.getBuckets(false).stream().map(b -> b.getName()).distinct().count())
        .isEqualTo(type.getBuckets(false).size());
  }
}
