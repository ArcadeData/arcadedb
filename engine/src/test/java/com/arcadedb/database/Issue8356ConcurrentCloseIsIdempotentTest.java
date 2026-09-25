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
package com.arcadedb.database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8356: two JVM shutdown hooks (DatabaseFactory's registry sweep and the
 * embedding ArcadeDBServer's own hook) can both call {@code close()} on the same {@link LocalDatabase}
 * instance at once. The {@code isOpen()} check each caller does beforehand is not atomic with the
 * {@code close()} call, so both can see the database open and both call in.
 * <p>
 * Before the fix, the only guard was the {@code open} boolean checked inside {@code closeDurableParts}'s
 * write-locked lambda - real teardown work (the async drain, the per-index flush and
 * {@code releaseBackgroundResources}, the graph-analytical-view shutdown) ran UNGUARDED ahead of it, so a
 * second caller silently repeated that work instead of a true no-op. This test drives the exact
 * interleaving the issue describes - a second {@code close()} call arriving while the first is confirmed
 * to already be in progress - and checks that the second call does not return, and does not observably run
 * any teardown of its own, until the first one has fully finished.
 */
class Issue8356ConcurrentCloseIsIdempotentTest {

  @TempDir
  private Path          tempDir;
  private LocalDatabase database;

  @AfterEach
  void tearDown() {
    LocalDatabase.TEST_CLOSE_HOOK = null;
    if (database != null && database.isOpen())
      database.close();
  }

  @Test
  void aSecondConcurrentCloseWaitsForTheFirstInsteadOfRepeatingTeardown() throws Exception {
    database = (LocalDatabase) new DatabaseFactory(tempDir.resolve("mydb").toString()).create();
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc");
      database.newDocument("Doc").set("name", "first").save();
    });

    final CountDownLatch firstIsInsideClosing = new CountDownLatch(1);
    final CountDownLatch releaseFirst         = new CountDownLatch(1);
    final AtomicInteger  hookInvocations      = new AtomicInteger(0);

    LocalDatabase.TEST_CLOSE_HOOK = () -> {
      hookInvocations.incrementAndGet();
      firstIsInsideClosing.countDown();
      try {
        releaseFirst.await(10, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final Thread firstCloser = new Thread(() -> database.close(), "first-closer");
    firstCloser.start();

    assertThat(firstIsInsideClosing.await(10, TimeUnit.SECONDS))
        .as("the first closer must reach the guarded section before the second one starts")
        .isTrue();

    final CountDownLatch secondReturned = new CountDownLatch(1);
    final Thread secondCloser = new Thread(() -> {
      database.close();
      secondReturned.countDown();
    }, "second-closer");
    secondCloser.start();

    // The second call must NOT return, and must NOT re-enter the guarded section (the hook fires once),
    // while the first is still parked inside it.
    assertThat(secondReturned.await(300, TimeUnit.MILLISECONDS))
        .as("a concurrent close() must wait for the in-progress one instead of returning early")
        .isFalse();
    assertThat(hookInvocations.get())
        .as("only the winning thread may run the guarded teardown section")
        .isEqualTo(1);

    releaseFirst.countDown();
    firstCloser.join(10_000);
    secondCloser.join(10_000);

    assertThat(secondReturned.await(0, TimeUnit.MILLISECONDS))
        .as("the second call must return once the first one completes")
        .isTrue();
    assertThat(hookInvocations.get())
        .as("the second call must not have run its own teardown pass")
        .isEqualTo(1);
    assertThat(database.isOpen()).isFalse();
  }
}
