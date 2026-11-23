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
package com.arcadedb.server.grpc;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The thread of a gRPC transaction ({@code arcadedb-tx-<id>}) ends with the transaction, and its contexts (page lists, undo journal,
 * buffers) go with it instead of staying in the static registry until the dead-thread sweep. A thread that ends with the transaction
 * still active keeps them, so the sweep can roll the transaction back. Each {@code TransactionContext} owns one single-thread executor
 * and is registered for one transaction only, so no thread is recycled for a later transaction.
 */
class GrpcTransactionThreadContextReleaseTest {
  private static final String DATABASE_PATH = "target/databases/GrpcTransactionThreadContextReleaseTest";
  private static final long   TIMEOUT_MS    = 10_000;

  private Database database;

  @BeforeEach
  void createDatabase() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    database = new DatabaseFactory(DATABASE_PATH).create();
    database.getSchema().createDocumentType("T");
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void theThreadOfAFinishedTransactionReleasesItsContexts() throws Exception {
    final ArcadeDbGrpcService.TransactionContext transaction = new ArcadeDbGrpcService.TransactionContext(database, "released", "owner");
    final AtomicReference<Thread> thread = new AtomicReference<>();

    // begin, write and commit as the separate calls of a client: each one a task of the transaction's own thread
    transaction.executor.submit(() -> {
      thread.set(Thread.currentThread());
      database.begin();
    }).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    transaction.executor.submit(() -> database.newDocument("T").set("n", 1).save()).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    transaction.executor.submit(() -> database.commit()).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    assertThat(thread.get().getName()).isEqualTo("arcadedb-tx-released");
    assertThat(isRegistered(thread.get())).as("registered while the thread lives").isTrue();

    transaction.shutdown();
    thread.get().join(TIMEOUT_MS);

    assertThat(thread.get().isAlive()).isFalse();
    assertThat(isRegistered(thread.get())).as("registered after the transaction's thread ended").isFalse();
    assertThat(database.countType("T", true)).isEqualTo(1);
  }

  @Test
  void aTransactionStillActiveWhenItsThreadEndsIsLeftToTheDeadThreadSweep() throws Exception {
    final ArcadeDbGrpcService.TransactionContext transaction = new ArcadeDbGrpcService.TransactionContext(database, "abandoned", "owner");
    final AtomicReference<Thread> thread = new AtomicReference<>();
    transaction.executor.submit(() -> {
      thread.set(Thread.currentThread());
      database.begin();
      database.newDocument("T").set("n", 1).save();
    }).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    transaction.shutdown();
    thread.get().join(TIMEOUT_MS);

    assertThat(thread.get().isAlive()).isFalse();
    assertThat(isRegistered(thread.get())).as("an active transaction is not dropped with the thread").isTrue();

    // the sweep finds the dead thread, rolls its transaction back and removes the entry
    final Method sweep = DatabaseContext.class.getDeclaredMethod("cleanupDeadThreads");
    sweep.setAccessible(true);
    sweep.invoke(null);

    assertThat(isRegistered(thread.get())).as("registered after the sweep").isFalse();
    assertThat(database.countType("T", true)).isZero();
  }

  /** Whether the static registry of {@link DatabaseContext} still holds the contexts of {@code thread}. */
  private static boolean isRegistered(final Thread thread) throws ReflectiveOperationException {
    // the registry is private and keyed by thread id, and the contexts of another thread cannot be asked for through the API
    final Field field = DatabaseContext.class.getDeclaredField("CONTEXTS");
    field.setAccessible(true);
    return ((Map<?, ?>) field.get(null)).containsKey(thread.threadId());
  }
}
