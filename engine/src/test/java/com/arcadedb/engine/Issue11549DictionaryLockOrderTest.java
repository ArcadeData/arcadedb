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
package com.arcadedb.engine;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11549: {@link Dictionary#getIdByName(String, boolean)} registered a new name by taking its own monitor and then
 * committing a transaction, whose commit takes the database READ lock. A DDL registers a new name the other way round: it holds
 * the database WRITE lock (the schema frame) and then asks for the monitor, from the {@code LocalProperty} constructor. Two
 * threads each registering a new name at the same moment, an async worker serializing a record with a fresh property name and a
 * {@code CREATE PROPERTY}, waited for each other forever. The JVM reported it as a Java-level deadlock in CI, and a
 * {@code @Timeout} could not end it, because a thread BLOCKED on a monitor ignores the interrupt.
 * <p>
 * The interleaving is built here instead of being waited for: the writer holds the write lock before the worker asks for the
 * name, and asks for its own only once the worker is parked. Before the fix the worker parks INSIDE the monitor (in the commit),
 * after it parks BEFORE it (on the read lock). The hang is detected with a timed join of daemon threads, never with
 * {@code @Timeout}, and on a hang the database is left open: closing it needs the write lock, which would hang the suite too.
 * <p>
 * In package {@code com.arcadedb.engine} next to the other dictionary tests.
 */
class Issue11549DictionaryLockOrderTest {
  private static final String DB_PATH = "target/databases/Issue11549DictionaryLockOrderTest";

  private Database database;
  private boolean  hung;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (!hung && database != null && database.isOpen())
      database.drop();
  }

  @Test
  void aNameRegisteredUnderTheWriteLockDoesNotDeadlockWithAWorkerRegisteringAnother() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final Dictionary dictionary = db.getSchema().getDictionary();

    final CountDownLatch writeLockHeld = new CountDownLatch(1);
    final CountDownLatch workerParked = new CountDownLatch(1);
    final AtomicInteger writerId = new AtomicInteger(-1);
    final AtomicInteger workerId = new AtomicInteger(-1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    // THE SCHEMA FRAME OF A DDL: THE WRITE LOCK FIRST, THEN A NEW NAME
    final Thread writer = new Thread(() -> {
      try {
        db.executeInWriteLock(() -> {
          writeLockHeld.countDown();
          if (!workerParked.await(30, TimeUnit.SECONDS))
            throw new IllegalStateException("the worker never parked");
          writerId.set(dictionary.getIdByName("registeredByTheDDL", true));
          return null;
        });
      } catch (final Throwable t) {
        failure.compareAndSet(null, t);
      }
    }, "Issue11549-writer");

    // THE ASYNC WORKER SERIALIZING A RECORD WITH A FRESH PROPERTY NAME
    final Thread worker = new Thread(() -> {
      try {
        workerId.set(dictionary.getIdByName("registeredByTheWorker", true));
      } catch (final Throwable t) {
        failure.compareAndSet(null, t);
      }
    }, "Issue11549-worker");

    writer.setDaemon(true);
    worker.setDaemon(true);

    writer.start();
    assertThat(writeLockHeld.await(30, TimeUnit.SECONDS)).as("the writer took the write lock").isTrue();

    worker.start();
    // THE WORKER CAN ONLY PARK ON THE DATABASE LOCK THE WRITER HOLDS: BEFORE THE FIX INSIDE THE MONITOR, AFTER IT OUTSIDE
    final long parkDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (worker.getState() != Thread.State.WAITING && worker.isAlive() && System.nanoTime() < parkDeadline)
      Thread.onSpinWait();
    assertThat(worker.getState()).as("the worker is parked on the database lock the writer holds").isEqualTo(Thread.State.WAITING);
    workerParked.countDown();

    writer.join(TimeUnit.SECONDS.toMillis(30));
    worker.join(TimeUnit.SECONDS.toMillis(30));

    hung = writer.isAlive() || worker.isAlive();
    assertThat(hung)
        .as("the writer (state %s) and the worker (state %s) wait for each other: the dictionary monitor and the database lock "
            + "are taken in opposite orders", writer.getState(), worker.getState())
        .isFalse();
    assertThat(failure.get()).isNull();

    assertThat(writerId.get()).isNotNegative();
    assertThat(workerId.get()).isNotNegative().isNotEqualTo(writerId.get());
    assertThat(dictionary.getNameById(writerId.get())).isEqualTo("registeredByTheDDL");
    assertThat(dictionary.getNameById(workerId.get())).isEqualTo("registeredByTheWorker");
  }
}
