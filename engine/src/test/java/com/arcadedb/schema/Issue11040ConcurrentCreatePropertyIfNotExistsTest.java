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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11040: concurrent {@code CREATE PROPERTY ... IF NOT EXISTS} on the same type must (1) never be refused, the
 * losers answer {@code created=false}, and (2) never lose the property from schema.json.
 */
class Issue11040ConcurrentCreatePropertyIfNotExistsTest extends TestHelper {

  private static final int THREADS = 8;
  private static final int ROUNDS  = 60;

  @Test
  void concurrentIfNotExistsIsNeverRefusedAndTheLosersReportNotCreated() throws Exception {
    for (int round = 0; round < ROUNDS; ++round) {
      final String typeName = "Refused" + round;
      database.command("sql", "CREATE VERTEX TYPE " + typeName);

      final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
      final ConcurrentLinkedQueue<Boolean> created = new ConcurrentLinkedQueue<>();
      race(typeName, errors, created);

      assertThat(errors).as("no statement refused in round %d", round).isEmpty();
      assertThat(created.stream().filter(c -> c).count()).as("exactly one creator in round %d", round).isEqualTo(1);
    }
  }

  @Test
  void concurrentIfNotExistsKeepsThePropertyAcrossReopen() throws Exception {
    final List<String> lost = new ArrayList<>();
    for (int round = 0; round < ROUNDS; ++round) {
      final String typeName = "Lost" + round;
      database.command("sql", "CREATE VERTEX TYPE " + typeName);
      race(typeName, new ConcurrentLinkedQueue<>(), new ConcurrentLinkedQueue<>());
    }

    reopenDatabase();

    for (int round = 0; round < ROUNDS; ++round) {
      final String typeName = "Lost" + round;
      if (!database.getSchema().getType(typeName).existsProperty("name"))
        lost.add(typeName);
    }
    assertThat(lost).as("types that lost the property from schema.json").isEmpty();
  }

  /**
   * Deterministic form of the loss: two creations of the same property are both waiting for the schema write lock, so
   * both read the dirty generation before either marked it. The one that gets the lock creates the property inside a
   * transaction, which postpones the schema save to the commit. The other is refused, and its failure must not declare
   * the schema clean over the winner's unsaved change, or the commit finds nothing to save and the property never
   * reaches schema.json.
   */
  @Test
  void aRefusedCreationDoesNotDeclareTheWinnersUnsavedChangeSaved() throws Exception {
    database.getSchema().createVertexType("Held");
    final DocumentType type = database.getSchema().getType("Held");
    final DatabaseInternal db = (DatabaseInternal) database;

    final CountDownLatch refusedDone = new CountDownLatch(1);
    final AtomicInteger winners = new AtomicInteger();
    final AtomicInteger refusals = new AtomicInteger();
    final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    final List<Thread> threads = new ArrayList<>();

    final Runnable creator = () -> {
      database.begin();
      try {
        type.createProperty("name", Type.STRING);
        winners.incrementAndGet();
        // THE REFUSED ONE MUST HAVE FAILED (AND RUN ITS CLEANUP) BEFORE THIS TRANSACTION COMMITS
        if (!refusedDone.await(30, TimeUnit.SECONDS))
          throw new IllegalStateException("the refused creation did not finish");
        database.commit();
      } catch (final AlreadyExistsException e) {
        refusals.incrementAndGet();
        database.rollback();
        refusedDone.countDown();
      } catch (final Throwable t) {
        errors.add(t);
        refusedDone.countDown();
      }
    };

    // BOTH THREADS BLOCK ON THE WRITE LOCK, HAVING ALREADY READ THE GENERATION
    db.executeInWriteLock(() -> {
      for (int i = 0; i < 2; ++i) {
        final Thread thread = new Thread(creator);
        threads.add(thread);
        thread.start();
      }
      Thread.sleep(500);
      return null;
    });

    for (final Thread thread : threads)
      thread.join(60_000);

    assertThat(errors).isEmpty();
    assertThat(winners.get()).isEqualTo(1);
    assertThat(refusals.get()).isEqualTo(1);

    reopenDatabase();
    assertThat(database.getSchema().getType("Held").existsProperty("name")).as("the property survives the reopen").isTrue();
  }

  private void race(final String typeName, final ConcurrentLinkedQueue<Throwable> errors,
      final ConcurrentLinkedQueue<Boolean> created) throws InterruptedException {
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>(THREADS);
    for (int i = 0; i < THREADS; ++i) {
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          // INSIDE A TRANSACTION, AS THE HTTP COMMAND HANDLER RUNS IT: THE SCHEMA SAVE IS THEN POSTPONED TO THE COMMIT
          database.transaction(() -> {
            try (final ResultSet rs = database.command("sql", "CREATE PROPERTY " + typeName + ".name IF NOT EXISTS STRING")) {
              final Result row = rs.next();
              created.add((Boolean) row.getProperty("created"));
            }
          });
        } catch (final Throwable t) {
          errors.add(t);
        }
      });
      threads.add(thread);
      thread.start();
    }
    start.countDown();
    for (final Thread thread : threads)
      thread.join(60_000);
  }
}
