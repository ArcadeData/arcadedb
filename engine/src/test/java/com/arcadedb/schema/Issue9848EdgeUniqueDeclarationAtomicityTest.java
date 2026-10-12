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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #9848: {@code ALTER TYPE <edgeType> WITH unique = ...} flipped the flag in one critical section
 * and materialised (or dropped) its backing index in another, and {@code DROP INDEX} judged whether the index backs a
 * constraint BEFORE it took the write lock that drops it. Between the two a concurrent {@code DROP INDEX} of the index
 * the alter was about to create passed the guard (the flag was still {@code false}, the index absent), then dropped the
 * index the alter built, leaving the type advertising UNIQUE with nothing enforcing it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9848EdgeUniqueDeclarationAtomicityTest extends TestHelper {

  private static final String VERTEX     = "Issue9848Person";
  private static final String EDGE       = "Issue9848Follows";
  private static final String INDEX_NAME = LocalEdgeType.uniqueIndexName(EDGE);

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType(VERTEX);
    database.getSchema().createEdgeType(EDGE);
  }

  private LocalEdgeType edgeType() {
    return (LocalEdgeType) database.getSchema().getType(EDGE);
  }

  /**
   * The race, made deterministic. A helper thread holds the database write lock and will run the ALTER under it once
   * released to do so; meanwhile a second thread asks to drop the index the ALTER is about to create. Before the fix the
   * drop judged the constraint against the not-yet-flipped flag (and an absent index) and only then waited for the lock,
   * so it dropped the index the ALTER had built in the meantime. After it, the judgement waits for the lock and sees the
   * constraint.
   */
  @Test
  void dropIndexJudgesTheConstraintUnderTheLockThatDropsTheIndex() throws Exception {
    final LocalDatabase embedded = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final CountDownLatch lockHeld = new CountDownLatch(1);
    final CountDownLatch runAlter = new CountDownLatch(1);
    final ExecutorService threads = Executors.newFixedThreadPool(2);

    try {
      final Future<?> alter = threads.submit(() -> embedded.executeInWriteLock(() -> {
        lockHeld.countDown();
        assertThat(runAlter.await(30, TimeUnit.SECONDS)).isTrue();
        database.command("sql", "ALTER TYPE " + EDGE + " WITH unique = true").close();
        return null;
      }));
      assertThat(lockHeld.await(30, TimeUnit.SECONDS)).as("the helper thread took the database write lock").isTrue();

      final AtomicReference<Throwable> dropOutcome = new AtomicReference<>();
      final Future<?> drop = threads.submit(() -> {
        try {
          database.getSchema().dropIndex(INDEX_NAME);
        } catch (final Throwable t) {
          dropOutcome.set(t);
        }
      });

      // A short wait EXPECTED TO TIME OUT: the drop is queued behind the write lock whatever it has decided so far, so
      // no elapsed-time claim is made and a stalled JVM can only make this more true.
      assertThatThrownBy(() -> drop.get(2, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);

      runAlter.countDown();
      alter.get(60, TimeUnit.SECONDS);
      drop.get(60, TimeUnit.SECONDS);

      assertThat(dropOutcome.get()).as("the drop must see the constraint the alter declared, and refuse")
          .isInstanceOf(SchemaException.class);
      assertThat(dropOutcome.get().getMessage()).contains(INDEX_NAME).contains("UNIQUE");
    } finally {
      runAlter.countDown();
      threads.shutdownNow();
      assertThat(threads.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(edgeType().isUnique()).isTrue();
    assertThat(database.getSchema().existsIndex(INDEX_NAME)).as("the index that enforces the constraint is still there").isTrue();
  }

  /**
   * The contract the issue asks for, observed from outside: the flag and its index change in ONE schema recording
   * session. A session announces its end through {@code SCHEMA_AFTER_FILE_CHANGES}, which a nested frame does not fire,
   * so two sessions are two critical sections and a window in which another session sees the flag without the index.
   */
  @Test
  void alterUniqueRunsAsASingleSchemaSession() {
    final AtomicInteger sessions = new AtomicInteger();
    final Callable<Void> counter = () -> {
      sessions.incrementAndGet();
      return null;
    };
    final DatabaseInternal internal = (DatabaseInternal) database;
    internal.registerCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, counter);
    try {
      database.command("sql", "ALTER TYPE " + EDGE + " WITH unique = true").close();
      assertThat(sessions.get()).as("declaring UNIQUE and building its index is one session").isEqualTo(1);

      sessions.set(0);
      database.command("sql", "ALTER TYPE " + EDGE + " WITH unique = false").close();
      assertThat(sessions.get()).as("withdrawing UNIQUE and dropping its index is one session").isEqualTo(1);
    } finally {
      internal.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, counter);
    }
  }

  /**
   * An index that cannot be built (the type already holds a duplicate pair) must not leave the type declaring UNIQUE
   * with no index behind it: the flag flip and the index are one change, so a refusal takes both back.
   */
  @Test
  void aRefusedIndexBuildLeavesTheFlagAsItWas() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex(VERTEX).save();
      final MutableVertex b = database.newVertex(VERTEX).save();
      a.newEdge(EDGE, b);
      a.newEdge(EDGE, b);
    });

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE " + EDGE + " WITH unique = true").close()).isNotNull();

    assertThat(edgeType().isUnique()).as("the declaration was not applied, so the type must not advertise it").isFalse();
    assertThat(edgeType().isLightweight()).isFalse();
    assertThat(database.getSchema().existsIndex(INDEX_NAME)).isFalse();
    assertThat(edgeType().getAllIndexes(true)).as("the refused build left no index, not even a partial one").isEmpty();
  }

  /**
   * What a refused ALTER leaves behind has to hold on disk too, not only in memory: the schema file is rewritten by the
   * session that failed, after the flags went back, so a reopen reads the type as it was.
   */
  @Test
  void aRefusedAlterLeavesTheSchemaFileAsItWas() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex(VERTEX).save();
      final MutableVertex b = database.newVertex(VERTEX).save();
      a.newEdge(EDGE, b);
      a.newEdge(EDGE, b);
    });

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE " + EDGE + " WITH unique = true").close()).isNotNull();
    reopenDatabase();

    assertThat(edgeType().isUnique()).isFalse();
    assertThat(database.getSchema().existsIndex(INDEX_NAME)).isFalse();
  }

  /**
   * Both settings of one statement are one change: the storage shape is refused first (the type already holds edge records),
   * and the UNIQUE request that came with it is not applied on its own, in memory or on disk.
   */
  @Test
  void aRefusedStorageShapeTakesTheUniqueRequestOfTheSameStatementWithIt() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex(VERTEX).save();
      final MutableVertex b = database.newVertex(VERTEX).save();
      a.newEdge(EDGE, b);
    });

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE " + EDGE + " WITH lightweight = true, unique = true").close())
        .hasMessageContaining("LIGHTWEIGHT");
    reopenDatabase();

    assertThat(edgeType().isLightweight()).isFalse();
    assertThat(edgeType().isUnique()).isFalse();
    assertThat(database.getSchema().existsIndex(INDEX_NAME)).isFalse();
  }

  @Test
  void declaringAndWithdrawingStillMaterialisesAndDropsTheIndex() {
    database.command("sql", "ALTER TYPE " + EDGE + " WITH unique = true").close();
    assertThat(edgeType().isUnique()).isTrue();
    assertThat(database.getSchema().existsIndex(INDEX_NAME)).isTrue();

    assertThatThrownBy(() -> database.getSchema().dropIndex(INDEX_NAME))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("UNIQUE");
    assertThat(database.getSchema().existsIndex(INDEX_NAME)).isTrue();

    database.command("sql", "ALTER TYPE " + EDGE + " WITH unique = false").close();
    assertThat(edgeType().isUnique()).isFalse();
    assertThat(database.getSchema().existsIndex(INDEX_NAME)).isFalse();
  }

  @Test
  void lightweightAndUniqueTogetherStillSettleTheStorageShapeFirst() {
    database.command("sql", "ALTER TYPE " + EDGE + " WITH lightweight = true, unique = true").close();

    assertThat(edgeType().isLightweight()).isTrue();
    assertThat(edgeType().isUnique()).isTrue();
    assertThat(database.getSchema().existsIndex(INDEX_NAME)).as("a lightweight type builds no index").isFalse();
  }
}
