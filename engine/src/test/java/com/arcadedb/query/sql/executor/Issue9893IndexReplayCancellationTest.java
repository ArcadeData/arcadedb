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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9893: the replay that reconciles an index entry whose lookup overlapped a commit (#9369) re-reads the whole key and
 * loads every record it holds, up to nine times, and never asked whether the statement was still wanted. A cancelled or
 * timed-out statement kept doing it, and the re-read turned the termination into a WARNING and a fallback.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9893IndexReplayCancellationTest extends TestHelper {
  /** More than the 256 entries a point lookup holds, so the lookup streams and reconciles entry by entry. */
  private static final int ENTRIES = 2_000;

  private final RunningQueryRegistry registry = new RunningQueryRegistry();

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Item");
      type.createProperty("k", String.class);
      database.command("sql", "CREATE INDEX ON Item (k) NOTUNIQUE");
      database.getSchema().createDocumentType("Other");
      for (int i = 0; i < ENTRIES; i++)
        database.newDocument("Item").set("k", "hot").set("n", i).save();
    });
  }

  @Test
  void terminatedStatementStopsTheReplayInsteadOfReloadingTheKey() throws Exception {
    final AtomicBoolean armed = new AtomicBoolean(false);
    final AtomicInteger loadsAfterTermination = new AtomicInteger();
    final AtomicBoolean terminated = new AtomicBoolean(false);
    final AtomicReference<Throwable> committerFailure = new AtomicReference<>();
    final AtomicReference<RunningQuery> statement = new AtomicReference<>();

    // On the first record load of the lookup a commit lands (so the lookup overlapped one and the entry is replayed) and the
    // statement is terminated: everything loaded after that is work the termination should have stopped
    final AfterRecordReadListener listener = record -> {
      if (terminated.get())
        loadsAfterTermination.incrementAndGet();
      else if (armed.compareAndSet(true, false)) {
        final Thread committer = new Thread(() -> {
          try {
            database.transaction(() -> database.newDocument("Other").set("x", 1).save());
          } catch (final Throwable e) {
            committerFailure.set(e);
          }
        });
        committer.start();
        try {
          committer.join(30_000);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        statement.get().terminate("root");
        terminated.set(true);
      }
      return record;
    };

    database.getSchema().getType("Item").getEvents().registerListener(listener);
    try (final RunningQuery q = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
      statement.set(q);
      armed.set(true);
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM Item WHERE k = 'hot'")) {
          while (rs.hasNext())
            rs.next();
        }
      }).isInstanceOf(QueryTerminatedException.class);
    } finally {
      database.getSchema().getType("Item").getEvents().unregisterListener(listener);
    }

    assertThat(committerFailure.get()).isNull();
    assertThat(terminated.get()).as("the commit and the termination must have happened during the lookup").isTrue();
    // Before the fix the replay re-read the key and loaded every record of it (ENTRIES - 1) before anything checked
    assertThat(loadsAfterTermination.get()).as("records loaded by the replay after the statement was terminated").isZero();
  }

  @Test
  void reReadOfTheKeyPassesATerminationThrough() {
    final FetchFromIndexStep step = indexStepOf("SELECT FROM Item WHERE k = 'hot'");
    // A healthy re-read answers every entry of the key
    final List<RID> all = step.lookupKeyAgain("hot", WorkGuard.forCommandDeadline(null));
    assertThat(all).hasSize(ENTRIES);

    try (final RunningQuery q = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
      q.terminate("root");
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase((DatabaseInternal) database);
      // Before the fix: logged as "Cannot look the key ... up again" and answered null, so the caller carried on
      assertThatThrownBy(() -> step.lookupKeyAgain("hot", WorkGuard.forCommandDeadline(context)))
          .isInstanceOf(QueryTerminatedException.class);
    }
  }

  @Test
  void reReadOfTheKeyPassesATimeoutThrough() {
    final FetchFromIndexStep step = indexStepOf("SELECT FROM Item WHERE k = 'hot'");
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);
    context.setCommandDeadline(System.currentTimeMillis() - 1, "test deadline");

    assertThatThrownBy(() -> step.lookupKeyAgain("hot", WorkGuard.forCommandDeadline(context)))
        .isInstanceOf(TimeoutException.class);
  }

  private FetchFromIndexStep indexStepOf(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      final FetchFromIndexStep step = findIndexStep(rs.getExecutionPlan().orElseThrow().getSteps());
      assertThat(step).as("the query must be answered by an index lookup").isNotNull();
      return step;
    }
  }

  private static FetchFromIndexStep findIndexStep(final List<ExecutionStep> steps) {
    if (steps == null)
      return null;
    for (final ExecutionStep step : steps) {
      if (step instanceof final FetchFromIndexStep indexStep)
        return indexStep;
      final FetchFromIndexStep nested = findIndexStep(step.getSubSteps());
      if (nested != null)
        return nested;
    }
    return null;
  }
}
