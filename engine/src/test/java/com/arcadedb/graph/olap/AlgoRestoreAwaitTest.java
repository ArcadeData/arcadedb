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
package com.arcadedb.graph.olap;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.PartialResultTimeoutException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.procedures.algo.AlgoWCC;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.utility.StallAwareStopwatch;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How a whole-graph algorithm waits for a Graph Analytical View that is being restored from disk (issue #9220).
 * <p>
 * The restore is held in flight with the shared build permits, so every case here is deterministic: nothing depends
 * on how fast the restore happens to be.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoRestoreAwaitTest {
  private static final String DB_PATH   = "./target/databases/test-algo-restore-await";
  private static final String VIEW_NAME = "restore-await-view";

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      database.newVertex("Node").set("name", "D").save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen())
      database.close();
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
  }

  /** Persists a view, then reopens the database so the view is a deferred restore-from-disk. */
  private GraphAnalyticalView persistThenReopen(final GraphAnalyticalView.UpdateMode mode) {
    GraphAnalyticalView.builder(database)
        .withName(VIEW_NAME)
        .withVertexTypes("Node")
        .withEdgeTypes("EDGE")
        .withUpdateMode(mode)
        .build();
    database.close();
    database = new DatabaseFactory(DB_PATH).open();
    final GraphAnalyticalView restored = GraphAnalyticalViewRegistry.get(database, VIEW_NAME);
    assertThat(restored).isNotNull();
    return restored;
  }

  private List<Result> runWcc(final CommandContext context) {
    return new AlgoWCC().execute(new Object[0], null, context).collect(Collectors.toList());
  }

  private BasicCommandContext newContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  private void assertComponentsOfTheFourNodeGraph(final List<Result> rows) {
    assertThat(rows).hasSize(4);
    assertThat(rows.stream().map(r -> (Integer) r.getProperty("componentId")).distinct().count()).isEqualTo(2);
  }

  @Test
  void theRestoringFlagIsRaisedByTheDeferredRestoreAndClearedWhenItEnds() throws Exception {
    final GraphAnalyticalView restored = persistThenReopen(GraphAnalyticalView.UpdateMode.OFF);
    assertThat(restored.isRestoring()).as("nothing has dispatched the deferred restore yet").isFalse();

    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    try {
      assertThat(restored.isReady()).as("the call that dispatches the restore is told not ready").isFalse();
      assertThat(restored.isRestoring()).as("the dispatched restore is parked on a build permit").isTrue();
    } finally {
      GraphAnalyticalView.releaseAllBuildPermitsForTest();
    }

    assertThat(restored.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    // awaitReady() returns once the view is READY, which the restore task publishes just before it clears the in-flight flag
    Awaitility.await("the restore has ended").atMost(10, TimeUnit.SECONDS).until(() -> !restored.isRestoring());
  }

  @Test
  void aRebuildAfterACommitIsNotARestore() throws Exception {
    GraphAnalyticalView.builder(database)
        .withName(VIEW_NAME)
        .withVertexTypes("Node")
        .withEdgeTypes("EDGE")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.ASYNCHRONOUS)
        .build();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, VIEW_NAME);

    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    try {
      database.transaction(() -> database.newVertex("Node").set("name", "E").save());
      assertThat(view.getStatus()).as("the commit dispatched an asynchronous rebuild").isEqualTo(GraphAnalyticalView.Status.BUILDING);
      assertThat(view.isRestoring())
          .as("a rebuild after a commit is not a restore, so algorithms must not wait for it")
          .isFalse();
    } finally {
      GraphAnalyticalView.releaseAllBuildPermitsForTest();
    }
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void anAlgorithmGivesUpAtTheConfiguredBudgetAndAnswersFromTheRecords() {
    persistThenReopen(GraphAnalyticalView.UpdateMode.OFF);
    database.getConfiguration().setValue(GlobalConfiguration.GAV_ALGO_RESTORE_AWAIT_TIMEOUT, 300L);

    final BasicCommandContext context = newContext();
    final List<Result> rows;
    final StallAwareStopwatch stopwatch;
    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    try {
      stopwatch = StallAwareStopwatch.start();
      rows = runWcc(context);
    } finally {
      GraphAnalyticalView.releaseAllBuildPermitsForTest();
    }

    stopwatch.assertGaveUpWithin(30_000L, "a 300 ms budget versus the 600 s it is lifted to by default setting changes");
    assertThat(stopwatch.elapsedMs()).as("it did wait for its budget").isGreaterThanOrEqualTo(250L);
    assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
        .as("the stalled restore never became usable, so the call took the record path")
        .isNotEqualTo(true);
    assertComponentsOfTheFourNodeGraph(rows);
  }

  @Test
  void theCommandDeadlineAbortsTheWait() {
    persistThenReopen(GraphAnalyticalView.UpdateMode.OFF);
    database.getConfiguration().setValue(GlobalConfiguration.GAV_ALGO_RESTORE_AWAIT_TIMEOUT, 600_000L);

    final BasicCommandContext context = newContext();
    context.setCommandDeadline(System.currentTimeMillis() + 200L, "test command timeout");
    final StallAwareStopwatch stopwatch;
    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    try {
      stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> runWcc(context)).isInstanceOf(TimeoutException.class);
    } finally {
      GraphAnalyticalView.releaseAllBuildPermitsForTest();
    }

    stopwatch.assertGaveUpWithin(30_000L, "a 200 ms command deadline versus the 600 s wait budget");
  }

  /**
   * A SQL {@code TIMEOUT n RETURN} clause pins a deadline that asks for "the rows so far". The wait runs inside the
   * procedure's {@code execute()}, before any row exists, and what it throws must be the partial-result timeout the
   * owning step recognises (and turns into an empty result set), not some other exception.
   */
  @Test
  void aReturnClauseDeadlineSurfacesAsAPartialResultTimeout() {
    persistThenReopen(GraphAnalyticalView.UpdateMode.OFF);
    database.getConfiguration().setValue(GlobalConfiguration.GAV_ALGO_RESTORE_AWAIT_TIMEOUT, 600_000L);

    final BasicCommandContext context = newContext();
    context.setCommandDeadline(System.currentTimeMillis() + 200L, "test TIMEOUT RETURN clause", true);
    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    try {
      assertThatThrownBy(() -> runWcc(context)).isInstanceOf(PartialResultTimeoutException.class);
    } finally {
      GraphAnalyticalView.releaseAllBuildPermitsForTest();
    }
  }

  @Test
  void aZeroBudgetDoesNotWaitAtAll() {
    persistThenReopen(GraphAnalyticalView.UpdateMode.OFF);
    database.getConfiguration().setValue(GlobalConfiguration.GAV_ALGO_RESTORE_AWAIT_TIMEOUT, 0L);

    final BasicCommandContext context = newContext();
    final List<Result> rows;
    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    try {
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      rows = runWcc(context);
      stopwatch.assertGaveUpWithin(30_000L, "no waiting versus a stalled restore that never ends");
    } finally {
      GraphAnalyticalView.releaseAllBuildPermitsForTest();
    }

    assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR)).isNotEqualTo(true);
    assertComponentsOfTheFourNodeGraph(rows);
  }
}
