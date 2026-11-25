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
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The scan of an edge type that holds LIGHTWEIGHT edges walks the vertices that carry them, and the length of that walk
 * is the size of the graph, not of the answer. It has to answer to the command deadline and to the termination of the
 * statement the way the other scan sources do: a type whose edges hang off a few vertices of a large vertex set spends
 * its whole time inside one call, with no row reaching a downstream check.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class LightweightEdgeTypeWalkBoundsTest extends TestHelper {
  /** More than one check interval of the guard (1024 steps of the walk), so the walk is checked inside a single call. */
  private static final int VERTICES = 3_000;

  private final RunningQueryRegistry registry = new RunningQueryRegistry();

  private RID[] nodes;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("Node").create();
      database.getSchema().buildEdgeType().withName("Link").withLightweight(true).create();
    });

    nodes = new RID[VERTICES];
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        nodes[i] = database.newVertex("Node").set("id", i).save().getIdentity();
    });

    // Every edge leaves a vertex at the head of the walk: past them, the walk crosses vertices that hold none
    database.transaction(() -> {
      database.lookupByRID(nodes[0], true).asVertex().modify().newEdge("Link", nodes[1]);
      database.lookupByRID(nodes[0], true).asVertex().modify().newEdge("Link", nodes[2]);
    });
  }

  @Test
  void theScanStopsAtAnExpiredDeadline() {
    final BasicCommandContext context = newContext();
    context.setCommandDeadline(System.currentTimeMillis() - 1, "an already expired deadline");

    final ResultSet result = new FetchFromLightweightEdgeTypeStep("Link", context).syncPull(context, 100);

    assertThatThrownBy(() -> drain(result)).isInstanceOf(TimeoutException.class).hasMessageContaining("an already expired deadline");
  }

  @Test
  void theCountStopsAtAnExpiredDeadline() {
    final BasicCommandContext context = newContext();
    context.setCommandDeadline(System.currentTimeMillis() - 1, "an already expired deadline");

    assertThatThrownBy(() -> FetchFromLightweightEdgeTypeStep.countEdgesOf(context, "Link")).isInstanceOf(TimeoutException.class)
        .hasMessageContaining("an already expired deadline");
  }

  @Test
  void theScanAnswersTheDeadlinePinnedAfterItWasReset() {
    final BasicCommandContext context = newContext();
    final FetchFromLightweightEdgeTypeStep step = new FetchFromLightweightEdgeTypeStep("Link", context);
    assertThat(drain(step.syncPull(context, 100))).hasSize(2);

    step.reset();
    context.setCommandDeadline(System.currentTimeMillis() - 1, "an already expired deadline");
    final ResultSet result = step.syncPull(context, 100);

    assertThatThrownBy(() -> drain(result)).isInstanceOf(TimeoutException.class).hasMessageContaining("an already expired deadline");
  }

  @Test
  void aStatementTerminatedInTheMiddleOfTheWalkStopsInsideTheCall() {
    try (final RunningQuery query = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
      final BasicCommandContext context = newContext();
      final ResultSet result = new FetchFromLightweightEdgeTypeStep("Link", context).syncPull(context, 100);

      // The two edges are served from the head of the walk, then nothing is left to serve for the 2,998 vertices behind them
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isTrue();
      result.next();

      query.terminate("root");
      assertThatThrownBy(result::hasNext).isInstanceOf(QueryTerminatedException.class);
    }
  }

  @Test
  void theCountOfATerminatedStatementStops() {
    try (final RunningQuery query = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
      final BasicCommandContext context = newContext();
      query.terminate("root");

      assertThatThrownBy(() -> FetchFromLightweightEdgeTypeStep.countEdgesOf(context, "Link")).isInstanceOf(QueryTerminatedException.class);
    }
  }

  @Test
  void aTerminatedStatementStopsWhateverTheStatementIs() {
    for (final String sql : List.of("SELECT FROM Link", "SELECT FROM Link LIMIT 1", "SELECT count(*) AS c FROM Link",
        "SELECT FROM Link WHERE @type = 'Link'", "TRAVERSE in, out FROM Link WHILE $depth < 1")) {
      try (final RunningQuery query = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
        query.terminate("root");

        final Throwable failure = catchThrowable(() -> {
          try (final ResultSet resultSet = database.query("sql", sql)) {
            drain(resultSet);
          }
        });

        assertThat(causeOfType(failure, QueryTerminatedException.class)).as(sql).isNotNull();
      }
    }
  }

  @Test
  void theWalkIsUnchangedWhenNothingBoundsIt() {
    final BasicCommandContext context = newContext();

    assertThat(drain(new FetchFromLightweightEdgeTypeStep("Link", context).syncPull(context, 100))).hasSize(2);
    assertThat(FetchFromLightweightEdgeTypeStep.countEdgesOf(context, "Link")).isEqualTo(2L);

    try (final ResultSet resultSet = database.query("sql", "SELECT count(*) AS c FROM Link")) {
      assertThat(resultSet.next().<Long>getProperty("c")).isEqualTo(2L);
    }
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM Link")) {
      assertThat(drain(resultSet)).hasSize(2);
    }
  }

  private BasicCommandContext newContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);
    return context;
  }

  private static <T extends Throwable> T causeOfType(final Throwable failure, final Class<T> type) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause())
      if (type.isInstance(cause))
        return type.cast(cause);
    return null;
  }

  private static List<Result> drain(final ResultSet resultSet) {
    final List<Result> rows = new ArrayList<>();
    while (resultSet.hasNext())
      rows.add(resultSet.next());
    return rows;
  }
}
