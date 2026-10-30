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

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11011: a view whose background build died of an {@link OutOfMemoryError} kept reading BUILDING forever, so a
 * client polling {@code schema:graphAnalyticalViews} could not tell a dead build from a slow one. A build that throws,
 * an {@link Error} included, ends FAILED with its cause, reported by the status row and logged at SEVERE with the view
 * name, and the view can be built again.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11011GavBuildFailureReportedTest extends TestHelper {
  private static final String VIEW = "failing";

  @Test
  void anAsyncBuildThatRunsOutOfMemoryEndsFailedWithItsCause() {
    createGraph();
    final CapturingLogger captured = new CapturingLogger();
    final Logger previousLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(captured);
    final GraphAnalyticalView view;
    try {
      // Held so the hook is in place before the build's task can start its scan
      GraphAnalyticalView.acquireAllBuildPermitsForTest();
      try {
        view = GraphAnalyticalView.builder(database).withName(VIEW).withVertexTypes("V").withEdgeTypes("E").buildAsync();
        view.setBeforeBuildScanForTest(() -> {
          throw new OutOfMemoryError("Java heap space");
        });
      } finally {
        GraphAnalyticalView.releaseAllBuildPermitsForTest();
      }

      assertThat(view.awaitReady(10, TimeUnit.SECONDS)).isFalse();
    } finally {
      LogManager.instance().setLogger(previousLogger);
    }

    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.FAILED);
    assertThat(view.getBuildError()).isInstanceOf(OutOfMemoryError.class);
    assertThat(captured.severe()).contains(VIEW).contains("ran out of heap");

    final Result row = viewRow();
    assertThat(row.<String>getProperty("status")).isEqualTo("FAILED");
    assertThat(row.<String>getProperty("buildError")).isEqualTo("OutOfMemoryError: Java heap space");

    // The definition stayed: rebuilt, the view is READY and the failure is gone from its row
    database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW " + VIEW).close();
    final Result rebuilt = viewRow();
    assertThat(rebuilt.<String>getProperty("status")).isEqualTo("READY");
    assertThat(rebuilt.<String>getProperty("buildError")).isNull();
    assertThat(rebuilt.<Integer>getProperty("edgeCount")).isEqualTo(10);

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW " + VIEW).close();
    assertThat(GraphAnalyticalViewRegistry.getFailure(database, VIEW)).isNull();
  }

  @Test
  void aDroppedFailedViewLeavesNoFailureBehind() {
    createGraph();
    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    final GraphAnalyticalView view;
    try {
      view = GraphAnalyticalView.builder(database).withName(VIEW).withVertexTypes("V").withEdgeTypes("E").buildAsync();
      view.setBeforeBuildScanForTest(() -> {
        throw new StackOverflowError();
      });
    } finally {
      GraphAnalyticalView.releaseAllBuildPermitsForTest();
    }
    assertThat(view.awaitReady(10, TimeUnit.SECONDS)).isFalse();
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.FAILED);
    assertThat(viewRow().<String>getProperty("buildError")).isEqualTo("StackOverflowError");

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW " + VIEW).close();

    assertThat(GraphAnalyticalViewRegistry.getFailure(database, VIEW)).isNull();
    try (final ResultSet rows = database.query("sql", "SELECT FROM schema:graphAnalyticalViews WHERE name = ?", VIEW)) {
      assertThat(rows.hasNext()).isFalse();
    }
  }

  private void createGraph() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      MutableVertex previous = database.newVertex("V").save();
      for (int i = 0; i < 10; i++) {
        final MutableVertex next = database.newVertex("V").save();
        previous.newEdge("E", next);
        previous = next;
      }
    });
  }

  private Result viewRow() {
    try (final ResultSet rows = database.query("sql", "SELECT FROM schema:graphAnalyticalViews WHERE name = ?", VIEW)) {
      assertThat(rows.hasNext()).isTrue();
      return rows.next();
    }
  }

  private static final class CapturingLogger implements Logger {
    private final StringBuilder severe = new StringBuilder();

    String severe() {
      synchronized (severe) {
        return severe.toString();
      }
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
        final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
        final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      append(level, message, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object... args) {
      append(level, message, args);
    }

    @Override
    public void flush() {
      // NO-OP
    }

    private void append(final Level level, final String message, final Object... args) {
      if (level != Level.SEVERE || message == null)
        return;
      String text;
      try {
        text = args != null && args.length > 0 ? String.format(message, args) : message;
      } catch (final RuntimeException e) {
        text = message;
      }
      synchronized (severe) {
        severe.append(text).append('\n');
      }
    }
  }
}
