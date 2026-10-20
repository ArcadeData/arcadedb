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
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphViewPins;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10415 (#10365, #10393): the Cypher MATCH operators that use a Graph Analytical View read whichever snapshot the view
 * served at each call. An operator maps a vertex to its node id, reads the neighbours, maps them back, and hands the ids on to
 * the next operator, while the rows stream out in batches: a rebuild between two batches - a compaction, an explicit REBUILD -
 * numbers the nodes afresh, and the later batches answered the old ids as other vertices, without failing. Every operator of a
 * statement now reads one pin of the view, taken when the statement first reads it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10415ViewPinnedPerStatementTest extends TestHelper {
  private static final int PAD     = 2_000;
  // More neighbours than a batch of rows, and more two-hop paths than a round of the fused walk: the ids an operator read
  // before the rebuild are still being consumed after it
  private static final int TARGETS = 20_000;

  private final List<RID> sources = new ArrayList<>();
  private       RID       deleted;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE Q");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE L");
    database.transaction(() -> {
      // padding first, so the base numbers the hub's targets after it and a deletion shifts them on the next build
      for (int i = 0; i < PAD; i++)
        database.newVertex("Q").set("id", 100_000 + i).save();
      deleted = database.newVertex("P").set("id", -1).save().getIdentity();
      final MutableVertex hub = database.newVertex("P").set("id", 0).save();
      sources.add(hub.getIdentity());
      sources.add(database.newVertex("P").set("id", 1).save().getIdentity());
      for (int i = 0; i < TARGETS; i++) {
        final MutableVertex middle = database.newVertex("Q").set("id", 1_000_000 + i).save();
        final MutableVertex end = database.newVertex("Q").set("id", 2_000_000 + i).save();
        hub.newEdge("K", middle);
        middle.newEdge("L", end);
      }
    });
  }

  /** The reproduction of #10365: a view-served MATCH streams its rows, and the view is rebuilt after the first one. */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "MATCH (a:P)-[:K]->(b:Q) RETURN a.id AS a, b.id AS b | GAVExpandAll",
      "MATCH (a:P)-[:K]->(b:Q)-[:L]->(c:Q) RETURN a.id AS a, c.id AS b | GAVFusedChain",
      "MATCH (a:P)-[:K]->(b:Q) WITH a, b MATCH (b)-[:L]->(c:Q) RETURN a.id AS a, c.id AS b | MATCH RELATIONSHIP" })
  void aRebuildWhileTheRowsStreamDoesNotChangeTheVerticesTheyName(final String query, final String operator) {
    final GraphAnalyticalView view = createView();
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).as("the view-served operator under test").contains(operator);
    }
    // an overlay deletion, so the rebuild below numbers every node after it afresh
    database.transaction(() -> deleted.asVertex().delete());
    assertThat(view.hasPendingChanges()).isTrue();

    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      rows.add(pair(rs.next()));
      view.build();
      assertThat(view.hasPendingChanges()).as("the rebuild published a new base").isFalse();
      while (rs.hasNext())
        rows.add(pair(rs.next()));
    }

    final List<String> expected = new ArrayList<>(TARGETS);
    final int offset = query.contains(":L") ? 2_000_000 : 1_000_000;
    for (int i = 0; i < TARGETS; i++)
      expected.add("0->" + (offset + i));
    assertThat(rows).containsExactlyInAnyOrderElementsOf(expected);
  }

  /** The operators of one statement share the pin: one snapshot per view per statement. */
  @Test
  void aStatementPinsEachViewOnce() {
    final GraphAnalyticalView view = createView();
    final BasicCommandContext statement = new BasicCommandContext();
    statement.openGraphViewPins();
    final GraphTraversalProvider first = GraphViewPins.pin(statement, view);
    assertThat(first).isInstanceOf(GraphAnalyticalView.Pinned.class);

    final BasicCommandContext derived = new BasicCommandContext();
    derived.setParent(statement);
    assertThat(GraphViewPins.pin(derived, view)).as("a sub-query of the statement").isSameAs(first);
    assertThat(GraphViewPins.pin(statement.copy(), view)).as("a parallel worker of the statement").isSameAs(first);
    assertThat(GraphViewPins.pin(statement, first)).as("a pin pinned again").isSameAs(first);

    // another statement, and a caller outside any statement, pin for themselves
    final BasicCommandContext next = new BasicCommandContext();
    next.openGraphViewPins();
    assertThat(GraphViewPins.pin(next, view)).isNotSameAs(first);
    assertThat(GraphViewPins.pin(new BasicCommandContext(), view)).isNotSameAs(first).isInstanceOf(GraphAnalyticalView.Pinned.class);

    // two statements of one script: the script's context opens no scope, so each statement opens its own and the second does
    // not inherit the first one's pins, while what the script itself evaluates outside a statement holds none
    final BasicCommandContext script = new BasicCommandContext();
    final BasicCommandContext firstStatement = new BasicCommandContext();
    firstStatement.setParentWithoutOverridingChild(script);
    firstStatement.openGraphViewPins();
    final BasicCommandContext secondStatement = new BasicCommandContext();
    secondStatement.setParentWithoutOverridingChild(script);
    secondStatement.openGraphViewPins();
    assertThat(script.getGraphViewPins()).isNull();
    assertThat(secondStatement.getGraphViewPins()).isNotNull().isNotSameAs(firstStatement.getGraphViewPins());
    assertThat(GraphViewPins.pin(secondStatement, view)).isNotSameAs(GraphViewPins.pin(firstStatement, view));
  }

  /**
   * A view with no snapshot yet has nothing to pin: the statement is handed the view itself, every read of which fails, and the
   * fallback is not kept, so the same statement pins the view once it is built - and keeps that pin from then on.
   */
  @Test
  void aViewBuiltDuringTheStatementIsPinnedFromItsFirstSnapshot() {
    final GraphAnalyticalView unbuilt = new GraphAnalyticalView(database);
    try {
      final BasicCommandContext statement = new BasicCommandContext();
      statement.openGraphViewPins();
      assertThat(GraphViewPins.pin(statement, unbuilt)).isSameAs(unbuilt);
      assertThatThrownBy(() -> unbuilt.getNodeId(sources.get(0))).isInstanceOf(IllegalStateException.class);

      unbuilt.build();
      final GraphTraversalProvider pinned = GraphViewPins.pin(statement, unbuilt);
      assertThat(pinned).isInstanceOf(GraphAnalyticalView.Pinned.class);
      unbuilt.build();
      assertThat(GraphViewPins.pin(statement, unbuilt)).as("the statement keeps the first snapshot it pinned").isSameAs(pinned);
    } finally {
      unbuilt.shutdown();
    }
  }

  /**
   * A statement of a script pins for itself: the one that follows a COMMIT reads what was committed, not the snapshot an earlier
   * statement of the script read.
   */
  @Test
  void aScriptStatementAfterACommitReadsWhatWasCommitted() {
    createView();
    final String count = "SELECT sum(out('K').size()) AS n FROM P";
    try (final ResultSet rs = database.command("sqlscript", count + ";\n"
        + "BEGIN;\n"
        + "CREATE EDGE K FROM " + sources.get(0) + " TO " + sources.get(1) + ";\n"
        + "COMMIT;\n"
        + count + ";")) {
      assertThat(rs.next().<Number>getProperty("n").longValue()).isEqualTo(TARGETS + 1L);
    }
  }

  private static String pair(final Result row) {
    return row.<Number>getProperty("a").intValue() + "->" + row.<Number>getProperty("b").intValue();
  }

  private GraphAnalyticalView createView() {
    database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P, Q) EDGE TYPES (K, L) PROPERTIES (id) UPDATE MODE SYNCHRONOUS").close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    return view;
  }
}
