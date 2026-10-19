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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10339: an edge created in the transaction that deletes its target stayed in a Graph Analytical View after the commit
 * when the target sat on the record slot of an earlier deleted vertex. The target's slot still names the deleted base node, so
 * once the target left the overflow the edge was added to the overlay against that node, while its deletion, resolved through
 * the overflow vertex the same delta deleted, named the overflow node: the two never met, and the deletion of a lightweight edge,
 * which has no identity to withdraw the addition by, left it in the view. A regular edge was withdrawn by its RID and went
 * unnoticed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10339ViewEdgeOfDeletedTargetTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE L");
  }

  @ParameterizedTest(name = "{0}, lightweight={1}, reused slot: {2}")
  @CsvSource({ "SYNCHRONOUS,true,TARGET", "SYNCHRONOUS,true,SOURCE", "SYNCHRONOUS,true,NONE", "SYNCHRONOUS,false,TARGET",
      "SYNCHRONOUS,false,SOURCE", "SYNCHRONOUS,false,NONE", "ASYNCHRONOUS,true,TARGET", "ASYNCHRONOUS,true,SOURCE",
      "ASYNCHRONOUS,true,NONE", "ASYNCHRONOUS,false,TARGET", "ASYNCHRONOUS,false,SOURCE", "ASYNCHRONOUS,false,NONE" })
  void anEdgeCreatedInTheTransactionThatDeletesItsTargetIsNotInTheView(final String mode, final boolean lightweight,
      final String reused) {
    final Graph graph = new Graph(mode, reused);

    database.transaction(() -> {
      link(graph, lightweight);
      graph.target.delete();
    });
    assertThat(graph.view.awaitReady(60, TimeUnit.SECONDS)).as("the change is merged").isTrue();

    assertThat(edgesAnsweredByCypher()).as("count(r) of the view").isZero();
    assertThat(graph.view.getEdgeCount()).as("the edges the view holds").isZero();
    assertNoNegativeDegree();
  }

  /** The same sequence with the target left alive: the edge is in the view, on every kind of slot. */
  @ParameterizedTest(name = "{0}, lightweight={1}, reused slot: {2}")
  @CsvSource({ "SYNCHRONOUS,true,TARGET", "SYNCHRONOUS,true,SOURCE", "SYNCHRONOUS,true,NONE", "SYNCHRONOUS,false,TARGET",
      "ASYNCHRONOUS,true,TARGET", "ASYNCHRONOUS,false,SOURCE" })
  void anEdgeToASurvivingTargetIsInTheView(final String mode, final boolean lightweight, final String reused) {
    final Graph graph = new Graph(mode, reused);

    database.transaction(() -> link(graph, lightweight));
    assertThat(graph.view.awaitReady(60, TimeUnit.SECONDS)).as("the change is merged").isTrue();

    assertThat(edgesAnsweredByCypher()).isEqualTo(1);
    assertThat(graph.view.getEdgeCount()).isEqualTo(1);

    // and leaves it with the vertex, in a later transaction
    database.transaction(() -> graph.target.delete());
    assertThat(graph.view.awaitReady(60, TimeUnit.SECONDS)).as("the change is merged").isTrue();

    assertThat(edgesAnsweredByCypher()).isZero();
    assertThat(graph.view.getEdgeCount()).isZero();
    assertNoNegativeDegree();
  }

  /** The edge created in the transaction that deletes its SOURCE: the same slot, the other end. */
  @ParameterizedTest(name = "{0}, lightweight={1}, reused slot: {2}")
  @CsvSource({ "SYNCHRONOUS,true,SOURCE", "SYNCHRONOUS,false,SOURCE", "SYNCHRONOUS,true,TARGET", "ASYNCHRONOUS,true,SOURCE",
      "ASYNCHRONOUS,false,TARGET" })
  void anEdgeCreatedInTheTransactionThatDeletesItsSourceIsNotInTheView(final String mode, final boolean lightweight,
      final String reused) {
    final Graph graph = new Graph(mode, reused);

    database.transaction(() -> {
      link(graph, lightweight);
      graph.source.delete();
    });
    assertThat(graph.view.awaitReady(60, TimeUnit.SECONDS)).as("the change is merged").isTrue();

    assertThat(edgesAnsweredByCypher()).isZero();
    assertThat(graph.view.getEdgeCount()).isZero();
    assertNoNegativeDegree();
  }

  private void link(final Graph graph, final boolean lightweight) {
    if (lightweight)
      graph.source.newLightEdge("L", graph.target);
    else
      graph.source.newEdge("L", graph.target).save();
  }

  /**
   * Two vertices created after the view was built, one of them on the slot of a base vertex deleted before ({@code reused}:
   * SOURCE or TARGET, NONE keeps the base vertex).
   */
  private final class Graph {
    final GraphAnalyticalView view;
    final MutableVertex       source;
    final MutableVertex       target;

    Graph(final String mode, final String reused) {
      final RID[] first = new RID[1];
      database.transaction(() -> first[0] = database.newVertex("P").set("id", 3L).save().getIdentity());

      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P) EDGE TYPES (L) PROPERTIES (id) UPDATE MODE " + mode)
          .close();
      view = GraphAnalyticalViewRegistry.get(database, "gav");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();

      final MutableVertex[] created = new MutableVertex[2];
      if (reused.equals("SOURCE")) {
        database.transaction(() -> first[0].asVertex().modify().delete());
        database.transaction(() -> created[0] = database.newVertex("P").set("id", 7L).save());
        database.transaction(() -> created[1] = database.newVertex("P").set("id", 9L).save());
        assertThat(created[0].getIdentity()).as("precondition: the source takes the freed slot").isEqualTo(first[0]);
      } else if (reused.equals("TARGET")) {
        // a slot freed by a transaction is not reused by that same transaction: the source is created beside the delete, the
        // target in the next one
        database.transaction(() -> {
          first[0].asVertex().modify().delete();
          created[0] = database.newVertex("P").set("id", 7L).save();
        });
        database.transaction(() -> created[1] = database.newVertex("P").set("id", 9L).save());
        assertThat(created[1].getIdentity()).as("precondition: the target takes the freed slot").isEqualTo(first[0]);
      } else {
        database.transaction(() -> created[0] = database.newVertex("P").set("id", 7L).save());
        database.transaction(() -> created[1] = database.newVertex("P").set("id", 9L).save());
        assertThat(created[0].getIdentity()).isNotEqualTo(first[0]);
        assertThat(created[1].getIdentity()).isNotEqualTo(first[0]);
      }
      source = created[0];
      target = created[1];
    }
  }

  private long edgesAnsweredByCypher() {
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:L]->() RETURN count(r) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private void assertNoNegativeDegree() {
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.degree() YIELD node, inDegree, outDegree, degree RETURN inDegree AS i, outDegree AS o, degree AS d")) {
      while (rs.hasNext()) {
        final var r = rs.next();
        assertThat(r.<Number>getProperty("i").longValue()).isGreaterThanOrEqualTo(0);
        assertThat(r.<Number>getProperty("o").longValue()).isGreaterThanOrEqualTo(0);
        assertThat(r.<Number>getProperty("d").longValue()).isGreaterThanOrEqualTo(0);
      }
    }
  }
}
