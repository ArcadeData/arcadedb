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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7022: returning the node itself after a variable-scoped {@code CALL (n0) { }} body removed one of its
 * labels answered {@code RecordNotFoundException: Record #7:0 not found} instead of the relabelled node.
 * <p>
 * A Cypher label is an ArcadeDB type and a type is a set of buckets, so there is no in-place retype: {@code REMOVE
 * n0:l11} rewrites the vertex under the remaining label and deletes the original, which changes its RID. The outer
 * row was left holding the record the body had just deleted, and the first thing that read through it - the final
 * {@code RETURN n0}, or the driver serializing the node it produced - failed on the RID that was gone.
 * <p>
 * The redirect that fixes it is {@code SubqueryStep.executeInnerQuery}'s
 * {@code LabelReplacements.of(context).redirect(outerRow)}, landed for issue #6977; it runs before
 * {@code refreshDocumentBindings}, which would otherwise re-read the outer binding by its dead RID. This class pins
 * the two dimensions of the report that #6977's own tests do not exercise, either of which could regress on its own:
 * the import is a <b>variable scope clause</b> ({@code CALL (n0)}) rather than {@code CALL (*)}, so it travels
 * through a different {@code filterSeedRow} branch, and the outer clause returns the <b>whole node</b> rather than
 * a derived value such as {@code labels(n)}, so nothing but the binding itself can carry the correction.
 * <p>
 * The body aggregates ({@code RETURN collect(...)}), which is what the report used and what makes the shape worth
 * pinning: an aggregating body emits a single row that carries none of the inner bindings, so the outer alias is
 * the only surviving reference to the moved node.
 * <p>
 * Plan coverage: every {@code CALL { }} shape here is declined by {@code CypherOptimizer} today - {@code EXPLAIN}
 * reports "Using Traditional Execution (Non-Optimized) / Reason: Query pattern not yet supported by optimizer" for
 * all of them - so the legacy pipeline is the only path these queries can take, and {@code SubqueryStep} is the only
 * place the redirect can live. Should the optimizer ever learn correlated subqueries, these assertions are what say
 * whether the new path kept the guarantee.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherScopedCallLabelWriteIssue7022Test {
  private Database database;

  @BeforeEach
  void setUp() {
    // Drop first rather than create outright: a run killed before @AfterEach leaves the directory behind and
    // create() refuses to overwrite it, which would fail every method here for an environmental reason.
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/testopencypher-scoped-call-labelwrite-7022");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void theReportedQueryReturnsTheRelabelledNode() {
    cypher("CREATE (:l11:l1 {id: 1})");

    // Verbatim from the report. Reading anything off the returned node is what used to throw: the row still held
    // the record REMOVE deleted, so reloading it answered "Record #7:0 not found".
    final List<String> rows = writingRows("""
        OPTIONAL MATCH (n0)
        WITH * WHERE n0 IS NOT NULL
        CALL (n0) {
          REMOVE n0:l11
          RETURN collect(toStringOrNull(1)) AS alias3
        }
        RETURN n0""", row -> {
      final Vertex node = row.getProperty("n0");
      // asVertex(true) forces a reload from storage, which is the read the reported failure came out of.
      final Vertex reloaded = node.asVertex(true);
      return reloaded.getTypeName() + "/" + reloaded.get("id");
    });

    assertThat(rows).containsExactly("l1/1");
    // The node is where the query said it is, with the label it said it has.
    assertThat(rows("MATCH (n) RETURN labels(n) AS v")).containsExactly("[l1]");
    assertThat(rows("MATCH (n:l1) RETURN n.id AS v")).containsExactly("1");
    assertThat(rows("MATCH (n:l11) RETURN n.id AS v")).isEmpty();
  }

  @Test
  void theAggregatingBodyKeepsItsOwnColumnAlongsideTheRedirectedNode() {
    cypher("CREATE (:l11:l1 {id: 1})");

    // The body's single aggregated row and the outer row are merged: the column the body produced has to survive
    // the redirect, and the node has to be the live one.
    assertThat(writingRows("""
        OPTIONAL MATCH (n0)
        WITH * WHERE n0 IS NOT NULL
        CALL (n0) {
          REMOVE n0:l11
          RETURN collect(toStringOrNull(1)) AS alias3
        }
        RETURN alias3[0] + '/' + head(labels(n0)) + '/' + n0.id AS v""")).containsExactly("1/l1/1");
  }

  @Test
  void everyAliasOfTheImportedNodeFollowsTheMove() {
    cypher("CREATE (:l11:l1 {id: 1})");

    // n1 is not imported into the body - only n0 is - but it names the same record, so the redirect has to reach it
    // too or the second alias answers with the vertex the body deleted.
    assertThat(writingRows("""
        MATCH (n0:l11)
        WITH n0, n0 AS n1, [n0] AS boxed
        CALL (n0) {
          REMOVE n0:l11
          RETURN collect(1) AS ignored
        }
        RETURN head(labels(n0)) + '/' + head(labels(n1)) + '/' + head(labels(boxed[0])) + '/' + n1.id AS v"""))
        .containsExactly("l1/l1/l1/1");
  }

  @Test
  void eachOuterRowFollowsTheNodeItsOwnBodyMoved() {
    cypher("CREATE (:l11:l1 {id: 1}) CREATE (:l11:l1 {id: 2}) CREATE (:l11:l1 {id: 3})");

    // Three distinct nodes, one relabelled per outer row: the redirect has to be per-record, not "the last thing
    // that moved".
    assertThat(writingRows("""
        OPTIONAL MATCH (n0)
        WITH * WHERE n0 IS NOT NULL
        CALL (n0) {
          REMOVE n0:l11
          RETURN collect(toStringOrNull(1)) AS alias3
        }
        RETURN head(labels(n0)) + '/' + n0.id AS v""")).containsExactlyInAnyOrder("l1/1", "l1/2", "l1/3");

    assertThat(rows("MATCH (n:l1) RETURN n.id AS v")).containsExactlyInAnyOrder("1", "2", "3");
  }

  @Test
  void theRedirectedNodeIsStillAnExpansionAnchorAfterTheCall() {
    cypher("CREATE (:l11:l1 {id: 1})-[:E {w: 7}]->(:l2 {id: 2})");

    // A label write re-creates every incident edge under new RIDs as well. The outer alias has to reach the
    // replacement's edge list, not the deleted vertex's, for a later MATCH to expand from it at all.
    assertThat(writingRows("""
        MATCH (n0:l11)
        CALL (n0) {
          REMOVE n0:l11
          RETURN collect(toStringOrNull(1)) AS alias3
        }
        WITH n0
        MATCH (n0)-[r:E]->(m)
        RETURN n0.id + '-[' + r.w + ']->' + m.id AS v""")).containsExactly("1-[7]->2");
  }

  @Test
  void theReportedShapeStillTakesTheLegacyPipeline() {
    cypher("CREATE (:l11:l1 {id: 1})");

    // A tripwire, not a requirement: nothing here wants the optimizer to keep declining correlated subqueries. It
    // fires the day CypherOptimizer learns them, which is precisely when the four assertions above stop covering
    // the path the reported query actually runs on - the redirect lives in SubqueryStep, and an optimized plan for
    // a CALL { } body would need its own. Answering it means re-running this class against the new plan and then
    // relaxing this assertion, never the other way round.
    final String plan = rows("""
        EXPLAIN OPTIONAL MATCH (n0)
        WITH * WHERE n0 IS NOT NULL
        CALL (n0) {
          REMOVE n0:l11
          RETURN collect(toStringOrNull(1)) AS alias3
        }
        RETURN n0""", false, row -> String.valueOf(row.<Object>getProperty("executionPlanAsString"))).get(0);

    assertThat(plan)
        .as("CypherOptimizer declines every correlated CALL { } today, so SubqueryStep is the only place the "
            + "label-write redirect can live and the legacy pipeline is the only path this class covers. If this "
            + "fails the optimizer has learnt the shape: check that the optimized plan still points the outer row "
            + "at the replacement vertex, then update this assertion")
        .contains("Non-Optimized");
  }

  private List<String> rows(final String query) {
    return rows(query, false, row -> String.valueOf(row.<Object>getProperty("v")));
  }

  /**
   * Same as {@link #rows(String)} for a query that also writes, which the read-only entry point rejects.
   */
  private List<String> writingRows(final String query) {
    return rows(query, true, row -> String.valueOf(row.<Object>getProperty("v")));
  }

  private List<String> writingRows(final String query, final Function<Result, String> extractor) {
    return rows(query, true, extractor);
  }

  private List<String> rows(final String query, final boolean writes, final Function<Result, String> extractor) {
    final List<String> out = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = writes ? database.command("opencypher", query) : database.query("opencypher", query)) {
        while (rs.hasNext())
          out.add(extractor.apply(rs.next()));
      }
    });
    return out;
  }

  private void cypher(final String query) {
    database.transaction(() -> database.command("opencypher", query));
  }
}
