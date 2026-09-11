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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.SelectStatement;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7477, reported as discussion #7473: a LIGHTWEIGHT edge type stores its edges inside the two vertices and
 * allocates no record, so the bucket a type scan reads is empty by construction. Every query that named the type
 * therefore answered zero - {@code SELECT FROM CITE}, {@code SELECT count(*) FROM CITE} and Studio's record count
 * alike - on a graph that held 75 million edges, which is what the bulk load in the report looked like when it had
 * in fact worked.
 * <p>
 * The scan now walks the vertices that hold those edges. This pins what it returns, that the count agrees with it,
 * and that neither a regular edge type nor the cheap {@code @out}/{@code @in} rewrite changed on the way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7477LightweightEdgeScanTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("Work").create();
      database.getSchema().buildEdgeType().withName("Cite").withLightweight(true).create();
      database.getSchema().buildEdgeType().withName("Wrote").create();
    });
  }

  @Test
  void aLightweightEdgeTypeIsScannedThroughTheVerticesThatHoldIt() {
    final RID[] works = newWorks(3);
    connect("Cite", works[0], works[1]);
    connect("Cite", works[0], works[2]);
    connect("Cite", works[1], works[2]);

    final List<Result> edges = query("select from Cite");
    assertThat(edges).hasSize(3);
    for (final Result edge : edges) {
      assertThat(edge.isEdge()).isTrue();
      assertThat(edge.getEdge().get().getTypeName()).isEqualTo("Cite");
    }

    assertThat(pairs("select from Cite")).containsExactlyInAnyOrder(
        works[0] + "->" + works[1], works[0] + "->" + works[2], works[1] + "->" + works[2]);
  }

  @Test
  void theCountAgreesWithTheScan() {
    final RID[] works = newWorks(3);
    connect("Cite", works[0], works[1]);
    connect("Cite", works[0], works[2]);

    // The count push-down reads countType(), which counts records: it has to decline on a type that keeps none, or
    // it answers 0 for a scan that returns 2.
    assertThat(query("select count(*) as c from Cite").getFirst().<Long>getProperty("c")).isEqualTo(2L);
    assertThat(query("select from Cite")).hasSize(2);
    // ...while the record count of the type itself is still, correctly, zero
    assertThat(database.countType("Cite", true)).isZero();
  }

  /**
   * The target of a count can be a context variable, resolved only at execution time - so the check cannot live in
   * the planner, where the name is still {@code $t}. It sits in {@link CountFromTypeStep} instead, and this pins
   * that the indirection does not reopen the issue one step away from the case above (issue #7477).
   */
  @Test
  void theCountAgreesWithTheScanThroughAVariableTargetToo() {
    final RID[] works = newWorks(3);
    connect("Cite", works[0], works[1]);
    connect("Cite", works[1], works[2]);

    assertThat(varTargetQuery("SELECT count(*) as c FROM $t").getFirst().<Long>getProperty("c")).isEqualTo(2L);
    assertThat(varTargetQuery("SELECT FROM $t")).hasSize(2);

    // ...and a regular edge type reached the same way is unaffected
    connect("Wrote", works[0], works[2]);
    assertThat(varTargetQuery("SELECT count(*) as c FROM $t", "Wrote").getFirst().<Long>getProperty("c")).isEqualTo(1L);
  }

  /** SKIP/LIMIT and the page boundary of the step's own {@code nRecords} batching. */
  @Test
  void theScanPaginates() {
    final RID[] works = newWorks(20);
    for (int i = 1; i < works.length; i++)
      connect("Cite", works[0], works[i]);

    assertThat(query("select from Cite")).hasSize(19);
    assertThat(query("select from Cite limit 5")).hasSize(5);
    assertThat(query("select from Cite skip 15")).hasSize(4);
  }

  /** A regular edge type keeps the bucket scan: its edges are records, and there is no vertex walk to pay for. */
  @Test
  void aRegularEdgeTypeIsUnchanged() {
    final RID[] works = newWorks(2);
    connect("Wrote", works[0], works[1]);

    assertThat(query("select from Wrote")).hasSize(1);
    assertThat(query("select count(*) as c from Wrote").getFirst().<Long>getProperty("c")).isEqualTo(1L);
    assertThat(explain("select from Wrote")).contains("FETCH FROM TYPE Wrote");
  }

  /**
   * A supertype that holds records with a lightweight subtype under it: the scan must return both shapes, and each
   * edge exactly once. The record edges come from the bucket, the lightweight ones from the vertex walk, and the
   * walk tells them apart by the storage shape of the entry rather than by what its type declares.
   */
  @Test
  void aMixedHierarchyReturnsEachEdgeOnce() {
    database.transaction(() -> database.getSchema().buildEdgeType().withName("Mentions").create());
    database.transaction(() -> database.getSchema().buildEdgeType().withName("Quotes").withLightweight(true)
        .withSuperType("Mentions").create());

    final RID[] works = newWorks(3);
    connect("Mentions", works[0], works[1]);
    connect("Quotes", works[0], works[2]);
    connect("Quotes", works[1], works[2]);

    assertThat(pairs("select from Mentions")).containsExactlyInAnyOrder(
        works[0] + "->" + works[1], works[0] + "->" + works[2], works[1] + "->" + works[2]);
    assertThat(query("select count(*) as c from Mentions").getFirst().<Long>getProperty("c")).isEqualTo(3L);

    assertThat(pairs("select from Quotes")).containsExactlyInAnyOrder(
        works[0] + "->" + works[2], works[1] + "->" + works[2]);
  }

  /**
   * A {@code @rid} filter can only ever name a record-backed edge - a lightweight one has no addressable identity -
   * so the RID short-circuit is correct on such a type and must keep winning over the walk. It matters on a mixed
   * hierarchy, where the record half is the only half a RID can reach and the walk would otherwise be paid in full
   * to answer a single-row lookup (issue #7477, and the RID short-circuit of #5824).
   */
  @Test
  void theRidShortCircuitStillWinsOnAMixedHierarchy() {
    database.transaction(() -> database.getSchema().buildEdgeType().withName("Mentions").create());
    database.transaction(() -> database.getSchema().buildEdgeType().withName("Quotes").withLightweight(true)
        .withSuperType("Mentions").create());

    final RID[] works = newWorks(3);
    connect("Mentions", works[0], works[1]);
    connect("Quotes", works[0], works[2]);

    final RID recordEdge = query("select from Mentions").stream()
        .map(r -> r.getEdge().get().getIdentity())
        .filter(rid -> rid.getPosition() >= 0)
        .findFirst().orElseThrow();

    assertThat(explain("select from Mentions where @rid = " + recordEdge))
        .as("the walk must not be paid to answer a single-row RID lookup")
        .doesNotContain("FETCH LIGHTWEIGHT EDGES OF TYPE");
    assertThat(query("select from Mentions where @rid = " + recordEdge)).hasSize(1);

    // ...and the unfiltered scan of the same type still returns both shapes
    assertThat(query("select from Mentions")).hasSize(2);
  }

  /**
   * The rewrite that reaches the edges of ONE vertex is both cheaper and already correct for a lightweight type, so
   * it must keep winning over the whole-graph walk.
   */
  @Test
  void theVertexRidRewriteStillWins() {
    final RID[] works = newWorks(3);
    connect("Cite", works[0], works[1]);
    connect("Cite", works[0], works[2]);
    connect("Cite", works[1], works[2]);

    assertThat(explain("select from Cite where @out = " + works[0])).contains("FETCH EDGES FROM VERTEX");
    assertThat(query("select from Cite where @out = " + works[0])).hasSize(2);
    assertThat(query("select from Cite where @in = " + works[2])).hasSize(2);
  }

  /**
   * The whole design rests on the outgoing entry always being written while the incoming one is the optional half,
   * so walking OUT alone is complete and yields each edge exactly once. A non-bidirectional LIGHTWEIGHT type is the
   * combination that would break if that ever stopped being true, and it is the one no other test constructs
   * (issue #7477).
   */
  @Test
  void aNonBidirectionalLightweightTypeIsWalkedCompletelyAndOnce() {
    database.transaction(() -> database.getSchema().buildEdgeType().withName("Follows").withLightweight(true)
        .withBidirectional(false).create());

    final RID[] works = newWorks(3);
    connect("Follows", works[0], works[1]);
    connect("Follows", works[0], works[2]);
    connect("Follows", works[1], works[2]);

    database.transaction(() -> assertThat(database.lookupByRID(works[2], true).asVertex()
        .countEdges(Vertex.DIRECTION.IN, "Follows"))
        .as("precondition: nothing was written to the incoming side, so only the OUT walk can find these")
        .isZero());

    assertThat(pairs("select from Follows")).containsExactlyInAnyOrder(
        works[0] + "->" + works[1], works[0] + "->" + works[2], works[1] + "->" + works[2]);
    assertThat(query("select count(*) as c from Follows").getFirst().<Long>getProperty("c")).isEqualTo(3L);
  }

  /**
   * TRAVERSE is not routed to the walk (issue #7480 - its dedup keys on {@code (bucketId, position)}, which every
   * lightweight edge of a type shares, so routing alone would hand back one edge of three). Until that is fixed it
   * refuses rather than reporting a clean empty traversal: answering zero rows for data the graph holds is the
   * defect this issue is about, and it would read as "this type has no edges" to someone who just watched SELECT
   * prove it does.
   */
  @Test
  void traverseOnALightweightEdgeTypeRefusesRatherThanAnsweringNothing() {
    final RID[] works = newWorks(3);
    connect("Cite", works[0], works[1]);
    connect("Cite", works[0], works[2]);
    connect("Cite", works[1], works[2]);

    assertThat(query("select from Cite")).as("precondition: the SELECT side is fixed").hasSize(3);

    assertThatThrownBy(() -> database.transaction(
        () -> database.query("sql", "traverse in, out from Cite").hasNext()))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("LIGHTWEIGHT")
        .hasMessageContaining("#7480");

    // a regular edge type is untouched
    connect("Wrote", works[0], works[2]);
    assertThat(query("traverse in, out from Wrote while $depth < 1")).hasSize(1);
  }

  /**
   * The walk opens every vertex type in the schema, not only the ones the edge type connects - nothing records
   * which vertex types an edge type has endpoints in. That is the documented cost, and this pins the correctness
   * half of it: an unrelated vertex type, including one carrying edges of a different type, changes neither what
   * the scan returns nor what the count says.
   */
  @Test
  void anUnrelatedVertexTypeIsWalkedWithoutDisturbingTheResult() {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("Author").create();
      database.getSchema().buildEdgeType().withName("Knows").withLightweight(true).create();
    });

    final RID[] works = newWorks(2);
    connect("Cite", works[0], works[1]);

    final RID[] authors = new RID[2];
    database.transaction(() -> {
      for (int i = 0; i < authors.length; i++)
        authors[i] = database.newVertex("Author").set("id", i).save().getIdentity();
    });
    connect("Knows", authors[0], authors[1]);

    assertThat(pairs("select from Cite")).containsExactly(works[0] + "->" + works[1]);
    assertThat(query("select count(*) as c from Cite").getFirst().<Long>getProperty("c")).isEqualTo(1L);

    // ...and symmetrically, from the other type's point of view
    assertThat(pairs("select from Knows")).containsExactly(authors[0] + "->" + authors[1]);
    assertThat(query("select count(*) as c from Knows").getFirst().<Long>getProperty("c")).isEqualTo(1L);
  }

  /**
   * UPDATE over a MIXED hierarchy reaches both shapes, so it modifies the record-backed edges and then refuses at
   * the first lightweight one. The statement is therefore not atomic on its own - it is the transaction that makes
   * it so, which is the ordinary ArcadeDB contract and is what this pins: the refusal names the reason, and the
   * rollback leaves the record half as it was rather than half-updated.
   */
  @Test
  void updateOverAMixedHierarchyRefusesAndRollsBackWholly() {
    database.transaction(() -> database.getSchema().buildEdgeType().withName("Mentions").create());
    database.transaction(() -> database.getSchema().buildEdgeType().withName("Quotes").withLightweight(true)
        .withSuperType("Mentions").create());

    final RID[] works = newWorks(3);
    connect("Mentions", works[0], works[1]);
    connect("Quotes", works[0], works[2]);

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "update Mentions set since = 2020").close()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Lightweight edges cannot be modified");

    // the record-backed half was modified before the refusal, and the transaction took it back with everything else
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "select since from Mentions")) {
        while (rs.hasNext())
          assertThat(rs.next().<Object>getProperty("since"))
              .as("no edge of a mixed hierarchy may survive a refused UPDATE half-written").isNull();
      }
    });

    // ...and the scan itself is unharmed
    assertThat(query("select from Mentions")).hasSize(2);
  }

  /** The walk is the plan, so EXPLAIN has to name it: it is O(V + E) where a bucket scan reads O(E). */
  @Test
  void explainNamesTheVertexWalk() {
    assertThat(explain("select from Cite")).contains("FETCH LIGHTWEIGHT EDGES OF TYPE Cite");
  }

  @Test
  void anEmptyGraphStillAnswersEmpty() {
    assertThat(query("select from Cite")).isEmpty();
    assertThat(query("select count(*) as c from Cite").getFirst().<Long>getProperty("c")).isZero();
  }

  /** A filter on the endpoints is applied on top of the walk, not lost by it. */
  @Test
  void aFilterOverTheWalkIsApplied() {
    final RID[] works = newWorks(3);
    connect("Cite", works[0], works[1]);
    connect("Cite", works[1], works[2]);

    assertThat(pairs("select from Cite where @in = " + works[2] + " or @in = " + works[1]))
        .containsExactlyInAnyOrder(works[0] + "->" + works[1], works[1] + "->" + works[2]);
  }

  /**
   * DELETE and UPDATE resolve their target through the same planner, so both used to address an empty bucket and
   * silently do nothing on a lightweight type. Both now reach the edges: the delete removes them from BOTH vertices'
   * edge lists, and the update refuses with the sentence that says why rather than reporting a no-op as a success.
   */
  @Test
  void deleteReachesTheEdgesAndUpdateSaysWhyItCannot() {
    final RID[] works = newWorks(3);
    connect("Cite", works[0], works[1]);
    connect("Cite", works[1], works[2]);

    assertThat(query("delete from Cite where @in = " + works[2]).getFirst().<Long>getProperty("count")).isEqualTo(1L);

    database.transaction(() -> {
      assertThat(database.lookupByRID(works[1], true).asVertex().countEdges(Vertex.DIRECTION.OUT, "Cite"))
          .as("the deleted edge must be gone from the source vertex's edge list").isZero();
      assertThat(database.lookupByRID(works[2], true).asVertex().countEdges(Vertex.DIRECTION.IN, "Cite"))
          .as("...and from the destination's").isZero();
    });

    assertThat(query("select from Cite")).hasSize(1);

    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "update Cite set since = 2020").close()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Lightweight edges cannot be modified");
  }

  private RID[] newWorks(final int count) {
    final RID[] rids = new RID[count];
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        rids[i] = database.newVertex("Work").set("id", i).save().getIdentity();
    });
    return rids;
  }

  private void connect(final String edgeType, final RID from, final RID to) {
    database.transaction(() -> database.lookupByRID(from, true).asVertex().modify().newEdge(edgeType, to));
  }

  private List<Result> query(final String sql) {
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", sql)) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });
    return results;
  }

  private List<String> pairs(final String sql) {
    final List<String> pairs = new ArrayList<>();
    for (final Result r : query(sql))
      pairs.add(r.getEdge().get().getOut() + "->" + r.getEdge().get().getIn());
    return pairs;
  }

  private List<Result> varTargetQuery(final String sql) {
    return varTargetQuery(sql, "Cite");
  }

  /** Runs {@code sql} with {@code $t} bound to {@code typeName}, the way {@code SELECT FROM $t} is driven. */
  private List<Result> varTargetQuery(final String sql, final String typeName) {
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      final SelectStatement statement = (SelectStatement) ((DatabaseInternal) database).getStatementCache().get(sql);
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      context.setVariable("$t", typeName);
      try (final ResultSet rs = new LocalResultSet(statement.createExecutionPlan(context))) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });
    return results;
  }

  private String explain(final String sql) {
    final StringBuilder plan = new StringBuilder();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "explain " + sql)) {
        while (rs.hasNext())
          plan.append(rs.next().toJSON());
      }
    });
    return plan.toString();
  }
}
