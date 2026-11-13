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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cypher lookups through relationship (edge) indexes.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherEdgeIndexTest {
  /**
   * Reproduction for GitHub issue #740 (Locstat).
   * <p>
   * OpenCypher queries that filter on an edge type's indexed properties via a WHERE clause do not use
   * the composite index on the edge type - they fall back to a full vertex scan + edge expansion, while
   * the equivalent SQL query uses the index.
   * <pre>
   *   MATCH ()-[t:TRANSFER]->()
   *   WHERE t.transactionId='74529884' and t.date=date('2026-02-28')
   *   RETURN t
   * </pre>
   */
  @Nested
  class Issue740EdgeIndexWhereClause {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-740-edge-index-where").create();

      database.getSchema().createVertexType("Account");
      final var transfer = database.getSchema().createEdgeType("TRANSFER");
      transfer.createProperty("transactionId", Type.STRING);
      transfer.createProperty("date", Type.DATE);
      // Composite index on the edge type, mirroring the client's TRANSFER_PK on (transactionId, date).
      database.getSchema().buildTypeIndex("TRANSFER", new String[] { "transactionId", "date" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Account").set("name", "A").save();
        final MutableVertex b = database.newVertex("Account").set("name", "B").save();

        // The one edge the client expects to find.
        a.newEdge("TRANSFER", b).set("transactionId", "74529884").set("date", "2026-02-28").save();

        // A pile of noise edges between the same two accounts and other accounts, so a full scan is
        // clearly distinguishable from a single-key index lookup by the profiled row counts.
        for (int i = 0; i < 500; i++) {
          final MutableVertex c = database.newVertex("Account").set("name", "N" + i).save();
          // %02d: a single-digit day ("2026-01-1") is not a valid ISO date, and until issue #8090 it was silently
          // stored as NULL instead of being refused, so a third of this noise carried no date at all.
          a.newEdge("TRANSFER", c).set("transactionId", "T" + i).set("date", "2026-01-%02d".formatted(1 + (i % 28))).save();
          c.newEdge("TRANSFER", b).set("transactionId", "S" + i).set("date", "2026-03-%02d".formatted(1 + (i % 28))).save();
        }
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void whereClauseOnEdgeReturnsCorrectResult() {
      // Correctness first: the query must return exactly the one matching edge.
      try (final ResultSet rs = database.query("opencypher",
          "MATCH ()-[t:TRANSFER]->() WHERE t.transactionId='74529884' AND t.date=date('2026-02-28') RETURN t")) {
        int count = 0;
        while (rs.hasNext()) {
          final Result r = rs.next();
          assertThat((String) ((Edge) r.getProperty("t")).get("transactionId")).isEqualTo("74529884");
          count++;
        }
        assertThat(count).isEqualTo(1);
      }
    }

    @Test
    void whereClauseOnEdgeUsesIndex() {
      final ResultSet rs = database.query("opencypher",
          "PROFILE MATCH ()-[t:TRANSFER]->() WHERE t.transactionId='74529884' AND t.date=date('2026-02-28') RETURN t");
      while (rs.hasNext())
        rs.next();
      final ExecutionPlan plan = rs.getExecutionPlan().get();
      final String planString = plan.prettyPrint(0, 2);
      rs.close();

      // The plan must be driven from the edge type's index, not a full vertex scan + edge expansion.
      assertThat(planString).contains("MATCH EDGE BY INDEX");
      assertThat(planString).contains("index: ");
      // And it must NOT fall back to expanding every edge (the pre-fix behaviour showed a MATCH
      // RELATIONSHIP scanning 1001 edges).
      assertThat(planString).doesNotContain("traversal: standard");
    }

    @Test
    void inlineDirectionsAndCorrectnessMatchWhereForm() {
      // Sanity across the shapes the optimization must not silently change: the incoming-direction form
      // must return the same single edge (found from its IN endpoint) as the SQL reference.
      try (final ResultSet rs = database.query("opencypher",
          "MATCH ()<-[t:TRANSFER]-() WHERE t.transactionId='74529884' AND t.date=date('2026-02-28') RETURN t")) {
        int count = 0;
        while (rs.hasNext()) {
          assertThat((String) ((Edge) rs.next().getProperty("t")).get("transactionId")).isEqualTo("74529884");
          count++;
        }
        assertThat(count).isEqualTo(1);
      }
    }

    @Test
    void parameterizedValuesUseIndexAndReturnCorrectResult() {
      // Parameters are row-independent, so they must drive the same index seek as literals.
      final java.util.Map<String, Object> params = new java.util.HashMap<>();
      params.put("tx", "74529884");
      try (final ResultSet rs = database.query("opencypher",
          "MATCH ()-[t:TRANSFER]->() WHERE t.transactionId=$tx AND t.date=date('2026-02-28') RETURN t", params)) {
        int count = 0;
        while (rs.hasNext()) {
          assertThat((String) ((Edge) rs.next().getProperty("t")).get("transactionId")).isEqualTo("74529884");
          count++;
        }
        assertThat(count).isEqualTo(1);
      }
    }

    @Test
    void unindexedEdgePropertyStillReturnsCorrectResultViaFallback() {
      // A filter on a non-indexed edge property cannot use the seek and must fall back to the normal
      // scan+expand plan, still returning the correct rows. TRANSFER has no index on 'note'.
      database.getSchema().getType("TRANSFER").createProperty("note", Type.STRING);
      // tag the one known edge with a note
      database.transaction(() ->
          database.command("sql", "UPDATE TRANSFER SET note='flagged' WHERE transactionId='74529884'"));

      final ResultSet rs = database.query("opencypher",
          "PROFILE MATCH ()-[t:TRANSFER]->() WHERE t.note='flagged' RETURN t");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      final String planString = rs.getExecutionPlan().get().prettyPrint(0, 2);
      rs.close();
      assertThat(count).isEqualTo(1);
      // No edge index covers 'note', so the edge-index seed must NOT be used.
      assertThat(planString).doesNotContain("MATCH EDGE BY INDEX");
    }

    @Test
    void sqlEquivalentUsesIndexForReference() {
      // Reference: the equivalent SQL query DOES use the edge-type index.
      final ResultSet rs = database.query("sql",
          "SELECT FROM TRANSFER WHERE transactionId='74529884' AND date='2026-02-28'");
      final ExecutionPlan plan = rs.getExecutionPlan().get();
      final String planString = plan.prettyPrint(0, 2);
      rs.close();

      assertThat(planString.toUpperCase()).contains("INDEX");
    }
  }

  /**
   * Follow-up to #740/#741 (Locstat): the OpenCypher edge-index seed introduced for
   * {@code MATCH ()-[t:TRANSFER]->() WHERE t.transactionId = ... AND t.date = ...} only fired when equality
   * predicates covered the <em>whole</em> index key. The same client's next two queries - the leading column
   * alone, and an {@code IN} list on it - still scanned every edge, while their SQL twins used the index:
   * <pre>
   *   MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN ['S28719720', '74529884'] RETURN t
   *   MATCH ()-[t:TRANSFER]->() WHERE t.transactionId = 'S28719720' OR t.transactionId = '74529884' RETURN t
   * </pre>
   * The seed now accepts a leading prefix of the key (walked as a range of the ordered index, as the vertex
   * {@code NodeIndexSeek} already did) and an {@code IN} list or an {@code OR} of equalities on a column (one seek
   * per value, results de-duplicated). The MATCH's WHERE filter is still re-applied above the seed, so every
   * shape here is checked for the exact rows it returns, not only for the plan it takes.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue741EdgeIndexInListAndPrefix extends TestHelper {
    private static final int NOISE = 200;

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Account").createProperty("accountNumber", Type.STRING);
        final EdgeType transfer = database.getSchema().createEdgeType("TRANSFER");
        transfer.createProperty("transactionId", Type.STRING);
        transfer.createProperty("date", Type.DATE);
        database.getSchema().buildTypeIndex("TRANSFER", new String[] { "transactionId", "date" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withIndexName("TRANSFER_PK").create();
      });

      database.transaction(() -> {
        final var a1 = database.newVertex("Account").set("accountNumber", "A1").save();
        final var a2 = database.newVertex("Account").set("accountNumber", "A2").save();
        a1.newEdge("TRANSFER", a2).set("transactionId", "S28719720", "date", "2026-01-01").save();
        a1.newEdge("TRANSFER", a2).set("transactionId", "74529884", "date", "2026-02-28").save();
        a1.newEdge("TRANSFER", a2).set("transactionId", "74529884", "date", "2026-02-27").save();
        for (int i = 0; i < NOISE; i++) {
          final var c = database.newVertex("Account").set("accountNumber", "N" + i).save();
          a1.newEdge("TRANSFER", c).set("transactionId", "NOISE-" + i, "date", "2026-03-%02d".formatted(i % 28 + 1)).save();
        }
      });
    }

    @Test
    void inListOnLeadingColumnUsesTheIndexAndReturnsExactlyTheListedEdges() {
      assertSeeksTheIndex("MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN ['S28719720', '74529884'] RETURN t",
          Map.of(), "74529884/2026-02-27", "74529884/2026-02-28", "S28719720/2026-01-01");
    }

    @Test
    void orOfEqualitiesOnTheSameColumnSeeksLikeAnInList() {
      assertSeeksTheIndex("MATCH ()-[t:TRANSFER]->() WHERE t.transactionId = 'S28719720' OR t.transactionId = '74529884' RETURN t",
          Map.of(), "74529884/2026-02-27", "74529884/2026-02-28", "S28719720/2026-01-01");
    }

    @Test
    void equalityOnTheLeadingColumnAloneWalksThePrefixRange() {
      assertSeeksTheIndex("MATCH ()-[t:TRANSFER]->() WHERE t.transactionId = '74529884' RETURN t",
          Map.of(), "74529884/2026-02-27", "74529884/2026-02-28");
    }

    @Test
    void inListOnTheLeadingColumnAndEqualityOnTheTrailingOneSeekTheWholeKey() {
      assertSeeksTheIndex(
          "MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN ['S28719720', '74529884'] AND t.date = date('2026-02-28') RETURN t",
          Map.of(), "74529884/2026-02-28");
    }

    @Test
    void parameterListSeeksOneKeyPerElement() {
      assertSeeksTheIndex("MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN $ids RETURN t",
          Map.of("ids", List.of("74529884", "S28719720", "MISSING")), "74529884/2026-02-27", "74529884/2026-02-28",
          "S28719720/2026-01-01");
    }

    @Test
    void repeatedListValuesReturnEachEdgeOnce() {
      assertSeeksTheIndex("MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN ['74529884', '74529884', 'S28719720', 'S28719720'] RETURN t",
          Map.of(), "74529884/2026-02-27", "74529884/2026-02-28", "S28719720/2026-01-01");
    }

    @Test
    void incomingDirectionBindsTheEndpointsTheWrittenWay() {
      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher",
            "PROFILE MATCH (a)<-[t:TRANSFER]-(b) WHERE t.transactionId IN ['S28719720', '74529884'] RETURN a.accountNumber AS a, b.accountNumber AS b");
        final List<String> rows = new ArrayList<>();
        while (rs.hasNext()) {
          final var r = rs.next();
          rows.add(r.getProperty("a") + "<-" + r.getProperty("b"));
        }
        assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("MATCH EDGE BY INDEX");
        assertThat(rows).containsExactlyInAnyOrder("A2<-A1", "A2<-A1", "A2<-A1");
      });
    }

    @Test
    void orAcrossDifferentPropertiesCannotSeedTheSeekAndStillAnswersCorrectly() {
      // The OR pins transactionId on one side only: seeking it would lose every row the other side admits.
      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher",
            "PROFILE MATCH ()-[t:TRANSFER]->() WHERE t.transactionId = 'S28719720' OR t.date = date('2026-02-28') RETURN t");
        final List<String> rows = collect(rs);
        assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain("MATCH EDGE BY INDEX");
        assertThat(rows).containsExactlyInAnyOrder("74529884/2026-02-28", "S28719720/2026-01-01");
      });
    }

    @Test
    void inListOnTheTrailingColumnAloneIsNotAPrefixAndFallsBack() {
      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher",
            "PROFILE MATCH ()-[t:TRANSFER]->() WHERE t.date IN [date('2026-02-28'), date('2026-01-01')] RETURN t");
        final List<String> rows = collect(rs);
        assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain("MATCH EDGE BY INDEX");
        assertThat(rows).containsExactlyInAnyOrder("74529884/2026-02-28", "S28719720/2026-01-01");
      });
    }

    @Test
    void emptyListAndUnboundParameterYieldNoRows() {
      database.transaction(() -> {
        assertThat(collect(database.query("opencypher", "MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN [] RETURN t"))).isEmpty();
        assertThat(collect(database.query("opencypher", "MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN $ids RETURN t",
            Map.of("ids", List.of())))).isEmpty();
      });
    }

    @Test
    void seekReadsOnlyTheListedKeysNotEveryEdge() {
      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher",
            "PROFILE MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN ['S28719720', '74529884'] RETURN t");
        collect(rs);
        final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
        // The profiled seed reports the rows it pulled from the index: the three listed edges, not the NOISE ones.
        assertThat(plan).contains("MATCH EDGE BY INDEX").contains("TRANSFER_PK on transactionId,date");
        assertThat(plan).contains("transactionId IN ['S28719720', '74529884']");
        assertThat(plan).doesNotContain("MATCH RELATIONSHIP");
      });
    }

    private void assertSeeksTheIndex(final String query, final Map<String, Object> params, final String... expectedRows) {
      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher", "PROFILE " + query, params);
        final List<String> rows = collect(rs);
        assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("MATCH EDGE BY INDEX").contains("TRANSFER_PK");
        assertThat(rows).containsExactlyInAnyOrder(expectedRows);
      });
    }

    private static List<String> collect(final ResultSet rs) {
      final List<String> rows = new ArrayList<>();
      while (rs.hasNext()) {
        final Edge edge = rs.next().getProperty("t");
        rows.add(edge.get("transactionId") + "/" + edge.get("date"));
      }
      rs.close();
      return rows;
    }
  }

  /**
   * Reproduction for GitHub issue #7008.
   * <p>
   * The edge-index scan seed introduced for issue #740 accepts a single-hop pattern without noticing that
   * the two endpoint variables may be the <em>same</em> variable. For the explicit self-loop shape
   * {@code (a)-[t:TRANSFER]->(a)} the seed bound the edge's OUT vertex to {@code a} and then immediately
   * overwrote it with the IN vertex, so the pattern's implicit {@code out == in} constraint was silently
   * dropped and edges between two different vertices came back as if they were self-loops.
   */
  @Nested
  class Issue7008EdgeIndexSelfLoop {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-7008-edge-index-self-loop").create();

      database.getSchema().createVertexType("Account");
      final var transfer = database.getSchema().createEdgeType("TRANSFER");
      transfer.createProperty("transactionId", Type.STRING);
      transfer.createProperty("date", Type.DATE);
      database.getSchema().buildTypeIndex("TRANSFER", new String[] { "transactionId" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
      // A second, composite index. The seed picks the longest index its equality predicates fully cover, so a
      // WHERE on transactionId alone still seeks the single-property one and a WHERE on both seeks this one.
      database.getSchema().buildTypeIndex("TRANSFER", new String[] { "transactionId", "date" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Account").set("name", "A").save();
        final MutableVertex b = database.newVertex("Account").set("name", "B").save();
        final MutableVertex c = database.newVertex("Account").set("name", "C").save();

        // A plain edge between two DIFFERENT accounts: no self-loop shape must ever match it.
        a.newEdge("TRANSFER", b).set("transactionId", "74529884").set("date", "2026-02-28").save();
        // A genuine self-loop on C: the self-loop shape must find exactly this one.
        c.newEdge("TRANSFER", c).set("transactionId", "11112222").set("date", "2026-02-28").save();
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void repeatedEndpointVariableDoesNotMatchANonSelfLoopEdge() {
      // The edge carrying '74529884' runs A -> B, so the self-loop pattern must return nothing.
      assertThat(namesOf("MATCH (a)-[t:TRANSFER]->(a) WHERE t.transactionId = '74529884' RETURN a.name AS n"))
          .isEmpty();
    }

    @Test
    void controlDistinctEndpointVariablesStillSeeTheNonSelfLoopEdge() {
      // Control for the assertion above: with two distinct variables the same edge IS matched, and its
      // endpoints really are two different accounts.
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (x)-[t:TRANSFER]->(y) WHERE t.transactionId = '74529884' RETURN x.name AS xn, y.name AS yn")) {
        assertThat(rs.hasNext()).isTrue();
        final Result r = rs.next();
        assertThat((String) r.getProperty("xn")).isEqualTo("A");
        assertThat((String) r.getProperty("yn")).isEqualTo("B");
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void repeatedEndpointVariableFindsAGenuineSelfLoop() {
      // The fix must not simply refuse every repeated-variable pattern: the real self-loop is still found.
      assertThat(namesOf("MATCH (a)-[t:TRANSFER]->(a) WHERE t.transactionId = '11112222' RETURN a.name AS n"))
          .containsExactly("C");
    }

    @Test
    void repeatedEndpointVariableHonoursTheIncomingDirectionForm() {
      // The written direction only decides which endpoint binds to which variable; for a repeated variable
      // both forms must behave identically.
      assertThat(namesOf("MATCH (a)<-[t:TRANSFER]-(a) WHERE t.transactionId = '74529884' RETURN a.name AS n"))
          .isEmpty();
      assertThat(namesOf("MATCH (a)<-[t:TRANSFER]-(a) WHERE t.transactionId = '11112222' RETURN a.name AS n"))
          .containsExactly("C");
    }

    @Test
    void repeatedEndpointVariableWithoutAWhereFilterIsStillCorrect() {
      // Without a WHERE on the edge the index seed does not apply at all; this pins the reference semantics
      // the seeded plan has to agree with.
      assertThat(namesOf("MATCH (a)-[t:TRANSFER]->(a) RETURN a.name AS n")).containsExactly("C");
    }

    @Test
    void selfLoopPatternStillUsesTheEdgeIndexSeed() {
      // The self-loop constraint has to be enforced without giving up the index speedup issue #740 added.
      // Pinning the plan shape is what keeps this file honest: the alternative fix - refusing the seed for a
      // repeated variable - would make every assertion above pass while silently losing the optimization.
      assertThat(profiledPlanOf("MATCH (a)-[t:TRANSFER]->(a) WHERE t.transactionId = '11112222' RETURN a.name AS n"))
          .contains("MATCH EDGE BY INDEX");
    }

    @Test
    void selfLoopConstraintSurvivesACompositeIndexSeek() {
      // A multi-column key drives a different branch of the seed's index choice, so the constraint has to hold
      // there too. Assert the composite index really is the one seeded, otherwise this only re-tests the
      // single-property path under a longer WHERE clause.
      final String nonSelfLoop = "MATCH (a)-[t:TRANSFER]->(a) "
          + "WHERE t.transactionId = '74529884' AND t.date = date('2026-02-28') RETURN a.name AS n";
      assertThat(profiledPlanOf(nonSelfLoop)).contains("on transactionId,date");
      assertThat(namesOf(nonSelfLoop)).isEmpty();

      final String selfLoop = "MATCH (a)-[t:TRANSFER]->(a) "
          + "WHERE t.transactionId = '11112222' AND t.date = date('2026-02-28') RETURN a.name AS n";
      assertThat(profiledPlanOf(selfLoop)).contains("on transactionId,date");
      assertThat(namesOf(selfLoop)).containsExactly("C");
    }

    @Test
    void labelledEndpointRoutesToTheOptimizerAndStaysCorrect() {
      // A label on the endpoint makes the cost-based optimizer accept the pattern (ExpandInto BOUND-TARGET)
      // instead of the legacy seed, so this pins the OTHER execution path against the same defect.
      final String nonSelfLoop =
          "MATCH (a:Account)-[t:TRANSFER]->(a) WHERE t.transactionId = '74529884' RETURN a.name AS n";
      assertThat(planOf(nonSelfLoop)).contains("Cost-Based Query Optimizer");
      assertThat(namesOf(nonSelfLoop)).isEmpty();

      final String selfLoop =
          "MATCH (a:Account)-[t:TRANSFER]->(a) WHERE t.transactionId = '11112222' RETURN a.name AS n";
      assertThat(planOf(selfLoop)).contains("Cost-Based Query Optimizer");
      assertThat(namesOf(selfLoop)).containsExactly("C");
    }

    /** Drains a PROFILEd run so the plan reflects what actually executed, not just what was planned. */
    private String profiledPlanOf(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", "PROFILE " + cypher)) {
        while (rs.hasNext())
          rs.next();
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    private String planOf(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + cypher)) {
        assertThat(rs.hasNext()).isTrue();
        return rs.next().getProperty("executionPlanAsString");
      }
    }

    private List<String> namesOf(final String cypher) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext())
          names.add(rs.next().getProperty("n"));
      }
      return names;
    }
  }

  /**
   * Issue #11481: an openCypher lookup of one relationship by an indexed property used the relationship index for one spelling only -
   * a single type, anonymous endpoints and the predicate in a {@code WHERE} clause. An inline property map, a label on either endpoint
   * and a union of types were planned as a scan of every vertex (of the label) and an expansion of every edge. Every spelling now
   * seeks the index, on both planners: the step-by-step one, and the cost-based one that takes labelled endpoints with a {@code WHERE}
   * clause. Each variant is checked for its plan AND for the rows the scan returns.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue11481RelationshipIndexSpellings extends TestHelper {
    private static final int VERTICES = 300;
    private static final int EDGES    = 3_000;

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.command("opencypher", "CREATE INDEX entity_uuid IF NOT EXISTS FOR (n:Entity) ON (n.uuid)");
        database.command("opencypher", "CREATE INDEX relates_uuid IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON (e.uuid)");
        database.command("opencypher", "CREATE INDEX mentions_uuid IF NOT EXISTS FOR ()-[e:MENTIONS]-() ON (e.uuid)");
      });
      database.transaction(() -> database.command("opencypher",
          "UNWIND range(1, $n) AS i CREATE (:Entity {uuid: 'v' + toString(i)})", Map.of("n", VERTICES)));
      database.transaction(() -> database.command("opencypher",
          "UNWIND range(1, $e) AS i MATCH (s:Entity {uuid: 'v' + toString(1 + i % $v)}), (t:Entity {uuid: 'v' + toString(1 + (i * 7 + 1) % $v)}) "
              + "CREATE (s)-[:RELATES_TO {uuid: 'e' + toString(i), fact: 'f' + toString(i % 3)}]->(t)",
          Map.of("e", EDGES, "v", VERTICES)));
      database.transaction(() -> database.command("opencypher",
          "MATCH (s:Entity {uuid: 'v1'}), (t:Entity {uuid: 'v2'}) CREATE (s)-[:MENTIONS {uuid: 'm1'}]->(t)"));
    }

    @Test
    void variantAAnonymousEndsSingleTypeWhere() {
      assertSeeks("MATCH (n)-[e:RELATES_TO]->(m) WHERE e.uuid = $u RETURN e.uuid AS uuid", Map.of("u", "e1500"), "e1500");
    }

    @Test
    void variantBWhereInList() {
      assertSeeks("MATCH (n)-[e:RELATES_TO]->(m) WHERE e.uuid IN $us RETURN e.uuid AS uuid ORDER BY uuid",
          Map.of("us", List.of("e1500", "e1000")), "e1000", "e1500");
    }

    @Test
    void variantCInlineMapOnTheRelationship() {
      assertSeeks("MATCH (n)-[e:RELATES_TO {uuid: $u}]->(m) RETURN e.uuid AS uuid", Map.of("u", "e1500"), "e1500");
    }

    @Test
    void variantDLabelledEndpointsWhereOnTheCostBasedPlan() {
      assertSeeks("MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity) WHERE e.uuid = $u RETURN e.uuid AS uuid", Map.of("u", "e1500"),
          "e1500");

      // The cost-based planner takes this shape, and anchors it on the relationship rather than on a scan of Entity
      final List<String> rows = new ArrayList<>();
      final String plan = run("MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity) WHERE e.uuid = $u RETURN e.uuid AS uuid",
          Map.of("u", "e1500"), rows);
      assertThat(plan).contains("Cost-Based").contains("EdgeIndexSeek").doesNotContain("NodeByLabelScan");

      // A longer pattern goes on from the endpoints the seek bound
      rows.clear();
      final String longer = run("MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity)-[f:RELATES_TO]->(o:Entity) WHERE e.uuid = $u "
          + "RETURN f.uuid AS uuid ORDER BY uuid", Map.of("u", "e10"), rows);
      assertThat(longer).contains("EdgeIndexSeek");
      // e10 reaches v72, whose outgoing edges are the i with 1 + i % 300 = 72
      final List<String> expected = new ArrayList<>();
      for (int i = 71; i <= EDGES; i += VERTICES)
        expected.add("e" + i);
      expected.sort(null);
      assertThat(rows).containsExactlyElementsOf(expected);
    }

    @Test
    void variantELabelledEndpointsAndInlineMap() {
      assertSeeks("MATCH (n:Entity)-[e:RELATES_TO {uuid: $u}]->(m:Entity) RETURN e.uuid AS uuid", Map.of("u", "e1500"), "e1500");
    }

    @Test
    void variantFOneLabelledEndpointWhere() {
      assertSeeks("MATCH (n:Entity)-[e:RELATES_TO]->(m) WHERE e.uuid = $u RETURN e.uuid AS uuid", Map.of("u", "e1500"), "e1500");
    }

    @Test
    void variantGTypeUnionWhere() {
      assertSeeks("MATCH (n)-[e:MENTIONS|RELATES_TO]->(m) WHERE e.uuid = $u RETURN e.uuid AS uuid", Map.of("u", "e1500"), "e1500");
      assertSeeks("MATCH (n)-[e:MENTIONS|RELATES_TO]->(m) WHERE e.uuid = $u RETURN e.uuid AS uuid", Map.of("u", "m1"), "m1");
    }

    @Test
    void variantHTypeUnionAndInlineMapDeletes() {
      assertSeeks("MATCH (n)-[e:MENTIONS|RELATES_TO {uuid: $u}]->(m) RETURN e.uuid AS uuid", Map.of("u", "m1"), "m1");

      // The delete shape of the report: the edge goes, nothing else does
      database.transaction(() -> database.command("opencypher", "MATCH (n)-[e:MENTIONS|RELATES_TO {uuid: $u}]->(m) DELETE e",
          Map.of("u", "e42")));
      assertThat(count("MATCH ()-[e:RELATES_TO]->() RETURN count(e) AS c")).isEqualTo(EDGES - 1);
      assertThat(count("MATCH ()-[e:RELATES_TO {uuid: 'e42'}]->() RETURN count(e) AS c")).isZero();
    }

    @Test
    void theNestedParameterShapeOfIssue11480SeeksTheRelationshipIndexToo() {
      assertSeeks("MATCH ()-[e:RELATES_TO {uuid: $d.u}]->() RETURN e.uuid AS uuid", Map.of("d", Map.of("u", "e7")), "e7");
    }

    @Test
    void theInlineMapStillFiltersOnTheEntriesTheIndexDoesNotHold() {
      // e1500 has fact f0 (1500 % 3): a fact that does not match leaves no row, as the scan does
      assertSeeks("MATCH (n)-[e:RELATES_TO {uuid: $u, fact: 'f0'}]->(m) RETURN e.uuid AS uuid", Map.of("u", "e1500"), "e1500");
      assertSeeks("MATCH (n)-[e:RELATES_TO {uuid: $u, fact: 'f1'}]->(m) RETURN e.uuid AS uuid", Map.of("u", "e1500"));
    }

    @Test
    void anEndpointLabelThatDoesNotMatchLeavesNoRow() {
      database.transaction(() -> database.command("opencypher",
          "MATCH (s:Entity {uuid: 'v1'}) CREATE (s)-[:RELATES_TO {uuid: 'x1'}]->(:Other {uuid: 'o1'})"));
      assertSeeks("MATCH (n:Entity)-[e:RELATES_TO {uuid: 'x1'}]->(m:Entity) RETURN e.uuid AS uuid", Map.of());
      assertSeeks("MATCH (n:Entity)-[e:RELATES_TO {uuid: 'x1'}]->(m:Other) RETURN m.uuid AS uuid", Map.of(), "o1");
      assertSeeks("MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity|Other) WHERE e.uuid = 'x1' RETURN m.uuid AS uuid", Map.of(), "o1");
    }

    @Test
    void theIncomingDirectionBindsTheEndpointsTheWrittenWay() {
      final List<String> rows = new ArrayList<>();
      final String plan = run("MATCH (m:Entity)<-[e:RELATES_TO {uuid: 'e10'}]-(n:Entity) RETURN n.uuid + '->' + m.uuid AS uuid",
          Map.of(), rows);
      assertThat(plan).contains("MATCH EDGE BY INDEX");
      // e10 goes from v(1 + 10 % 300) = v11 to v(1 + 71 % 300) = v72
      assertThat(rows).containsExactly("v11->v72");
    }

    @Test
    void aTypeOfTheUnionWithNoIndexKeepsTheScan() {
      database.transaction(() -> database.command("opencypher",
          "MATCH (s:Entity {uuid: 'v1'}), (t:Entity {uuid: 'v3'}) CREATE (s)-[:NOINDEX {uuid: 'e1500'}]->(t)"));
      final List<String> rows = new ArrayList<>();
      final String plan = run("MATCH (n)-[e:NOINDEX|RELATES_TO]->(m) WHERE e.uuid = $u RETURN type(e) AS uuid ORDER BY uuid",
          Map.of("u", "e1500"), rows);
      assertThat(plan).doesNotContain("MATCH EDGE BY INDEX");
      assertThat(rows).containsExactly("NOINDEX", "RELATES_TO");
    }

    @Test
    void aSubtypeNamedWithItsParentIsReturnedOnce() {
      database.transaction(() -> {
        database.getSchema().createEdgeType("RELATES_TO_CHILD").addSuperType("RELATES_TO");
        database.command("opencypher",
            "MATCH (s:Entity {uuid: 'v1'}), (t:Entity {uuid: 'v3'}) CREATE (s)-[:RELATES_TO_CHILD {uuid: 'c1'}]->(t)");
      });
      assertSeeks("MATCH (n)-[e:RELATES_TO|RELATES_TO_CHILD {uuid: 'c1'}]->(m) RETURN type(e) AS uuid", Map.of(), "RELATES_TO_CHILD");
    }

    @Test
    void aSelectiveEndpointOfANonUniqueSeekStillAnchorsTheNodeIndex() {
      // The endpoint pinned by its index is one vertex: expanding its edges beats a seek of a third of the edges
      database.command("opencypher", "CREATE INDEX relates_fact IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON (e.fact)");
      final List<String> rows = new ArrayList<>();
      final String plan = run("MATCH (n:Entity)-[e:RELATES_TO]->(m) WHERE n.uuid = 'v11' AND e.fact = 'f1' RETURN e.uuid AS uuid ORDER BY uuid",
          Map.of(), rows);
      assertThat(plan).doesNotContain("MATCH EDGE BY INDEX");
      assertThat(rows).isNotEmpty();
    }

    @Test
    void aViewBackedChainAfterTheSeekKeepsTheRelationshipTheSeekBound() throws InterruptedException {
      // The seek binds e below two hops a graph analytical view walks: fused into one operator, those hops would emit only their own
      // variables, and e.uuid would read null
      database.transaction(() -> {
        database.command("opencypher", "UNWIND range(1, 60) AS i CREATE (:P {name: 'p' + toString(i)})");
        database.command("opencypher",
            "MATCH (a:P {name: 'p1'}), (b:P {name: 'p2'}), (c:P {name: 'p3'}), (d:P {name: 'p4'}) "
                + "CREATE (a)-[:KNOWS {uuid: 'k1'}]->(b)-[:FOLLOWS]->(c)-[:LIKES]->(d)");
        database.command("opencypher",
            "MATCH (a:P), (b:P) WHERE a.name STARTS WITH 'p5' AND b.name = 'p6' CREATE (a)-[:KNOWS {uuid: 'n' + a.name}]->(b)");
      });
      database.command("opencypher", "CREATE CONSTRAINT FOR ()-[k:KNOWS]-() REQUIRE k.uuid IS UNIQUE");
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav11481 VERTEX TYPES (Entity, P) EDGE TYPES (FOLLOWS, LIKES) UPDATE MODE OFF");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav11481");
      final long deadline = System.currentTimeMillis() + 60_000;
      while (!view.isReady() && System.currentTimeMillis() < deadline)
        Thread.sleep(20);
      assertThat(view.isReady()).isTrue();
      try {
        final List<String> rows = new ArrayList<>();
        final String plan = run("MATCH (a:P)-[e:KNOWS]->(b:P)-[:FOLLOWS]->(c:P)-[:LIKES]->(d:P) WHERE e.uuid = 'k1' "
            + "RETURN e.uuid + '/' + d.name AS uuid", Map.of(), rows);
        assertThat(plan).contains("EdgeIndexSeek").contains("GAV");
        assertThat(rows).containsExactly("k1/p4");
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav11481");
      }
    }

    private void assertSeeks(final String query, final Map<String, Object> parameters, final String... expected) {
      final List<String> rows = new ArrayList<>();
      final String plan = run(query, parameters, rows);
      assertThat(plan).contains("MATCH EDGE BY INDEX");
      assertThat(rows).containsExactly(expected);
    }

    private String run(final String query, final Map<String, Object> parameters, final List<String> rows) {
      try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, parameters)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          rows.add(row.getProperty("uuid"));
        }
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }
  }
}
