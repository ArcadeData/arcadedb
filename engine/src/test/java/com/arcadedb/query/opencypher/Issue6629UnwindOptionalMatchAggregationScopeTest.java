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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6629: {@code OPTIONAL MATCH (x)-[:TYPE]->(y) ... WITH y, count(x) AS cnt} is fused by the planner into
 * {@code CountEdgesStep} (and, for two consecutive OPTIONAL MATCHes, {@code CountChainedEdgesStep}), which counts
 * edges directly instead of materializing the OPTIONAL MATCH rows. Both steps used to emit one output row per
 * INPUT row instead of aggregating, on the (previously unstated) assumption that exactly one input row reaches
 * them per distinct value of the bound vertex. UNWIND breaks that assumption - it fans a single bound vertex out
 * into several input rows - so the WITH aggregation boundary was silently lost: N UNWIND rows produced N output
 * rows, each carrying a per-row count, instead of one grouped row carrying the total.
 * <p>
 * Both steps now group their input rows by the WITH clause's non-aggregated (grouping-key) values and sum the
 * per-row edge count within each group, which is correct both with and without a row-multiplying clause upstream.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6629UnwindOptionalMatchAggregationScopeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-unwind-optional-count-6629").create();
    database.transaction(() -> {
      database.getSchema().createVertexType("X");
      database.getSchema().createEdgeType("L");

      final MutableVertex x1 = database.newVertex("X").set("_id", "x1").save();
      final MutableVertex x2 = database.newVertex("X").set("_id", "x2").save();
      x1.newEdge("L", x2);
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void unwindThenOptionalMatchCollapsesToOneGroupWithSummedCount() {
    // The reported witness: UNWIND fans x1 out into 2 rows; each row's OPTIONAL MATCH finds the
    // same edge, so the WITH boundary must collapse them into ONE row with c=2 (matching Neo4j/
    // Memgraph/FalkorDB), not two rows with c=1 each.
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:X {_id:'x1'}) UNWIND [1, 2] AS u OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN a, c");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Long>getProperty("c")).isEqualTo(2L);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void unwindThenOptionalMatchWithNoMatchesCollapsesToOneGroupWithZeroCount() {
    // Control 3 from the issue: zero optional matches must still be ONE group with c=0, not one
    // zero-count row per UNWIND element.
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:X {_id:'x2'}) UNWIND [1, 2] AS u OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN a, c");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Long>getProperty("c")).isEqualTo(0L);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void optionalMatchWithoutUnwindStillAggregatesToOneRow() {
    // Control 1 from the issue: the same aggregation without UNWIND already collapsed correctly -
    // must keep doing so.
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:X {_id:'x1'}) OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN a, c");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Long>getProperty("c")).isEqualTo(1L);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void unwindWithoutOptionalMatchStillAggregatesToOneRow() {
    // Control 2 from the issue: UNWIND followed by a plain count(*) (no OPTIONAL MATCH, so no
    // CountEdgesStep fusion at all) already collapsed correctly - must keep doing so.
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:X {_id:'x1'}) UNWIND [1, 2] AS u WITH a, count(*) AS c RETURN a, c");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Long>getProperty("c")).isEqualTo(2L);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void unwindBeforeChainedOptionalMatchCountAggregatesAcrossUnwoundRows() {
    // Same collapse-of-grouping-boundary bug, but through CountChainedEdgesStep (two consecutive
    // OPTIONAL MATCH clauses fused into one chained edge count) rather than CountEdgesStep.
    database.transaction(() -> {
      database.getSchema().createVertexType("Question");
      database.getSchema().createVertexType("Answer");
      database.getSchema().createVertexType("Comment");
      database.getSchema().createEdgeType("HAS_ANSWER");
      database.getSchema().createEdgeType("HAS_COMMENT");

      final MutableVertex q1 = database.newVertex("Question").set("id", 1).save();
      final MutableVertex a1 = database.newVertex("Answer").set("id", 1).save();
      q1.newEdge("HAS_ANSWER", a1);
      for (int i = 0; i < 3; i++) {
        final MutableVertex c = database.newVertex("Comment").set("id", 100 + i).save();
        a1.newEdge("HAS_COMMENT", c);
      }
    });

    // UNWIND fans q1 out into 2 rows before the chained OPTIONAL MATCH; each row finds all 3
    // comments through the HAS_ANSWER -> HAS_COMMENT chain, so the total must be 2 * 3 = 6, in
    // ONE row per question.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (q:Question) UNWIND [1, 2] AS u \
        OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a) \
        OPTIONAL MATCH (a)-[:HAS_COMMENT]->(x) \
        WITH q, count(x) AS cnt \
        RETURN q.id AS qid, cnt""");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Integer>getProperty("qid")).isEqualTo(1);
    assertThat(row.<Long>getProperty("cnt")).isEqualTo(6L);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }
}
