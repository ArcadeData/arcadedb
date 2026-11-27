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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #12943. The schedule of a MATCH starts a depth-first pass at the cheapest root candidate. When that node's only neighbour
 * waits for an alias through a {@code $matched} filter, the pass schedules nothing, yet it left the node marked as visited: a later edge that
 * reached it was scheduled from it as if it were bound, and its walk started from an alias no step had bound, so the pattern answered no rows.
 * <p>
 * Graph: V0(c=1), V1(c=1), W2(c=2), W3(c=1) with 0 -A-> 1, 2 -A-> 1 and 3 -A-> 1, W extending V. The typed {@code a2} (W) is a cheaper root than
 * the {@code a0} (V, which holds every vertex).
 */
class Issue12943MatchMatchedDependencyRootTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
    database.getSchema().createVertexType("W").addSuperType("V");
    database.getSchema().createEdgeType("A");
    database.transaction(() -> {
      final MutableVertex v0 = database.newVertex("V").set("id", 0).set("c", 1).save();
      final MutableVertex v1 = database.newVertex("V").set("id", 1).set("c", 1).save();
      final MutableVertex w2 = database.newVertex("W").set("id", 2).set("c", 2).save();
      final MutableVertex w3 = database.newVertex("W").set("id", 3).set("c", 1).save();
      v0.newEdge("A", v1).save();
      w2.newEdge("A", v1).save();
      w3.newEdge("A", v1).save();
    });
  }

  @Test
  void aMatchedWhereOnAMiddleNodeInOneChain() {
    assertThat(rows("MATCH {type: V, as: a0}.out('A'){as: a1, where: ($matched.a0.c = c)}.in('A'){type: W, as: a2} "
        + "RETURN a0.id AS a0, a1.id AS a1, a2.id AS a2")).containsExactly("0,1,2", "0,1,3", "3,1,2", "3,1,3");
  }

  @Test
  void theMatchedWhereInItsOwnClauseAnswersTheSame() {
    // the second spelling of the issue: the where: of a1 on a clause of its own, merged into the same pattern by the shared alias
    // (the merge onto the hop that binds a1 is #12903)
    assertThat(rows("MATCH {type: V, as: a0}.out('A'){as: a1}, {as: a1, where: ($matched.a0.c = c)}.in('A'){type: W, as: a2} "
        + "RETURN a0.id AS a0, a1.id AS a1, a2.id AS a2")).containsExactly("0,1,2", "0,1,3", "3,1,2", "3,1,3");
  }

  @Test
  void aMiddleNodeReadingBothEndsNeverAnswersWrongRows() {
    // a1 waits for a0 AND a2 through $matched: whichever end starts, its pass schedules nothing, and one root cannot bind both
    // ends before a1. Before the fix this answered no rows (the second end was walked from unbound); the planner may refuse
    // the statement, but whenever it answers, the answer must be the right one
    final String match = "MATCH {type: V, as: a0}.out('A'){as: a1, where: ($matched.a0.c = c AND $matched.a2.c = c)}.in('A'){type: W, as: a2} "
        + "RETURN a0.id AS a0, a1.id AS a1, a2.id AS a2";
    try {
      assertThat(rows(match)).containsExactly("0,1,3", "3,1,3");
    } catch (final CommandExecutionException e) {
      assertThat(e.getMessage()).contains("cannot be evaluated");
    }
  }

  @Test
  void theSameStatementWithAnAsBigTargetStillAnswers() {
    // a2 is a V as well: not a cheaper root than a0, so the schedule never started at it
    assertThat(rows("MATCH {type: V, as: a0}.out('A'){as: a1, where: ($matched.a0.c = c)}.in('A'){type: V, as: a2} "
        + "RETURN a0.id AS a0, a1.id AS a1, a2.id AS a2")).containsExactly("0,1,0", "0,1,2", "0,1,3", "3,1,0", "3,1,2", "3,1,3");
  }

  @Test
  void aMatchedWhereOnTheLastNodeStillFilters() {
    assertThat(rows("MATCH {type: V, as: a0}.out('A'){as: a1}.in('A'){type: W, as: a2, where: ($matched.a0.c = c)} "
        + "RETURN a0.id AS a0, a1.id AS a1, a2.id AS a2")).containsExactly("0,1,3", "2,1,2", "3,1,3");
  }

  private List<String> rows(final String match) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", match)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder sb = new StringBuilder();
        for (final String name : row.getPropertyNames()) {
          if (!sb.isEmpty())
            sb.append(',');
          sb.append((Object) row.getProperty(name));
        }
        out.add(sb.toString());
      }
    }
    out.sort(null);
    return out;
  }
}
