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
import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #12945. The {@code pathAlias} of a {@code while:}/{@code maxDepth:} hop is the path from the hop's source to its target,
 * whichever end the planner starts the walk from. A walk started at the target (a selective {@code where:} makes it the root) crosses the
 * vertices in the opposite order and used to report them that way.
 * <p>
 * The graph is the chain 0 -A-> 1 -A-> 2 -A-> 3 of vertices {@code V(id)}.
 */
class Issue12945MatchPathAliasReverseWalkTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("A");
    database.transaction(() -> {
      final MutableVertex[] v = new MutableVertex[4];
      for (int i = 0; i < 4; i++)
        v[i] = database.newVertex("V").set("id", i).save();
      for (int i = 0; i < 3; i++)
        v[i].newEdge("A", v[i + 1]).save();
    });
  }

  @Test
  void aForwardWalkReportsThePathFromTheSourceToTheTarget() {
    assertThat(rows("MATCH {type: V, as: a, where: (id = 3)}.in('A'){as: b, while: ($depth < 3), pathAlias: p} RETURN a.id AS a, b.id AS b, p"))
        .containsExactly("3,0,[2, 1, 0]", "3,1,[2, 1]", "3,2,[2]", "3,3,[]");
  }

  @Test
  void aWalkStartedAtTheTargetReportsTheSamePath() {
    final String match = "MATCH {type: V, as: a}.in('A'){type: V, as: b, where: (id < 1), while: ($depth < 3), pathAlias: p} RETURN a.id AS a, b.id AS b, p";
    assertThat(explain(match)).as("the planner must start at b and walk the hop backwards, or this test checks nothing").contains("<----");
    assertThat(rows(match)).containsExactly("0,0,[]", "1,0,[0]", "2,0,[1, 0]", "3,0,[2, 1, 0]");
  }

  @Test
  void anOutHopWalkedFromItsTargetReportsThePathToo() {
    final String match = "MATCH {type: V, as: a}.out('A'){type: V, as: b, where: (id = 3), while: ($depth < 3), pathAlias: p} RETURN a.id AS a, b.id AS b, p";
    assertThat(explain(match)).contains("<----");
    assertThat(rows(match)).containsExactly("0,3,[1, 2, 3]", "1,3,[2, 3]", "2,3,[3]", "3,3,[]");
  }

  @Test
  void theDepthAliasIsTheSameFromEitherEnd() {
    assertThat(rows("MATCH {type: V, as: a}.in('A'){type: V, as: b, where: (id < 1), while: ($depth < 3), depthAlias: d} RETURN a.id AS a, b.id AS b, d"))
        .containsExactly("0,0,0", "1,0,1", "2,0,2", "3,0,3");
  }

  private String explain(final String match) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + match)) {
      return rs.next().getProperty("executionPlanAsString");
    }
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
          final Object value = row.getProperty(name);
          if (value instanceof List<?> list) {
            final List<Integer> ids = new ArrayList<>();
            for (final Object item : list)
              ids.add(database.lookupByRID(((Identifiable) item).getIdentity(), true).asVertex().getInteger("id"));
            sb.append(ids);
          } else
            sb.append(value);
        }
        out.add(sb.toString());
      }
    }
    out.sort(null);
    return out;
  }
}
