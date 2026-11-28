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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #12949: a SQL MATCH node that is the root of the pattern, and whose {@code where:} reads
 * {@code $currentMatch}, never matched. The root is fetched by a SELECT built from the node, in which nothing bound
 * {@code $currentMatch}, so {@code $currentMatch.c = 2} was {@code null = 2}. The same {@code where:} on a node reached through a
 * hop worked, because the traversers bind the variable to the candidate.
 * <p>
 * Each case runs on a graph small enough for the planner to prefetch the root (fewer than 100 records) and on one too big for that,
 * so the root is fetched by the plan of the single-node pattern or of the edge pattern instead.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12949MatchRootCurrentMatchTest {

  abstract static class Cases extends TestHelper {
    abstract int fillers();

    /** {@code V{id:0,c:1} -A-> V{id:1,c:2}}, plus {@link #fillers()} isolated vertices with {@code c = 0} and a negative id. */
    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("V").createProperty("id", Integer.class).createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
      database.getSchema().createEdgeType("A");
      final int fillers = fillers();
      database.transaction(() -> {
        final MutableVertex v0 = database.newVertex("V").set("id", 0).set("c", 1).save();
        final MutableVertex v1 = database.newVertex("V").set("id", 1).set("c", 2).save();
        v0.newEdge("A", v1).save();
        for (int i = 0; i < fillers; i++)
          database.newVertex("V").set("id", -1 - i).set("c", 0).save();
      });
    }

    @Test
    void aPlainWhereOnTheRootStillMatches() {
      assertThat(rows("MATCH {type:V, as:a, where:(c = 2)} RETURN a.id AS a")).containsExactly("1");
    }

    @Test
    void currentMatchOnTheRootOfASingleNodePattern() {
      assertThat(rows("MATCH {type:V, as:a, where:($currentMatch.c = 2)} RETURN a.id AS a")).containsExactly("1");
    }

    @Test
    void currentMatchOnTheRootOfAnEdgePattern() {
      assertThat(rows("MATCH {type:V, as:a, where:($currentMatch.c = 1)}.out('A'){as:b} RETURN a.id AS a, b.id AS b"))
          .containsExactly("0,1");
    }

    @Test
    void currentMatchOnTheRootAndOnTheHopTarget() {
      assertThat(rows(
          "MATCH {type:V, as:a, where:($currentMatch.c = 1)}.out('A'){as:b, where:($currentMatch.c = 2)} RETURN a.id AS a, b.id AS b"))
          .containsExactly("0,1");
      assertThat(rows(
          "MATCH {type:V, as:a, where:($currentMatch.c = 1)}.out('A'){as:b, where:($currentMatch.c = 1)} RETURN a.id AS a, b.id AS b"))
          .isEmpty();
    }

    @Test
    void currentMatchOnAHopTargetIsUnchanged() {
      assertThat(rows("MATCH {type:V, as:a}.out('A'){as:b, where:($currentMatch.c = 2)} RETURN a.id AS a, b.id AS b"))
          .containsExactly("0,1");
    }

    @Test
    void currentMatchOnTheRootReachedBackward() {
      // b is the cheaper root (its where: is an indexed equality), so a is reached from b through the reverse traverser
      assertThat(rows(
          "MATCH {type:V, as:a, where:($currentMatch.c = 1)}.out('A'){type:V, as:b, where:(id = 1)} RETURN a.id AS a, b.id AS b"))
          .containsExactly("0,1");
    }

    @Test
    void currentMatchNextToAnIndexedConjunct() {
      assertThat(rows("MATCH {type:V, as:a, where:(id = 1 AND $currentMatch.c = 2)} RETURN a.id AS a")).containsExactly("1");
      assertThat(rows("MATCH {type:V, as:a, where:(id = 1 AND $currentMatch.c = 1)} RETURN a.id AS a")).isEmpty();
      assertThat(rows("MATCH {type:V, as:a, where:(id = 0 AND $currentMatch.c = 1)}.out('A'){as:b} RETURN a.id AS a, b.id AS b"))
          .containsExactly("0,1");
    }

    @Test
    void currentMatchInsideADisjunction() {
      assertThat(rows("MATCH {type:V, as:a, where:($currentMatch.c = 2 OR id = 0)} RETURN a.id AS a ORDER BY a"))
          .containsExactly("0", "1");
      assertThat(rows("MATCH {type:V, as:a, where:(not ($currentMatch.c <> 2))} RETURN a.id AS a")).containsExactly("1");
    }

    @Test
    void currentMatchReadThroughAMethodOfTheRecord() {
      assertThat(rows("MATCH {type:V, as:a, where:($currentMatch.out('A').size() > 0)} RETURN a.id AS a")).containsExactly("0");
      assertThat(rows("MATCH {type:V, as:a, where:($currentMatch.@rid = @rid AND c > 0)} RETURN a.id AS a ORDER BY a"))
          .containsExactly("0", "1");
    }

    @Test
    void currentMatchOnARootFetchedByRid() {
      final String rid;
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM V WHERE id = 1")) {
        rid = rs.next().getProperty("rid").toString();
      }
      assertThat(rows("MATCH {type:V, rid:" + rid + ", as:a, where:($currentMatch.c = 2)} RETURN a.id AS a")).containsExactly("1");
      assertThat(rows("MATCH {type:V, rid:" + rid + ", as:a, where:($currentMatch.c = 1)} RETURN a.id AS a")).isEmpty();
    }

    @Test
    void currentMatchOnTheRootOfASecondDisjointPattern() {
      assertThat(rows(
          "MATCH {type:V, as:a, where:($currentMatch.c = 1)}, {type:V, as:b, where:($currentMatch.c = 2)} RETURN a.id AS a, b.id AS b"))
          .containsExactly("0,1");
    }

    @Test
    void currentMatchIsRestoredAfterTheRootIsFetched() {
      // THE VARIABLE IS UNBOUND OUTSIDE OF A NODE: THE RETURN DOES NOT SEE THE LAST CANDIDATE OF THE ROOT
      assertThat(rows("MATCH {type:V, as:a, where:($currentMatch.c = 2)} RETURN a.id AS a, $currentMatch AS cm")).containsExactly("1,null");
    }

    List<String> rows(final String match) {
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
      return out;
    }
  }

  /** Two vertices: the planner prefetches every root. */
  @Nested
  class Prefetched extends Cases {
    @Override
    int fillers() {
      return 0;
    }
  }

  /** Too many vertices to prefetch: the root is fetched by the plan of its pattern. */
  @Nested
  class FetchedByThePattern extends Cases {
    @Override
    int fillers() {
      return 300;
    }

    @Test
    void theIndexedConjunctStillServesTheFetch() {
      final String plan;
      try (final ResultSet rs = database.query("sql", "EXPLAIN MATCH {type:V, as:a, where:(id = 1 AND $currentMatch.c = 2)} RETURN a.id AS a")) {
        plan = rs.next().getProperty("executionPlanAsString");
      }
      assertThat(plan).contains("FETCH FROM INDEX");
    }
  }
}
