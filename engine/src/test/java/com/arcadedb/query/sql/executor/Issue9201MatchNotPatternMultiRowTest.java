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
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9201: a SQL MATCH with a NOT pattern kept excluding every row after the first one the NOT pattern matched,
 * because the sub-steps of the NOT filter were reused for each row without being reset.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9201MatchNotPatternMultiRowTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE PROPERTY P.id LONG");
      database.command("sql", "CREATE EDGE TYPE K");
      for (int i = 0; i < 4; i++)
        database.command("sql", "CREATE VERTEX P SET id = ?", (long) i);
      final int[][] edges = { { 0, 1 }, { 1, 0 }, { 1, 2 }, { 2, 3 } };
      for (final int[] e : edges)
        database.command("sql", "CREATE EDGE K FROM (SELECT FROM P WHERE id = ?) TO (SELECT FROM P WHERE id = ?)", (long) e[0],
            (long) e[1]);
    });
  }

  @Test
  void excludedRowsDoNotHideLaterRows() {
    assertThat(pairs("MATCH {type: P, as: x}.out('K'){as: y}, NOT {as: y}.out('K'){as: x} RETURN x.id AS x, y.id AS y"))
        .containsExactly("(1,2)", "(2,3)");
  }

  @Test
  void twoHopNotPatternAndFilteredNotBranchAlsoResetPerRow() {
    // P0->P1->P2->P3 chain plus 0->1 reciprocal; NOT of a two-hop path back to x keeps only rows without a 2-hop return
    assertThat(pairs("MATCH {type: P, as: x}.out('K'){as: y}, NOT {as: y}.out('K'){as: z}.out('K'){as: x} RETURN x.id AS x, y.id AS y"))
        .containsExactly("(0,1)", "(1,0)", "(1,2)", "(2,3)");
    assertThat(pairs("MATCH {type: P, as: x}.out('K'){as: y}, NOT {as: y}.out('K'){as: x, where: (id < 2)} RETURN x.id AS x, y.id AS y"))
        .containsExactly("(1,2)", "(2,3)");
  }

  @Test
  void randomGraphMatchesReciprocalEdgeCount() {
    database.transaction(() -> database.command("sql", "DELETE FROM P"));
    final int n = 60;
    final Random r = new Random(7);
    final Set<Long> seen = new HashSet<>();
    final List<int[]> edges = new ArrayList<>();
    while (edges.size() < 240) {
      final int a = r.nextInt(n), b = r.nextInt(n);
      if (a != b && seen.add(a * 1000L + b))
        edges.add(new int[] { a, b });
    }
    database.transaction(() -> {
      final RID[] v = new RID[n];
      for (int i = 0; i < n; i++)
        v[i] = database.newVertex("P").set("id", (long) i).save().getIdentity();
      for (final int[] e : edges)
        v[e[0]].asVertex().newEdge("K", v[e[1]]);
    });
    int expected = 0;
    for (final int[] e : edges)
      if (!seen.contains(e[1] * 1000L + e[0]))
        expected++;
    assertThat(pairs("MATCH {type: P, as: x}.out('K'){as: y}, NOT {as: y}.out('K'){as: x} RETURN x.id AS x, y.id AS y"))
        .hasSize(expected);
  }

  private List<String> pairs(final String query) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        out.add("(" + row.getProperty("x") + "," + row.getProperty("y") + ")");
      }
    }
    Collections.sort(out);
    return out;
  }
}
