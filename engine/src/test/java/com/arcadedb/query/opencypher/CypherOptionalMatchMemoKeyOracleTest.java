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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Oracle test for the match memo of an OPTIONAL MATCH (issue #11083): in a statement that cannot write, the second OPTIONAL MATCH hands the
 * matches of the previous input row to the next one when both hold the same values for the names the clause reads. Its WHERE and its
 * inline maps read the first OPTIONAL MATCH's variables (x, e1), so those have to be part of the key: each statement fans one anchor out
 * to several x and reads a value of x that differs between them. Expected rows are what Neo4j 5.26 answers for the same statements.
 */
class CypherOptionalMatchMemoKeyOracleTest {
  private static final String MEMO_SETUP = "CREATE (a0:A {id: 0, k: 1}), (a1:A {id: 1, k: 2}), (x1:B {id: 11, k: 1}), (x2:B {id: 12, k: 2}), (x3:B {id: 13}), (y1:B {id: 21, k: 1}), (y2:B {id: 22, k: 2}), (y3:B {id: 23, k: 2}), (a0)-[:R1 {w: 1}]->(x1), (a0)-[:R1 {w: 2}]->(x2), (a0)-[:R1 {w: 3}]->(x3), (a0)-[:R2 {w: 1}]->(y1), (a0)-[:R2 {w: 2}]->(y2), (a0)-[:R2 {w: 3}]->(y3), (a1)-[:R1 {w: 2}]->(x1), (a1)-[:R1 {w: 1}]->(x2), (a1)-[:R2 {w: 2}]->(y2)";

  /** Each case is the statement followed by the rows Neo4j 5.26 answers for it, one entry per row, columns sorted. */
  private static final String[][] MEMO = {
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE y.k = x.k RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=21}", "{a=0, x=12, y=22}", "{a=0, x=12, y=23}", "{a=0, x=13, y=null}", "{a=1, x=11, y=null}", "{a=1, x=12, y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE y.k > x.k RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=22}", "{a=0, x=11, y=23}", "{a=0, x=12, y=null}", "{a=0, x=13, y=null}", "{a=1, x=11, y=22}", "{a=1, x=12, y=null}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE y.k <> x.k OR x IS NULL RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=22}", "{a=0, x=11, y=23}", "{a=0, x=12, y=21}", "{a=0, x=13, y=null}", "{a=1, x=11, y=22}", "{a=1, x=12, y=null}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE e2.w = e1.w RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=21}", "{a=0, x=12, y=22}", "{a=0, x=13, y=23}", "{a=1, x=11, y=22}", "{a=1, x=12, y=null}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE e2.w >= coalesce(e1.w, 0) RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=21}", "{a=0, x=11, y=22}", "{a=0, x=11, y=23}", "{a=0, x=12, y=22}", "{a=0, x=12, y=23}", "{a=0, x=13, y=23}", "{a=1, x=11, y=22}", "{a=1, x=12, y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE y.id = coalesce(x.id, -1) + 10 RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=21}", "{a=0, x=12, y=22}", "{a=0, x=13, y=23}", "{a=1, x=11, y=null}", "{a=1, x=12, y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE y IN [x, a] RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=null}", "{a=0, x=12, y=null}", "{a=0, x=13, y=null}", "{a=1, x=11, y=null}", "{a=1, x=12, y=null}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE any(v IN keys(x) WHERE y[v] = x[v]) RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=21}", "{a=0, x=12, y=22}", "{a=0, x=12, y=23}", "{a=0, x=13, y=null}", "{a=1, x=11, y=null}", "{a=1, x=12, y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE y.k = x['k'] RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=21}", "{a=0, x=12, y=22}", "{a=0, x=12, y=23}", "{a=0, x=13, y=null}", "{a=1, x=11, y=null}", "{a=1, x=12, y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE labels(y) = labels(x) AND y.k = x.k RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=21}", "{a=0, x=12, y=22}", "{a=0, x=12, y=23}", "{a=0, x=13, y=null}", "{a=1, x=11, y=null}", "{a=1, x=12, y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE size(keys(x)) = y.k RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=22}", "{a=0, x=11, y=23}", "{a=0, x=12, y=22}", "{a=0, x=12, y=23}", "{a=0, x=13, y=21}", "{a=1, x=11, y=22}", "{a=1, x=12, y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE e2 <> e1 RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=21}", "{a=0, x=11, y=22}", "{a=0, x=11, y=23}", "{a=0, x=12, y=21}", "{a=0, x=12, y=22}", "{a=0, x=12, y=23}", "{a=0, x=13, y=21}", "{a=0, x=13, y=22}", "{a=0, x=13, y=23}", "{a=1, x=11, y=22}", "{a=1, x=12, y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE y.k = CASE WHEN x IS NULL THEN 1 ELSE x.k END RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=21}", "{a=0, x=12, y=22}", "{a=0, x=12, y=23}", "{a=0, x=13, y=null}", "{a=1, x=11, y=null}", "{a=1, x=12, y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE y.k = reduce(s = 0, q IN [x.k, e1.w] | s + coalesce(q, 0)) RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=22}", "{a=0, x=11, y=23}", "{a=0, x=12, y=null}", "{a=0, x=13, y=null}", "{a=1, x=11, y=null}", "{a=1, x=12, y=null}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE toString(y.k) = toString(x.k) RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=21}", "{a=0, x=12, y=22}", "{a=0, x=12, y=23}", "{a=0, x=13, y=null}", "{a=1, x=11, y=null}", "{a=1, x=12, y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y {k: x.k}) RETURN a.id AS a, x.id AS x, y.id AS y ORDER BY a, x, y", "{a=0, x=11, y=21}", "{a=0, x=12, y=22}", "{a=0, x=12, y=23}", "{a=0, x=13, y=null}", "{a=1, x=11, y=null}", "{a=1, x=12, y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) WITH a, x.k AS kk OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE y.k = kk RETURN a.id AS a, kk, y.id AS y ORDER BY a, kk, y", "{a=0, kk=1, y=21}", "{a=0, kk=2, y=22}", "{a=0, kk=2, y=23}", "{a=0, kk=null, y=null}", "{a=1, kk=1, y=null}", "{a=1, kk=2, y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) WITH a, e1.w AS ww OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE e2.w = ww RETURN a.id AS a, ww, y.id AS y ORDER BY a, ww, y", "{a=0, ww=1, y=21}", "{a=0, ww=2, y=22}", "{a=0, ww=3, y=23}", "{a=1, ww=1, y=null}", "{a=1, ww=2, y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) WITH a, x.k AS kk OPTIONAL MATCH (a)-[e2:R2]->(y {k: kk}) RETURN a.id AS a, kk, y.id AS y ORDER BY a, kk, y", "{a=0, kk=1, y=21}", "{a=0, kk=2, y=22}", "{a=0, kk=2, y=23}", "{a=0, kk=null, y=null}", "{a=1, kk=1, y=null}", "{a=1, kk=2, y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) WITH a, [x.k, e1.w] AS pair OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE y.k = pair[0] AND e2.w >= pair[1] RETURN a.id AS a, pair, y.id AS y ORDER BY a, pair, y", "{a=0, pair=[1, 1], y=21}", "{a=0, pair=[2, 2], y=22}", "{a=0, pair=[2, 2], y=23}", "{a=0, pair=[null, 3], y=null}", "{a=1, pair=[1, 2], y=null}", "{a=1, pair=[2, 1], y=22}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) WITH a, x.k AS kk, e1.w AS ww OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE coalesce(y.k, 0) >= coalesce(kk, 0) AND e2.w <= coalesce(ww, 9) RETURN a.id AS a, kk, ww, y.id AS y ORDER BY a, kk, ww, y", "{a=0, kk=1, ww=1, y=21}", "{a=0, kk=2, ww=2, y=22}", "{a=0, kk=null, ww=3, y=21}", "{a=0, kk=null, ww=3, y=22}", "{a=0, kk=null, ww=3, y=23}", "{a=1, kk=1, ww=2, y=22}", "{a=1, kk=2, ww=1, y=null}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE y.k = x.k RETURN a.id AS a, count(*) AS c, count(y) AS cy, collect(y.id) AS ys ORDER BY a", "{a=0, c=4, cy=3, ys=[23, 22, 21]}", "{a=1, c=2, cy=1, ys=[22]}" },
        { "MATCH (a:A) OPTIONAL MATCH (a)-[e1:R1]->(x) OPTIONAL MATCH (a)-[e2:R2]->(y) WHERE y.k = x.k RETURN DISTINCT a.id AS a, y.id AS y ORDER BY a, y", "{a=0, y=21}", "{a=0, y=22}", "{a=0, y=23}", "{a=0, y=null}", "{a=1, y=22}", "{a=1, y=null}" }
  };

  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypherOptMemoOracle").create();
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private void check(final String setup, final String[][] cases) {
    database.transaction(() -> database.command("opencypher", setup));
    for (final String[] testCase : cases) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", testCase[0])) {
        while (rs.hasNext())
          rows.add(render(rs.next()));
      }
      assertThat(rows).as(testCase[0]).containsExactly(Arrays.copyOfRange(testCase, 1, testCase.length));
    }
  }

  private static String render(final Result row) {
    final Map<String, Object> sorted = new TreeMap<>();
    for (final String name : row.getPropertyNames())
      sorted.put(name, row.getProperty(name));
    return sorted.toString();
  }

  @Test
  void theSecondOptionalMatchReadsRowStateThatDiffersBetweenRowsOfOneAnchor() {
    check(MEMO_SETUP, MEMO);
  }
}
