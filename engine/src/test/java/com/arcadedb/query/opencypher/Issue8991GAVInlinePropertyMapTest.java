/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8991: once a Graph Analytical View served a single hop, the inline property map on the target node was ignored. The
 * view is a performance feature, so the oracle is the answer the same statements give without it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8991GAVInlinePropertyMapTest extends TestHelper {
  private static final String[] QUERIES = {
      "MATCH (x {id: 1})-[:T]->(y {y: 1}) RETURN y.id AS id ORDER BY id",
      "MATCH (x {id: 1})-[:T]->(y:P {y: 1}) RETURN y.id AS id ORDER BY id",
      "MATCH (x:P)-[:T]->(y {y: 1}) RETURN count(*) AS c",
      "MATCH (x:P) OPTIONAL MATCH (x)-[:T]->(y {y: 1}) RETURN x.id AS x, y.id AS y ORDER BY x, y",
      "MATCH (x {id: 1})-[:T]->(y) WHERE y.y = 1 RETURN y.id AS id ORDER BY id" };

  @Test
  void inlinePropertyMapOnTargetStillFiltersWithAView() throws Exception {
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:P {id: 1, y: 1}), (b:P {id: 2, y: 1}), (c:P {id: 3, y: 2}), (d:Q {id: 4, y: 2}),
               (a)-[:T]->(b), (a)-[:T]->(c), (a)-[:T]->(d), (c)-[:T]->(b)"""));

    final List<List<String>> expected = new ArrayList<>();
    for (final String q : QUERIES)
      expected.add(rows(q));
    assertThat(expected.get(0)).hasSize(1);

    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g");
    GraphAnalyticalViewRegistry.get(database, "g").awaitReady(60, TimeUnit.SECONDS);

    for (int i = 0; i < QUERIES.length; i++)
      assertThat(rows(QUERIES[i])).as(QUERIES[i]).isEqualTo(expected.get(i));
  }

  private List<String> rows(final String query) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      rs.stream().forEach(r -> out.add(r.toJSON().toString()));
    }
    return out;
  }
}
