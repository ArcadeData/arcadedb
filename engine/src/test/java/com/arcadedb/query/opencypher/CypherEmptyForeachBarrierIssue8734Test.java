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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8734: an empty {@code FOREACH} between clauses of a query changed the cardinality of an {@code OPTIONAL MATCH}
 * over a Cartesian product. The barrier must not change the rows of the query without it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherEmptyForeachBarrierIssue8734Test extends TestHelper {
  private static final String LEFT = """
      OPTIONAL MATCH (), (n0 {k6:false})
      WHERE n0.k1 = true
      WITH *
      WHERE n0 IS NOT NULL
      UNWIND n0.k10 AS alias0
      REMOVE n0.klist, n0:l9:l8
      RETURN {n0:n0,alias0:alias0} AS __layer_row""";

  private static final String RIGHT = """
      FOREACH (elem0 IN [] | CREATE (:BarrierSentinel))
      OPTIONAL MATCH (), (n0 {k6:false})
      WHERE n0.k1 = true
      FOREACH (elem1 IN [] | CREATE (:BarrierSentinel))
      WITH *
      WHERE n0 IS NOT NULL
      FOREACH (elem2 IN [] | CREATE (:BarrierSentinel))
      UNWIND n0.k10 AS alias0
      FOREACH (elem3 IN [] | CREATE (:BarrierSentinel))
      REMOVE n0.klist, n0:l9:l8
      RETURN {n0:n0,alias0:alias0} AS __layer_row""";

  @Override
  protected void beginTest() {
    database.command("opencypher",
        "CREATE (:l11:l6 {k1:true, k6:false, k10:[0]}), (:l11:l5 {k1:true, k6:false, k10:[0]}), (:l8:l11 {k1:true, k6:false, k10:[0]}),"
            + " (:l3:l9 {k1:true, k6:false, k10:[0]}) WITH count(*) AS _ UNWIND range(0,97) AS i CREATE ()");
  }

  @Test
  void withoutBarrierReturnsEveryCombination() {
    assertThat(rows(LEFT)).hasSize(408);
  }

  @Test
  void emptyForeachDoesNotChangeTheRows() {
    final List<String> left = rows(LEFT);
    final List<String> right = rows(RIGHT);
    assertThat(right).hasSize(408);
    assertThat(right).containsExactlyInAnyOrderElementsOf(left);
    assertThat(database.getSchema().existsType("BarrierSentinel")).isFalse();
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(String.valueOf((Object) r.getProperty("__layer_row")));
      }
    }
    return rows;
  }
}
