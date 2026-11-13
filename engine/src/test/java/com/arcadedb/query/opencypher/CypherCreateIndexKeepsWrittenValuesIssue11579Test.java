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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11579: openCypher {@code CREATE INDEX} on a label whose nodes already held values of one kind in the property declared
 * the property with that kind, so every later write was converted to it: on integers {@code 2.5} was stored as {@code 2}, the
 * string {@code '7'} as the number {@code 7} and a date as epoch milliseconds; on strings the number {@code 7} as {@code '7'}.
 * A range index does not constrain the values of a property (Neo4j declares no type), so a write after the index must store
 * what the same write stores on a label without the index, and the index must find it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCreateIndexKeepsWrittenValuesIssue11579Test extends TestHelper {
  private static final String[] WRITES = { "2.5", "7", "'7'", "date('2024-05-06')", "datetime('2024-05-06T07:08:09Z')", "true",
      "[1, 2]" };

  @Test
  void writesOnIntegersKeepTheirValues() {
    assertWritesKeptAfterIndex("UNWIND range(1, 3) AS i CREATE (:%s {x: i})");
  }

  @Test
  void writesOnStringsKeepTheirValues() {
    assertWritesKeptAfterIndex("UNWIND ['a', 'b', 'c'] AS s CREATE (:%s {x: s})");
  }

  @Test
  void writesOnBooleansKeepTheirValues() {
    assertWritesKeptAfterIndex("CREATE (:%s {x: true}), (:%s {x: false})");
  }

  @Test
  void writesOnDatesKeepTheirValues() {
    assertWritesKeptAfterIndex("CREATE (:%s {x: date('2024-01-01')})");
  }

  @Test
  void writesOnDatetimesKeepTheirValues() {
    assertWritesKeptAfterIndex("CREATE (:%s {x: datetime('2024-01-01T10:00:00Z')})");
  }

  @Test
  void theExistingValuesAreFoundAsBefore() {
    database.transaction(() -> database.command("opencypher", "UNWIND range(1, 3) AS i CREATE (:N {x: i})"));
    database.command("opencypher", "CREATE INDEX FOR (n:N) ON (n.x)");

    assertThat(lookup(database.getSchema().getType("N").getIndexByProperties("x"), 2)).hasSize(1);
    assertThat(xs("MATCH (n:N) WHERE n.x = 2 RETURN n.x AS x")).containsExactly(2L);
    assertThat(xs("MATCH (n:N) WHERE n.x > 1 RETURN n.x AS x ORDER BY x")).containsExactly(2L, 3L);
    // Cypher: 2 = '2' is false
    assertThat(xs("MATCH (n:N) WHERE n.x = '2' RETURN n.x AS x")).isEmpty();
  }

  /**
   * Populates an indexed label and an unindexed one with the same nodes, then writes every value of {@link #WRITES} to both:
   * the indexed label must store what the unindexed one stores, and its index must find the node under that value.
   */
  private void assertWritesKeptAfterIndex(final String populate) {
    database.transaction(() -> {
      database.command("opencypher", populate.formatted("Plain", "Plain"));
      database.command("opencypher", populate.formatted("N", "N"));
    });
    database.command("opencypher", "CREATE INDEX FOR (n:N) ON (n.x)");
    final TypeIndex index = database.getSchema().getType("N").getIndexByProperties("x");

    for (final String value : WRITES) {
      final Object expected = written("Plain", value).get("x");
      final Vertex vertex = written("N", value);
      final Object stored = vertex.get("x");
      assertThat(stored).as("x: %s", value).isEqualTo(expected);
      assertThat(stored.getClass()).as("x: %s", value).isEqualTo(expected.getClass());
      if (!(stored instanceof List))
        assertThat(lookup(index, stored)).as("index lookup of x: %s", value).contains(vertex.getIdentity());
      database.transaction(() -> database.command("opencypher", "MATCH (n {w: true}) DELETE n"));
    }
    assertThat(database.getSchema().getType("N").getPropertyIfExists("x")).isNull();
  }

  private Vertex written(final String label, final String value) {
    final Vertex[] vertex = new Vertex[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "CREATE (n:" + label + " {w: true, x: " + value + "}) RETURN n")) {
        vertex[0] = rs.next().<Vertex>getProperty("n");
      }
    });
    // read it back from storage, not the instance the statement built
    return vertex[0].getIdentity().asVertex(true);
  }

  private List<RID> lookup(final TypeIndex index, final Object value) {
    final List<RID> found = new ArrayList<>();
    database.transaction(() -> {
      final IndexCursor cursor = index.get(new Object[] { value });
      while (cursor.hasNext())
        found.add(cursor.next().getIdentity());
    });
    return found;
  }

  private List<Long> xs(final String query) {
    final List<Long> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        out.add(rs.next().<Number>getProperty("x").longValue());
    }
    return out;
  }
}
