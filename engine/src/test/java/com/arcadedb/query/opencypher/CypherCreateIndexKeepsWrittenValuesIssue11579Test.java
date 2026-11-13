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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
   * A uniqueness constraint on a populated label declared the property the same way, so 2.5 on integers was stored as 2 and then
   * refused as a duplicate of the 2 already there. It must keep every write as given and refuse only an equal value.
   */
  @Test
  void uniqueConstraintKeepsWrittenValues() {
    database.transaction(() -> database.command("opencypher", "UNWIND range(1, 3) AS i CREATE (:U {x: i})"));
    database.command("opencypher", "CREATE CONSTRAINT FOR (n:U) REQUIRE n.x IS UNIQUE");
    assertThat(database.getSchema().getType("U").getPropertyIfExists("x")).isNull();

    for (final String value : new String[] { "2.5", "date('2024-05-06')", "[1, 2]" }) {
      final Object expected = written("Plain", value).get("x");
      final Vertex vertex = written("U", value);
      assertThat(vertex.get("x")).as("x: %s", value).isEqualTo(expected);
      assertThat(vertex.get("x").getClass()).as("x: %s", value).isEqualTo(expected.getClass());
    }

    // an equal value is still a duplicate, whatever numeric type it is written with (Cypher: 2 = 2.0)
    for (final String duplicate : new String[] { "2", "2.0", "2.5" })
      assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "CREATE (:U {x: " + duplicate + "})")))
          .as("x: %s", duplicate).isInstanceOf(DuplicatedKeyException.class);
    assertThat(xs("MATCH (n:U) WHERE n.x = 2.5 RETURN n.x AS x")).hasSize(1);
  }

  /**
   * The existence constraints need a declared property to make it mandatory, so they still declare the kind the data holds.
   */
  @Test
  void existenceConstraintStillDeclaresTheKindOfTheData() {
    database.transaction(() -> database.command("opencypher", "UNWIND range(1, 3) AS i CREATE (:K {x: i})"));
    database.command("opencypher", "CREATE CONSTRAINT FOR (n:K) REQUIRE n.x IS NODE KEY");
    assertThat(database.getSchema().getType("K").getProperty("x").getType()).isEqualTo(Type.LONG);
    assertThat(database.getSchema().getType("K").getProperty("x").isMandatory()).isTrue();
  }

  /**
   * The index keys every kind as text, so 2, '2' and 2.0 meet under one key: each predicate must still answer what it answers
   * without the index, where a number never equals or compares with a string.
   */
  @Test
  void predicatesAcrossKindsAnswerAsWithoutTheIndex() {
    final String populate = "CREATE (:%1$s {id: 1, x: 2}), (:%1$s {id: 2, x: '2'}), (:%1$s {id: 3, x: 2.5}), (:%1$s {id: 4, x: 10}), "
        + "(:%1$s {id: 5, x: '10'}), (:%1$s {id: 6, x: true}), (:%1$s {id: 7, x: 'true'}), (:%1$s {id: 8, x: date('2024-05-06')}), "
        + "(:%1$s {id: 9, x: '2024-05-06'})";
    final String[] queries = { //
        "MATCH (n:%s) WHERE n.x = 2 RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s) WHERE n.x = 2.0 RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s) WHERE n.x = '2' RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s {x: 10}) RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s) WHERE n.x IN [2, '10'] RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s) WHERE n.x = true RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s) WHERE n.x = 'true' RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s) WHERE n.x = date('2024-05-06') RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s) WHERE n.x = '2024-05-06' RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s) WHERE n.x > 2 RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s) WHERE n.x >= 2 AND n.x < 3 RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s) WHERE n.x > '1' RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s) WHERE n.x < '3' RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s) WHERE n.x STARTS WITH '1' RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s) WHERE n.x > date('2024-01-01') RETURN n.id AS id ORDER BY id", //
        "MATCH (n:%s) RETURN n.id AS id ORDER BY n.x, id" };
    database.transaction(() -> {
      database.command("opencypher", populate.formatted("Plain"));
      database.command("opencypher", populate.formatted("N"));
    });
    database.command("opencypher", "CREATE INDEX FOR (n:N) ON (n.x)");
    assertThat(database.getSchema().getType("N").getPropertyIfExists("x")).isNull();

    for (final String query : queries)
      assertThat(ids(query.formatted("N"))).as(query).isEqualTo(ids(query.formatted("Plain")));

    assertThat(ids("MATCH (n:N) WHERE n.x = 2 RETURN n.id AS id ORDER BY id")).containsExactly(1L);
    assertThat(ids("MATCH (n:N) WHERE n.x = '2' RETURN n.id AS id ORDER BY id")).containsExactly(2L);
    assertThat(ids("MATCH (n:N) WHERE n.x > '1' RETURN n.id AS id ORDER BY id")).containsExactly(2L, 5L, 7L, 9L);
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
    return column(query, "x");
  }

  private List<Long> ids(final String query) {
    return column(query, "id");
  }

  private List<Long> column(final String query, final String column) {
    final List<Long> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        out.add(rs.next().<Number>getProperty(column).longValue());
    }
    return out;
  }
}
