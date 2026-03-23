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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that SQL MATCH statements use GAV expand-into optimization (isConnectedTo)
 * when both endpoints are already bound.
 */
class MatchGAVExpandIntoTest extends TestHelper {

  @Test
  void matchExpandIntoWithGAV() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    // Build graph: A -> B -> C, A -> C
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex b = database.newVertex("Person").set("name", "Bob").save();
    final MutableVertex c = database.newVertex("Person").set("name", "Charlie").save();
    a.newEdge("KNOWS", b);
    b.newEdge("KNOWS", c);
    a.newEdge("KNOWS", c);
    database.commit();

    // Build GAV
    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(new String[] { "Person" }, new String[] { "KNOWS" });
    try {
      // Two-pattern MATCH where both endpoints get bound — the second pattern
      // should benefit from expand-into (isConnectedTo)
      database.begin();
      final ResultSet rs = database.query("sql",
          "MATCH {type: Person, as: a, where: (name = 'Alice')} -KNOWS-> {as: b}," +
          " {as: a} -KNOWS-> {as: c, where: (name = 'Charlie')} " +
          "RETURN a.name as aName, b.name as bName, c.name as cName");

      final List<String> bNames = new ArrayList<>();
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("aName")).isEqualTo("Alice");
        assertThat(row.<String>getProperty("cName")).isEqualTo("Charlie");
        bNames.add(row.getProperty("bName"));
      }
      // Alice knows Bob and Charlie, so b should be Bob and Charlie
      assertThat(bNames).containsExactlyInAnyOrder("Bob", "Charlie");
      database.commit();
    } finally {
      gav.drop();
    }
  }

  @Test
  void matchExpandIntoNotConnected() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    // Build graph: A -> B, C is isolated
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex b = database.newVertex("Person").set("name", "Bob").save();
    final MutableVertex c = database.newVertex("Person").set("name", "Charlie").save();
    a.newEdge("KNOWS", b);
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(new String[] { "Person" }, new String[] { "KNOWS" });
    try {
      // MATCH where Alice -KNOWS-> Charlie should return nothing (not connected)
      database.begin();
      final ResultSet rs = database.query("sql",
          "MATCH {type: Person, as: a, where: (name = 'Alice')} -KNOWS-> {as: b, where: (name = 'Charlie')} " +
          "RETURN a.name as aName, b.name as bName");
      assertThat(rs.hasNext()).isFalse();
      database.commit();
    } finally {
      gav.drop();
    }
  }

  @Test
  void matchSameResultsWithAndWithoutGAV() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    // Build graph: A -> B -> C
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex b = database.newVertex("Person").set("name", "Bob").save();
    final MutableVertex c = database.newVertex("Person").set("name", "Charlie").save();
    a.newEdge("KNOWS", b);
    b.newEdge("KNOWS", c);
    database.commit();

    final String query = "MATCH {type: Person, as: a, where: (name = 'Alice')} -KNOWS-> {as: b} -KNOWS-> {as: c} " +
        "RETURN a.name as aName, b.name as bName, c.name as cName";

    // Query without GAV
    database.begin();
    final ResultSet rsWithout = database.query("sql", query);
    final List<String> withoutResults = new ArrayList<>();
    while (rsWithout.hasNext()) {
      final Result row = rsWithout.next();
      withoutResults.add(row.getProperty("aName") + "->" + row.getProperty("bName") + "->" + row.getProperty("cName"));
    }
    database.commit();

    // Build GAV and query again
    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(new String[] { "Person" }, new String[] { "KNOWS" });
    try {
      database.begin();
      final ResultSet rsWith = database.query("sql", query);
      final List<String> withResults = new ArrayList<>();
      while (rsWith.hasNext()) {
        final Result row = rsWith.next();
        withResults.add(row.getProperty("aName") + "->" + row.getProperty("bName") + "->" + row.getProperty("cName"));
      }
      database.commit();

      assertThat(withResults).containsExactlyInAnyOrderElementsOf(withoutResults);
      assertThat(withResults).containsExactly("Alice->Bob->Charlie");
    } finally {
      gav.drop();
    }
  }
}
