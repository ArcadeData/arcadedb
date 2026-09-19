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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the two SQL shapes Studio's graph-expansion picker is built on (issue #7847).
 * <p>
 * The picker lives in JavaScript, where a test can only assert on the text of the command it builds. What that
 * text MEANS is an engine question, and if either shape stops working the picker fails in the browser with an
 * error a Studio test would never have seen. So the shapes are pinned here, next to the engine that answers
 * them, and the JS test that asserts the builder produces them names this class.
 * <ul>
 *   <li>the per-type count, which is what lets the picker show how big a choice is WITHOUT fetching a
 *       supernode's edges in order to count them;</li>
 *   <li>the filtered expansion, whose edge type names arrive as named PARAMETERS - the reason the builder needs
 *       no string-escaping convention, and the reason a type name containing a quote cannot break it.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7847EdgeExpansionQueriesTest extends TestHelper {

  private String hub;

  private void buildGraph() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Knows");
    database.command("sql", "CREATE EDGE TYPE Bought");
    // A type name carrying a quote: free-form names are exactly why the builder passes them as parameters.
    database.command("sql", "CREATE EDGE TYPE `it's`");

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX Person SET name = 'hub'");
      database.command("sql", "CREATE VERTEX Person SET name = 'a'");
      database.command("sql", "CREATE VERTEX Person SET name = 'b'");
      database.command("sql", "CREATE VERTEX Person SET name = 'c'");

      edge("Knows", "hub", "a");
      edge("Knows", "hub", "b");
      edge("Knows", "hub", "c");
      edge("Bought", "hub", "a");
      edge("it's", "hub", "b");
      // One incoming edge, so the two directions cannot be confused with each other.
      edge("Knows", "a", "hub");
    });

    hub = database.query("sql", "SELECT @rid AS r FROM Person WHERE name = 'hub'").next().getProperty("r").toString();
  }

  private void edge(final String type, final String from, final String to) {
    database.command("sql", "CREATE EDGE `" + type + "` FROM (SELECT FROM Person WHERE name = ?) "
        + "TO (SELECT FROM Person WHERE name = ?)", from, to);
  }

  /** The counts the picker renders, per direction, aggregated by the server. */
  @Test
  void theEdgeTypeCountIsAggregatedPerTypeAndPerDirection() {
    buildGraph();

    assertThat(countsOf("out")).containsExactlyInAnyOrderEntriesOf(
        Map.of("Knows", 3L, "Bought", 1L, "it's", 1L));
    assertThat(countsOf("in")).containsExactlyInAnyOrderEntriesOf(Map.of("Knows", 1L));
  }

  /** A node with no edges in a direction answers nothing, which is what makes the picker's empty state real. */
  @Test
  void aNodeWithNoEdgesInADirectionAnswersNoRows() {
    buildGraph();
    final String leaf = database.query("sql", "SELECT @rid AS r FROM Person WHERE name = 'c'").next()
        .<Object>getProperty("r").toString();

    final ResultSet rs = database.query("sql",
        "select @type as type, count(*) as total from (select expand( outE() ) from " + leaf + ") group by @type");
    assertThat(rs.hasNext()).isFalse();
  }

  /** The filtered expansion: only the chosen types, named by parameter. */
  @Test
  void theExpansionTakesItsEdgeTypesAsNamedParameters() {
    buildGraph();

    final Map<String, Object> params = new HashMap<>();
    params.put("t0", "Knows");
    params.put("t1", "Bought");

    final ResultSet rs = database.query("sql", "select expand( outE(:t0, :t1) ) from " + hub, params);
    int count = 0;
    while (rs.hasNext()) {
      assertThat(typeNameOf(rs.next())).isIn("Knows", "Bought");
      count++;
    }
    assertThat(count).as("3 Knows + 1 Bought, and not the it's edge").isEqualTo(4);
  }

  /**
   * The reason the names are parameters and not quoted literals: a type name can contain a quote, and the
   * escaping convention for a SQL string literal here is backslash rather than doubling - a detail a builder
   * gets wrong once and then carries.
   */
  @Test
  void anEdgeTypeNameCarryingAQuoteNeedsNoEscapingAtAll() {
    buildGraph();

    final ResultSet rs = database.query("sql", "select expand( outE(:t0) ) from " + hub, Map.of("t0", "it's"));
    assertThat(rs.hasNext()).isTrue();
    assertThat(typeNameOf(rs.next())).isEqualTo("it's");
    assertThat(rs.hasNext()).isFalse();
  }

  /** The ceiling the picker offers, which is what keeps a supernode from flooding the canvas. */
  @Test
  void theExpansionCanBeBounded() {
    buildGraph();

    final ResultSet rs = database.query("sql", "select expand( outE() ) from " + hub + " limit 2");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  /** The edge's type. Read off the element, not as an "@type" property: a Result exposes the record's own. */
  private static String typeNameOf(final Result row) {
    return row.getElement().orElseThrow().getTypeName();
  }

  private Map<String, Long> countsOf(final String direction) {
    final Map<String, Long> counts = new LinkedHashMap<>();
    final ResultSet rs = database.query("sql", "select @type as type, count(*) as total from "
        + "(select expand( " + direction + "E() ) from " + hub + ") group by @type");
    while (rs.hasNext()) {
      final Result row = rs.next();
      counts.put(row.getProperty("type"), ((Number) row.getProperty("total")).longValue());
    }
    return counts;
  }
}
