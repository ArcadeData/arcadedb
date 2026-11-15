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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11933: openCypher's {@code =} reads a text that spells a RID against a number as the id {@code id()}
 * encodes it to ({@code 1 = '#0:1'}, the id()/RID-text interop of issue #4183), while an index seek on a numeric property looked the
 * text up as a key and found nothing. The seek answered no rows where the scan of the same statement answered the vertex. The seek
 * now looks up the number the text stands for; a text column still looks up the text.
 * <p>
 * Every statement runs on {@code P}, whose properties are indexed, and on its twin {@code Q}, which has no index and can only be
 * scanned: the two have to answer alike.
 */
class CypherRidTextIndexSeekIssue11933Test extends TestHelper {
  private static final int VERTICES = 40;

  private static final Map<String, Object> PARAMS = Map.of("rid", "#0:1", "ids", List.of("#0:1", "#0:2", 3L));

  @BeforeEach
  void createGraph() {
    for (final String[] types : new String[][] { { "P", "R" }, { "Q", "S" } }) {
      final String vertex = types[0];
      database.command("sql", "CREATE VERTEX TYPE " + vertex);
      database.command("sql", "CREATE EDGE TYPE " + types[1]);
      database.command("sql", "CREATE PROPERTY " + types[1] + ".w LONG");
      for (final String property : List.of("id LONG", "g LONG", "f DOUBLE", "i INTEGER", "s STRING", "a LONG", "b LONG"))
        database.command("sql", "CREATE PROPERTY " + vertex + "." + property);
    }
    database.command("sql", "CREATE INDEX ON P (id) UNIQUE");
    database.command("sql", "CREATE INDEX ON P (g) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON P (f) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON P (i) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON P (s) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON P (a, b) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON R (w) NOTUNIQUE");
    database.transaction(() -> {
      for (final String[] types : new String[][] { { "P", "R" }, { "Q", "S" } }) {
        for (int n = 0; n < VERTICES; n++)
          database.command("opencypher", "CREATE (:" + types[0] + " {id: $n, g: $g, f: $f, i: $n, s: $s, a: $a, b: $b})",
              Map.of("n", n, "g", n % 10, "f", n * 0.5, "s", n == 7 ? "#0:1" : "s" + n, "a", n % 4, "b", n % 3)).close();
        database.command("opencypher",
            "MATCH (x:" + types[0] + "), (y:" + types[0] + ") WHERE y.id = x.id + 1 CREATE (x)-[:" + types[1] + " {w: x.id}]->(y)")
            .close();
      }
    });
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "MATCH (p:{V}) WHERE p.id = '#0:1' RETURN p.id AS v",
      "MATCH (p:{V}) WHERE '#0:1' = p.id RETURN p.id AS v",
      "MATCH (p:{V} {id: '#0:1'}) RETURN p.id AS v",
      "MATCH (p:{V}) WHERE p.id = $rid RETURN p.id AS v",
      "MATCH (p:{V} {id: $rid}) RETURN p.id AS v",
      "MATCH (p:{V}) WHERE p.id IN ['#0:1', '#0:2', 3] RETURN p.id AS v",
      "MATCH (p:{V}) WHERE p.id IN $ids RETURN p.id AS v",
      "MATCH (p:{V}) WHERE p.id IN ['#0:1', 1] RETURN p.id AS v",
      "MATCH (p:{V}) WHERE p.g = '#0:1' RETURN p.id AS v",
      "MATCH (p:{V}) WHERE p.i = '#0:2' RETURN p.id AS v",
      "MATCH (p:{V}) WHERE p.f = '#0:1' RETURN p.id AS v",
      "MATCH (p:{V}) WHERE p.s = '#0:1' RETURN p.id AS v",
      "MATCH (p:{V}) WHERE p.a = '#0:1' AND p.b = '#0:2' RETURN p.id AS v",
      "MATCH (p:{V}) WHERE p.a = '#0:1' RETURN p.id AS v",
      "MATCH (p:{V}) WHERE p.id = '#0:1' OR p.g = '#0:2' RETURN p.id AS v",
      "OPTIONAL MATCH (p:{V}) WHERE p.id = '#0:1' RETURN p.id AS v",
      "OPTIONAL MATCH (p:{V}) WHERE p.id IN ['#0:1', '#0:2'] RETURN p.id AS v",
      "UNWIND ['#0:1', '#0:2'] AS r MATCH (p:{V}) WHERE p.id = r RETURN p.id AS v",
      "MATCH (p:{V} {id: '#0:1'})-[:{E}]->() RETURN count(*) AS v",
      "MATCH (p:{V})-[:{E}]->() WHERE p.id = '#0:1' RETURN count(*) AS v",
      "MATCH ()-[:{E}]->(p:{V}) WHERE p.id = '#0:2' RETURN count(*) AS v",
      "MATCH ()-[r:{E}]->() WHERE r.w = '#0:1' RETURN r.w AS v",
      "MATCH ()-[r:{E}]->() WHERE r.w IN ['#0:1', '#0:2'] RETURN r.w AS v" })
  void theSeekAnswersAsTheScan(final String template) {
    final List<Object> seek = column(template.replace("{V}", "P").replace("{E}", "R"));
    final List<Object> scan = column(template.replace("{V}", "Q").replace("{E}", "S"));
    assertThat(scan).as("the scan of %s", template).isNotEmpty();
    assertThat(seek).as(template).isEqualTo(scan);
  }

  /**
   * A property map matched per input row compares through {@code InlineProperties}, which does not read a RID spelled as text as an
   * id: the scan finds nothing there, and the seek of that path, which keeps the text as the key, finds nothing either.
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "UNWIND ['#0:1', '#0:2'] AS r MATCH (p:{V} {id: r}) RETURN p.id AS v",
      "MATCH (p:{V} {id: '#0:1'})-[:{E}]->(q) RETURN q.id AS v" })
  void aPropertyMapMatchedPerRowAnswersAsItsScan(final String template) {
    assertThat(column(template.replace("{V}", "P").replace("{E}", "R"))).as(template)
        .isEqualTo(column(template.replace("{V}", "Q").replace("{E}", "S")));
  }

  @Test
  void theSeekLooksUpTheIdTheTextStandsFor() {
    assertThat(column("MATCH (p:P) WHERE p.id = '#0:1' RETURN p.id AS v")).containsExactly(1L);
    assertThat(column("MATCH (p:P) WHERE p.s = '#0:1' RETURN p.id AS v")).containsExactly(7L);
    // The unique key pins the vertex the count starts from (issue #11594): vertex 1 has one outgoing relationship
    assertThat(column("MATCH (p:P)-[:R]->() WHERE p.id = '#0:1' RETURN count(*) AS v")).containsExactly(1L);
    assertThat(column("MATCH (p:Q)-[:S]->() WHERE p.id = '#0:1' RETURN count(*) AS v")).containsExactly(1L);
    try (final ResultSet rs = database.query("opencypher", "PROFILE MATCH (p:P) WHERE p.id = '#0:1' RETURN p.id AS v")) {
      while (rs.hasNext())
        rs.next();
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("NodeIndexSeek").doesNotContain("NodeByLabelScan");
    }
  }

  /** The text stands for the id it encodes, compared as any number is: a FLOAT is not truncated to an integer first. */
  @Test
  void aFloatIsComparedWithTheIdAsANumber() {
    final Result row = single(
        "RETURN 1 = '#0:1' AS a, 1.0 = '#0:1' AS b, 1.5 = '#0:1' AS c, '#0:1' = 1.5 AS d, 1.5 > '#0:1' AS e, 0.5 < '#0:1' AS f, "
            + "1.5 IN ['#0:1'] AS g, 1.0 IN ['#0:1'] AS h");
    assertThat(row.<Boolean>getProperty("a")).isTrue();
    assertThat(row.<Boolean>getProperty("b")).isTrue();
    assertThat(row.<Boolean>getProperty("c")).isFalse();
    assertThat(row.<Boolean>getProperty("d")).isFalse();
    assertThat(row.<Boolean>getProperty("e")).isTrue();
    assertThat(row.<Boolean>getProperty("f")).isTrue();
    assertThat(row.<Boolean>getProperty("g")).isFalse();
    assertThat(row.<Boolean>getProperty("h")).isTrue();
  }

  private Result single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next();
    }
  }

  private List<Object> column(final String query) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, PARAMS)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty("v"));
    }
    values.sort((x, y) -> x == null ? (y == null ? 0 : -1) : y == null ? 1 : ((Comparable<Object>) x).compareTo(y));
    return values;
  }
}
