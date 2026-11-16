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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12245: the WHERE of a WITH that aggregates (or is DISTINCT) read a variable the projection collapsed as null, so
 * {@code WITH n.b AS v, count(*) AS c WHERE n.b IS NOT NULL} filtered every row out. A WHERE expression that repeats a
 * grouping key (or reads a property of a projected variable) resolves against the projected column, as Neo4j does; a
 * reference to a collapsed variable that is no grouping key is refused instead of being read as null.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12245WithCollapsingWhereScopeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Node {id: 1, b: true}), (:Node {id: 2, b: false}), (:Node {id: 3, b: true})"));
  }

  @Test
  void whereOnTheGroupingKeyExpressionAfterCountKeepsTheGroups() {
    assertThat(rows("MATCH (n:Node) WITH n.b AS v, count(*) AS c WHERE n.b IS NOT NULL RETURN v, c ORDER BY v"))//
        .containsExactly("false:1", "true:2");
  }

  @Test
  void whereOnTheProjectedAliasIsUnchanged() {
    assertThat(rows("MATCH (n:Node) WITH n.b AS v, count(*) AS c WHERE v IS NOT NULL RETURN v, c ORDER BY v"))//
        .containsExactly("false:1", "true:2");
  }

  @Test
  void whereBuiltOnTheGroupingKeyFiltersTheGroups() {
    assertThat(rows("MATCH (n:Node) WITH n.b AS v, count(*) AS c WHERE n.b = true RETURN v, c"))//
        .containsExactly("true:2");
    assertThat(rows("MATCH (n:Node) WITH n.b AS v, count(*) AS c WHERE NOT n.b RETURN v, c"))//
        .containsExactly("false:1");
    assertThat(rows("MATCH (n:Node) WITH n.b AS v, count(*) AS c WHERE n.b IN [false] AND c > 0 RETURN v, c"))//
        .containsExactly("false:1");
  }

  @Test
  void whereRepeatingAProjectedPredicateReadsItsColumn() {
    final List<Object> counts = new ArrayList<>();
    database.query("opencypher", "MATCH (n:Node) WITH n.id > 1 AS big, count(*) AS c WHERE n.id > 1 RETURN big, c").stream()
        .forEach(r -> counts.add(r.getProperty("big") + ":" + r.getProperty("c")));
    assertThat(counts).containsExactly("true:2");
  }

  @Test
  void aLiteralFirstPredicateStillMatchesItsProjection() {
    // The WHERE is canonicalized to n.id > 1: the rewrite has to run on the predicate as written
    final List<Object> counts = new ArrayList<>();
    database.query("opencypher", "MATCH (n:Node) WITH 1 < n.id AS big, count(*) AS c WHERE 1 < n.id RETURN big, c").stream()
        .forEach(r -> counts.add(r.getProperty("big") + ":" + r.getProperty("c")));
    assertThat(counts).containsExactly("true:2");
  }

  @Test
  void aGroupingKeyInsideAListLiteralReadsItsColumn() {
    assertThat(rows("MATCH (n:Node) WITH n.b AS v, count(*) AS c WHERE true IN [n.b] RETURN v, c")).containsExactly("true:2");
  }

  @Test
  void aProjectedAliasThatReusesAnIncomingNameKeepsItsMeaning() {
    // b in the WHERE is the column b (from a), not the incoming b that is projected as c
    final List<String> values = new ArrayList<>();
    database.query("opencypher", "WITH 1 AS a, 2 AS b WITH DISTINCT a AS b, b AS c WHERE b = 1 RETURN b, c").stream()
        .forEach(r -> values.add(r.getProperty("b") + ":" + r.getProperty("c")));
    assertThat(values).containsExactly("1:2");
  }

  @Test
  void whereReadingAPropertyOfAProjectedVariableFollowsItsColumn() {
    final List<Object> ids = new ArrayList<>();
    database.query("opencypher", "MATCH (n:Node) WITH n AS m, count(*) AS c WHERE n.id > 1 RETURN m.id AS id ORDER BY id")
        .stream().forEach(r -> ids.add(r.getProperty("id")));
    assertThat(ids).containsExactly(2, 3);
  }

  @Test
  void whereAfterDistinctOnTheProjectedExpressionKeepsTheRows() {
    final List<Object> values = new ArrayList<>();
    database.query("opencypher", "MATCH (n:Node) WITH DISTINCT n.b AS v WHERE n.b IS NOT NULL RETURN v ORDER BY v").stream()
        .forEach(r -> values.add(r.getProperty("v")));
    assertThat(values).containsExactly(false, true);
  }

  @Test
  void whereOnACollapsedVariableThatIsNoGroupingKeyIsRefused() {
    assertThatThrownBy(() -> rows("MATCH (n:Node) WITH n.b AS v, count(*) AS c WHERE n.id > 1 RETURN v, c"))//
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("'n'");
    assertThatThrownBy(() -> rows("MATCH (n:Node) WITH DISTINCT n.b AS v WHERE n.id > 1 RETURN v, 0 AS c"))//
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("'n'");
  }

  @Test
  void whereOfAPlainWithStillSeesTheIncomingVariables() {
    final List<Object> values = new ArrayList<>();
    database.query("opencypher", "MATCH (n:Node) WITH n.b AS v WHERE n.b IS NOT NULL RETURN v ORDER BY v").stream()
        .forEach(r -> values.add(r.getProperty("v")));
    assertThat(values).containsExactly(false, true, true);
  }

  @Test
  void whereOfAnyWithNamingAnUnboundVariableIsRefused() {
    assertThatThrownBy(() -> rows("MATCH (n:Node) WITH n.b AS v WHERE zz.b IS NOT NULL RETURN v, 0 AS c"))//
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("'zz'");
    assertThatThrownBy(() -> rows("MATCH (n:Node) WITH *, n.b AS v WHERE zz.b IS NOT NULL RETURN v, 0 AS c"))//
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("'zz'");
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    for (final Result r : database.query("opencypher", query).stream().toList())
      rows.add(r.getProperty("v") + ":" + r.getProperty("c"));
    return rows;
  }
}
