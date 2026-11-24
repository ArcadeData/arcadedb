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
import com.arcadedb.database.Document;
import com.arcadedb.query.opencypher.executor.steps.FinalProjectionStep;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the final projection of a RETURN says about its rows: a row that holds exactly one node or relationship and nothing else is
 * that element (with the name it was returned under, which the wire protocols answer as the field name), and a row that holds
 * anything more is a plain row; for {@code RETURN *} the variables come sorted by name and the executor's own bindings stay out.
 */
class CypherReturnProjectionElementContractTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE B");
    database.command("sql", "CREATE EDGE TYPE R");
    database.transaction(() -> database.command("opencypher", "CREATE (:A {x: 1})-[:R {w: 5}]->(:B {y: 2})"));
  }

  @Test
  void aSingleReturnedNodeIsTheElementUnderItsName() {
    final Result named = only("MATCH (n:A) RETURN n");
    assertThat(named.isElement()).isTrue();
    assertThat(named.getElement().orElseThrow().getTypeName()).isEqualTo("A");
    assertThat(named.<Object>getMetadata(FinalProjectionStep.PROJECTION_NAME_METADATA)).isEqualTo("n");

    final Result renamed = only("MATCH (n:A) RETURN n AS node");
    assertThat(renamed.isElement()).isTrue();
    assertThat(renamed.<Object>getMetadata(FinalProjectionStep.PROJECTION_NAME_METADATA)).isEqualTo("node");

    final Result star = only("MATCH (n:A) RETURN *");
    assertThat(star.isElement()).isTrue();
    assertThat(star.getElement().orElseThrow().getTypeName()).isEqualTo("A");
    assertThat(star.<Object>getMetadata(FinalProjectionStep.PROJECTION_NAME_METADATA)).isEqualTo("n");
    assertThat(star.getPropertyNames()).containsExactly("n");

    final Result edge = only("MATCH ()-[r:R]->() RETURN r");
    assertThat(edge.isElement()).isTrue();
    assertThat(((Document) edge.getElement().orElseThrow()).getTypeName()).isEqualTo("R");
  }

  @Test
  void aRowOfMoreThanOneThingIsAPlainRow() {
    // two nodes
    final Result two = only("MATCH (a:A), (b:B) RETURN a, b");
    assertThat(two.isElement()).isFalse();
    assertThat(two.getPropertyNames()).containsExactly("a", "b");

    // a node and a value
    final Result mixed = only("MATCH (n:A) RETURN n, n.x AS x");
    assertThat(mixed.isElement()).isFalse();
    assertThat(mixed.getPropertyNames()).containsExactly("n", "x");

    // a node alone, but as a property of a value: not the node
    assertThat(only("MATCH (n:A) RETURN n.x AS x").isElement()).isFalse();
    assertThat(only("MATCH (n:A) RETURN [n] AS list").isElement()).isFalse();
  }

  @Test
  void returnStarOfSeveralVariablesIsAPlainRowSortedByName() {
    final Result two = only("MATCH (b:B), (a:A) RETURN *");
    assertThat(two.isElement()).isFalse();
    assertThat(two.getPropertyNames()).containsExactly("a", "b");

    final Result path = only("MATCH (z:A)-[r:R]->(m:B) RETURN *");
    assertThat(path.isElement()).isFalse();
    assertThat(path.getPropertyNames()).containsExactly("m", "r", "z");

    // a node and a value in scope: more than the node is projected
    final Result withValue = only("MATCH (n:A) WITH n, 7 AS seven RETURN *");
    assertThat(withValue.isElement()).isFalse();
    assertThat(withValue.getPropertyNames()).containsExactly("n", "seven");

    // a scalar alone is no element
    final Result scalar = only("WITH 7 AS seven RETURN *");
    assertThat(scalar.isElement()).isFalse();
    assertThat(scalar.getPropertyNames()).containsExactly("seven");
  }

  @Test
  void aStarAmongItemsKeepsTheVariablesThenTheItemsInWrittenOrder() {
    final Result row = only("MATCH (b:B), (a:A) RETURN *, a.x + 1 AS next, b.y AS why");
    assertThat(row.isElement()).isFalse();
    assertThat(row.getPropertyNames()).containsExactly("a", "b", "next", "why");
    assertThat(row.<Number>getProperty("next").intValue()).isEqualTo(2);
    assertThat(row.<Number>getProperty("why").intValue()).isEqualTo(2);
  }

  @Test
  void aReturnedColumnThatIsAbsentIsNotInTheRow() {
    final Result row = only("OPTIONAL MATCH (n:A {x: 99}) RETURN n");
    assertThat(row.isElement()).isFalse();
    assertThat((Object) row.getProperty("n")).isNull();
  }

  private Result only(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    assertThat(rows).as(cypher).hasSize(1);
    return rows.getFirst();
  }
}
