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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5293: the IN operator must use the same equality semantics as the =
 * operator. IN had its own hand-rolled comparator that tried {@code a.equals(b)} before dispatching on
 * type, so it inherited Java's Double.equals contract where NaN equals itself: {@code NaN IN [NaN]}
 * returned true while {@code NaN = NaN} returned false. Neo4j and Memgraph both return false for both.
 * The same divergence made IN disagree with = on maps holding nulls (Java's Map.equals treats
 * null == null as true, Cypher requires 3VL null propagation) and on array-typed elements.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherInEqualitySemanticsIssue5293Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-in5293").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private Result querySingleRow(final String cypher) {
    final ResultSet rs = database.query("opencypher", cypher);
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(rs.hasNext()).isFalse();
    return row;
  }

  @Test
  void nanInListOfNanMustMatchEqualsSemantics() {
    // The exact reproducer from the issue: both must be false, like Neo4j and Memgraph.
    final Result row = querySingleRow(
        "RETURN sqrt(-1) = sqrt(-1) AS equalsResult, sqrt(-1) IN [sqrt(-1)] AS inResult");
    assertThat((Boolean) row.getProperty("equalsResult")).isFalse();
    assertThat((Boolean) row.getProperty("inResult")).isFalse();
  }

  @Test
  void nanNotInListOfNanIsTrue() {
    final Result row = querySingleRow("RETURN NOT (sqrt(-1) IN [sqrt(-1)]) AS result");
    assertThat((Boolean) row.getProperty("result")).isTrue();
  }

  @Test
  void nanInNonNanListIsFalse() {
    final Result row = querySingleRow("RETURN sqrt(-1) IN [1.0, 2.0, 3.0] AS result");
    assertThat((Boolean) row.getProperty("result")).isFalse();
  }

  @Test
  void nanInListAlsoContainingNanAmongNumbersIsFalse() {
    final Result row = querySingleRow("RETURN sqrt(-1) IN [1.0, sqrt(-1), 3.0] AS result");
    assertThat((Boolean) row.getProperty("result")).isFalse();
  }

  @Test
  void nanInsideNestedListIsFalse() {
    // List equality is element-wise, so a NaN element must make the enclosing lists unequal too.
    final Result row = querySingleRow("RETURN [1.0, sqrt(-1)] IN [[1.0, sqrt(-1)]] AS result");
    assertThat((Boolean) row.getProperty("result")).isFalse();
  }

  @Test
  void storedNanPropertyIsNotInListOfItself() {
    database.transaction(() -> database.command("opencypher", "CREATE (:NaNTest {value: sqrt(-1)})"));

    // Control: ordinary equality does not consider the stored NaN equal to itself.
    final ResultSet equalsRs = database.query("opencypher",
        "MATCH (n:NaNTest) WHERE n.value = n.value RETURN n.value AS v");
    assertThat(equalsRs.hasNext()).isFalse();

    // IN must agree with =.
    final ResultSet inRs = database.query("opencypher",
        "MATCH (n:NaNTest) WHERE n.value IN [n.value] RETURN n.value AS v");
    assertThat(inRs.hasNext()).isFalse();
  }

  @Test
  void mapWithNullValueInListPropagatesNullLikeEquals() {
    // Java's Map.equals treats null == null as true; Cypher must propagate null (3VL).
    final Result row = querySingleRow(
        "RETURN {a: null} = {a: null} AS equalsResult, {a: null} IN [{a: null}] AS inResult");
    assertThat((Boolean) row.getProperty("equalsResult")).isNull();
    assertThat((Boolean) row.getProperty("inResult")).isNull();
  }

  @Test
  void positiveAndNegativeZeroAreEqualUnderIn() {
    // Java's Double.equals says -0.0 != 0.0, but Cypher (and =) compare them numerically as equal.
    final Result row = querySingleRow(
        "RETURN 0.0 = -0.0 AS equalsResult, -0.0 IN [0.0] AS inResult");
    assertThat((Boolean) row.getProperty("equalsResult")).isTrue();
    assertThat((Boolean) row.getProperty("inResult")).isTrue();
  }

  @Test
  void nonNanMembershipStillWorks() {
    final Result row = querySingleRow(
        "RETURN 5 IN [1, 5, 9] AS found, 7 IN [1, 5, 9] AS notFound, 1 IN [1.0] AS crossNumeric, "
            + "'b' IN ['a', 'b'] AS str, true IN [false, true] AS bool");
    assertThat((Boolean) row.getProperty("found")).isTrue();
    assertThat((Boolean) row.getProperty("notFound")).isFalse();
    assertThat((Boolean) row.getProperty("crossNumeric")).isTrue();
    assertThat((Boolean) row.getProperty("str")).isTrue();
    assertThat((Boolean) row.getProperty("bool")).isTrue();
  }

  @Test
  void numberIsNotEqualToItsStringForm() {
    // Cypher TCK invariant: 5 IN ["5"] is false, consistently with 5 = "5".
    final Result row = querySingleRow("RETURN 5 = '5' AS equalsResult, 5 IN ['5'] AS inResult");
    assertThat((Boolean) row.getProperty("equalsResult")).isFalse();
    assertThat((Boolean) row.getProperty("inResult")).isFalse();
  }

  @Test
  void nullSemanticsOfInArePreserved() {
    final Result row = querySingleRow(
        "RETURN null IN [1, 2] AS nullLeft, 5 IN [1, null, 3] AS nullElementNotFound, "
            + "1 IN [1, null] AS nullElementFound, 3 IN null AS nullList");
    assertThat((Boolean) row.getProperty("nullLeft")).isNull();
    assertThat((Boolean) row.getProperty("nullElementNotFound")).isNull();
    assertThat((Boolean) row.getProperty("nullElementFound")).isTrue();
    assertThat((Boolean) row.getProperty("nullList")).isNull();
  }

  @Test
  void listMembershipWithNestedListsStillWorks() {
    final Result row = querySingleRow(
        "RETURN [1, 2] IN [[1, 2], [3]] AS found, [1, 2] IN [[2, 1]] AS notFound, "
            + "[1, null] IN [[1, null]] AS nullElement");
    assertThat((Boolean) row.getProperty("found")).isTrue();
    assertThat((Boolean) row.getProperty("notFound")).isFalse();
    assertThat((Boolean) row.getProperty("nullElement")).isNull();
  }

  @Test
  void inWithParameterListStillWorks() {
    final ResultSet rs = database.query("opencypher", "RETURN 2 IN $values AS result",
        Map.of("values", List.of(1, 2, 3)));
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isTrue();
  }

  @Test
  void whereInFiltersRowsConsistentlyWithEquals() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Item {name: 'a'}), (:Item {name: 'b'}), (:Item {name: 'c'})"));

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Item) WHERE n.name IN ['a', 'c'] RETURN n.name AS name ORDER BY name");
    assertThat(rs.hasNext()).isTrue();
    assertThat((String) rs.next().getProperty("name")).isEqualTo("a");
    assertThat(rs.hasNext()).isTrue();
    assertThat((String) rs.next().getProperty("name")).isEqualTo("c");
    assertThat(rs.hasNext()).isFalse();
  }
}
