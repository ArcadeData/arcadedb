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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #13466: {@code SET n.p = EXISTS((n)-[:R]->(:L))} was refused as a pattern expression in a
 * SET value. A pattern inside a function-style {@code exists()} is a boolean check, as in RETURN (issue #4993); a bare pattern
 * is still refused.
 */
class CypherSetExistsPatternIssue13466Test extends TestHelper {
  private void createGraph() {
    database.transaction(() -> database.command("opencypher", "CREATE (:U {id: 1})-[:R]->(:L {id: 9}), (:U {id: 2})").close());
  }

  private List<Object> flagsById() {
    final List<Object> flags = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (u:U) RETURN u.has AS has ORDER BY u.id")) {
      while (rs.hasNext())
        flags.add(rs.next().getProperty("has"));
    }
    return flags;
  }

  @Test
  void setStoresTheBooleanOfAnExistsPattern() {
    createGraph();

    database.transaction(() -> database.command("opencypher", "MATCH (u:U) SET u.has = EXISTS((u)-[:R]->(:L))").close());

    assertThat(flagsById()).containsExactly(true, false);
  }

  @Test
  void setStoresTheNegationOfAnExistsPattern() {
    createGraph();

    database.transaction(() -> database.command("opencypher", "MATCH (u:U) SET u.has = NOT exists((u)-[:R]->(:L))").close());

    assertThat(flagsById()).containsExactly(false, true);
  }

  @Test
  void aBarePatternInASetValueIsStillRefused() {
    createGraph();

    assertThatThrownBy(() -> database.command("opencypher", "MATCH (u:U) SET u.has = (u)-[:R]->(:L)").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Pattern expressions are not allowed in SET values");
  }

  @Test
  void aBarePatternBesideAnExistsPatternIsStillRefused() {
    createGraph();

    assertThatThrownBy(
        () -> database.command("opencypher", "MATCH (u:U) SET u.has = [EXISTS((u)-[:R]->(:L)), (u)-[:R]->(:L)]").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Pattern expressions are not allowed in SET values");
  }

  @Test
  void setOfADynamicPropertyStoresTheBooleanOfAnExistsPattern() {
    createGraph();

    database.transaction(() -> database.command("opencypher", "MATCH (u:U) SET u['has'] = exists((u)-[:R]->(:L))").close());

    assertThat(flagsById()).containsExactly(true, false);
  }

  @Test
  void aParenthesizedPatternIsStillTheWholeArgumentOfExists() {
    createGraph();

    database.transaction(() -> database.command("opencypher", "MATCH (u:U) SET u.has = exists(((u)-[:R]->(:L)))").close());

    assertThat(flagsById()).containsExactly(true, false);
  }

  @Test
  void anExistsPatternInsideALargerSetValueIsEvaluatedPerRow() {
    createGraph();

    database.transaction(() -> database.command("opencypher",
        "MATCH (u:U) SET u.has = CASE WHEN exists((u)-[:R]->(:L)) THEN 'y' ELSE 'n' END").close());

    assertThat(flagsById()).containsExactly("y", "n");
  }

  /**
   * exists() tests the pattern only when the pattern is its whole argument. A pattern that is a part of the argument used to be
   * picked out of it and tested alone, the list or the function around it silently dropped.
   */
  @Test
  void aPatternThatIsOnlyAPartOfTheExistsArgumentIsRefused() {
    createGraph();

    for (final String value : new String[] { "exists([(u)-[:R]->(:L)])", "exists(coalesce((u)-[:R]->(:L)))",
        "exists((u)-[:R]->(:L) = true)" }) {
      assertThatThrownBy(() -> database.command("opencypher", "MATCH (u:U) SET u.has = " + value).close())
          .as(value)
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("Pattern expressions are not allowed in SET values");
      assertThatThrownBy(() -> database.command("opencypher", "MATCH (u:U) SET u['has'] = " + value).close())
          .as(value)
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("Pattern expressions are not allowed in SET values");
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (u:U) RETURN " + value + " AS has").close())
          .as(value)
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("Pattern expressions are not allowed in RETURN projections");
    }
  }

  @Test
  void returnRefusesABarePatternBehindAnExistsPattern() {
    createGraph();

    assertThatThrownBy(
        () -> database.query("opencypher", "MATCH (u:U) RETURN exists((u)-[:R]->(:L)) AS a, [(u)-[:R]->(:L)] AS b").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Pattern expressions are not allowed in RETURN projections");
    assertThatThrownBy(
        () -> database.query("opencypher", "MATCH (u:U) RETURN [exists((u)-[:R]->(:L)), (u)-[:R]->(:L)] AS a").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Pattern expressions are not allowed in RETURN projections");
  }

  @Test
  void withAndLetProjectTheBooleanOfAnExistsPattern() {
    createGraph();

    database.transaction(
        () -> database.command("opencypher", "MATCH (u:U) WITH u, exists((u)-[:R]->(:L)) AS has SET u.has = has").close());
    assertThat(flagsById()).containsExactly(true, false);

    database.transaction(
        () -> database.command("opencypher", "MATCH (u:U) LET has = NOT exists((u)-[:R]->(:L)) SET u.has = has").close());
    assertThat(flagsById()).containsExactly(false, true);
  }

  @Test
  void withAndLetStillRefuseABarePattern() {
    createGraph();

    assertThatThrownBy(() -> database.command("opencypher", "MATCH (u:U) WITH u, (u)-[:R]->(:L) AS has SET u.has = 1").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Pattern expressions are not allowed in WITH projections");
    assertThatThrownBy(
        () -> database.command("opencypher", "MATCH (u:U) WITH u, exists([(u)-[:R]->(:L)]) AS has SET u.has = has").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Pattern expressions are not allowed in WITH projections");
    assertThatThrownBy(() -> database.command("opencypher", "MATCH (u:U) LET has = (u)-[:R]->(:L) SET u.has = 1").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Pattern expressions are not allowed in LET");
  }

  /**
   * In a WHERE the pattern of a CASE WHEN condition is a boolean, and exists() around the CASE tests the value of the CASE:
   * null (no branch taken) for the node without the relationship. Lowering the call to the pattern alone gave the same rows
   * here only by accident, so the THEN value is chosen to tell the two apart: the pattern matches node 1, the CASE is null
   * for it.
   */
  @Test
  void existsAroundACaseTestsTheValueOfTheCaseNotThePatternOfItsCondition() {
    createGraph();

    database.transaction(() -> database.command("opencypher",
        "MATCH (u:U) WHERE exists(CASE WHEN (u)-[:R]->(:L) THEN null ELSE 1 END) SET u.has = true").close());

    assertThat(flagsById()).containsExactly(null, true);
  }

  @Test
  void anExistsSubqueryWithAPatternPredicateIsNotABarePattern() {
    createGraph();

    database.transaction(() -> database.command("opencypher",
        "MATCH (u:U) SET u.has = EXISTS { MATCH (u)-[:R]->(x) WHERE (x)<-[:R]-() }").close());

    assertThat(flagsById()).containsExactly(true, false);
  }
}
