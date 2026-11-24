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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12749: a MATCH node marked {@code optional: true} whose root estimate is 0 (an empty type, or a where: an index
 * answers with nothing) emptied the whole result, because the planner short-circuited to an empty plan on any estimate of 0.
 */
class MatchOptionalEmptyEstimateIssue12749Test extends TestHelper {

  @BeforeEach
  void graph() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE OB");
      database.command("sql", "CREATE VERTEX TYPE OA");
      database.command("sql", "CREATE PROPERTY OA.id INTEGER");
      database.command("sql", "CREATE INDEX ON OA (id) UNIQUE");
      database.command("sql", "CREATE VERTEX TYPE OEmpty");
      // SAME SHAPE AS OEmpty, BUT HOLDING ONE VERTEX NOTHING LINKS TO: ITS ESTIMATE IS NOT 0, SO IT TAKES THE NORMAL PLAN
      database.command("sql", "CREATE VERTEX TYPE OLone");
      database.command("sql", "CREATE VERTEX OLone");
      database.command("sql", "CREATE EDGE TYPE OR1");
      database.command("opencypher", "CREATE (b1:OB {id: 1}), (b2:OB {id: 2}), (a1:OA {id: 10}), (b1)-[:OR1]->(a1)");
    });
  }

  private List<String> rows(final String query) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Object e = r.getProperty("e");
        out.add(r.getProperty("b") + ":" + (e == null ? "null" : "set"));
      }
    }
    out.sort(null);
    return out;
  }

  @Test
  void optionalOutOnEmptyType() {
    assertThat(rows("MATCH {type: OB, as: b}-->{type: OEmpty, as: e, optional: true} RETURN b.id AS b, e AS e"))
        .containsExactly("1:null", "2:null");
  }

  @Test
  void optionalInOnEmptyType() {
    assertThat(rows("MATCH {type: OB, as: b}<--{type: OEmpty, as: e, optional: true} RETURN b.id AS b, e AS e"))
        .containsExactly("1:null", "2:null");
  }

  @Test
  void optionalAtEndOfTwoHopChain() {
    assertThat(rows("MATCH {type: OB, as: b}-OR1->{type: OA, as: a}-->{type: OEmpty, as: e, optional: true} RETURN b.id AS b, e AS e"))
        .containsExactly("1:null");
  }

  @Test
  void optionalWhereIndexAnswersNothing() {
    assertThat(rows("MATCH {type: OB, as: b}-->{type: OA, as: e, where: (id = 999), optional: true} RETURN b.id AS b, e AS e"))
        .containsExactly("1:null", "2:null");
  }

  @Test
  void optionalWhereIndexAnswersSomething() {
    assertThat(rows("MATCH {type: OB, as: b}-->{type: OA, as: e, where: (id = 10), optional: true} RETURN b.id AS b, e AS e"))
        .containsExactly("1:set", "2:null");
  }

  @Test
  void mandatoryNodeOnEmptyTypeStillAnswersNothing() {
    assertThat(rows("MATCH {type: OB, as: b}-->{type: OEmpty, as: e} RETURN b.id AS b, e AS e")).isEmpty();
    try (final ResultSet rs = database.query("sql", "EXPLAIN MATCH {type: OB, as: b}-->{type: OEmpty, as: e} RETURN b.id AS b, e AS e")) {
      final ExecutionPlan plan = rs.getExecutionPlan().get();
      assertThat(plan.getSteps()).anyMatch(s -> s instanceof EmptyStep);
    }
  }

  @Test
  void emptyTypeAnswersLikeANonEmptyTypeNothingReaches() {
    assertThat(rows("MATCH {type: OB, as: b}-->{type: OEmpty, as: e, optional: true} RETURN b.id AS b, e AS e"))
        .isEqualTo(rows("MATCH {type: OB, as: b}-->{type: OLone, as: e, optional: true} RETURN b.id AS b, e AS e"));
  }

  @Test
  void optionalBetweenTwoMandatoryNodesAnswersLikeANonEmptyType() {
    final List<String> emptyType = rows(
        "MATCH {type: OB, as: b}-->{type: OEmpty, as: e, optional: true}-->{type: OA, as: a} RETURN b.id AS b, e AS e");
    final List<String> lonelyType = rows(
        "MATCH {type: OB, as: b}-->{type: OLone, as: e, optional: true}-->{type: OA, as: a} RETURN b.id AS b, e AS e");
    assertThat(emptyType).isEqualTo(lonelyType);
  }

  @Test
  void optionalOnEmptyTypeWithMandatoryHopAfterTheRoot() {
    assertThat(rows("MATCH {type: OA, as: a}<-OR1-{type: OB, as: b}-->{type: OEmpty, as: e, optional: true} RETURN b.id AS b, e AS e"))
        .containsExactly("1:null");
  }

  @Test
  void notPatternOnEmptyTypeUnaffected() {
    assertThat(rows("MATCH {type: OB, as: b}, NOT {as: b}-->{type: OEmpty} RETURN b.id AS b, null AS e"))
        .containsExactly("1:null", "2:null");
  }
}
