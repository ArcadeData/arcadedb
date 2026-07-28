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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #5490.
 * <p>
 * A relationship inline {@code WHERE} predicate on a variable-length pattern, the
 * {@code WHERE r.tag = 'ok'} in {@code -[r:E*1..2 WHERE r.tag = 'ok']->}, was discarded: neither the
 * {@code MATCH} executor nor the {@code EXISTS {}} evaluator handed it to the traverser, so
 * {@code WHERE false} still returned every row. The fixed-length spelling and the variable-length
 * property-map spelling were unaffected, as were pattern comprehensions.
 * <p>
 * Semantics: as with the inline property map, every relationship traversed by the path must satisfy
 * the predicate, which is the same rule as the clause-level {@code all(e IN r WHERE ...)} spelling.
 */
class Issue5490VarLengthRelInlineWhereTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-5490-varlen-rel-inline-where").create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:A {v:1}), (b:A {v:10}), (c:A {v:2}), (d:A {v:7}), (e:A {v:8})");
      database.command("opencypher", "MATCH (a:A {v:1}), (b:A {v:10}), (c:A {v:2}), (d:A {v:7}), (e:A {v:8}) "
          + "CREATE (a)-[:E {tag:'ok', w:5}]->(b), (a)-[:E {tag:'bad', w:99}]->(c), "
          + "(b)-[:E {tag:'ok', w:1}]->(d), (c)-[:E {tag:'ok', w:2}]->(e)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).as("query returned no row: %s", query).isTrue();
      final Number n = rs.next().getProperty("c");
      return n == null ? 0L : n.longValue();
    }
  }

  private List<Integer> queryInts(final String query, final String field) {
    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Number n = row.getProperty(field);
        values.add(n == null ? null : n.intValue());
      }
    }
    return values;
  }

  // ---------------------------------------------------------------- MATCH, variable length

  @Test
  void matchVariableLengthAppliesTheRelationshipInlineWhere() {
    assertThat(count("MATCH (a:A {v:1})-[r:E*1..1 WHERE r.tag = 'ok']->(x:A) RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void matchVariableLengthHonorsAnAlwaysFalsePredicate() {
    assertThat(count("MATCH (a:A {v:1})-[r:E*1..1 WHERE false]->(x:A) RETURN count(*) AS c")).isZero();
  }

  @Test
  void matchVariableLengthHonorsAnAlwaysTruePredicate() {
    assertThat(count("MATCH (a:A {v:1})-[r:E*1..1 WHERE true]->(x:A) RETURN count(*) AS c")).isEqualTo(2);
  }

  /**
   * Every relationship of the path must satisfy the predicate. From a the all-'ok' paths are
   * a-&gt;b (one hop) and a-&gt;b-&gt;d (two hops). The a-&gt;c edge is 'bad', so neither a-&gt;c nor
   * a-&gt;c-&gt;e survives even though the second edge of the latter is 'ok'.
   */
  @Test
  void matchVariableLengthRequiresEveryRelationshipToSatisfyThePredicate() {
    assertThat(queryInts("MATCH (a:A {v:1})-[r:E*1..2 WHERE r.tag = 'ok']->(x:A) RETURN x.v AS v ORDER BY v", "v"))
        .containsExactly(7, 10);
  }

  @Test
  void matchUnboundedVariableLengthAppliesThePredicate() {
    assertThat(queryInts("MATCH (a:A {v:1})-[r:E* WHERE r.tag = 'ok']->(x:A) RETURN x.v AS v ORDER BY v", "v"))
        .containsExactly(7, 10);
  }

  @Test
  void matchVariableLengthPredicateSeesOuterBindings() {
    // r.w = 5 satisfies w < a.v + 10 = 11; the 'bad' edge's w = 99 does not.
    assertThat(count("MATCH (a:A {v:1})-[r:E*1..1 WHERE r.w < a.v + 10]->(x:A) RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void matchVariableLengthAgreesWithTheClauseLevelAllSpelling() {
    final List<Integer> viaInline = queryInts(
        "MATCH (a:A {v:1})-[r:E*1..2 WHERE r.tag = 'ok']->(x:A) RETURN x.v AS v ORDER BY v", "v");
    final List<Integer> viaClause = queryInts(
        "MATCH (a:A {v:1})-[r:E*1..2]->(x:A) WHERE all(e IN r WHERE e.tag = 'ok') RETURN x.v AS v ORDER BY v", "v");
    assertThat(viaInline).containsExactlyElementsOf(viaClause);
  }

  // ---------------------------------------------------------------- EXISTS {}, variable length

  @Test
  void existsVariableLengthAppliesTheRelationshipInlineWhere() {
    assertThat(count("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[r:E*1..1 WHERE r.tag = 'nope']->(x:A) } "
        + "RETURN count(*) AS c")).isZero();
    assertThat(count("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[r:E*1..1 WHERE r.tag = 'ok']->(x:A) } "
        + "RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void existsVariableLengthAppliesTheRelationshipPropertyMap() {
    assertThat(count("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[r:E*1..1 {tag:'nope'}]->(x:A) } "
        + "RETURN count(*) AS c")).isZero();
    assertThat(count("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[r:E*1..1 {tag:'ok'}]->(x:A) } "
        + "RETURN count(*) AS c")).isEqualTo(1);
  }

  // ---------------------------------------------------------------- controls that must keep working

  @Test
  void fixedLengthRelationshipInlineWhereStillWorks() {
    assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(x:A) RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void variableLengthPropertyMapStillWorks() {
    assertThat(count("MATCH (a:A {v:1})-[r:E*1..1 {tag:'ok'}]->(x:A) RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void patternComprehensionVariableLengthStillWorks() {
    assertThat(count("MATCH (a:A {v:1}) RETURN size([(a)-[r:E*1..1 WHERE r.tag = 'ok']->(x:A) | x.v]) AS c"))
        .isEqualTo(1);
  }

  @Test
  void variableLengthWithoutAnyPredicateIsUnaffected() {
    assertThat(count("MATCH (a:A {v:1})-[r:E*1..1]->(x:A) RETURN count(*) AS c")).isEqualTo(2);
  }
}
