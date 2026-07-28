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
 * Regression test for GitHub issues #5463 and #5462, two reports of the same defect with the same
 * reproducer.
 * <p>
 * A relationship inline {@code WHERE} predicate that reads an edge property must select the same
 * rows no matter what the projection does with the relationship variable afterwards. The reported
 * symptom was that {@code RETURN count(*)} answered 0 while {@code RETURN count(r)} answered 1 for
 * one and the same {@code MATCH}: the edge binding was elided from the plan whenever nothing
 * outside the pattern referenced {@code r}, so the inline predicate was evaluated against an
 * unbound variable and rejected every row. Referencing {@code r} anywhere downstream - through
 * {@code count(r)}, {@code r.tag} or even a {@code collect(r.tag)} sitting next to the
 * {@code count(*)} - kept the binding alive and masked the defect.
 * <p>
 * The behavior is pinned here for both pattern-parsing paths that accept a relationship inline
 * predicate: the {@code MATCH} path and the pattern-comprehension / {@code EXISTS} path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5463CountStarRelInlineWhereTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-5463-count-star-rel-inline-where").create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:A {v:1}), (b:A {v:2})");
      database.command("opencypher", "MATCH (a:A {v:1}), (b:A {v:2}) "
          + "CREATE (a)-[:E {tag: 'ok'}]->(b), (a)-[:E {tag: 'bad'}]->(b)");
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
      assertThat(rs.hasNext()).isTrue();
      final long value = rs.next().<Number>getProperty("c").longValue();
      assertThat(rs.hasNext()).as("a single aggregate row").isFalse();
      return value;
    }
  }

  private List<Object> column(final String query, final String name) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(name));
    }
    return values;
  }

  @Test
  void countStarAgreesWithCountRelationshipOnInlinePropertyPredicate() {
    assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN count(*) AS c"))
        .as("countStar").isEqualTo(1);
    assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN count(r) AS c"))
        .as("countRelationship").isEqualTo(1);
    assertThat(column("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN r.tag AS tag", "tag"))
        .as("materializedRows").containsExactly("ok");
  }

  @Test
  void countStarHonoursTheInlinePredicateOnEveryTagValue() {
    assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'bad']->(b:A) RETURN count(*) AS c"))
        .as("bad").isEqualTo(1);
    assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'nope']->(b:A) RETURN count(*) AS c"))
        .as("noMatch").isZero();
    assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag IS NOT NULL]->(b:A) RETURN count(*) AS c"))
        .as("allTagged").isEqualTo(2);
  }

  @Test
  void aSecondProjectionItemDoesNotChangeTheCount() {
    // issue #5462: adding a non-filtering output expression that mentions r used to flip count(*)
    // from 0 to 1, because it was the only thing keeping the edge binding alive
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN count(*) AS c, collect(r.tag) AS tags")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Number>getProperty("c").longValue()).as("countStar").isEqualTo(1);
      assertThat(row.<List<Object>>getProperty("tags")).as("tags").containsExactly("ok");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void literalInlinePredicateAndClauseLevelWhereKeepWorking() {
    assertThat(count("MATCH (a:A {v:1})-[r:E WHERE 1=1]->(b:A) RETURN count(*) AS c"))
        .as("literalInline").isEqualTo(2);
    assertThat(count("MATCH (a:A {v:1})-[r:E]->(b:A) WHERE r.tag = 'ok' RETURN count(*) AS c"))
        .as("clauseLevel").isEqualTo(1);
  }

  @Test
  void countStarWithAnonymousEndpointsAndUnlabelledTarget() {
    assertThat(count("MATCH (:A {v:1})-[r:E WHERE r.tag = 'ok']->(b) RETURN count(*) AS c"))
        .as("anonymousSource").isEqualTo(1);
    assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->() RETURN count(*) AS c"))
        .as("anonymousTarget").isEqualTo(1);
    assertThat(count("MATCH ()-[r:E WHERE r.tag = 'ok']->() RETURN count(*) AS c"))
        .as("bothEndsAnonymous").isEqualTo(1);
  }

  @Test
  void directionVariantsHonourTheInlinePredicate() {
    assertThat(count("MATCH (b:A {v:2})<-[r:E WHERE r.tag = 'ok']-(a:A) RETURN count(*) AS c"))
        .as("rightToLeft").isEqualTo(1);
    assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']-(b:A) RETURN count(*) AS c"))
        .as("undirected").isEqualTo(1);
    assertThat(count("MATCH (a:A {v:1})-[r WHERE r.tag = 'ok']->(b:A) RETURN count(*) AS c"))
        .as("untypedRelationship").isEqualTo(1);
  }

  @Test
  void otherAggregatesSeeTheSameRows() {
    assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN count(b) AS c"))
        .as("countTargetNode").isEqualTo(1);
    assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN count(DISTINCT b) AS c"))
        .as("countDistinctTargetNode").isEqualTo(1);
    assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) WITH count(*) AS c RETURN c AS c"))
        .as("countStarThroughWith").isEqualTo(1);
    assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN sum(b.v) AS c"))
        .as("sumOverTargetProperty").isEqualTo(2);
  }

  @Test
  void countStarGroupedByTheSourceNode() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN a.v AS v, count(*) AS c")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Number>getProperty("v").intValue()).isEqualTo(1);
      assertThat(row.<Number>getProperty("c").longValue()).isEqualTo(1);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void optionalMatchKeepsTheRowAndStillFilters() {
    assertThat(count("MATCH (a:A {v:1}) OPTIONAL MATCH (a)-[r:E WHERE r.tag = 'ok']->(b:A) RETURN count(b) AS c"))
        .as("optionalHit").isEqualTo(1);
    assertThat(count("MATCH (a:A {v:1}) OPTIONAL MATCH (a)-[r:E WHERE r.tag = 'zz']->(b:A) RETURN count(b) AS c"))
        .as("optionalMiss").isZero();
  }

  @Test
  void patternComprehensionAndExistsShareTheSamePredicateSemantics() {
    assertThat(count("MATCH (a:A {v:1}) RETURN size([(a)-[r:E WHERE r.tag = 'ok']->(b:A) | b.v]) AS c"))
        .as("patternComprehension").isEqualTo(1);
    assertThat(count("MATCH (a:A {v:1}) RETURN size([(a)-[r:E WHERE r.tag = 'nope']->(b:A) | b.v]) AS c"))
        .as("patternComprehensionNoMatch").isZero();
    assertThat(count("MATCH (a:A {v:1}) RETURN COUNT { (a)-[r:E WHERE r.tag = 'ok']->(:A) } AS c"))
        .as("countSubquery").isEqualTo(1);
    assertThat(count("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[r:E WHERE r.tag = 'ok']->(:A) } RETURN count(*) AS c"))
        .as("existsSubquery").isEqualTo(1);
    assertThat(count("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[r:E WHERE r.tag = 'nope']->(:A) } RETURN count(*) AS c"))
        .as("existsSubqueryNoMatch").isZero();
  }
}
