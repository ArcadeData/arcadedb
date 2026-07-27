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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #5464.
 * <p>
 * An {@code EXISTS { ... }} subquery whose pattern carries a relationship inline {@code WHERE}
 * predicate must evaluate the predicate like any other filter. Before the fix a trivially true
 * predicate such as {@code [r:E WHERE 1=1]} flipped the result from {@code true} to {@code false}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5464ExistsRelInlineWhereTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-5464-exists-rel-inline-where").create();
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

  private boolean existsBool(final String existsBody) {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:A {v:1}) RETURN EXISTS { " + existsBody + " } AS v")) {
      assertThat(rs.hasNext()).isTrue();
      return Boolean.TRUE.equals(rs.next().<Boolean>getProperty("v"));
    }
  }

  @Test
  void patternOnlyExistsWithTriviallyTrueInlineWhere() {
    assertThat(existsBool("(a)-[:E]->(:A)")).as("plainExists").isTrue();
    assertThat(existsBool("(a)-[r:E]->(:A)")).as("namedRelationshipExists").isTrue();
    assertThat(existsBool("(a)-[r:E WHERE 1=1]->(:A)")).as("inlineTrueExists").isTrue();
  }

  @Test
  void patternOnlyExistsWithRelationshipPropertyInlineWhere() {
    assertThat(existsBool("(a)-[r:E WHERE r.tag = 'ok']->(:A)")).as("matchingProperty").isTrue();
    assertThat(existsBool("(a)-[r:E WHERE r.tag = 'bad']->(:A)")).as("otherMatchingProperty").isTrue();
    assertThat(existsBool("(a)-[r:E WHERE r.tag = 'nope']->(:A)")).as("nonMatchingProperty").isFalse();
  }

  @Test
  void explicitMatchExistsWithInlineWhere() {
    assertThat(existsBool("MATCH (a)-[r:E WHERE 1=1]->(:A)")).as("explicitMatchInlineTrue").isTrue();
    assertThat(existsBool("MATCH (a)-[r:E WHERE r.tag = 'ok']->(:A)")).as("explicitMatchInlineProp").isTrue();
    assertThat(existsBool("MATCH (a)-[r:E WHERE r.tag = 'nope']->(:A)")).as("explicitMatchInlineNoMatch").isFalse();
  }

  @Test
  void explicitMatchExistsWithClauseLevelWhereStillWorks() {
    assertThat(existsBool("MATCH (a)-[r:E]->(:A) WHERE r.tag = 'ok'")).as("clauseWhereMatch").isTrue();
    assertThat(existsBool("MATCH (a)-[r:E]->(:A) WHERE r.tag = 'nope'")).as("clauseWhereNoMatch").isFalse();
  }

  @Test
  void nodeInlineWhereInsideExists() {
    assertThat(existsBool("(a)-[:E]->(b:A WHERE b.v = 2)")).as("nodeInlineTrue").isTrue();
    assertThat(existsBool("(a)-[:E]->(b:A WHERE b.v = 99)")).as("nodeInlineFalse").isFalse();
  }

  @Test
  void notExistsWithInlineWhere() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:A {v:1}) WHERE NOT EXISTS { (a)-[r:E WHERE r.tag = 'nope']->(:A) } RETURN a.v AS v")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("v").intValue()).isEqualTo(1);
    }
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:A {v:1}) WHERE NOT EXISTS { (a)-[r:E WHERE r.tag = 'ok']->(:A) } RETURN a.v AS v")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void countAndCollectSubqueriesShareTheSameRewrite() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:A {v:1}) RETURN COUNT { (a)-[r:E WHERE r.tag = 'ok']->(:A) } AS c")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1);
    }
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:A {v:1}) RETURN COLLECT { MATCH (a)-[r:E WHERE r.tag = 'ok']->(b:A) RETURN b.v } AS l")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<List<Object>>getProperty("l")).hasSize(1);
    }
  }

  @Test
  void clauseKeywordInsideAStringLiteralIsNotMistakenForAClause() {
    // 'RETURN' as a value must not be scanned as the subquery's RETURN clause
    assertThat(existsBool("(a)-[r:E WHERE r.tag = 'RETURN']->(:A)")).as("literalKeyword").isFalse();
    assertThat(existsBool("(a)-[r:E WHERE r.tag <> 'RETURN']->(:A)")).as("literalKeywordNegated").isTrue();
  }

  @Test
  void regularMatchWithInlineWhereIsUnaffected() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:A {v:1})-[r:E WHERE 1=1]->(b:A) RETURN count(*) AS c")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").intValue()).isEqualTo(2);
    }
  }
}
