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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5206: a non-aggregating CASE expression used as a grouping key beside
 * count() in the same RETURN clause must be evaluated per input row, like any other grouping key,
 * matching openCypher / Neo4j semantics. ArcadeDB returned the ELSE value (0) instead of the value
 * produced by every input row (1).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherCaseGroupingKeyIssue5206Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-case5206").create();
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:A {v:1}), (a)-[:R {w:10}]->(:B {v:2}), (a)-[:R {w:20}]->(:B {v:3})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void caseGroupingKeyBesideCount() {
    final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH (a:A)-[:R]->(b) RETURN a.v AS av, CASE WHEN b.v > 1 THEN 1 ELSE 0 END AS flag, count(b) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("av")).intValue()).isEqualTo(1);
    assertThat(((Number) row.getProperty("flag")).intValue()).isEqualTo(1);
    assertThat(((Number) row.getProperty("cnt")).intValue()).isEqualTo(2);
    assertThat(rs.hasNext()).isFalse();
    // A grouping key referencing the counted variable must NOT use the count-edges fast path
    assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain("COUNT EDGES RETURN");
  }

  @Test
  void caseGroupingKeySplitsGroups() {
    // b.v = 2 -> flag 0, b.v = 3 -> flag 1: two distinct groups, one row each
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:A)-[:R]->(b) RETURN CASE WHEN b.v > 2 THEN 1 ELSE 0 END AS flag, count(b) AS cnt ORDER BY flag");
    assertThat(rs.hasNext()).isTrue();
    Result row = rs.next();
    assertThat(((Number) row.getProperty("flag")).intValue()).isEqualTo(0);
    assertThat(((Number) row.getProperty("cnt")).intValue()).isEqualTo(1);
    assertThat(rs.hasNext()).isTrue();
    row = rs.next();
    assertThat(((Number) row.getProperty("flag")).intValue()).isEqualTo(1);
    assertThat(((Number) row.getProperty("cnt")).intValue()).isEqualTo(1);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void caseGroupingKeyInWith() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:A)-[:R]->(b) WITH a.v AS av, CASE WHEN b.v > 1 THEN 1 ELSE 0 END AS flag, b "
            + "RETURN av, flag, count(b) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("av")).intValue()).isEqualTo(1);
    assertThat(((Number) row.getProperty("flag")).intValue()).isEqualTo(1);
    assertThat(((Number) row.getProperty("cnt")).intValue()).isEqualTo(2);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void countStarGroupedByCountedVariable() {
    // Same guard hole with count(*): the check was skipped entirely, so grouping on the
    // counted/target variable was evaluated on the anchor row where b is unbound
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:A)-[:R]->(b) RETURN b.v AS bv, count(*) AS cnt ORDER BY bv");
    assertThat(rs.hasNext()).isTrue();
    Result row = rs.next();
    assertThat(((Number) row.getProperty("bv")).intValue()).isEqualTo(2);
    assertThat(((Number) row.getProperty("cnt")).intValue()).isEqualTo(1);
    assertThat(rs.hasNext()).isTrue();
    row = rs.next();
    assertThat(((Number) row.getProperty("bv")).intValue()).isEqualTo(3);
    assertThat(((Number) row.getProperty("cnt")).intValue()).isEqualTo(1);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void countGroupedByRelationshipProperty() {
    // A named relationship variable in the grouping key also slipped through the old
    // text-based guard, evaluating to null on the anchor row
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:A)-[r:R]->(b) RETURN r.w AS w, count(b) AS cnt ORDER BY w");
    assertThat(rs.hasNext()).isTrue();
    Result row = rs.next();
    assertThat(((Number) row.getProperty("w")).intValue()).isEqualTo(10);
    assertThat(((Number) row.getProperty("cnt")).intValue()).isEqualTo(1);
    assertThat(rs.hasNext()).isTrue();
    row = rs.next();
    assertThat(((Number) row.getProperty("w")).intValue()).isEqualTo(20);
    assertThat(((Number) row.getProperty("cnt")).intValue()).isEqualTo(1);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void anchorOnlyGroupingStillCorrect() {
    // The valid fast-path shape must keep returning correct results and keep the optimization
    final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH (a:A)-[:R]->(b) RETURN a.v AS av, count(b) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("av")).intValue()).isEqualTo(1);
    assertThat(((Number) row.getProperty("cnt")).intValue()).isEqualTo(2);
    assertThat(rs.hasNext()).isFalse();
    assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("COUNT EDGES RETURN");
  }

  @Test
  void caseOnAnchorVariableGroupingKey() {
    // A CASE that references only the anchor variable is safe for the fast path and must
    // still produce correct per-group values
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:A)-[:R]->(b) RETURN CASE WHEN a.v > 0 THEN 1 ELSE 0 END AS flag, count(b) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("flag")).intValue()).isEqualTo(1);
    assertThat(((Number) row.getProperty("cnt")).intValue()).isEqualTo(2);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void caseWithoutAggregationStillPerRow() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:A)-[:R]->(b) RETURN b.v AS bv, CASE WHEN b.v > 1 THEN 1 ELSE 0 END AS flag ORDER BY bv");
    assertThat(rs.hasNext()).isTrue();
    Result row = rs.next();
    assertThat(((Number) row.getProperty("bv")).intValue()).isEqualTo(2);
    assertThat(((Number) row.getProperty("flag")).intValue()).isEqualTo(1);
    row = rs.next();
    assertThat(((Number) row.getProperty("bv")).intValue()).isEqualTo(3);
    assertThat(((Number) row.getProperty("flag")).intValue()).isEqualTo(1);
    assertThat(rs.hasNext()).isFalse();
  }
}
