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
 * Regression test for issue #11761: a node pattern whose variable an earlier OPTIONAL MATCH bound to null cannot match. A bare
 * {@code (b)} over such a variable was planned as "already bound, nothing to check" and let the null row through, so a later
 * disconnected component of the same OPTIONAL MATCH expanded over every node instead of leaving the input row once with nulls.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class CypherOptionalMatchNullBoundNodeIssue11761Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypher11761").create();
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:Node {id: 1}), (b:Node {id: 2}), (c:Node {id: 3}), (a)-[:R]->(b), (b)-[:R]->(c)"""));
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private List<Result> rows(final String query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private long count(final String query) {
    final List<Result> rows = rows(query);
    assertThat(rows).hasSize(1);
    return ((Number) rows.getFirst().getProperty("rows")).longValue();
  }

  @Test
  void caseANullFromMissingLabelKeepsOneRow() {
    assertThat(count("""
        OPTIONAL MATCH (n:Missing)
        OPTIONAL MATCH (n), (d:Node)
        RETURN count(*) AS rows""")).isEqualTo(1L);

    final List<Result> rows = rows("""
        OPTIONAL MATCH (n:Missing)
        OPTIONAL MATCH (n), (d:Node)
        RETURN n, d""");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Object>getProperty("n")).isNull();
    assertThat(rows.getFirst().<Object>getProperty("d")).isNull();
  }

  @Test
  void caseANullBoundNodeWrittenSecond() {
    assertThat(count("""
        OPTIONAL MATCH (n:Missing)
        OPTIONAL MATCH (d:Node), (n)
        RETURN count(*) AS rows""")).isEqualTo(1L);
  }

  @Test
  void caseBNullFromRelationshipPattern() {
    final List<Result> rows = rows("""
        MATCH (a:Node {id: 3})
        OPTIONAL MATCH (a)-[:R]->(b)
        OPTIONAL MATCH (b), (c:Node)
        RETURN a.id AS aid, b.id AS bid, c.id AS cid
        ORDER BY cid""");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Object>getProperty("aid")).isEqualTo(3);
    assertThat(rows.getFirst().<Object>getProperty("bid")).isNull();
    assertThat(rows.getFirst().<Object>getProperty("cid")).isNull();

    assertThat(count("""
        MATCH (a:Node {id: 3})
        OPTIONAL MATCH (a)-[:R]->(b)
        OPTIONAL MATCH (b), (c:Node)
        RETURN a.id AS aid, count(*) AS rows""")).isEqualTo(1L);
  }

  @Test
  void labelAndPropertyFormsAlsoKeepOneRow() {
    assertThat(count("""
        MATCH (a:Node {id: 3})
        OPTIONAL MATCH (a)-[:R]->(b)
        OPTIONAL MATCH (b:Node), (c:Node)
        RETURN count(*) AS rows""")).isEqualTo(1L);
    assertThat(count("""
        MATCH (a:Node {id: 3})
        OPTIONAL MATCH (a)-[:R]->(b)
        OPTIONAL MATCH (b {id: 2}), (c:Node)
        RETURN count(*) AS rows""")).isEqualTo(1L);
  }

  @Test
  void thirdDisconnectedComponent() {
    assertThat(count("""
        MATCH (a:Node {id: 3})
        OPTIONAL MATCH (a)-[:R]->(b)
        OPTIONAL MATCH (b), (c:Node), (d:Node)
        RETURN count(*) AS rows""")).isEqualTo(1L);
    assertThat(count("""
        MATCH (a:Node {id: 3})
        OPTIONAL MATCH (a)-[:R]->(b)
        OPTIONAL MATCH (c:Node), (b), (d:Node)
        RETURN count(*) AS rows""")).isEqualTo(1L);
  }

  @Test
  void nonNullBoundNodeStillExpands() {
    final List<Result> rows = rows("""
        MATCH (a:Node {id: 1})
        OPTIONAL MATCH (a)-[:R]->(b)
        OPTIONAL MATCH (b), (c:Node)
        RETURN b.id AS bid, c.id AS cid
        ORDER BY cid""");
    assertThat(rows).hasSize(3);
    for (int i = 0; i < 3; i++) {
      assertThat(rows.get(i).<Object>getProperty("bid")).isEqualTo(2);
      assertThat(rows.get(i).<Object>getProperty("cid")).isEqualTo(i + 1);
    }
  }

  @Test
  void nonOptionalMatchOnNullBoundNodeDropsTheRow() {
    assertThat(count("""
        OPTIONAL MATCH (n:Missing)
        MATCH (n)
        RETURN count(*) AS rows""")).isEqualTo(0L);
    assertThat(count("""
        MATCH (a:Node {id: 3})
        OPTIONAL MATCH (a)-[:R]->(b)
        MATCH (b), (c:Node)
        RETURN count(*) AS rows""")).isEqualTo(0L);
  }

  @Test
  void nullCarriedThroughWithCannotMatch() {
    assertThat(count("""
        OPTIONAL MATCH (n:Missing)
        WITH n
        OPTIONAL MATCH (n), (d:Node)
        RETURN count(*) AS rows""")).isEqualTo(1L);
    assertThat(count("""
        OPTIONAL MATCH (n:Missing)
        WITH n
        MATCH (n)
        RETURN count(*) AS rows""")).isEqualTo(0L);
  }

  @Test
  void zeroLengthNamedPathOverNullBoundNode() {
    final List<Result> rows = rows("""
        OPTIONAL MATCH (n:Missing)
        OPTIONAL MATCH p = (n), (d:Node)
        RETURN p, d""");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Object>getProperty("p")).isNull();
    assertThat(rows.getFirst().<Object>getProperty("d")).isNull();

    assertThat(count("""
        MATCH (a:Node {id: 1})
        OPTIONAL MATCH p = (a), (d:Node)
        RETURN count(p) AS rows""")).isEqualTo(3L);
  }

  @Test
  void boundNodeFromNonOptionalMatchUnchanged() {
    assertThat(count("""
        MATCH (a:Node)
        MATCH (a), (c:Node)
        RETURN count(*) AS rows""")).isEqualTo(9L);
    assertThat(count("""
        MATCH (a:Node)
        OPTIONAL MATCH (a), (c:Node {id: 1})
        RETURN count(*) AS rows""")).isEqualTo(3L);
  }
}
