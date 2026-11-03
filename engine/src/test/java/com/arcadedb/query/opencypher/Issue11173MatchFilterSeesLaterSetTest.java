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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11173: the filter of a MATCH must read the graph as it was before a later clause of the same query writes the key it
 * filters on, whatever the pull batch size. A write that names the key waits for the MATCH of every row; an opaque map write
 * keeps streaming, so the upsert shapes {@code MATCH (n {id: row.id}) SET n += row} are not made eager.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11173MatchFilterSeesLaterSetTest extends TestHelper {
  private static final String EAGER = "EAGER";

  @Test
  void matchWhereMatchesEveryRowBeforeTheSetRuns() {
    assertThat(count("""
        CREATE (b:B {k: 1})
        WITH b
        UNWIND range(1, 150) AS i
        CREATE (a:A {i: i})-[:R]->(b)
        WITH a, b
        MATCH (a)-[:R]->(c:B) WHERE c.k = 1
        SET c.k = 2
        RETURN count(*) AS c""")).isEqualTo(150L);
  }

  @Test
  void matchInlinePropertyMatchesEveryRowBeforeTheRemoveRuns() {
    assertThat(count("""
        CREATE (b:B {k: 1})
        WITH b
        UNWIND range(1, 150) AS i
        CREATE (a:A {i: i})-[:R]->(b)
        WITH a, b
        MATCH (a)-[:R]->(c:B {k: 1})
        REMOVE c.k
        RETURN count(*) AS c""")).isEqualTo(150L);
  }

  @Test
  void matchWhereMatchesEveryRowBeforeASetOfAMapLiteralRuns() {
    assertThat(count("""
        CREATE (b:B {k: 1})
        WITH b
        UNWIND range(1, 150) AS i
        CREATE (a:A {i: i})-[:R]->(b)
        WITH a, b
        MATCH (a)-[:R]->(c:B) WHERE c.k = 1
        SET c += {k: 2}
        RETURN count(*) AS c""")).isEqualTo(150L);
  }

  @Test
  void matchWhereMatchesEveryRowBeforeAMergeActionRuns() {
    assertThat(count("""
        CREATE (b:B {k: 1})
        WITH b
        UNWIND range(1, 150) AS i
        CREATE (a:A {i: i})-[:R]->(b)
        WITH a, b
        MATCH (a)-[:R]->(c:B) WHERE c.k = 1
        MERGE (m:M {id: 1}) ON CREATE SET c.k = 2 ON MATCH SET c.k = 2
        RETURN count(*) AS c""")).isEqualTo(150L);
  }

  @Test
  void barrierIsPlannedWhereAWriteNamesAKeyTheMatchFiltersOn() {
    assertThat(explain("MATCH (a)-[:R]->(b) WHERE b.k = 1 SET b.k = 2")).contains(EAGER);
    assertThat(explain("MATCH (a)-[:R]->(b {k: 1}) REMOVE b.k")).contains(EAGER);
    assertThat(explain("MATCH (a)-[:R]->(b) WHERE b.k = 1 SET b += {k: 2}")).contains(EAGER);
    assertThat(explain("MATCH (a)-[:R]->(b) WHERE b.k = 1 FOREACH (x IN [1] | SET b.k = 2)")).contains(EAGER);
    assertThat(explain("MATCH (a)-[:R]->(b) WHERE b.k = 1 CALL (b) { SET b.k = 2 }")).contains(EAGER);
    assertThat(explain("MATCH (a)-[:R]->(b) WHERE b.k = 1 CALL (b) { SET b.other = 2 }")).doesNotContain(EAGER);
  }

  @Test
  void upsertShapesKeepStreaming() {
    // an opaque map write may rewrite the lookup key, but holding the whole input for it is not worth it
    assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n += row")).doesNotContain(EAGER);
    assertThat(explain("LOAD CSV WITH HEADERS FROM 'file:///x.csv' AS row MATCH (n:P {id: row.id}) SET n += row")).doesNotContain(EAGER);
    // a write of another key, and a statement that starts with a one-node MATCH (a different node on every row)
    assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n.name = row.name")).doesNotContain(EAGER);
    assertThat(explain("MATCH (n:P) WHERE n.k = 1 SET n.k = 2")).doesNotContain(EAGER);
  }

  private String explain(final String query) {
    try (final ResultSet resultSet = database.command("opencypher", "EXPLAIN " + query)) {
      return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private long count(final String query) {
    try (final ResultSet resultSet = database.command("opencypher", query)) {
      final Result row = resultSet.next();
      return ((Number) row.getProperty("c")).longValue();
    }
  }
}
