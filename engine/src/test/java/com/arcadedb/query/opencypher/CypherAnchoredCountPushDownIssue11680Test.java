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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #11680: a {@code count(*)} over a pattern anchored at one vertex by a selective index lookup was
 * taken by a CSR count push-down ({@code COUNT CHAIN PATHS}, {@code COUNT STAR JOIN}), which walks a whole label to count the
 * neighbours of that one vertex, so its cost grew with the graph while the answer did not. Such a statement is now left to the
 * ordinary plan, an index seek plus an expansion from it, which is what the same statement with {@code sum(1)} already got.
 * <p>
 * Every {@code Record} has 10 {@code Section}s (chained by {@code PARENT_OF}) and 2 {@code Chunk}s; {@code Record.external_id}
 * has a UNIQUE index. A count with no selective anchor keeps its push-down.
 */
class CypherAnchoredCountPushDownIssue11680Test extends TestHelper {
  private static final int RECORDS = 300;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Record");
    database.command("sql", "CREATE VERTEX TYPE Section");
    database.command("sql", "CREATE VERTEX TYPE Chunk");
    database.command("sql", "CREATE EDGE TYPE OF_RECORD");
    database.command("sql", "CREATE EDGE TYPE PARENT_OF");
    database.command("sql", "CREATE EDGE TYPE LOF_RECORD LIGHTWEIGHT");
    database.command("sql", "CREATE PROPERTY Record.external_id STRING");
    database.command("sql", "CREATE PROPERTY Record.tenant STRING");
    database.command("sql", "CREATE PROPERTY Record.code INTEGER");
    database.command("sql", "CREATE PROPERTY Section.n INTEGER");
    database.command("sql", "CREATE INDEX ON Record (external_id) UNIQUE");
    database.command("sql", "CREATE INDEX ON Record (tenant, code) UNIQUE");
    database.command("sql", "CREATE INDEX ON Section (n) NOTUNIQUE");

    database.transaction(() -> {
      database.command("opencypher",
          "UNWIND range(0, $last) AS i CREATE (r:Record {external_id: 'r' + toString(i), tenant: 't' + toString(i % 3), code: i}) "
              + "WITH r UNWIND range(0, 9) AS j CREATE (s:Section {n: j})-[:OF_RECORD]->(r) CREATE (s)-[:LOF_RECORD]->(r)",
          Map.of("last", RECORDS - 1));
      database.command("opencypher",
          "MATCH (r:Record)<-[:OF_RECORD]-(s:Section) WITH r, s ORDER BY s.n WITH r, collect(s) AS ss "
              + "UNWIND range(1, size(ss) - 1) AS k WITH ss[k - 1] AS p, ss[k] AS c CREATE (p)-[:PARENT_OF]->(c)");
      database.command("opencypher", "MATCH (r:Record) UNWIND range(0, 1) AS j CREATE (:Chunk {n: j})-[:OF_RECORD]->(r)");
    });
  }

  @Test
  void aChainAnchoredByAUniqueKeyIsNotPushedDown() {
    assertAnchored("MATCH (r:Record {external_id: 'r5'})<-[:OF_RECORD]-(s:Section) RETURN count(*) AS c", 10);
    assertAnchored("MATCH (r:Record {external_id: 'r5'})<-[:OF_RECORD]-(s:Section)<-[:PARENT_OF]-(p:Section) RETURN count(*) AS c",
        9);
    assertAnchored("MATCH (s:Section)-[:OF_RECORD]->(r:Record {external_id: 'r5'}) RETURN count(*) AS c", 10);
    assertAnchored("MATCH (s:Section)-[:OF_RECORD]->(r:Record) WHERE r.external_id = 'r5' RETURN count(*) AS c", 10);
    assertAnchored("MATCH (s:Section)-[:LOF_RECORD]->(r:Record {external_id: 'r5'}) RETURN count(*) AS c", 10);
  }

  @Test
  void aStarAnchoredByAUniqueKeyIsNotPushedDown() {
    assertAnchored("MATCH (r:Record {external_id: 'r5'})<-[:OF_RECORD]-(s:Section) OPTIONAL MATCH (p:Section)-[:PARENT_OF]->(s) "
        + "RETURN count(*) AS c", 10);
    assertAnchored("MATCH (r:Record {external_id: 'r5'})<-[:OF_RECORD]-(s:Section), (p:Section)-[:PARENT_OF]->(s) RETURN count(*) AS c",
        9);
    assertAnchored("MATCH (r:Record {external_id: 'r5'})<-[:OF_RECORD]-(s:Section) MATCH (s)<-[:PARENT_OF]-(p) RETURN count(*) AS c", 9);
    assertAnchored("MATCH (r:Record {external_id: 'r5'}) MATCH (r)<-[:OF_RECORD]-(s:Section) MATCH (r)<-[:OF_RECORD]-(c:Chunk) "
        + "RETURN count(*) AS c", 20);
  }

  @Test
  void otherSelectiveAnchorsAreNotPushedDown() {
    // a parameter is as selective as the literal it stands for
    assertAnchored("MATCH (s:Section)-[:OF_RECORD]->(r:Record {external_id: $id}) RETURN count(*) AS c", Map.of("id", "r5"), 10);
    // a short IN list over a UNIQUE key: two seeks
    assertAnchored("MATCH (s:Section)-[:OF_RECORD]->(r:Record) WHERE r.external_id IN ['r5', 'r6'] RETURN count(*) AS c", 20);
    // a composite UNIQUE index fully covered by equalities
    assertAnchored("MATCH (s:Section)-[:OF_RECORD]->(r:Record {tenant: 't2', code: 5}) RETURN count(*) AS c", 10);
    // a pass-through WITH is folded away before the push-downs read the statement (issue #9597): the plan that runs is the
    // unfolded one, whose anchor is the seek
    assertAnchored("MATCH (s:Section)-[:OF_RECORD]->(r:Record {external_id: 'r5'}) WITH s RETURN count(*) AS c", 10);
  }

  @Test
  void anIdAnchorIsNotPushedDownWhereTheOrdinaryPlanLooksTheRecordUp() {
    final Map<String, Object> params = Map.of("rid", recordRid("r5").toString());
    // the pipeline of a statement with an OPTIONAL MATCH starts from the first node, which it looks up by id
    assertAnchored("MATCH (r:Record)<-[:OF_RECORD]-(s:Section) WHERE id(r) = $rid OPTIONAL MATCH (p:Section)-[:PARENT_OF]->(s) "
        + "RETURN count(*) AS c", params, 10);
    // the optimizer's plan filters a scan of the label on id(), no cheaper than the push-down, which is kept
    assertPushedDown("MATCH (r:Record)<-[:OF_RECORD]-(s:Section) WHERE id(r) = $rid RETURN count(*) AS c", params, "COUNT CHAIN PATHS",
        10);
  }

  @Test
  void aCountWithNoSelectiveAnchorKeepsItsPushDown() {
    assertPushedDown("MATCH (s:Section)-[:OF_RECORD]->(r:Record) RETURN count(*) AS c", Map.of(), "COUNT CHAIN PATHS", RECORDS * 10L);
    assertPushedDown("MATCH (r:Record)<-[:OF_RECORD]-(s:Section), (p:Section)-[:PARENT_OF]->(s) RETURN count(*) AS c", Map.of(),
        "COUNT STAR JOIN", RECORDS * 9L);
    // a NOTUNIQUE key estimated at a tenth of the label is not selective
    assertPushedDown("MATCH (s:Section {n: 3})-[:OF_RECORD]->(r:Record) RETURN count(*) AS c", Map.of(), "COUNT CHAIN PATHS",
        RECORDS);
    // the leading column of a composite UNIQUE index alone is a prefix range, not a key
    assertPushedDown("MATCH (s:Section)-[:OF_RECORD]->(r:Record {tenant: 't2'}) RETURN count(*) AS c", Map.of(), "COUNT CHAIN PATHS",
        RECORDS / 3 * 10L);
  }

  private void assertAnchored(final String query, final long expected) {
    assertAnchored(query, Map.of(), expected);
  }

  private void assertAnchored(final String query, final Map<String, Object> params, final long expected) {
    assertThat(explainOf(query, params)).as(query).doesNotContain("COUNT CHAIN PATHS").doesNotContain("COUNT STAR JOIN");
    assertThat(scalarOf(query, params)).as(query).isEqualTo(expected);
    // the same statement with sum(1) never reached the push-downs: the oracle for the count
    assertThat(scalarOf(query.replace("count(*)", "sum(1)"), params)).as(query).isEqualTo(expected);
  }

  private void assertPushedDown(final String query, final Map<String, Object> params, final String operator, final long expected) {
    assertThat(explainOf(query, params)).as(query).contains(operator);
    assertThat(scalarOf(query, params)).as(query).isEqualTo(expected);
    assertThat(scalarOf(query.replace("count(*)", "sum(1)"), params)).as(query).isEqualTo(expected);
  }

  private RID recordRid(final String externalId) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Record WHERE external_id = ?", externalId)) {
      return rs.next().getIdentity().orElseThrow();
    }
  }

  private long scalarOf(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private String explainOf(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, params)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
