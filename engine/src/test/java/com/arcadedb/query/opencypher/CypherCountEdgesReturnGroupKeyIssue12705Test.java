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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #12705: the COUNT EDGES RETURN shortcut keyed its groups by the raw value, so a list stored as a
 * primitive array and the same number held as INTEGER and DOUBLE answered the same key on several rows. GROUP BY and
 * Neo4j answer one row per key.
 */
class CypherCountEdgesReturnGroupKeyIssue12705Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Grp");
    database.getSchema().createVertexType("Tgt");
    database.getSchema().createEdgeType("R");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND $rows AS row CREATE (:Grp {id: row.id, l: row.l, k: row.k})",
          Map.of("rows", List.of(Map.of("id", 1, "l", List.of(3), "k", 1), Map.of("id", 2, "l", List.of(3), "k", 1))));
      database.command("opencypher", "CREATE (:Grp {id: 3, l: [3], k: 1}), (:Grp {id: 4, l: [3], k: 1.0})");
      database.newVertex("Grp").set("id", 5).set("l", new long[] { 3L }).set("k", 1).save();
      database.command("opencypher", "CREATE (:Tgt {id: 10})");
      database.command("opencypher", "MATCH (c:Grp), (a:Tgt) CREATE (c)-[:R]->(a)");
    });
  }

  private List<Result> run(final String query) {
    try (final ResultSet plan = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(plan.getExecutionPlan().get().prettyPrint(0, 2)).contains("COUNT EDGES RETURN");
    }
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  @Test
  void listKeyStoredAsArrayAndAsListIsOneGroup() {
    final List<Result> rows = run("MATCH (n:Grp)-[r]-(m:Tgt) RETURN n.l AS g, count(*) AS c");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.getFirst().getProperty("c")).longValue()).isEqualTo(5L);
  }

  @Test
  void integerAndDoubleKeyIsOneGroupReportingTheFirstSeenKey() {
    final List<Result> rows = run("MATCH (n:Grp)-[r]-(m:Tgt) RETURN n.k AS g, count(*) AS c");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.getFirst().getProperty("c")).longValue()).isEqualTo(5L);
    assertThat((Object) rows.getFirst().getProperty("g")).isEqualTo(1);
  }

  @Test
  void missingKeyIsOneNullGroup() {
    final List<Result> rows = run("MATCH (n:Grp)-[r]-(m:Tgt) RETURN n.missing AS g, count(*) AS c");
    assertThat(rows).hasSize(1);
    assertThat((Object) rows.getFirst().getProperty("g")).isNull();
    assertThat(((Number) rows.getFirst().getProperty("c")).longValue()).isEqualTo(5L);
  }

  @Test
  void twoKeysAreOneGroup() {
    final List<Result> rows = run("MATCH (n:Grp)-[r]-(m:Tgt) RETURN n.l AS g, n.k AS g2, count(*) AS c");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.getFirst().getProperty("c")).longValue()).isEqualTo(5L);
  }
}
