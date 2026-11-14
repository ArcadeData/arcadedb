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
 * Regression test for GitHub issue #11691: a MERGE with several ON MATCH SET (or ON CREATE SET) clauses kept only the
 * last one, silently dropping the earlier assignments. Every clause must be applied, in order.
 */
class CypherMergeRepeatedActionsIssue11691Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-merge-repeated-actions-11691").create();
    database.getSchema().createVertexType("Node");
    database.transaction(() -> database.command("opencypher", "CREATE (:Node {id:3, v:3, s:'c', b:true})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  private Result single(final String query) {
    final Result[] holder = new Result[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();
        holder[0] = rs.next();
        assertThat(rs.hasNext()).isFalse();
      }
    });
    return holder[0];
  }

  private Result node(final int id) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Node {id:$id}) RETURN n.v AS v, n.s AS s, n.b AS b", "id", id)) {
      return rs.next();
    }
  }

  @Test
  void twoOnMatchClausesAreBothApplied() {
    final Result r = single("MATCH (y:Node {id:3}) MERGE (z:Node {id:3}) ON MATCH SET z.v = 10 ON MATCH SET z.s = 'updated' RETURN z.v AS v, z.s AS s");
    assertThat(r.<Number>getProperty("v").intValue()).isEqualTo(10);
    assertThat(r.<String>getProperty("s")).isEqualTo("updated");
    assertThat(node(3).<Number>getProperty("v").intValue()).isEqualTo(10);
    assertThat(node(3).<String>getProperty("s")).isEqualTo("updated");
  }

  @Test
  void threeOnMatchClausesAreAllAppliedInOrder() {
    final Result r = single(
        "MERGE (z:Node {id:3}) ON MATCH SET z.v = 10 ON MATCH SET z.s = 'updated' ON MATCH SET z.b = false, z.v = 11 RETURN z.v AS v, z.s AS s, z.b AS b");
    assertThat(r.<Number>getProperty("v").intValue()).isEqualTo(11);
    assertThat(r.<String>getProperty("s")).isEqualTo("updated");
    assertThat(r.<Boolean>getProperty("b")).isFalse();
    final Result stored = node(3);
    assertThat(stored.<Number>getProperty("v").intValue()).isEqualTo(11);
    assertThat(stored.<Boolean>getProperty("b")).isFalse();
  }

  @Test
  void repeatedOnCreateClausesAreAllApplied() {
    final Result r = single(
        "MERGE (z:Node {id:4}) ON CREATE SET z.v = 1 ON CREATE SET z.s = 'new' ON CREATE SET z.v = 2 RETURN z.v AS v, z.s AS s");
    assertThat(r.<Number>getProperty("v").intValue()).isEqualTo(2);
    assertThat(r.<String>getProperty("s")).isEqualTo("new");
    assertThat(node(4).<Number>getProperty("v").intValue()).isEqualTo(2);
    assertThat(node(4).<String>getProperty("s")).isEqualTo("new");
  }

  @Test
  void interleavedActionsApplyOnlyTheMatchingBranch() {
    final String query = "MERGE (z:Node {id:$id}) ON CREATE SET z.v = 1 ON MATCH SET z.v = 100 ON CREATE SET z.s = 'created' ON MATCH SET z.s = 'matched' RETURN z.v AS v, z.s AS s";
    final Result[] holder = new Result[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query, "id", 5)) {
        holder[0] = rs.next();
      }
    });
    assertThat(holder[0].<Number>getProperty("v").intValue()).isEqualTo(1);
    assertThat(holder[0].<String>getProperty("s")).isEqualTo("created");

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query, "id", 5)) {
        holder[0] = rs.next();
      }
    });
    assertThat(holder[0].<Number>getProperty("v").intValue()).isEqualTo(100);
    assertThat(holder[0].<String>getProperty("s")).isEqualTo("matched");
  }

  @Test
  void commaSeparatedSingleClauseControl() {
    final Result r = single("MATCH (y:Node {id:3}) MERGE (z:Node {id:3}) ON MATCH SET z.v = 10, z.s = 'updated' RETURN z.v AS v, z.s AS s");
    assertThat(r.<Number>getProperty("v").intValue()).isEqualTo(10);
    assertThat(r.<String>getProperty("s")).isEqualTo("updated");
  }

  @Test
  void laterOnMatchClauseReadsTheWriteOfAnEarlierOne() {
    final Result r = single("MERGE (z:Node {id:3}) ON MATCH SET z.v = 10 ON MATCH SET z.v = z.v + 1 RETURN z.v AS v");
    assertThat(r.<Number>getProperty("v").intValue()).isEqualTo(11);
    assertThat(node(3).<Number>getProperty("v").intValue()).isEqualTo(11);
  }

  @Test
  void laterOnCreateClauseReadsTheWriteOfAnEarlierOne() {
    final Result r = single("MERGE (z:Node {id:4}) ON CREATE SET z.v = 10 ON CREATE SET z.v = z.v + 1 RETURN z.v AS v");
    assertThat(r.<Number>getProperty("v").intValue()).isEqualTo(11);
    assertThat(node(4).<Number>getProperty("v").intValue()).isEqualTo(11);
  }

  @Test
  void itemsOfOneClauseStillReadTheValuesFromBeforeIt() {
    // Control: within a single SET clause the items are simultaneous, so z.v + 1 reads the matched value 3
    final Result r = single("MERGE (z:Node {id:3}) ON MATCH SET z.v = 10, z.w = z.v + 1 RETURN z.v AS v, z.w AS w");
    assertThat(r.<Number>getProperty("v").intValue()).isEqualTo(10);
    assertThat(r.<Number>getProperty("w").intValue()).isEqualTo(4);
  }

}
