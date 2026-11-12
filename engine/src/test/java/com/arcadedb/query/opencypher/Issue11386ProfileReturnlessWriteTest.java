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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11386: {@code PROFILE} of a statement with no RETURN answers no rows, as the statement it wraps does and as Neo4j's
 * PROFILE does. The eager profiled run collected whatever the last write clause passed on (the matched nodes of a SET, the
 * deleted ones of a DELETE, an empty row of a CREATE) and returned it, so a client iterating a profiled write saw phantom
 * rows. Also covers the declared columns of a statement, which a wire protocol announces for a result that has no row to
 * read them from.
 */
class Issue11386ProfileReturnlessWriteTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("PF");
      for (int i = 0; i < 5; i++)
        database.command("opencypher", "CREATE (:PF {i: $i})", Map.of("i", i));
    });
  }

  @Test
  void profileOfACreateAnswersNoRows() {
    assertProfiledWriteAnswersNoRows("PROFILE CREATE (:PF {i: 10})");
    assertThat(count("MATCH (n:PF {i: 10}) RETURN count(n) AS c")).isEqualTo(1L);
  }

  @Test
  void profileOfASetAnswersNoRows() {
    assertProfiledWriteAnswersNoRows("PROFILE MATCH (n:PF) SET n.x = 1");
    assertThat(count("MATCH (n:PF) WHERE n.x = 1 RETURN count(n) AS c")).isEqualTo(5L);
  }

  @Test
  void profileOfADeleteAnswersNoRows() {
    assertProfiledWriteAnswersNoRows("PROFILE MATCH (n:PF {i: 0}) DELETE n");
    assertThat(count("MATCH (n:PF) RETURN count(n) AS c")).isEqualTo(4L);
  }

  @Test
  void profileOfAMergeAnswersNoRows() {
    assertProfiledWriteAnswersNoRows("PROFILE MERGE (:PF {i: 11})");
    assertThat(count("MATCH (n:PF {i: 11}) RETURN count(n) AS c")).isEqualTo(1L);
  }

  @Test
  void profileOfARemoveAnswersNoRows() {
    database.transaction(() -> database.command("opencypher", "MATCH (n:PF) SET n.y = 1"));
    assertProfiledWriteAnswersNoRows("PROFILE MATCH (n:PF) REMOVE n.y");
    assertThat(count("MATCH (n:PF) WHERE n.y IS NOT NULL RETURN count(n) AS c")).isZero();
  }

  /** Control: a profiled write with a RETURN keeps its rows. */
  @Test
  void profileOfAWriteWithReturnKeepsItsRows() {
    final List<Object> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "PROFILE MATCH (n:PF) SET n.x = 2 RETURN n.i AS i")) {
        while (rs.hasNext())
          rows.add(rs.next().getProperty("i"));
        assertThat(rs.getExecutionPlan()).isPresent();
      }
    });
    assertThat(rows).hasSize(5);
  }

  /** The profile reports the rows the statement answered, not the ones the write clause passed on. */
  @Test
  void theProfileOfAReturnlessWriteReportsZeroRowsReturned() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "PROFILE MATCH (n:PF) SET n.x = 3")) {
        assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("Rows Returned: 0");
      }
    });
  }

  @Test
  void declaredColumnsAreTheReturnItemsInOrder() {
    assertThat(declaredColumns("MATCH (n:PF) WHERE n.i > 100 RETURN n.i AS i, n AS node")).containsExactly("i", "node");
    assertThat(declaredColumns("MATCH (n:PF) RETURN n.i, n")).containsExactly("n.i", "n");
    assertThat(declaredColumns("EXPLAIN MATCH (n:PF) RETURN n.i AS i")).containsExactly("i");
    assertThat(declaredColumns("PROFILE MATCH (n:PF) RETURN n.i AS i")).containsExactly("i");
    assertThat(declaredColumns("  // a comment\n MATCH (n:PF) RETURN n.i AS i;")).containsExactly("i");
    assertThat(declaredColumns("MATCH (n:PF) RETURN n.i AS a UNION MATCH (n:PF) RETURN n.i AS a")).containsExactly("a");
  }

  /** The unaliased names a zero-row result announces are the keys a row of the same statement carries. */
  @Test
  void declaredColumnsOfAnUnaliasedExpressionMatchTheRowKeys() {
    final String query = "MATCH (n:PF) WHERE n.i = 1 RETURN n.i + 1, toUpper('a')";
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(new ArrayList<>(rs.next().getPropertyNames())).isEqualTo(declaredColumns(query));
    }
  }

  @Test
  void aStatementWithNoReturnDeclaresNoColumns() {
    assertThat(declaredColumns("CREATE (:PF {i: 99})")).isNull();
    assertThat(declaredColumns("MATCH (n:PF) WHERE n.i > 100 SET n.x = 1")).isNull();
  }

  private List<String> declaredColumns(final String query) {
    return OpenCypherQueryEngine.declaredColumns((DatabaseInternal) database, query);
  }

  private void assertProfiledWriteAnswersNoRows(final String query) {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        assertThat(rs.hasNext()).as("PROFILE of a RETURN-less write answers no rows: %s", query).isFalse();
        assertThat(rs.getExecutionPlan()).as("the profile is still reported").isPresent();
        assertThat(rs.getStatistics()).as("the counters are still reported").isPresent();
      }
    });
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
