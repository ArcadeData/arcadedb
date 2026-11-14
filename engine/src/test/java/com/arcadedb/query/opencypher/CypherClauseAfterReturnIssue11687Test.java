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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11687: a RETURN followed by another clause in the same query part was accepted, and the clauses before the last
 * RETURN were silently discarded ({@code RETURN 1 AS id RETURN 0 AS id} answered {@code id=0}). RETURN ends a query part,
 * so anything after it but its own ORDER BY / SKIP / LIMIT is a syntax error.
 */
class CypherClauseAfterReturnIssue11687Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Node");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Node {id: 1}), (:Node {id: 2}), (:Node {id: 3})");
    });
  }

  @Test
  void twoConsecutiveReturnsAreRejected() {
    assertThatThrownBy(() -> query("RETURN 1 AS id RETURN 0 AS id"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("RETURN can only be used at the end of the query");
  }

  @Test
  void boundReturnFollowedByAnotherReturnIsRejected() {
    assertThatThrownBy(() -> query("MATCH (a:Node {id:1}) RETURN a.id AS id RETURN 0 AS id"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("RETURN can only be used at the end of the query");
  }

  @Test
  void readingClauseAfterReturnIsRejected() {
    assertThatThrownBy(() -> query("MATCH (a:Node) RETURN a.id AS id MATCH (b:Node) RETURN b.id AS id"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("RETURN can only be used at the end of the query");
    assertThatThrownBy(() -> query("RETURN 1 AS id WITH 2 AS x RETURN x"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("RETURN can only be used at the end of the query");
  }

  @Test
  void singleReturnWithOrderBySkipLimitIsUnchanged() {
    assertThat(ids("MATCH (a:Node) RETURN a.id AS id ORDER BY id DESC SKIP 1 LIMIT 1")).containsExactly(2L);
    // A standalone OFFSET after RETURN is folded into the RETURN's paging (issue #9940), not a clause after it
    assertThat(ids("MATCH (a:Node) RETURN a.id AS id ORDER BY id LIMIT 2 OFFSET 1")).containsExactly(2L, 3L);
  }

  @Test
  void unionBranchesKeepTheirOwnReturn() {
    assertThat(ids("RETURN 1 AS id UNION ALL RETURN 0 AS id")).containsExactlyInAnyOrder(1L, 0L);
  }

  @Test
  void subqueriesKeepTheirOwnReturn() {
    assertThat(ids("CALL { RETURN 1 AS x } RETURN x AS id")).containsExactly(1L);
    assertThat(ids("MATCH (a:Node) WHERE EXISTS { MATCH (b:Node) WHERE b.id = a.id + 1 RETURN b } "
        + "RETURN a.id AS id ORDER BY id")).containsExactly(1L, 2L);
    assertThat(ids("RETURN COUNT { MATCH (b:Node) RETURN b } AS id")).containsExactly(3L);
  }

  private ResultSet query(final String cypher) {
    return database.query("opencypher", cypher);
  }

  private List<Long> ids(final String cypher) {
    final List<Long> ids = new ArrayList<>();
    try (final ResultSet rs = query(cypher)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        ids.add(((Number) r.getProperty("id")).longValue());
      }
    }
    return ids;
  }
}
