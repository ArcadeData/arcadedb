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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11586: in {@code MATCH ... UNWIND ... MATCH} the cost-based optimizer planned both MATCH clauses
 * as one physical plan and ran the UNWIND after it, so the second MATCH read the UNWIND variable before it was bound and
 * returned no rows. Clauses apply in order: the second MATCH runs once per UNWIND row. Each answer is checked against the
 * same statement with {@code WITH r} before the UNWIND, which always kept the clauses in order.
 */
class CypherMatchUnwindMatchIssue11586Test extends TestHelper {

  private static final String[] QUERIES = {
      "MATCH (r:Record {id: 1}) UNWIND r.chunk_ids AS cid MATCH (c:Chunk {id: cid}) RETURN c.id AS v ORDER BY v",
      "MATCH (r:Record {id: 1}) UNWIND r.chunk_ids AS cid MATCH (c:Chunk) WHERE c.id = cid RETURN c.id AS v ORDER BY v",
      "MATCH (r:Record {id: 1}) UNWIND r.chunk_ids AS cid MATCH (c:Chunk) WHERE c.id > cid RETURN count(*) AS v",
      "MATCH (r:Record) WHERE r.id = 1 UNWIND r.chunk_ids AS cid MATCH (c:Chunk) WHERE c.id = cid RETURN c.id AS v ORDER BY v",
      "MATCH (r:Record {id: 1}) UNWIND r.chunk_ids AS cid MATCH (r)-[:HAS]->(c:Chunk) WHERE c.k = cid RETURN c.id AS v" };

  private static final List<List<Long>> EXPECTED = List.of(List.of(10L, 11L), List.of(10L, 11L), List.of(5L),
      List.of(10L, 11L), List.of(13L));

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (r:Record {id: 1, chunk_ids: [10, 11]}), (:Chunk {id: 10}), (:Chunk {id: 11}), (:Chunk {id: 12}), "
            + "(r)-[:HAS]->(:Chunk {id: 13, k: 10})"));
  }

  @Test
  void theSecondMatchRunsOncePerUnwindRow() {
    for (int i = 0; i < QUERIES.length; i++)
      assertThat(values(QUERIES[i])).as(QUERIES[i]).isEqualTo(EXPECTED.get(i));
  }

  @Test
  void sameAnswersWithWithBeforeTheUnwind() {
    for (int i = 0; i < QUERIES.length; i++) {
      final String withFirst = QUERIES[i].replace(" UNWIND ", " WITH r UNWIND ");
      assertThat(values(withFirst)).as(withFirst).isEqualTo(EXPECTED.get(i));
    }
  }

  private List<Long> values(final String query) {
    final List<Long> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        out.add(rs.next().<Number>getProperty("v").longValue());
    }
    return out;
  }
}
