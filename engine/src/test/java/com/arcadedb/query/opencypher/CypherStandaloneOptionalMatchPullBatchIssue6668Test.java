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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6668: a standalone {@code OPTIONAL MATCH} used as the first clause
 * of a query kept its match cursor in a local variable that was re-created on every
 * {@code fetchMore} call, so once the pattern had more rows than the pull batch size (100) it
 * re-ran the scan from the beginning on every subsequent batch, emitting the first 100 rows
 * repeatedly and never terminating.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherStandaloneOptionalMatchPullBatchIssue6668Test {
  private static final int NODE_COUNT = 250;

  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypher6668").create();
    database.transaction(() -> {
      for (int i = 0; i < NODE_COUNT; i++)
        database.command("opencypher", "CREATE (:Person {id: $id})", java.util.Map.of("id", i));
    });
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

  /**
   * A standalone leading OPTIONAL MATCH whose cardinality exceeds the 100-row pull batch must
   * terminate and emit each matched node exactly once, not repeat the first batch forever.
   */
  @Test
  void standaloneOptionalMatchExceedingPullBatchTerminatesWithoutDuplicates() {
    database.transaction(() -> {
      final List<Result> rows = rows("OPTIONAL MATCH (n:Person) RETURN n.id AS id");

      assertThat(rows).hasSize(NODE_COUNT);

      final Set<Object> ids = new HashSet<>();
      for (final Result row : rows)
        ids.add(row.getProperty("id"));
      assertThat(ids).hasSize(NODE_COUNT);
    });
  }

  /**
   * The no-match case must still resolve to a single all-NULL row once the (empty) pattern scan
   * is exhausted.
   */
  @Test
  void standaloneOptionalMatchWithNoMatchesEmitsSingleNullRow() {
    database.transaction(() -> {
      final List<Result> rows = rows("OPTIONAL MATCH (n:Person {id: -1}) RETURN n");

      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().<Object>getProperty("n")).isNull();
    });
  }

  /**
   * A leading OPTIONAL MATCH whose cardinality fits within a single pull batch must still work
   * (this shape already passed before the fix, it pins the non-regressed case).
   */
  @Test
  void standaloneOptionalMatchWithinPullBatchStillWorks() {
    database.transaction(() -> {
      final List<Result> rows = rows("OPTIONAL MATCH (n:Person {id: 0}) RETURN n.id AS id");

      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().<Object>getProperty("id")).isEqualTo(0);
    });
  }
}
