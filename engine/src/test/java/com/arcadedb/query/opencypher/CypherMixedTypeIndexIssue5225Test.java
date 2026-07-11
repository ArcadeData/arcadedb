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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5225. Creating a Cypher index on an initially-untyped property infers and
 * declares the property's type from the existing data ({@code INTEGER} here, see issue #4222). A later
 * {@code SET n.val = "1"} is therefore coerced to the integer {@code 1}, so a strict-typed Cypher lookup
 * {@code WHERE n.val = "1"} correctly returns no rows (matching Neo4j: {@code 1 = "1"} is false). The bug
 * was that a <em>range</em> predicate against a String bound (e.g. {@code WHERE n.val < "zzz"}) crashed
 * the index path with an internal {@code ClassCastException} / {@code NumberFormatException}, while the
 * same predicate without an index completed. Cross-type comparisons are undefined (null) in Cypher, so
 * the range must complete and return the same result as the full-scan path, never throw.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMixedTypeIndexIssue5225Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue5225").create();
    database.command("cypher", "CREATE (:BugIdx {id: 1, val: 1})");
    database.command("cypher", "CREATE (:BugIdx {id: 2, val: 2})");
    database.command("cypher", "CREATE (:BugIdx {id: 3})");
    database.command("cypher", "CREATE INDEX FOR (n:BugIdx) ON (n.val)");
    database.command("cypher", "MATCH (n:BugIdx) WHERE n.id = 1 SET n.val = \"1\" RETURN n.val");
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private long count(final String query) {
    final ResultSet rs = database.command("cypher", query);
    assertThat(rs.hasNext()).isTrue();
    return ((Number) rs.next().getProperty("c")).longValue();
  }

  private long rows(final String query) {
    long n = 0;
    final ResultSet rs = database.command("cypher", query);
    while (rs.hasNext()) {
      rs.next();
      n++;
    }
    return n;
  }

  @Test
  void equalityLookupIsCypherConsistent() {
    // val was coerced to the integer 1, so a String-typed equality returns no rows (Neo4j: 1 = "1" is false).
    assertThat(rows("MATCH (n:BugIdx) WHERE n.val = \"1\" RETURN n.id, n.val")).isZero();
    // The correctly-typed lookup still finds the node through the index.
    assertThat(rows("MATCH (n:BugIdx) WHERE n.val = 1 RETURN n.id, n.val")).isEqualTo(1);
  }

  @Test
  void rangeAgainstStringBoundDoesNotThrow() {
    // Upper bound only, lower bound only, and both bounds: none may throw; all match Neo4j's null-comparison
    // semantics (no numeric value is orderable against a String bound) -> count 0.
    assertThat(count("MATCH (n:BugIdx) WHERE n.val < \"zzz\" RETURN count(n) AS c")).isZero();
    assertThat(count("MATCH (n:BugIdx) WHERE n.val > \"a\" RETURN count(n) AS c")).isZero();
    assertThat(count("MATCH (n:BugIdx) WHERE n.val >= \"a\" AND n.val <= \"zzz\" RETURN count(n) AS c")).isZero();
  }

  @Test
  void indexPathMatchesFullScanPath() {
    // Within the same database, dropping the index must not change results: the index is only an access
    // path, never a semantic change. Capture results with the index, then after dropping it.
    final long eqIndexed = rows("MATCH (n:BugIdx) WHERE n.val = \"1\" RETURN n.id");
    final long rangeIndexed = count("MATCH (n:BugIdx) WHERE n.val < \"zzz\" RETURN count(n) AS c");

    database.getSchema().dropIndex(database.getSchema().getType("BugIdx").getIndexByProperties("val").getName());

    assertThat(rows("MATCH (n:BugIdx) WHERE n.val = \"1\" RETURN n.id")).isEqualTo(eqIndexed);
    assertThat(count("MATCH (n:BugIdx) WHERE n.val < \"zzz\" RETURN count(n) AS c")).isEqualTo(rangeIndexed);
  }

  @Test
  void numericRangeStillUsesIndexCorrectly() {
    // Guard against over-eager fallback: a numeric bound on the numeric index must still return correct rows.
    assertThat(count("MATCH (n:BugIdx) WHERE n.val >= 1 AND n.val <= 5 RETURN count(n) AS c")).isEqualTo(2);
    assertThat(count("MATCH (n:BugIdx) WHERE n.val > 1 RETURN count(n) AS c")).isEqualTo(1);
  }
}
