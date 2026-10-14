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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5225. Creating a Cypher index on an initially-untyped property infers and
 * declares the property's type from the existing data ({@code INTEGER} here, see issue #4222). A later
 * {@code SET n.val = "1"} is therefore coerced to the integer {@code 1}. The bug was that a <em>range</em> predicate
 * against a String bound (e.g. {@code WHERE n.val < "zzz"}) crashed the index path with an internal
 * {@code ClassCastException} / {@code NumberFormatException}, while the same predicate without an index completed.
 * Cross-type comparisons are undefined (null) in Cypher, so the range must complete and return the same result as the
 * full-scan path, never throw.
 * <p>
 * Equality is different: since #9794 a declaration defines the text form of its values in both directions, so
 * {@code WHERE n.val = "1"} finds the node the same text was written into, on the index path and on the scan path alike
 * (#9935).
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

  private List<Integer> ids(final String query) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.command("cypher", query)) {
      while (rs.hasNext())
        ids.add(((Number) rs.next().getProperty("id")).intValue());
    }
    return ids;
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
    // The write was coerced: val holds the integer 1, not the text "1"
    try (final ResultSet rs = database.query("cypher", "MATCH (n:BugIdx) WHERE n.id = 1 RETURN n.val AS v")) {
      assertThat(rs.next().<Object>getProperty("v")).isInstanceOf(Integer.class).isEqualTo(1);
    }
    // val is declared INTEGER, so the text it was written with reads back as the same integer (#9794, #9935)
    assertThat(ids("MATCH (n:BugIdx) WHERE n.val = \"1\" RETURN n.id AS id")).containsExactly(1);
    assertThat(ids("MATCH (n:BugIdx) WHERE n.val IN [\"1\"] RETURN n.id AS id")).containsExactly(1);
    assertThat(ids("MATCH (n:BugIdx {val: \"1\"}) RETURN n.id AS id")).containsExactly(1);
    // A text with no integer form matches nothing, and the correctly-typed lookup still finds the node through the index
    assertThat(rows("MATCH (n:BugIdx) WHERE n.val = \"one\" RETURN n.id")).isZero();
    assertThat(ids("MATCH (n:BugIdx) WHERE n.val = 1 RETURN n.id AS id")).containsExactly(1);
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
    // Both paths must agree on a non-empty answer, or the comparison below proves nothing
    assertThat(eqIndexed).isEqualTo(1);

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
