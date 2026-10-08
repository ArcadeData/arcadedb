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
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9348: ArcadeDB has no native duration type, a duration is stored as its ISO-8601 text, and an index handed that
 * text. An index-backed range predicate on a duration property therefore ordered lexically ({@code P10D} before
 * {@code P2D}) where the same predicate without the index compares durations component by component.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9348DurationRangeIndexTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue9348");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void rangeOnAnIndexedDurationAnswersLikeTheUnindexedScan() {
    // The index comes first: over existing text values the property would be declared STRING (issue #8384), which is text
    // by contract, so a duration property is one the index was created on while it was still undeclared
    database.command("cypher", "CREATE INDEX FOR (s:S) ON (s.d)");
    insertDurations();

    assertThat(names("MATCH (s:S) WHERE s.d > duration({days: 2}) RETURN s.n AS r")).containsExactlyInAnyOrder("3d", "10d");
    assertThat(names("MATCH (s:S) WHERE s.d >= duration({days: 3}) RETURN s.n AS r")).containsExactlyInAnyOrder("3d", "10d");
    assertThat(names("MATCH (s:S) WHERE s.d < duration({days: 10}) RETURN s.n AS r")).containsExactlyInAnyOrder("2d", "3d");
    assertThat(names("MATCH (s:S) WHERE s.d <= duration({days: 3}) AND s.d > duration({days: 1}) RETURN s.n AS r"))
        .containsExactlyInAnyOrder("2d", "3d");
  }

  @Test
  void rangeOnAnIndexedDurationWithParameters() {
    database.command("cypher", "CREATE INDEX FOR (s:S) ON (s.d)");
    insertDurations();

    assertThat(names("MATCH (s:S) WHERE s.d > $lo RETURN s.n AS r", Map.of("lo", CypherDuration.parse("P2D"))))
        .containsExactlyInAnyOrder("3d", "10d");
    assertThat(names("MATCH (s:S) WHERE s.d >= $lo RETURN s.n AS r", Map.of("lo", CypherDuration.parse("P3D"))))
        .containsExactlyInAnyOrder("3d", "10d");
    assertThat(names("MATCH (s:S) WHERE s.d < $hi RETURN s.n AS r", Map.of("hi", CypherDuration.parse("P10D"))))
        .containsExactlyInAnyOrder("2d", "3d");
    assertThat(names("MATCH (s:S) WHERE s.d > $lo AND s.d <= $hi RETURN s.n AS r",
        Map.of("lo", CypherDuration.parse("P2D"), "hi", CypherDuration.parse("P10D")))).containsExactlyInAnyOrder("3d", "10d");
  }

  @Test
  void orderByOnAnIndexedDurationSortsAsDurations() {
    database.command("cypher", "CREATE INDEX FOR (s:S) ON (s.d)");
    insertDurations();

    assertThat(names("MATCH (s:S) RETURN s.n AS r ORDER BY s.d")).containsExactly("2d", "3d", "10d");
    assertThat(names("MATCH (s:S) RETURN s.n AS r ORDER BY s.d DESC")).containsExactly("10d", "3d", "2d");
  }

  @Test
  void equalityOnAnIndexedDurationStillFindsTheRecord() {
    database.command("cypher", "CREATE INDEX FOR (s:S) ON (s.d)");
    insertDurations();

    assertThat(names("MATCH (s:S) WHERE s.d = duration({days: 10}) RETURN s.n AS r")).containsExactly("10d");
  }

  private void insertDurations() {
    database.transaction(() -> {
      database.command("cypher", "CREATE (:S {n: '2d', d: duration({days: 2})})");
      database.command("cypher", "CREATE (:S {n: '10d', d: duration({days: 10})})");
      database.command("cypher", "CREATE (:S {n: '3d', d: duration({days: 3})})");
    });
  }

  private List<String> names(final String query) {
    return names(query, Map.of());
  }

  private List<String> names(final String query, final Map<String, Object> parameters) {
    final List<String> result = new ArrayList<>();
    try (final ResultSet rs = database.query("cypher", query, parameters)) {
      while (rs.hasNext())
        result.add(rs.next().getProperty("r"));
    }
    return result;
  }
}
