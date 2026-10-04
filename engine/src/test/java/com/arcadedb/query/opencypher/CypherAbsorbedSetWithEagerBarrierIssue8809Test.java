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
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8809: a SET that MERGE/CREATE absorbs (#8735) must not also run as a SET step of its own, and the eager barrier
 * of #8733 must still go in front of a later MATCH that reads the property the absorbed SET writes. Both at once.
 */
class CypherAbsorbedSetWithEagerBarrierIssue8809Test extends TestHelper {
  private static final String  EAGER    = "EAGER";
  // A SET step prints as "+ SET ..." at the start of its line; MERGE's own "ON CREATE SET" does not match
  private static final Pattern SET_STEP = Pattern.compile("(?m)^\\s*\\+ SET\\b");

  @Test
  void absorbedMergeSetIsNotAlsoPlannedAsASetStep() {
    final String plan = explain("UNWIND $rows AS r MERGE (v:V8809a {id: r.id}) SET v.name = r.name", Map.of("rows", rows(0, 2)));
    assertThat(SET_STEP.matcher(plan).find()).as(plan).isFalse();
  }

  @Test
  void absorbedCreateSetIsNotAlsoPlannedAsASetStep() {
    final String plan = explain("UNWIND $rows AS r CREATE (v:V8809b {id: r.id}) SET v.name = r.name", Map.of("rows", rows(0, 2)));
    assertThat(SET_STEP.matcher(plan).find()).as(plan).isFalse();
  }

  @Test
  void absorbedSetKeepsTheBarrierForALaterReadAndWritesEachNewNodeOnce() {
    final String query = "UNWIND $rows AS r MERGE (v:V8809c {id: r.id}) SET v.k = true WITH v MATCH (m:V8809c {k: true}) RETURN count(*) AS c";
    final String plan = explain(query, Map.of("rows", rows(0, 2)));
    assertThat(plan).as(plan).contains(EAGER);
    assertThat(SET_STEP.matcher(plan).find()).as(plan).isFalse();

    database.transaction(() -> {
      final long before = updates(database);
      try (final ResultSet rs = database.command("opencypher", query, Map.of("rows", rows(0, 10)))) {
        // every one of the 10 rows sees all 10 nodes the absorbed SET marked, because the barrier drained the writes first
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(100L);
      }
      assertThat(updates(database) - before).isZero();
    });
  }

  @Test
  void absorbedSetAfterAReadKeepsTheBarrierAndWritesEachNewNodeOnce() {
    database.command("opencypher", "UNWIND range(0, 9) AS i CREATE (:A8809 {id: i})").close();
    final String query = "MATCH (a:A8809) MERGE (n:X8809 {id: a.id}) SET n.k = true WITH n MATCH (m:X8809 {k: true}) RETURN count(*) AS c";
    assertThat(explain(query, Map.of())).contains(EAGER);

    database.transaction(() -> {
      final long before = updates(database);
      try (final ResultSet rs = database.command("opencypher", query)) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(100L);
      }
      assertThat(updates(database) - before).isZero();
    });
  }

  @Test
  void absorbedSetCountsEachAssignedPropertyOnce() {
    try (final ResultSet rs = database.command("opencypher", "CREATE (v:V8809d {id: 1}) SET v.name = 'x', v.other = 2")) {
      while (rs.hasNext())
        rs.next();
      assertThat(rs.getStatistics().orElseThrow().getPropertiesSet()).isEqualTo(3);
    }
  }

  private String explain(final String query, final Map<String, Object> params) {
    try (final ResultSet resultSet = database.command("opencypher", "EXPLAIN " + query, params)) {
      return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private static long updates(final Database db) {
    return ((Number) db.getStats().get("updateRecord")).longValue();
  }

  private static List<Map<String, Object>> rows(final int from, final int to) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = from; i < to; i++)
      rows.add(Map.of("id", (long) i, "name", "n" + i));
    return rows;
  }
}
