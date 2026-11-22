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
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code UNWIND $rows AS row MERGE (n:T {id: row.id}) SET n = row} (how a graph library stores a node by an id) writes the key the
 * MERGE matched by with the value it matched it by, so the MERGE of the next row finds what it would have found without the SET of
 * the one before. It needs no eager barrier between the two clauses, and the SET is then written with the node it creates, one save
 * where a barrier made two: the node was saved small, and then grown by the SET, which turned a record of any size into a chunk
 * chain on a full page.
 * <p>
 * A SET that can change what a MERGE reads keeps its barrier: the barrier is the guarantee of #10889 and #11005.
 */
class CypherMergeSetOfMatchedKeysNeedsNoBarrierTest extends TestHelper {
  private static final String  EAGER    = "EAGER";
  private static final Pattern SET_STEP = Pattern.compile("(?m)^\\s*\\+ SET\\b");

  @Test
  void aMapSetOfTheRowTheMergeMatchedByIsAbsorbedWithoutABarrier() {
    final String plan = explain("UNWIND $rows AS row MERGE (n:KeyedSet {id: row.id}) SET n = row");
    assertThat(plan).as(plan).doesNotContain(EAGER);
    assertThat(SET_STEP.matcher(plan).find()).as(plan).isFalse();
  }

  @Test
  void aMapMergeSetOfTheRowIsPlannedWithoutABarrierToo() {
    final String plan = explain("UNWIND $rows AS row MERGE (n:KeyedSet {id: row.id}) SET n += row");
    assertThat(plan).as(plan).doesNotContain(EAGER);
  }

  @Test
  void aSetOfAnotherMapKeepsTheBarrier() {
    // The map the MERGE matched by is `row`: `other` may carry another id, which the SET of one row would give the node for the
    // MERGE of the next one
    final String plan = explain("UNWIND $rows AS row WITH row, {id: 99} AS other MERGE (n:KeyedSet {id: row.id}) SET n = other");
    assertThat(plan).as(plan).contains(EAGER);
  }

  @Test
  void aComputedMatchKeyKeepsTheBarrierWhereKeysTakenFromTheRowDoNot() {
    final String plan = explain("UNWIND $rows AS row MERGE (n:KeyedSet {id: row.id, other: row.other}) SET n = row");
    // every key it matches by is `row.key`: none needed
    assertThat(plan).as(plan).doesNotContain(EAGER);
    final String computed = explain("UNWIND $rows AS row MERGE (n:KeyedSet {id: row.id + 1}) SET n = row");
    assertThat(computed).as(computed).contains(EAGER);
  }

  @Test
  void aPropertyWriteOfAMatchedKeyKeepsTheBarrier() {
    final String plan = explain("UNWIND $rows AS row MERGE (n:KeyedSet {id: row.id}) SET n.id = row.other");
    assertThat(plan).as(plan).contains(EAGER);
  }

  @Test
  void aSetAfterAnEarlierReadOfAnotherKeyKeepsTheBarrier() {
    database.command("opencypher", "UNWIND range(0, 4) AS i CREATE (:Seed {id: i, name: 'x'})").close();
    final String plan = explain("MATCH (s:Seed) WHERE s.name = 'x' WITH s.id AS id UNWIND [{id: id}] AS row MERGE (n:KeyedSet {id: row.id}) SET n = row");
    assertThat(plan).as(plan).contains(EAGER);
  }

  @Test
  void rowsRepeatingAnIdMergeIntoOneNodeAndTheLastRowsPropertiesWin() {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 30; i++)
      rows.add(Map.of("id", (long) (i % 10), "name", "n" + i));

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "UNWIND $rows AS row MERGE (n:KeyedSet {id: row.id}) SET n = row RETURN n.id AS id",
          Map.of("rows", rows))) {
        int returned = 0;
        while (rs.hasNext()) {
          rs.next();
          ++returned;
        }
        assertThat(returned).isEqualTo(30);
      }
    });

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:KeyedSet) RETURN n.id AS id, n.name AS name ORDER BY id")) {
      int count = 0;
      while (rs.hasNext()) {
        final var row = rs.next();
        assertThat(((Number) row.getProperty("id")).longValue()).isEqualTo(count);
        assertThat((String) row.getProperty("name")).isEqualTo("n" + (20 + count));
        ++count;
      }
      assertThat(count).isEqualTo(10);
    }
  }

  @Test
  void aMapSetReplacesWhatAMatchedNodeHeldBeforeAndKeepsItsKey() {
    database.command("opencypher", "CREATE (:KeyedSet {id: 1, old: 'gone', name: 'before'})").close();
    database.transaction(() -> database.command("opencypher", "UNWIND $rows AS row MERGE (n:KeyedSet {id: row.id}) SET n = row",
        Map.of("rows", List.of(Map.of("id", 1L, "name", "after")))).close());

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:KeyedSet) RETURN n.id AS id, n.name AS name, n.old AS old")) {
      final var row = rs.next();
      assertThat(((Number) row.getProperty("id")).longValue()).isEqualTo(1L);
      assertThat((String) row.getProperty("name")).isEqualTo("after");
      assertThat((Object) row.getProperty("old")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  private String explain(final String query) {
    try (final ResultSet resultSet = database.command("opencypher", "EXPLAIN " + query, Map.of("rows", List.of(Map.of("id", 1L, "other", 2L))))) {
      return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
