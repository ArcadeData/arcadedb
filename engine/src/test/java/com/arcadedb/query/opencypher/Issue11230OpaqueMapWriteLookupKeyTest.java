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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11230: an opaque map write ({@code SET n += map}, {@code SET n = map}) that can rewrite the key a MATCH looked the node up
 * by must wait for the MATCH of every row, or the MATCH of a later duplicate lookup sees the write of an earlier row and the row
 * count depends on the pull batch. The upsert shape, where the written map is the very row the lookup key is read from
 * ({@code MATCH (n {id: row.id}) SET n += row}), rewrites the key with the value it was found by and keeps streaming.
 */
class Issue11230OpaqueMapWriteLookupKeyTest extends TestHelper {
  private static final String EAGER = "EAGER";
  private static final int    ROWS  = 150;

  @Test
  void duplicateLookupsMatchBeforeAnOpaqueWriteOfAnotherMapRewritesTheKey() {
    database.command("opencypher", "CREATE (:P {id: 1})");
    final List<Object> rows = new ArrayList<>();
    for (int i = 0; i < ROWS; i++)
      rows.add(Map.of("id", 1, "props", Map.of("id", 2)));
    assertThat(count("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n += row.props RETURN count(*) AS c", Map.of("rows", rows)))
        .isEqualTo((long) ROWS);
  }

  @Test
  void duplicateLookupsMatchBeforeAWholeMapReplacementRewritesTheKey() {
    database.command("opencypher", "CREATE (:P {id: 1})");
    final List<Object> rows = new ArrayList<>();
    for (int i = 0; i < ROWS; i++)
      rows.add(Map.of("id", 1, "props", Map.of("id", 2)));
    assertThat(count("UNWIND $rows AS row MATCH (n:P) WHERE n.id = row.id SET n = row.props RETURN count(*) AS c", Map.of("rows", rows)))
        .isEqualTo((long) ROWS);
  }

  @Test
  void theUpsertShapeStillAnswersEveryRow() {
    database.command("opencypher", "CREATE (:P {id: 1})");
    final List<Object> rows = new ArrayList<>();
    for (int i = 0; i < ROWS; i++)
      rows.add(Map.of("id", 1, "name", "x"));
    assertThat(count("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n += row RETURN count(*) AS c", Map.of("rows", rows)))
        .isEqualTo((long) ROWS);
  }

  @Test
  void barrierIsPlannedWhereTheWrittenMapIsNotTheRowTheKeyComesFrom() {
    assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n += row.props")).contains(EAGER);
    assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n = row.props")).contains(EAGER);
    assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n += $other")).contains(EAGER);
    assertThat(explain("UNWIND $rows AS row MATCH (n:P) WHERE n.id = row.id SET n += row.props")).contains(EAGER);
    // the key is read from another property of the row than the one the map is written to
    assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.oldId}) SET n += row")).contains(EAGER);
    assertThat(explain("UNWIND $rows AS row MATCH (n:P) WHERE n.id > row.id SET n += row")).contains(EAGER);
    // the map is a row, but not the one the lookup was fed from
    assertThat(explain("UNWIND $rows AS row UNWIND $others AS other MATCH (n:P {id: row.id}) SET n += other")).contains(EAGER);
    // a name rebound by a WITH is not the row the key was read from
    assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) WITH n, {id: 7} AS row SET n += row")).contains(EAGER);
    // a key projected from another property of the row than the one it is looked up by
    assertThat(explain("UNWIND [{id: 1, o: 2}] AS row WITH row, row.o AS id MATCH (n:P {id: id}) SET n += row")).contains(EAGER);
    // the write goes through another variable than the one the key was looked up by
    assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}), (m:P {id: row.other}) SET m += row")).contains(EAGER);
    // a subquery whose body writes an opaque map
    assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) CALL (n, row) { SET n += row.props }")).contains(EAGER);
    // a key computed per row
    assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n[row.key] = 1")).contains(EAGER);
  }

  @Test
  void upsertShapesKeepStreaming() {
    assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n += row")).doesNotContain(EAGER);
    assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n = row")).doesNotContain(EAGER);
    assertThat(explain("UNWIND $rows AS row MATCH (n:P) WHERE n.id = row.id SET n += row")).doesNotContain(EAGER);
    assertThat(explain("UNWIND $rows AS row MATCH (n:P) WHERE row.id = n.id SET n += row")).doesNotContain(EAGER);
    assertThat(explain("UNWIND $rows AS row MATCH (n:P) WHERE row.id = n.id AND n.id > 0 SET n += row")).contains(EAGER);
    assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) WITH n, row SET n += row")).doesNotContain(EAGER);
    assertThat(explain("LOAD CSV WITH HEADERS FROM 'file:///x.csv' AS row MATCH (n:P {id: row.id}) SET n += row")).doesNotContain(EAGER);
    assertThat(explain("LOAD CSV WITH HEADERS FROM 'file:///x.csv' AS row WITH row, row.id AS id MATCH (n:P {id: id}) SET n += row"))
        .doesNotContain(EAGER);
    assertThat(explain("MATCH (n:P) WHERE n.k = 1 SET n += $map")).doesNotContain(EAGER);
  }

  private String explain(final String query) {
    try (final ResultSet resultSet = database.command("opencypher", "EXPLAIN " + query)) {
      return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private long count(final String query, final Map<String, Object> parameters) {
    try (final ResultSet resultSet = database.command("opencypher", query, parameters)) {
      final Result row = resultSet.next();
      return ((Number) row.getProperty("c")).longValue();
    }
  }
}
