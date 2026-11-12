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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class MaterializedViewPerRecordClassifierTest extends TestHelper {

  @BeforeEach
  void createTypes() {
    database.transaction(() -> {
      database.getSchema().createVertexType("User");
      database.getSchema().createVertexType("Other");
      database.getSchema().createEdgeType("FriendOf");
    });
  }

  @Test
  void userStatsShapeIsPerRecord() {
    assertThat(MaterializedViewQueryClassifier.isPerRecord(
        "SELECT id AS userId, out('FriendOf').size() AS friends FROM User", database)).isTrue();
  }

  @Test
  void plainProjectionWithWhereIsPerRecord() {
    assertThat(MaterializedViewQueryClassifier.isPerRecord("SELECT name FROM User WHERE age > 18", database)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT name FROM User ORDER BY name",
      "SELECT name FROM User LIMIT 10",
      "SELECT name FROM User SKIP 5",
      "SELECT DISTINCT name FROM User",
      "SELECT name FROM User LET $x = 1",
      "SELECT name, tags FROM User UNWIND tags",
      "SELECT name FROM User WHERE id IN (SELECT id FROM Other)",
      "SELECT name, (SELECT count(*) FROM Other) AS c FROM User",
      "SELECT name FROM User WHERE id IN (SELECT id FROM User)",
      "SELECT name AS _sourceRID FROM User",
      // FACETS adds a facet result computed over every matching record, so a row is no longer a function of one source record
      "SELECT name FROM User FACETS f: name TERMS TOP 10" })
  void rejectedByThePerRecordGuards(final String sql) {
    // simple, so the rejection can only come from a guard of isPerRecord itself
    assertThat(MaterializedViewQueryClassifier.isSimple(sql, database)).as("isSimple: " + sql).isTrue();
    assertThat(MaterializedViewQueryClassifier.isPerRecord(sql, database)).as(sql).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT expand(out('FriendOf')) FROM User",
      "SELECT count(*) FROM User",
      "SELECT name, count(*) FROM User GROUP BY name",
      "SELECT FROM #1:0" })
  void rejectedByIsSimple(final String sql) {
    assertThat(MaterializedViewQueryClassifier.isPerRecord(sql, database)).as(sql).isFalse();
  }
}
