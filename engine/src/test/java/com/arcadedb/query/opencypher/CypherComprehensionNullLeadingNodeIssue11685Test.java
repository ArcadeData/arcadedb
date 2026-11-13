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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11685: a pattern comprehension whose leading node variable is bound to null (an OPTIONAL
 * MATCH miss, or {@code WITH null AS a}) was treated as an unbound variable and scanned the whole graph, instead of
 * producing an empty list.
 */
class CypherComprehensionNullLeadingNodeIssue11685Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-comprehension-null-leading-11685");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:Node {id:1}), (b:Node {id:2}), (c:Node {id:3}),
               (a)-[:R {k:1}]->(b), (b)-[:R {k:2}]->(c)"""));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void optionalMatchMissYieldsEmptyList() {
    try (final ResultSet rs = database.query("opencypher",
        "OPTIONAL MATCH (a:Node {id: 999}) RETURN a, [(a)-[:R]->(b) | b.id] AS targets")) {
      final Result row = rs.next();
      assertThat((Object) row.getProperty("a")).isNull();
      assertThat((List<?>) row.getProperty("targets")).isEmpty();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void withNullLeadingNodeYieldsEmptyList() {
    assertThat(single("WITH null AS a RETURN [(a)-[:R]->(b) | b.id] AS r")).isEqualTo(List.of());
  }

  @Test
  void sizeOfNullLeadingComprehensionIsZero() {
    assertThat(((Number) single("OPTIONAL MATCH (a:Node {id: 999}) RETURN size([(a)-[:R]->(b) | b.id]) AS r")).longValue())
        .isEqualTo(0L);
  }

  @Test
  void nullBoundEndNodeYieldsEmptyList() {
    assertThat(single("MATCH (a:Node {id: 1}) WITH a, null AS b RETURN [(a)-[:R]->(b) | 1] AS r")).isEqualTo(List.of());
  }

  @Test
  void boundControlsAreUnchanged() {
    assertThat(single("MATCH (a:Node {id: 1}) RETURN [(a)-[:R]->(b) | b.id] AS r")).isEqualTo(List.of(2));
    assertThat(single("MATCH (a:Node {id: 3}) RETURN [(a)-[:R]->(b) | b.id] AS r")).isEqualTo(List.of());
    assertThat(((List<?>) single("RETURN [(a)-[:R]->(b) | b.id] AS r"))).hasSize(2);
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().getProperty("r");
    }
  }
}
