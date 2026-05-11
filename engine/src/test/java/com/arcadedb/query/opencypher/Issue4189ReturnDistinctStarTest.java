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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4189.
 * <p>
 * {@code RETURN DISTINCT *} must preserve distinct rows when the in-scope
 * variables hold different values. Previously the deduplication key was built
 * from the unexpanded {@code *} return item, collapsing every row to the same
 * key.
 */
class Issue4189ReturnDistinctStarTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/test-4189-return-distinct-star").create();
    database.getSchema().createVertexType("Person");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name:'Alice'}), (:Person {name:'Bob'})");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void returnDistinctStarKeepsDistinctRows() {
    final ResultSet rs = database.query("opencypher", "MATCH (p:Person) RETURN DISTINCT *");

    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Vertex p = r.<Vertex>getProperty("p");
      assertThat(p).isNotNull();
      names.add(p.getString("name"));
    }
    assertThat(names).hasSize(2).containsExactlyInAnyOrder("Alice", "Bob");
  }

  @Test
  void returnDistinctStarAfterTrivialWith() {
    final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WITH p RETURN DISTINCT *");

    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) {
      final Vertex p = rs.next().<Vertex>getProperty("p");
      assertThat(p).isNotNull();
      names.add(p.getString("name"));
    }
    assertThat(names).hasSize(2).containsExactlyInAnyOrder("Alice", "Bob");
  }

  /**
   * Control case: {@code RETURN *} (no DISTINCT) must keep returning all rows. This protects
   * against the regression of treating star expansion as deduplication-only.
   */
  @Test
  void returnStarWithoutDistinctKeepsAllRows() {
    final ResultSet rs = database.query("opencypher", "MATCH (p:Person) RETURN * ORDER BY p.name");

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  /**
   * Control case: actual duplicates under {@code RETURN DISTINCT *} must still collapse.
   */
  @Test
  void returnDistinctStarStillCollapsesActualDuplicates() {
    final ResultSet rs = database.query("opencypher", "UNWIND [1,1,2] AS x RETURN DISTINCT *");

    final List<Long> values = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      values.add(r.<Long>getProperty("x"));
    }
    assertThat(values).hasSize(2).containsExactlyInAnyOrder(1L, 2L);
  }

}
