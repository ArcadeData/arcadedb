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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ORDER BY must produce a deterministic, stable order even when the sort column holds values that are
 * not {@link Comparable} (lists, embedded documents/maps, custom types). Previously the comparison was
 * skipped for such values and silently returned "equal", leaving the affected rows in arbitrary order.
 */
class OrderByNonComparableTest extends TestHelper {
  private static final String TYPE_NAME = "doc";

  @Test
  void orderByListPropertyIsDeterministic() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);

    // Insert in an order that does NOT match the sorted order, so a no-op comparison would leave the
    // rows in insertion order and fail the assertions below.
    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + TYPE_NAME + " CONTENT {\"id\": \"x\", \"tags\": [\"b\"]}");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " CONTENT {\"id\": \"y\", \"tags\": [\"a\"]}");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " CONTENT {\"id\": \"z\", \"tags\": [\"c\"]}");
    });

    // ASC: lists ordered by their string form -> [a] < [b] < [c] -> ids y, x, z
    final List<String> asc = idsOrderedBy("ORDER BY tags ASC");
    assertThat(asc).containsExactly("y", "x", "z");

    // DESC must be the exact reverse, proving the comparison is real and direction-aware.
    final List<String> desc = idsOrderedBy("ORDER BY tags DESC");
    assertThat(desc).containsExactly("z", "x", "y");
  }

  @Test
  void orderByEmbeddedMapPropertyIsDeterministic() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);

    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + TYPE_NAME + " CONTENT {\"id\": \"x\", \"meta\": {\"k\": 2}}");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " CONTENT {\"id\": \"y\", \"meta\": {\"k\": 1}}");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " CONTENT {\"id\": \"z\", \"meta\": {\"k\": 3}}");
    });

    // The result must be a deterministic permutation of all rows (not silently left unordered) and the
    // DESC order must be the exact reverse of the ASC order.
    final List<String> asc = idsOrderedBy("ORDER BY meta ASC");
    final List<String> desc = idsOrderedBy("ORDER BY meta DESC");

    assertThat(asc).containsExactlyInAnyOrder("x", "y", "z");
    final List<String> reversed = new ArrayList<>(asc);
    Collections.reverse(reversed);
    assertThat(desc).containsExactlyElementsOf(reversed);
  }

  private List<String> idsOrderedBy(final String orderByClause) {
    final List<String> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " " + orderByClause)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        ids.add(r.getProperty("id"));
      }
    }
    return ids;
  }
}
