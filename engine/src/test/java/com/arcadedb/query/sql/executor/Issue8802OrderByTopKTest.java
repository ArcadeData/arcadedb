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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #8802: an ORDER BY bounded by LIMIT (or SKIP + LIMIT) keeps its best rows in a heap instead of sorting
 * the buffer every few rows. The answer must be the slice of the unbounded sort, ties included: among equal keys the row
 * that arrived first wins, as the stable sort-and-truncate did.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8802OrderByTopKTest extends TestHelper {
  private static final int N = 5_000;

  @BeforeEach
  void load() {
    database.command("sql", "CREATE VERTEX TYPE V BUCKETS 1");
    database.command("sql", "CREATE PROPERTY V.seq INTEGER");
    database.command("sql", "CREATE PROPERTY V.x INTEGER");
    database.command("sql", "CREATE PROPERTY V.y INTEGER");
    database.transaction(() -> {
      for (int i = 0; i < N; i++)
        // many ties on x (50 distinct values), a second key y to order the ties by
        database.newVertex("V").set("seq", i, "x", (i * 31) % 50, "y", (i * 17) % 7).save();
    });
  }

  private List<Integer> seqs(final String sql) {
    final List<Integer> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        result.add(rs.next().<Number>getProperty("seq").intValue());
    }
    return result;
  }

  private void assertBoundedIsSliceOfUnbounded(final String orderBy) {
    final List<Integer> all = seqs("SELECT seq, x, y FROM V ORDER BY " + orderBy);
    assertThat(all).hasSize(N);
    for (final int[] window : new int[][] { { 0, 1 }, { 0, 10 }, { 0, 77 }, { 5, 10 }, { 1000, 10 }, { 4990, 100 }, { 0, N }, { 0, N + 100 } }) {
      final int skip = window[0], limit = window[1];
      final String sql = "SELECT seq, x, y FROM V ORDER BY " + orderBy + (skip > 0 ? " SKIP " + skip : "") + " LIMIT " + limit;
      final List<Integer> expected = all.subList(Math.min(skip, N), Math.min(skip + limit, N));
      assertThat(seqs(sql)).as(sql).isEqualTo(expected);
    }
  }

  @Test
  void singleKeyWithTies() {
    assertBoundedIsSliceOfUnbounded("x ASC");
    assertBoundedIsSliceOfUnbounded("x DESC");
  }

  @Test
  void compositeKeyWithTies() {
    assertBoundedIsSliceOfUnbounded("x ASC, y DESC");
    assertBoundedIsSliceOfUnbounded("y DESC, x ASC");
  }

  @Test
  void limitZeroAndSkipPastTheEnd() {
    assertThat(seqs("SELECT seq FROM V ORDER BY x LIMIT 0")).isEmpty();
    assertThat(seqs("SELECT seq FROM V ORDER BY x SKIP " + (N + 10) + " LIMIT 5")).isEmpty();
  }
}
