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
package com.arcadedb.engine.timeseries;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TagFilter}, including the {@code matchesMapped} method that
 * handles subset column projections.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TagFilterTest {

  @Test
  void testMatchesFullSchemaOrder() {
    // row[0]=ts, row[1]=col0, row[2]=col1, row[3]=col2
    final Object[] row = { 1000L, "shard-a", 42.0, "region-eu" };
    final TagFilter filter = TagFilter.eq(0, "shard-a");
    assertThat(filter.matches(row)).isTrue();

    final TagFilter noMatch = TagFilter.eq(0, "shard-b");
    assertThat(noMatch.matches(row)).isFalse();
  }

  @Test
  void testMatchesMappedNullColumnIndicesFallsBackToMatches() {
    // null columnIndices → full schema order, same semantics as matches()
    final Object[] row = { 1000L, "shard-a", 42.0, "region-eu" };
    final TagFilter filter = TagFilter.eq(2, "region-eu");
    assertThat(filter.matchesMapped(row, null)).isTrue();
    assertThat(filter.matchesMapped(row, null)).isEqualTo(filter.matches(row));
  }

  @Test
  void testMatchesMappedSubsetColumnsCorrectly() {
    // Regression: column index 2 in a subset projection [0, 2] must resolve to row[2], not row[3].
    // Schema layout (non-ts): col0="shard", col1="value", col2="region"
    // Projection selects col0 and col2 (columnIndices=[0,2]).
    // Row: row[0]=ts, row[1]=col0, row[2]=col2  (col1 omitted)
    final Object[] row = { 1000L, "shard-a", "region-eu" };
    final int[] columnIndices = { 0, 2 };

    // Filter on col2 ("region-eu"), schema index 2 → should map to row[2]
    final TagFilter filter = TagFilter.eq(2, "region-eu");
    assertThat(filter.matchesMapped(row, columnIndices)).isTrue();

    // Without the fix, matches() would check row[3] (out of bounds or wrong) — demonstrate the difference:
    // matches(row) uses row[cond.columnIndex + 1] = row[3], which is out of range → returns false
    assertThat(filter.matches(row)).isFalse(); // row[3] doesn't exist

    final TagFilter noMatch = TagFilter.eq(2, "region-us");
    assertThat(noMatch.matchesMapped(row, columnIndices)).isFalse();
  }

  @Test
  void testMatchesMappedColumnNotInSubset() {
    // If a tag column is not present in the selected subset, matchesMapped returns false
    final Object[] row = { 1000L, "shard-a" }; // only col0 selected
    final int[] columnIndices = { 0 };

    // Filter on col2, which was not included in columnIndices=[0]
    final TagFilter filter = TagFilter.eq(2, "region-eu");
    assertThat(filter.matchesMapped(row, columnIndices)).isFalse();
  }

  @Test
  void testMatchesMappedMultipleConditions() {
    // Two conditions on non-adjacent columns in a subset projection
    // Schema: col0=shard, col1=value, col2=region, col3=env
    // Projection: [0, 3] → row[1]=col0, row[2]=col3
    final Object[] row = { 2000L, "shard-b", "prod" };
    final int[] columnIndices = { 0, 3 };

    final TagFilter filter = TagFilter.eq(0, "shard-b").and(3, "prod");
    assertThat(filter.matchesMapped(row, columnIndices)).isTrue();

    final TagFilter partial = TagFilter.eq(0, "shard-b").and(3, "staging");
    assertThat(partial.matchesMapped(row, columnIndices)).isFalse();
  }
}
