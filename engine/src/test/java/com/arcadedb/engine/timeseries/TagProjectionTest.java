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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The index arithmetic behind issue #7733, on its own rather than through a scan.
 * <p>
 * {@link TagProjection} widens a projection with the columns a tag filter needs, and narrows the row back once the
 * filter has passed. The mapping is the delicate part: the scan set is SORTED, because that is the order the
 * decompressors emit columns in whatever order they were asked for them, so the positions to keep are computed
 * against that order and not against the caller's array. A scan test can only ever exercise the shapes its fixture
 * happens to produce - each of them widening by exactly one column - which is why this pins the shapes it cannot
 * (code review on PR #7747).
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7733">issue #7733</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TagProjectionTest {

  /** A row over the scan set, timestamp first, whose values are their own non-timestamp column indices. */
  private static Object[] rowOver(final int[] scanIndices) {
    final Object[] row = new Object[scanIndices.length + 1];
    row[0] = 1_000L;
    for (int i = 0; i < scanIndices.length; i++)
      row[i + 1] = "col" + scanIndices[i];
    return row;
  }

  @Test
  void aProjectionThatAlreadyCarriesEveryFilterColumnIsHandedBackUntouched() {
    final int[] columnIndices = { 0, 2 };
    final TagProjection projection = TagProjection.of(columnIndices, TagFilter.eq(0, "web1").and(2, "eu"));

    assertThat(projection.scanIndices()).as("the caller's own array, not a copy").isSameAs(columnIndices);

    final Object[] row = rowOver(columnIndices);
    assertThat(projection.narrow(row)).as("and the row is not copied either").isSameAs(row);
  }

  @Test
  void aNullProjectionMeansEveryColumnAndNeedsNoWidening() {
    final TagProjection projection = TagProjection.of(null, TagFilter.eq(3, "web1"));

    assertThat(projection.scanIndices()).isNull();
    final Object[] row = rowOver(new int[] { 0, 1, 2, 3 });
    assertThat(projection.narrow(row)).isSameAs(row);
  }

  @Test
  void twoConditionsOnTwoDifferentMissingColumnsBothJoinTheScan() {
    // Asking for column 4 alone, filtering on 1 and 3: the scan reads {1, 3, 4} and hands back {4}.
    final TagProjection projection = TagProjection.of(new int[] { 4 }, TagFilter.eq(1, "web1").and(3, "eu"));

    assertThat(projection.scanIndices()).containsExactly(1, 3, 4);

    final Object[] narrowed = projection.narrow(rowOver(projection.scanIndices()));
    assertThat(narrowed).as("{timestamp, the one column the caller asked for}").containsExactly(1_000L, "col4");
  }

  @Test
  void twoConditionsOnTheSameMissingColumnAddItOnce() {
    final TagProjection projection = TagProjection.of(new int[] { 2 }, TagFilter.eq(0, "web1").and(0, "web2"));

    assertThat(projection.scanIndices()).as("named twice, read once").containsExactly(0, 2);
    assertThat(projection.narrow(rowOver(projection.scanIndices()))).containsExactly(1_000L, "col2");
  }

  /**
   * The mapping is against the SORTED scan set: a filter column that sorts BEFORE the projection's own columns
   * shifts every one of them, which is the case a row-order bug would survive if the widening column always
   * landed last.
   */
  @Test
  void aFilterColumnThatSortsFirstShiftsTheKeptPositions() {
    final TagProjection projection = TagProjection.of(new int[] { 1, 3 }, TagFilter.eq(0, "web1"));

    assertThat(projection.scanIndices()).containsExactly(0, 1, 3);
    assertThat(projection.narrow(rowOver(projection.scanIndices())))
        .as("positions 1 and 2 of the scan row, not 0 and 1")
        .containsExactly(1_000L, "col1", "col3");
  }

  /** A filter column between two projected ones lands between them, and the kept positions straddle it. */
  @Test
  void aFilterColumnInTheMiddleIsSkippedOnTheWayBack() {
    final TagProjection projection = TagProjection.of(new int[] { 0, 5 }, TagFilter.eq(2, "web1"));

    assertThat(projection.scanIndices()).containsExactly(0, 2, 5);
    assertThat(projection.narrow(rowOver(projection.scanIndices()))).containsExactly(1_000L, "col0", "col5");
  }

  @Test
  void noFilterMeansNoWidening() {
    final int[] columnIndices = { 1, 2 };
    final TagProjection projection = TagProjection.of(columnIndices, null);

    assertThat(projection.scanIndices()).isSameAs(columnIndices);
  }

  /**
   * The precondition {@link TagProjection#narrow} depends on, refused rather than silently mis-mapped: an
   * unsorted or duplicated projection would attribute values to the wrong columns, because the scan set is read
   * in the decompressor's schema order.
   */
  @Test
  void anUnsortedOrDuplicatedProjectionIsRefused() {
    assertThatThrownBy(() -> TagProjection.of(new int[] { 3, 1 }, TagFilter.eq(0, "web1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ascending")
        .hasMessageContaining("[3, 1]");

    assertThatThrownBy(() -> TagProjection.of(new int[] { 1, 1 }, TagFilter.eq(0, "web1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("duplicate-free");
  }
}
