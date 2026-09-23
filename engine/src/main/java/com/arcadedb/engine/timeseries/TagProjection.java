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

import java.util.Arrays;

/**
 * The columns a SEALED scan has to read so that a tag filter can be evaluated, and the columns the caller asked
 * to be handed back (issue #7733).
 * <p>
 * The two are not always the same set. A sealed slow-path block is filtered by testing the MATERIALISED row, so a
 * condition on a column the projection leaves out has nothing to test and used to drop every row: the query
 * answered nothing, with HTTP 200 and {@code truncated:false}, while the same query over a fast-path block - whose
 * decision is made on the block's tag metadata and never looks at the projection - answered the matching rows. The
 * answer therefore changed as compaction moved data between the two.
 * <p>
 * The filter columns are read as well, and the row is narrowed to the projection only once it has passed. Nothing
 * is read that neither side needs, and a query whose projection already covers the filter - the common one - pays
 * nothing at all: {@link #scanIndices()} IS the caller's array and {@link #narrow} hands the row straight back.
 * <p>
 * Both index arrays are in the non-timestamp schema order the sealed decompressor emits columns in, which is what
 * makes the narrowed row identical to the one an unfiltered scan of the same projection would have produced.
 * <p>
 * <b>The projection is expected ASCENDING and without duplicates</b>, which is what every caller builds - the
 * column resolvers in {@code TimeSeriesGateway} walk the schema in order - and what the decompressors already
 * assume: they emit the selected columns in schema order whatever order they were asked in, so a row built from
 * an unsorted projection maps its values to the wrong names on EVERY read path, filtered or not. Nothing here
 * re-sorts the caller's array or refuses it: a widened scan set is sorted because the decompressor's own order is
 * what {@link #narrow} has to read, and an already-covering projection is handed back untouched.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class TagProjection {

  private final int[]   scanIndices;
  /** Positions of {@link #scanIndices} the caller asked for, or {@code null} when that is all of them. */
  private final int[]   keep;

  private TagProjection(final int[] scanIndices, final int[] keep) {
    this.scanIndices = scanIndices;
    this.keep = keep;
  }

  /**
   * The scan set for {@code columnIndices} under {@code tagFilter}.
   *
   * @param columnIndices the caller's projection, {@code null} for every column - in which case there is nothing
   *                      to widen, because every filter column is already there
   */
  static TagProjection of(final int[] columnIndices, final TagFilter tagFilter) {
    if (columnIndices == null || tagFilter == null)
      return new TagProjection(columnIndices, null);

    requireAscendingAndDistinct(columnIndices);

    int missing = 0;
    for (final TagFilter.Condition cond : tagFilter.getConditions())
      if (!contains(columnIndices, columnIndices.length, cond.columnIndex()))
        ++missing;

    // The common case, and the one worth not paying for: the projection already carries every filter column, so
    // the caller's own array is the scan set and no row will be copied.
    if (missing == 0)
      return new TagProjection(columnIndices, null);

    // `missing` counts a column named by two conditions twice, which only over-allocates: the fill below skips a
    // column already added and the array is trimmed to what was really written.
    final int[] widened = Arrays.copyOf(columnIndices, columnIndices.length + missing);
    int count = columnIndices.length;
    for (final TagFilter.Condition cond : tagFilter.getConditions())
      if (!contains(widened, count, cond.columnIndex()))
        widened[count++] = cond.columnIndex();

    // The decompressor emits columns in schema order whatever order it was asked in, so the scan set is sorted and
    // the positions to keep are computed against that order rather than against the caller's.
    final int[] scanIndices = Arrays.copyOf(widened, count);
    Arrays.sort(scanIndices);

    final int[] keep = new int[columnIndices.length];
    int kept = 0;
    for (int i = 0; i < scanIndices.length; i++)
      if (contains(columnIndices, columnIndices.length, scanIndices[i]))
        keep[kept++] = i;

    return new TagProjection(scanIndices, Arrays.copyOf(keep, kept));
  }

  /** The columns the scan must materialise: the projection plus whatever the filter needs on top of it. */
  int[] scanIndices() {
    return scanIndices;
  }

  /**
   * Narrows a row built over {@link #scanIndices()} down to the caller's projection, timestamp included. Returns
   * {@code row} itself when nothing was widened, so the common path allocates nothing.
   */
  Object[] narrow(final Object[] row) {
    if (keep == null)
      return row;

    final Object[] narrowed = new Object[keep.length + 1];
    narrowed[0] = row[0];
    for (int i = 0; i < keep.length; i++)
      narrowed[i + 1] = row[keep[i] + 1];
    return narrowed;
  }

  /**
   * Enforces the precondition {@link #narrow} depends on (code review on PR #7747).
   * <p>
   * A real check and not an {@code assert}, although this is a contract between engine paths rather than anything
   * a user can reach: assertions are off in every default production JVM, and the failure mode a violation
   * produces is the silent kind - the widened {@link #narrow} reads the decompressor's schema order, so an
   * unsorted projection would hand back values attributed to the WRONG columns rather than fail. The check is a
   * handful of int comparisons over a tiny array, once per scan and not once per row, which is not a price worth
   * trading for that.
   */
  private static void requireAscendingAndDistinct(final int[] columnIndices) {
    for (int i = 1; i < columnIndices.length; i++)
      if (columnIndices[i] <= columnIndices[i - 1])
        throw new IllegalArgumentException(
            "A TimeSeries projection must be ascending and duplicate-free, and this one is not: "
                + Arrays.toString(columnIndices));
  }

  private static boolean contains(final int[] array, final int length, final int value) {
    for (int i = 0; i < length; i++)
      if (array[i] == value)
        return true;
    return false;
  }
}
