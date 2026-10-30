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
package com.arcadedb.index.columnar.spike;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;

/**
 * Spike-only native match sources: they build the match bitmap (same stripe layout as
 * {@link SpikeTermsCounter#matchBitmap}) without SQL and without {@code Result} objects.
 */
final class SpikeMatchSource {
  private SpikeMatchSource() {
  }

  /**
   * Builds the bitmap of the records whose indexed LONG property is in [fromInclusive, toExclusive) straight from the index
   * cursor. Positions on pages the segment does not cover are not set, they are added to uncoveredOut[0].
   */
  static long[] indexRange(final Database db, final SpikeSegment layout, final String property, final long fromInclusive,
      final long toExclusive, final long[] uncoveredOut) {
    final TypeIndex index = db.getSchema().getType(SpikeDataset.TYPE).getPolymorphicIndexByProperties(property);
    if (index == null)
      throw new IllegalArgumentException("No index on " + SpikeDataset.TYPE + "." + property);

    final int maxRecordsInPage = layout.maxRecordsInPage();
    final int wordsPerPage = maxRecordsInPage / 64;
    final int pageCount = layout.pageCount();
    final long[] match = new long[layout.presence().length];
    long uncovered = 0;

    // closed even when the loop throws: the benchmark calls this once per round
    try (final IndexCursor cursor = ((RangeIndex) index).range(true, new Object[] { fromInclusive }, true,
        new Object[] { toExclusive }, false)) {
      while (cursor.hasNext()) {
        final Identifiable identifiable = cursor.next();
        final long position = identifiable.getIdentity().getPosition();
        final long page = position / maxRecordsInPage;
        if (page >= pageCount) {
          uncovered++;
          continue;
        }
        final int slot = (int) (position % maxRecordsInPage);
        match[(int) page * wordsPerPage + slot / 64] |= 1L << (slot % 64);
      }
    }
    uncoveredOut[0] = uncovered;
    return match;
  }

  /**
   * Scans the stored values of the segment for ordinalOf(value) + 1, sequentially in row order as a real columnar filter
   * would: one running bit offset into the packed words, no per row rank or random-access unpack. An unknown value gives an
   * empty bitmap.
   */
  static long[] columnEquals(final SpikeSegment column, final SpikeDictionary dictionary, final String value) {
    final long[] match = new long[column.presence().length];
    final int ordinal = dictionary.ordinalOf(value);
    if (ordinal < 0)
      return match;

    final long target = ordinal + 1;
    final long[] presence = column.presence();
    final long[] packed = column.packed();
    final int width = column.bitWidth();
    final long mask = (1L << width) - 1;
    // rows are stored in presence order, so walking the presence bits in order visits the rows 0, 1, 2, ...
    long bit = 0;
    for (int w = 0; w < presence.length; w++) {
      long bits = presence[w];
      long hits = 0;
      while (bits != 0) {
        final int slot = Long.numberOfTrailingZeros(bits);
        bits &= bits - 1;
        final int word = (int) (bit >>> 6);
        final int shift = (int) (bit & 63);
        long v = packed[word] >>> shift;
        if (shift + width > 64)
          v |= packed[word + 1] << (64 - shift);
        if ((v & mask) == target)
          hits |= 1L << slot;
        bit += width;
      }
      match[w] = hits;
    }
    return match;
  }

  static long cardinality(final long[] bitmap) {
    long total = 0;
    for (final long word : bitmap)
      total += Long.bitCount(word);
    return total;
  }
}
