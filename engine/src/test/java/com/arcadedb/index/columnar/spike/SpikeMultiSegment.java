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

import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.engine.LocalBucket;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Spike-only columnar segment of one bucket and one multi-valued string property: the same presence stripes as
 * {@link SpikeSegment}, a per row offset array and the dictionary ordinals (no MISSING slot) bit-packed in row order. An empty
 * or absent list is a row with no values.
 */
final class SpikeMultiSegment {
  private final int             maxRecordsInPage;
  private final int             pageCount;
  private final int             rows;
  private final int             bitWidth;
  private final long[]          presence;
  private final int[]           pageRowStart;
  private final int[]           rowOffsets;
  private final long[]          packed;
  private final SpikeDictionary dictionary;

  private SpikeMultiSegment(final int maxRecordsInPage, final int pageCount, final int rows, final int bitWidth, final long[] presence,
      final int[] pageRowStart, final int[] rowOffsets, final long[] packed, final SpikeDictionary dictionary) {
    this.maxRecordsInPage = maxRecordsInPage;
    this.pageCount = pageCount;
    this.rows = rows;
    this.bitWidth = bitWidth;
    this.presence = presence;
    this.pageRowStart = pageRowStart;
    this.rowOffsets = rowOffsets;
    this.packed = packed;
    this.dictionary = dictionary;
  }

  static SpikeMultiSegment build(final LocalBucket bucket, final String property, final SpikeDictionary dictionary) {
    final int maxRecordsInPage = bucket.getMaxRecordsInPage();
    if (maxRecordsInPage % 64 != 0)
      throw new IllegalArgumentException("maxRecordsInPage must be a multiple of 64, got " + maxRecordsInPage);
    final int wordsPerPage = maxRecordsInPage / 64;
    final int pageCount = bucket.getTotalPages();
    final long[] presence = new long[pageCount * wordsPerPage];
    int[] offsets = new int[1025];
    int[] values = new int[4096];
    int rows = 0;
    int total = 0;

    final Iterator<Record> it = bucket.iterator();
    while (it.hasNext()) {
      final Record record = it.next();
      final long position = record.getIdentity().getPosition();
      final int page = (int) (position / maxRecordsInPage);
      final int slot = (int) (position % maxRecordsInPage);
      presence[page * wordsPerPage + slot / 64] |= 1L << (slot % 64);
      final List<String> list = ((Document) record).getList(property);
      final int rowStart = total;
      if (list != null)
        for (final String value : list) {
          if (value == null)
            continue;
          final int ordinal = dictionary.getOrAdd(value);
          // a value listed twice in one record counts once
          boolean duplicate = false;
          for (int i = rowStart; i < total && !duplicate; i++)
            duplicate = values[i] == ordinal;
          if (duplicate)
            continue;
          if (total == values.length)
            values = Arrays.copyOf(values, total * 2);
          values[total++] = ordinal;
        }
      if (rows + 2 > offsets.length)
        offsets = Arrays.copyOf(offsets, offsets.length * 2);
      offsets[++rows] = total;
    }

    final int[] pageRowStart = new int[pageCount + 1];
    for (int p = 0; p < pageCount; p++) {
      int count = 0;
      for (int w = 0; w < wordsPerPage; w++)
        count += Long.bitCount(presence[p * wordsPerPage + w]);
      pageRowStart[p + 1] = pageRowStart[p] + count;
    }

    // ordinals are 0..size-1
    final int bitWidth = Math.max(1, 32 - Integer.numberOfLeadingZeros(Math.max(1, dictionary.size() - 1)));
    final long[] packed = new long[(int) (((long) total * bitWidth + 63) / 64)];
    for (int i = 0; i < total; i++) {
      final long bit = (long) i * bitWidth;
      final int word = (int) (bit >>> 6);
      final int shift = (int) (bit & 63);
      packed[word] |= (long) values[i] << shift;
      if (shift + bitWidth > 64)
        packed[word + 1] |= (long) values[i] >>> (64 - shift);
    }
    return new SpikeMultiSegment(maxRecordsInPage, pageCount, rows, bitWidth, presence, pageRowStart,
        Arrays.copyOf(offsets, rows + 1), packed, dictionary);
  }

  int maxRecordsInPage() {
    return maxRecordsInPage;
  }

  int pageCount() {
    return pageCount;
  }

  int rows() {
    return rows;
  }

  int bitWidth() {
    return bitWidth;
  }

  long[] presence() {
    return presence;
  }

  int[] pageRowStart() {
    return pageRowStart;
  }

  /** Index into the value stream of the first value of the row; rowStart(row + 1) is the exclusive end. */
  int rowStart(final int row) {
    return rowOffsets[row];
  }

  /** The dictionary ordinal at the given index of the value stream. */
  int valueAt(final int index) {
    final long bit = (long) index * bitWidth;
    final int word = (int) (bit >>> 6);
    final int shift = (int) (bit & 63);
    long v = packed[word] >>> shift;
    if (shift + bitWidth > 64)
      v |= packed[word + 1] << (64 - shift);
    return (int) (v & ((1L << bitWidth) - 1));
  }

  long serializedBytes() {
    return 4L + 4 + 4 + 4 + presence.length * 8L + pageRowStart.length * 4L + rowOffsets.length * 4L + packed.length * 8L;
  }
}
