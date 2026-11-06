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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Spike-only columnar segment of one bucket and one string property: a presence bitmap indexed by RID position, a per page
 * running row count, and the dictionary ordinals (+1, 0 meaning MISSING) bit-packed in row order.
 */
final class SpikeSegment {
  static final int MISSING = -1;

  private final int              maxRecordsInPage;
  private final int              wordsPerPage;
  private final int              pageCount;
  private final long             rows;
  private final int              bitWidth;
  private final long[]           presence;
  private final int[]            pageRowStart;
  private final long[]           packed;
  private final SpikeDictionary  dictionary;

  private SpikeSegment(final int maxRecordsInPage, final int pageCount, final long rows, final int bitWidth, final long[] presence,
      final int[] pageRowStart, final long[] packed, final SpikeDictionary dictionary) {
    this.maxRecordsInPage = maxRecordsInPage;
    this.wordsPerPage = maxRecordsInPage / 64;
    this.pageCount = pageCount;
    this.rows = rows;
    this.bitWidth = bitWidth;
    this.presence = presence;
    this.pageRowStart = pageRowStart;
    this.packed = packed;
    this.dictionary = dictionary;
  }

  /** The dictionary may be pre-populated: ordinals are global, so the bit width follows the whole dictionary size. */
  static SpikeSegment build(final LocalBucket bucket, final String property, final SpikeDictionary dictionary) {
    final int maxRecordsInPage = bucket.getMaxRecordsInPage();
    if (maxRecordsInPage % 64 != 0)
      throw new IllegalArgumentException("maxRecordsInPage must be a multiple of 64, got " + maxRecordsInPage);
    final int wordsPerPage = maxRecordsInPage / 64;
    final int pageCount = bucket.getTotalPages();
    final long[] presence = new long[pageCount * wordsPerPage];
    int[] values = new int[1024];
    int rows = 0;

    final Iterator<Record> it = bucket.iterator();
    while (it.hasNext()) {
      final Record record = it.next();
      final long position = record.getIdentity().getPosition();
      final int page = (int) (position / maxRecordsInPage);
      final int slot = (int) (position % maxRecordsInPage);
      presence[page * wordsPerPage + slot / 64] |= 1L << (slot % 64);
      final String value = ((Document) record).getString(property);
      if (rows == values.length)
        values = Arrays.copyOf(values, rows * 2);
      values[rows++] = value == null ? 0 : dictionary.getOrAdd(value) + 1;
    }

    final int[] pageRowStart = new int[pageCount + 1];
    for (int p = 0; p < pageCount; p++) {
      int count = 0;
      for (int w = 0; w < wordsPerPage; w++)
        count += Long.bitCount(presence[p * wordsPerPage + w]);
      pageRowStart[p + 1] = pageRowStart[p] + count;
    }

    // stored values are 0..size, so the width must hold `size`
    final int bitWidth = Math.max(1, 32 - Integer.numberOfLeadingZeros(dictionary.size()));
    final long[] packed = new long[(int) (((long) rows * bitWidth + 63) / 64)];
    for (int row = 0; row < rows; row++) {
      final long bit = (long) row * bitWidth;
      final int word = (int) (bit >>> 6);
      final int shift = (int) (bit & 63);
      packed[word] |= (long) values[row] << shift;
      if (shift + bitWidth > 64)
        packed[word + 1] |= (long) values[row] >>> (64 - shift);
    }
    return new SpikeSegment(maxRecordsInPage, pageCount, rows, bitWidth, presence, pageRowStart, packed, dictionary);
  }

  int maxRecordsInPage() {
    return maxRecordsInPage;
  }

  int pageCount() {
    return pageCount;
  }

  long rows() {
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

  /** The bit-packed stored values in row order, for sequential scans. Not a copy: callers must not modify it. */
  long[] packed() {
    return packed;
  }

  boolean isPresent(final long position) {
    final int page = (int) (position / maxRecordsInPage);
    if (page >= pageCount)
      return false;
    final int slot = (int) (position % maxRecordsInPage);
    return (presence[page * wordsPerPage + slot / 64] & (1L << (slot % 64))) != 0;
  }

  /** Returns the dictionary ordinal, or MISSING for a present row without value. */
  int ordinalAt(final long position) {
    if (!isPresent(position))
      throw new IllegalArgumentException("Position " + position + " is not present in the segment");
    return readStored(rowOf(position)) - 1;
  }

  private long rowOf(final long position) {
    final int page = (int) (position / maxRecordsInPage);
    final int slot = (int) (position % maxRecordsInPage);
    final int base = page * wordsPerPage;
    final int wordInPage = slot / 64;
    long row = pageRowStart[page];
    for (int w = 0; w < wordInPage; w++)
      row += Long.bitCount(presence[base + w]);
    return row + Long.bitCount(presence[base + wordInPage] & ((1L << (slot % 64)) - 1));
  }

  private int readStored(final long row) {
    final long bit = row * bitWidth;
    final int word = (int) (bit >>> 6);
    final int shift = (int) (bit & 63);
    long v = packed[word] >>> shift;
    if (shift + bitWidth > 64)
      v |= packed[word + 1] << (64 - shift);
    return (int) (v & ((1L << bitWidth) - 1));
  }

  /** Returns the stored value (ordinal + 1, 0 meaning MISSING) of the given row. */
  int storedValueAtRow(final long row) {
    return readStored(row);
  }

  byte[] serialize() {
    final byte[][] strings = new byte[dictionary.size()][];
    int dictionaryBytes = 0;
    for (int i = 0; i < strings.length; i++) {
      strings[i] = dictionary.valueOf(i).getBytes(StandardCharsets.UTF_8);
      dictionaryBytes += 4 + strings[i].length;
    }
    final int size = 4 + 4 + 8 + 4 + presence.length * 8 + pageRowStart.length * 4 + packed.length * 8 + 4 + dictionaryBytes;
    final ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(maxRecordsInPage).putInt(pageCount).putLong(rows).putInt(bitWidth);
    for (final long w : presence)
      buffer.putLong(w);
    for (final int s : pageRowStart)
      buffer.putInt(s);
    for (final long w : packed)
      buffer.putLong(w);
    buffer.putInt(strings.length);
    for (final byte[] s : strings)
      buffer.putInt(s.length).put(s);
    return buffer.array();
  }

  long heapBytes() {
    return presence.length * 8L + pageRowStart.length * 4L + packed.length * 8L;
  }
}
