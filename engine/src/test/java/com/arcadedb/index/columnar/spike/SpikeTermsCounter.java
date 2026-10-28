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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Spike-only terms counter: counts the dictionary ordinals of the matched rows of a {@link SpikeSegment}, and the GROUP BY
 * oracle it is compared against.
 */
final class SpikeTermsCounter {
  record Counts(int[] byOrdinal, long missing, long uncovered) {
  }

  record Bucket(String value, long count) {
  }

  private static final Comparator<Bucket> ORDER = Comparator.comparingLong(Bucket::count).reversed().thenComparing(Bucket::value);

  private SpikeTermsCounter() {
  }

  /** Runs the filter and returns the RID positions of the matches, ascending. */
  static long[] positions(final Database db, final String whereClause, final Object... params) {
    long[] positions = new long[1024];
    int size = 0;
    try (final ResultSet rs = db.query("sql", "SELECT @rid FROM " + SpikeDataset.TYPE + " WHERE " + whereClause, params)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        if (size == positions.length)
          positions = Arrays.copyOf(positions, size * 2);
        positions[size++] = ((RID) r.getProperty("@rid")).getPosition();
      }
    }
    positions = Arrays.copyOf(positions, size);
    Arrays.sort(positions);
    return positions;
  }

  /** Same stripe layout as the presence bitmap; positions on pages the segment does not cover are not set. */
  static long[] matchBitmap(final SpikeSegment segment, final long[] sortedPositions) {
    return matchBitmap(segment.presence().length, segment.maxRecordsInPage(), sortedPositions);
  }

  /** Layout-only variant, shared with the multi-valued segment: positions beyond the stripes are not set. */
  static long[] matchBitmap(final int words, final int maxRecordsInPage, final long[] sortedPositions) {
    final int wordsPerPage = maxRecordsInPage / 64;
    final long pageCount = words / wordsPerPage;
    final long[] match = new long[words];
    for (final long position : sortedPositions) {
      final long page = position / maxRecordsInPage;
      if (page >= pageCount)
        break;
      final int slot = (int) (position % maxRecordsInPage);
      match[(int) page * wordsPerPage + slot / 64] |= 1L << (slot % 64);
    }
    return match;
  }

  static Counts count(final SpikeSegment segment, final long[] matchBitmap, final long[] sortedPositions, final int cardinality) {
    final int wordsPerPage = segment.maxRecordsInPage() / 64;
    final long[] presence = segment.presence();
    final int[] pageRowStart = segment.pageRowStart();
    final int[] byOrdinal = new int[cardinality];
    long missing = 0;

    for (int page = 0; page < segment.pageCount(); page++) {
      final int base = page * wordsPerPage;
      long rank = pageRowStart[page];
      for (int w = 0; w < wordsPerPage; w++) {
        final long present = presence[base + w];
        long bits = matchBitmap[base + w] & present;
        while (bits != 0) {
          final int bit = Long.numberOfTrailingZeros(bits);
          bits &= bits - 1;
          final int stored = segment.storedValueAtRow(rank + Long.bitCount(present & ((1L << bit) - 1)));
          if (stored == 0)
            missing++;
          else
            byOrdinal[stored - 1]++;
        }
        rank += Long.bitCount(present);
      }
    }

    final long firstUncovered = (long) segment.pageCount() * segment.maxRecordsInPage();
    int idx = Arrays.binarySearch(sortedPositions, firstUncovered);
    if (idx < 0)
      idx = -idx - 1;
    return new Counts(byOrdinal, missing, sortedPositions.length - idx);
  }

  /**
   * Same result as {@link #count} over the block-encoded segment: matched rows are walked in row order and each touched block
   * is decoded once into a scratch, a block without matched rows is never decoded.
   */
  static Counts countBlocks(final SpikeBlockSegment blocks, final long[] matchBitmap, final int cardinality) {
    final SpikeSegment segment = blocks.segment();
    final int wordsPerPage = segment.maxRecordsInPage() / 64;
    final long[] presence = segment.presence();
    final int[] pageRowStart = segment.pageRowStart();
    final int[] byOrdinal = new int[cardinality];
    final int[] scratch = new int[SpikeBlockSegment.BLOCK_ROWS];
    int decoded = -1;
    long missing = 0;

    for (int page = 0; page < segment.pageCount(); page++) {
      final int base = page * wordsPerPage;
      long rank = pageRowStart[page];
      for (int w = 0; w < wordsPerPage; w++) {
        final long present = presence[base + w];
        long bits = matchBitmap[base + w] & present;
        while (bits != 0) {
          final int bit = Long.numberOfTrailingZeros(bits);
          bits &= bits - 1;
          final long row = rank + Long.bitCount(present & ((1L << bit) - 1));
          final int block = (int) (row >>> SpikeBlockSegment.BLOCK_SHIFT);
          if (block != decoded) {
            blocks.decodeBlock(block, scratch);
            decoded = block;
          }
          final int stored = scratch[(int) (row & (SpikeBlockSegment.BLOCK_ROWS - 1))];
          if (stored == 0)
            missing++;
          else
            byOrdinal[stored - 1]++;
        }
        rank += Long.bitCount(present);
      }
    }
    return new Counts(byOrdinal, missing, 0L);
  }

  /**
   * Counts records per value of a multi-valued segment: a record counts once per value it lists, and a matched row with no
   * values counts as missing. The match bitmap uses the presence stripe layout, so nothing can be uncovered here.
   */
  static Counts countMulti(final SpikeMultiSegment segment, final long[] matchBitmap, final int cardinality) {
    final int wordsPerPage = segment.maxRecordsInPage() / 64;
    final long[] presence = segment.presence();
    final int[] pageRowStart = segment.pageRowStart();
    final int[] byOrdinal = new int[cardinality];
    long missing = 0;

    for (int page = 0; page < segment.pageCount(); page++) {
      final int base = page * wordsPerPage;
      long rank = pageRowStart[page];
      for (int w = 0; w < wordsPerPage; w++) {
        final long present = presence[base + w];
        long bits = matchBitmap[base + w] & present;
        while (bits != 0) {
          final int bit = Long.numberOfTrailingZeros(bits);
          bits &= bits - 1;
          final int row = (int) (rank + Long.bitCount(present & ((1L << bit) - 1)));
          final int from = segment.rowStart(row);
          final int to = segment.rowStart(row + 1);
          if (from == to)
            missing++;
          for (int i = from; i < to; i++)
            byOrdinal[segment.valueAt(i)]++;
        }
        rank += Long.bitCount(present);
      }
    }
    return new Counts(byOrdinal, missing, 0L);
  }

  /** Ranks on the counts first (count descending, value ascending) and resolves the strings of the winners last. */
  static List<Bucket> topN(final Counts counts, final SpikeDictionary dictionary, final int n) {
    final int[] byOrdinal = counts.byOrdinal();
    final int[] top = new int[Math.min(n, byOrdinal.length)];
    int size = 0;
    for (int ordinal = 0; ordinal < byOrdinal.length; ordinal++) {
      if (byOrdinal[ordinal] == 0)
        continue;
      int pos = size;
      while (pos > 0 && ranksBefore(ordinal, top[pos - 1], byOrdinal, dictionary))
        pos--;
      if (pos >= top.length)
        continue;
      if (size < top.length)
        size++;
      System.arraycopy(top, pos, top, pos + 1, size - pos - 1);
      top[pos] = ordinal;
    }
    final List<Bucket> result = new ArrayList<>(size);
    for (int i = 0; i < size; i++)
      result.add(new Bucket(dictionary.valueOf(top[i]), byOrdinal[top[i]]));
    return result;
  }

  private static boolean ranksBefore(final int a, final int b, final int[] byOrdinal, final SpikeDictionary dictionary) {
    if (byOrdinal[a] != byOrdinal[b])
      return byOrdinal[a] > byOrdinal[b];
    return dictionary.valueOf(a).compareTo(dictionary.valueOf(b)) < 0;
  }

  /** The single place the GROUP BY statement is built, so the benchmark can EXPLAIN exactly what it times. */
  static String groupByQuery(final String whereClause) {
    return groupByQuery(whereClause, SpikeDataset.BRAND);
  }

  static String groupByQuery(final String whereClause, final String property) {
    return "SELECT " + property + ", count(*) AS cnt FROM " + SpikeDataset.TYPE + " WHERE " + whereClause + " GROUP BY " + property;
  }

  /** The oracle: a SQL GROUP BY, with the null group routed to missingOut[0] and ties broken here, not by SQL. */
  static List<Bucket> groupBy(final Database db, final String whereClause, final int n, final long[] missingOut,
      final Object... params) {
    return groupBy(db, whereClause, SpikeDataset.BRAND, n, missingOut, params);
  }

  static List<Bucket> groupBy(final Database db, final String whereClause, final String property, final int n, final long[] missingOut,
      final Object... params) {
    return collect(db, groupByQuery(whereClause, property), property, n, missingOut, params);
  }

  /**
   * The multi-valued oracle: SQL UNWIND of the list property in a subquery, then GROUP BY. Records with an empty or absent
   * list produce no row here (UNWIND drops them), so the caller counts the missing ones separately.
   */
  static List<Bucket> groupByUnwind(final Database db, final String whereClause, final String property, final int n,
      final Object... params) {
    return collect(db, groupByUnwindQuery(whereClause, property), property, n, new long[1], params);
  }

  /** The single place the UNWIND oracle statement is built, so the benchmark can EXPLAIN exactly what it times. */
  static String groupByUnwindQuery(final String whereClause, final String property) {
    return "SELECT " + property + ", count(*) AS cnt FROM (SELECT " + property + " FROM " + SpikeDataset.TYPE + " WHERE " + whereClause
        + " UNWIND " + property + ") GROUP BY " + property;
  }

  private static List<Bucket> collect(final Database db, final String sql, final String property, final int n, final long[] missingOut,
      final Object... params) {
    final List<Bucket> buckets = new ArrayList<>();
    try (final ResultSet rs = db.query("sql", sql, params)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final String value = r.getProperty(property);
        final long count = ((Number) r.getProperty("cnt")).longValue();
        if (value == null)
          missingOut[0] = count;
        else
          buckets.add(new Bucket(value, count));
      }
    }
    buckets.sort(ORDER);
    return buckets.size() > n ? new ArrayList<>(buckets.subList(0, n)) : buckets;
  }
}
