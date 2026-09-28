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

package com.arcadedb.containers.ha.chaos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The {@code ChaosOp} records one node holds as read from its buckets (by RID), not through the unique index on
 * {@code id}. Compared with the same node's index scan it names the records the index does not reach: a second record
 * with an existing id, a record the index has no entry for, a record without an id, and an index entry with no record.
 * {@code count(*)} reads the buckets, so these are the records that make a node's count disagree with its own scan.
 */
public final class RecordScan {
  /** The id of a record whose {@code id} property is missing or null. */
  public static final long NO_ID = Long.MIN_VALUE;

  private long[] rids = new long[1024];
  private long[] ids  = new long[1024];
  private int    size;

  public void add(final long rid, final long id) {
    if (size == rids.length) {
      rids = Arrays.copyOf(rids, size * 2);
      ids = Arrays.copyOf(ids, size * 2);
    }
    rids[size] = rid;
    ids[size++] = id;
  }

  public int size() {
    return size;
  }

  public static long rid(final int bucket, final long position) {
    return ((long) bucket << 40) | position;
  }

  public static long parseRid(final String rid) {
    final int colon = rid.indexOf(':');
    if (!rid.startsWith("#") || colon < 0)
      throw new IllegalArgumentException("Not a RID: " + rid);
    return rid(Integer.parseInt(rid.substring(1, colon)), Long.parseLong(rid.substring(colon + 1)));
  }

  public static String formatRid(final long rid) {
    return "#" + (rid >>> 40) + ":" + (rid & ((1L << 40) - 1));
  }

  /**
   * @return one line per record the node's index does not account for, and per index entry with no record, up to
   * {@code limit} lines, preceded by a count line; empty when the buckets and the index agree
   */
  public List<String> anomalies(final int node, final NodeSnapshot index, final int limit) {
    final Integer[] order = new Integer[size];
    for (int i = 0; i < size; i++)
      order[i] = i;
    Arrays.sort(order, (a, b) -> Long.compare(ids[a], ids[b]));

    final List<String> lines = new ArrayList<>();
    final String prefix = "node " + node + " records: ";
    int distinct = 0;
    for (int i = 0; i < size; ) {
      final long id = ids[order[i]];
      int end = i + 1;
      while (end < size && ids[order[end]] == id)
        ++end;
      if (id == NO_ID) {
        for (int j = i; j < end && lines.size() < limit; j++)
          lines.add(prefix + formatRid(rids[order[j]]) + " has no id");
      } else {
        ++distinct;
        if (end - i > 1 && lines.size() < limit) {
          final List<String> held = new ArrayList<>();
          for (int j = i; j < end; j++)
            held.add(formatRid(rids[order[j]]));
          lines.add(prefix + Ledger.format(id) + " is held by " + (end - i) + " records " + held + " (duplicate id)");
        }
        if (!index.holds(id) && lines.size() < limit)
          lines.add(prefix + Ledger.format(id) + " at " + formatRid(rids[order[i]]) + " is not in the index");
      }
      i = end;
    }

    final long[] sorted = new long[size];
    for (int i = 0; i < size; i++)
      sorted[i] = ids[order[i]];
    index.forEachKey(key -> {
      if (lines.size() < limit && Arrays.binarySearch(sorted, key) < 0)
        lines.add(prefix + "index entry " + Ledger.format(key) + " has no record");
    });

    if (!lines.isEmpty())
      lines.addFirst(prefix + size + " records, " + distinct + " distinct ids, " + index.rows() + " index entries");
    return lines;
  }
}
