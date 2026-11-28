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
package com.arcadedb.bench.server;

import java.util.Arrays;

/**
 * Splits a Docker {@code --cpuset-cpus} list among cluster nodes and sizes each node's memory and heap.
 */
public final class Cpusets {
  private Cpusets() {
  }

  /**
   * Splits a list such as {@code 0-3,8,10-11} into {@code parts} contiguous groups, in the listed order, whose sizes differ by
   * at most one (the first groups get the extra CPU). An empty list gives empty strings (unpinned); fewer CPUs than parts gives
   * the whole set to every part.
   */
  public static String[] split(final String cpus, final int parts) {
    final String[] result = new String[parts];
    final int[] ids = parse(cpus);
    if (ids.length < parts) {
      Arrays.fill(result, format(ids, 0, ids.length));
      return result;
    }
    final int base = ids.length / parts;
    final int extra = ids.length % parts;
    int from = 0;
    for (int i = 0; i < parts; i++) {
      final int to = from + base + (i < extra ? 1 : 0);
      result[i] = format(ids, from, to);
      from = to;
    }
    return result;
  }

  public static int nodeMemGb(final int memLimitGb, final int nodes) {
    return Math.max(2, memLimitGb / nodes);
  }

  public static String nodeHeap(final int nodeMemGb) {
    return Math.max(1, nodeMemGb * 6 / 10) + "g";
  }

  private static int[] parse(final String cpus) {
    if (cpus == null || cpus.isBlank())
      return new int[0];
    int[] ids = new int[16];
    int count = 0;
    for (final String token : cpus.split(",")) {
      final String t = token.trim();
      if (t.isEmpty())
        continue;
      final int dash = t.indexOf('-');
      final int lo = Integer.parseInt(dash < 0 ? t : t.substring(0, dash).trim());
      final int hi = dash < 0 ? lo : Integer.parseInt(t.substring(dash + 1).trim());
      if (hi < lo)
        throw new IllegalArgumentException("Invalid cpuset range: " + t);
      for (int id = lo; id <= hi; id++) {
        if (count == ids.length)
          ids = Arrays.copyOf(ids, count * 2);
        ids[count++] = id;
      }
    }
    return Arrays.copyOf(ids, count);
  }

  private static String format(final int[] ids, final int from, final int to) {
    final StringBuilder sb = new StringBuilder();
    int i = from;
    while (i < to) {
      int j = i;
      while (j + 1 < to && ids[j + 1] == ids[j] + 1)
        j++;
      if (sb.length() > 0)
        sb.append(',');
      sb.append(ids[i]);
      if (j > i)
        sb.append('-').append(ids[j]);
      i = j + 1;
    }
    return sb.toString();
  }
}
