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
package com.arcadedb.index.vector;

import com.arcadedb.database.RID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Plans the second phase of a grouped search whose answer is merged from several sources - one index per bucket, or an
 * index's committed rows plus the calling transaction's own (issue #8002).
 * <p>
 * The {@code groupBy} / {@code groupSize} rule ({@link GroupAdmissionState}, applied in score order) selects the
 * {@code limit} groups with the best peaks and keeps each one's {@code groupSize} best members. A source answering
 * that rule on its own rows returns ITS best {@code limit} groups, which picks the overall winners correctly - a group
 * that wins overall has, in the source holding its peak, fewer than {@code limit} groups peaking above it - but not
 * their members: a winner whose peak lives in one source can have members in another source that ranked it outside
 * its local top {@code limit}, and that source never returns them. Admission over the merged rows then leaves the
 * winner short of its {@code groupSize} although the corpus holds more of it.
 * <p>
 * This class finds, for every source that may have cut such members, the winning groups it has to be asked about
 * again with a group-restricted search, and the floor below which nothing it returns can matter:
 * <ul>
 *   <li>A source that returned fewer than {@code limit} groups returned every group it has, so it is never asked.</li>
 *   <li>A source that returned a winner returned that winner's best members, so it is not asked about it.</li>
 *   <li>A winner absent from a source that returned {@code limit} groups peaks, in that source, no higher than the
 *       source's {@code limit}-th best peak. When the winner already holds {@code groupSize} members at or above that
 *       value, nothing the source could return would displace one, and it is not asked. This is what keeps the common
 *       case - strong winners, filled everywhere - at a single phase.</li>
 * </ul>
 * Everything is "higher is better": a distance-ranked caller passes negated distances. Query-scoped, not thread-safe.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GroupedTopUpPlanner {
  /** A group-restricted search to run against source {@code source}, keeping only values strictly above {@code floor}. */
  public record TopUp(int source, Set<Object> groupKeys, float floor) {
  }

  private final int                          limit;
  private final int                          groupSize;
  /** Best value seen for each RID across every source: a record offered twice counts once (issue #7057). */
  private final HashMap<RID, Float>          bestByRid      = new HashMap<>();
  private final HashMap<RID, Object>         keyByRid       = new HashMap<>();
  private final List<HashMap<Object, Float>> peaksBySource  = new ArrayList<>();
  private final List<Boolean>                cappedBySource = new ArrayList<>();

  public GroupedTopUpPlanner(final int limit, final int groupSize) {
    this.limit = limit;
    this.groupSize = groupSize;
  }

  /**
   * Registers one source's answer.
   *
   * @param rids   the rows it returned
   * @param values their values, parallel to {@code rids}, higher is better
   * @param keys   their group keys, parallel to {@code rids}
   * @param capped whether the source applied the {@code limit} cap itself, and so may have left groups out. A source
   *               that returned every row it holds (a transaction's pending rows) passes {@code false}.
   *
   * @return the source's index, as {@link TopUp#source()} will name it
   */
  public int addSource(final List<RID> rids, final float[] values, final List<Object> keys, final boolean capped) {
    final HashMap<Object, Float> peaks = new HashMap<>();
    for (int i = 0; i < rids.size(); i++) {
      final RID rid = rids.get(i);
      final float value = values[i];
      final Object key = keys.get(i);
      peaks.merge(key, value, Math::max);
      final Float previous = bestByRid.get(rid);
      if (previous == null || value > previous) {
        bestByRid.put(rid, value);
        keyByRid.put(rid, key);
      }
    }
    peaksBySource.add(peaks);
    cappedBySource.add(capped);
    return peaksBySource.size() - 1;
  }

  /** The group-restricted searches to run; empty when the merged answer is already complete. */
  public List<TopUp> plan() {
    final HashMap<Object, GroupRows> groups = new HashMap<>();
    for (final Map.Entry<RID, Float> e : bestByRid.entrySet())
      groups.computeIfAbsent(keyByRid.get(e.getKey()), k -> new GroupRows()).add(e.getValue());
    if (groups.size() == 0)
      return List.of();

    // Winners: the `limit` best peaks overall. A group tied with the last winner is kept too - which of the tied ones
    // the final admission opens depends on row order, so each of them must arrive complete.
    final List<GroupRows> byPeak = new ArrayList<>(groups.values());
    byPeak.sort((a, b) -> Float.compare(b.peak, a.peak));
    final float lastWinningPeak = byPeak.get(Math.min(limit, byPeak.size()) - 1).peak;
    final HashMap<Object, Float> floorOfWinner = new HashMap<>();
    for (final Map.Entry<Object, GroupRows> e : groups.entrySet())
      if (e.getValue().peak >= lastWinningPeak)
        floorOfWinner.put(e.getKey(), e.getValue().floor(groupSize));

    List<TopUp> out = null;
    for (int s = 0; s < peaksBySource.size(); s++) {
      final HashMap<Object, Float> peaks = peaksBySource.get(s);
      if (!cappedBySource.get(s) || peaks.size() < limit)
        continue;

      // The source's limit-th best peak bounds the peak - and so every member - of each group it left out.
      final float[] sourcePeaks = new float[peaks.size()];
      int n = 0;
      for (final float p : peaks.values())
        sourcePeaks[n++] = p;
      Arrays.sort(sourcePeaks);
      final float cutOff = sourcePeaks[sourcePeaks.length - limit];

      Set<Object> missing = null;
      float floor = Float.POSITIVE_INFINITY;
      for (final Map.Entry<Object, Float> winner : floorOfWinner.entrySet()) {
        if (peaks.containsKey(winner.getKey()) || winner.getValue() >= cutOff)
          continue;
        if (missing == null)
          missing = new HashSet<>();
        missing.add(winner.getKey());
        floor = Math.min(floor, winner.getValue());
      }
      if (missing != null) {
        if (out == null)
          out = new ArrayList<>();
        out.add(new TopUp(s, missing, floor));
      }
    }
    return out != null ? out : List.of();
  }

  /** One group's merged values, deduplicated by RID. */
  private static final class GroupRows {
    private float[] values = new float[4];
    private int     size;
    private float   peak   = Float.NEGATIVE_INFINITY;

    private void add(final float value) {
      if (size == values.length)
        values = Arrays.copyOf(values, size * 2);
      values[size++] = value;
      if (value > peak)
        peak = value;
    }

    /** The {@code groupSize}-th best value, or negative infinity when the group holds fewer: it still takes anything. */
    private float floor(final int groupSize) {
      if (size < groupSize)
        return Float.NEGATIVE_INFINITY;
      final float[] sorted = Arrays.copyOf(values, size);
      Arrays.sort(sorted);
      return sorted[size - groupSize];
    }
  }
}
