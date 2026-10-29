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

import com.arcadedb.test.support.FacetSnapshot;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Scripted {@link FacetReader}: {@link #data} is the {@code GROUP BY} truth per node and, unless a script says otherwise, the
 * facets are an exact settled copy of it. The script applies to the unfiltered query only; a filtered query reads
 * {@link #data} and answers {@link #filteredFacets} (default: what {@code GROUP BY} says).
 */
final class FakeFacetReader implements FacetReader {
  final Map<Integer, SortedMap<String, Long>> data           = new HashMap<>();
  final Map<Integer, SortedMap<String, Long>> filteredFacets = new HashMap<>();
  final Map<Integer, Deque<FacetSnapshot>>    script         = new HashMap<>();
  final Set<Integer>                          serverError    = new HashSet<>();
  final Set<Integer>                          unreadable     = new HashSet<>();
  final Set<Integer>                          facetsServerError = new HashSet<>();
  final Set<Integer>                          facetsUnreadable  = new HashSet<>();
  final Set<Integer>                          clientError       = new HashSet<>();
  /** Remaining GROUP BY reads of a node that fail with an IOException. */
  final Map<Integer, Integer>                 groupByFailures   = new HashMap<>();
  int                                         groupByCalls;
  int                                         groupByCallsAtFirstFacetRead = -1;
  Runnable                                    afterFirstFacetRead;
  Runnable                                    afterEveryFacetRead;

  static FacetSnapshot sample(final boolean exact, final long lagMs, final String source, final SortedMap<String, Long> counts) {
    return sample(exact, 0L, lagMs, source, counts);
  }

  static FacetSnapshot sample(final boolean exact, final long patched, final long lagMs, final String source,
      final SortedMap<String, Long> counts) {
    final Map<String, String> sources = new TreeMap<>();
    for (final String alias : new String[] { "brand", "tags", "size" })
      sources.put(alias, source);
    return new FacetSnapshot(exact, patched, lagMs, sources, new TreeMap<>(counts));
  }

  @Override
  public synchronized SortedMap<String, Long> groupBy(final int node, final String where) throws IOException {
    check(node);
    ++groupByCalls;
    final int failures = groupByFailures.getOrDefault(node, 0);
    if (failures > 0) {
      groupByFailures.put(node, failures - 1);
      throw new IOException("node " + node + " GROUP BY failed");
    }
    return new TreeMap<>(truth(node, where));
  }

  @Override
  public synchronized FacetSnapshot facets(final int node, final String where) throws IOException {
    check(node);
    if (facetsUnreadable.contains(node))
      throw new IOException("node " + node + " facets failed");
    if (facetsServerError.contains(node))
      throw new HttpNodeReader.ServerErrorException(node, 500, "boom");
    if (groupByCallsAtFirstFacetRead < 0)
      groupByCallsAtFirstFacetRead = groupByCalls;
    final Deque<FacetSnapshot> scripted = script.get(node);
    final FacetSnapshot snapshot;
    if (where.isEmpty() && scripted != null && !scripted.isEmpty())
      snapshot = scripted.size() > 1 ? scripted.poll() : scripted.peek();
    else
      snapshot = sample(true, 0L, "index", where.isEmpty() ? truth(node, where) : filteredFacets.getOrDefault(node, truth(node, where)));
    if (afterFirstFacetRead != null) {
      final Runnable action = afterFirstFacetRead;
      afterFirstFacetRead = null;
      action.run();
    }
    if (afterEveryFacetRead != null)
      afterEveryFacetRead.run();
    return snapshot;
  }

  private SortedMap<String, Long> truth(final int node, final String where) {
    return data.get(node);
  }

  private void check(final int node) throws IOException {
    if (unreadable.contains(node))
      throw new IOException("node " + node + " down");
    if (clientError.contains(node))
      throw new HttpNodeReader.ClientErrorException(node, 400, "bad query");
    if (serverError.contains(node))
      throw new HttpNodeReader.ServerErrorException(node, 500, "boom");
  }
}
