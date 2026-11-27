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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayList;
import java.util.List;

/**
 * The record ids that other indexes than the one an index search reads name for the same AND block, so that the search loads only
 * the records every index names (issue #13289). {@code WHERE updated_at = ? AND id > ?}, with an index on each property, fetched
 * from one of the two indexes, loaded every record it named and evaluated the other condition on it: 256 records loaded for the 5
 * rows of the issue. The other conditions' indexes are read first, the record ids they all name kept in a primitive set, and an
 * entry of the search whose record is not in it is dropped before its record is loaded.
 * <p>
 * Nothing records query statistics, so whether a second index is worth reading is decided while it is read: it is read only while
 * it holds no more entries than the search itself, whose entries are counted alongside, never ahead (a copy of the search reads
 * its entries, no record). An index that holds more is given up, and costs at most twice the entries of the search, read from the
 * indexes alone. One that holds fewer costs as many entries again from each side, and spares the load of every record of the search
 * it does not name. So does every further index, each read under the same bound and kept to the records the ones before it kept.
 * Past {@link #MAX_RIDS} entries an index is given up too, which bounds the memory an execution holds.
 * <p>
 * The conditions the indexes answer stay in the filter after the load: dropping an entry is the only thing the set does, and only
 * while the set is the answer of the indexes for the records as they are. A commit that published since the indexes were read
 * may have given a record the values the conditions ask for, so from then on no entry is dropped and the filter decides, as it did
 * before. The set therefore never decides alone whether a record is a row, and a record is never lost to it.
 * <p>
 * Planned by {@code SelectExecutionPlanner} only where the statement reads every entry of the search anyway (no LIMIT, or rows a
 * sort or an aggregation consume whole): reading a second index whole for a search that a LIMIT stops after a few rows could cost
 * more than the rows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class IndexIntersection {
  /** Entries past which an index is given up: about 16 MB of record ids. */
  static final int MAX_RIDS = 1 << 20;

  private final List<FetchFromIndexStep> searches;

  // runtime, per execution: a cached plan is copied for each one
  private boolean    resolved;
  private RidHashSet rids;
  private long       sequence;
  private long[]     entriesRead;
  private boolean[]  givenUp;
  private long       searchEntries;
  private boolean    searchExhausted;
  private long       droppedEntries;

  /**
   * @param searches the index searches of conditions the search this intersection filters leaves to the filter, one per index
   */
  public IndexIntersection(final List<FetchFromIndexStep> searches) {
    if (searches.isEmpty())
      throw new IllegalArgumentException("An index intersection needs at least one index search");
    this.searches = searches;
  }

  /**
   * Reads the indexes of the intersection, once per execution, before the first entry of {@code search} is handed on.
   *
   * @param search the search this intersection filters, which is only read through a copy here: its own entries are untouched
   */
  void resolve(final CommandContext context, final FetchFromIndexStep search) {
    if (resolved)
      return;
    resolved = true;

    final DatabaseInternal database = context.getDatabase();
    // The conditions may read the variables a LET before the search sets, as the search itself does before its first entry
    search.pullPrevious(context, AbstractExecutionStep.DEFAULT_FETCH_RECORDS_PER_PULL);
    // Sampled before the first entry of an index is read: a commit that published after it may have changed what they name
    sequence = database.getPageManager().getPublicationSequence();

    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    entriesRead = new long[searches.size()];
    givenUp = new boolean[searches.size()];
    final FetchFromIndexStep counter = (FetchFromIndexStep) search.copy(context);
    RidHashSet kept = null;
    try {
      for (int i = 0; i < searches.size(); i++) {
        final FetchFromIndexStep other = searches.get(i);
        final RidHashSet found = new RidHashSet();
        long entries = 0;
        boolean complete = true;
        try {
          Identifiable value;
          while ((value = other.nextIdentifiable(context)) != null) {
            guard.checkPeriodically((int) entries);
            ++entries;
            // read only while it holds no more entries than the search: the search is counted as far as this index got, never ahead
            while (searchEntries < entries && !searchExhausted) {
              if (counter.nextIdentifiable(context) == null)
                searchExhausted = true;
              else
                ++searchEntries;
            }
            if (entries > searchEntries || entries > MAX_RIDS) {
              complete = false;
              break;
            }
            final RID rid = value.getIdentity();
            if (kept == null || kept.contains(rid))
              found.add(rid);
          }
        } finally {
          other.releaseCursors();
        }
        entriesRead[i] = entries;
        givenUp[i] = !complete;
        if (complete)
          kept = found;
      }
    } finally {
      counter.releaseCursors();
    }
    rids = kept;
  }

  /**
   * Whether the entry of the search naming {@code value} is dropped before its record is loaded: an intersection read, a record
   * that not every index it kept named, and no commit published since they were read.
   */
  boolean excludes(final Object value, final DatabaseInternal database) {
    if (rids == null || !(value instanceof Identifiable identifiable))
      return false;
    final RID rid = identifiable.getIdentity();
    if (rid.getBucketId() < 0 || rids.contains(rid))
      return false;
    // The set names what matched when the indexes were read: a record a commit changed since may match now, the filter decides
    final long current = database.getPageManager().getPublicationSequence();
    if ((sequence & 1) != 0 || current != sequence)
      return false;
    ++droppedEntries;
    return true;
  }

  void reset() {
    resolved = false;
    rids = null;
    sequence = 0;
    entriesRead = null;
    givenUp = null;
    searchEntries = 0;
    searchExhausted = false;
    droppedEntries = 0;
    for (final FetchFromIndexStep search : searches)
      search.reset();
  }

  void close() {
    for (final FetchFromIndexStep search : searches)
      search.releaseCursors();
  }

  IndexIntersection copy(final CommandContext context) {
    final List<FetchFromIndexStep> copies = new ArrayList<>(searches.size());
    for (final FetchFromIndexStep search : searches)
      copies.add((FetchFromIndexStep) search.copy(context));
    return new IndexIntersection(copies);
  }

  /** The searches of the intersection. Surfaced for tests. */
  List<FetchFromIndexStep> getSearches() {
    return searches;
  }

  void prettyPrint(final StringBuilder result, final String spaces, final boolean profiling) {
    for (int i = 0; i < searches.size(); i++) {
      result.append("\n").append(spaces).append("  intersected with index ").append(searches.get(i).describeSearch());
      if (profiling && entriesRead != null) {
        if (givenUp[i])
          result.append(" (given up after ").append(entriesRead[i]).append(" entries: ")
              .append(entriesRead[i] > MAX_RIDS ? "too many to hold)" : "more than the search holds)");
        else
          result.append(" (").append(entriesRead[i]).append(" entries)");
      }
    }
    if (profiling && entriesRead != null)
      result.append("\n").append(spaces).append("  ").append(droppedEntries).append(" entries dropped without loading their record");
  }
}
