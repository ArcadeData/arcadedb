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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Decides how the records an index search matched are best loaded, and serves them (issue #8333).
 * <p>
 * Fetching records in index order is one random page access per record: cheap while the type fits the page cache and
 * the range is small, far slower than scanning the type once the range holds a large share of it. So the entries the
 * search returns are read first, without loading any record, and:
 * <ul>
 *   <li>once more of them match than the scan threshold, the fetcher answers {@link Outcome#SCAN} and the caller serves
 *   the rows from a scan of the type instead;</li>
 *   <li>otherwise the matching records are served in physical order (bucket, then position), so every page is read
 *   once, in a forward sweep. A range too large to hold at once is read a second time and served in sorted chunks.</li>
 * </ul>
 * The SQL and the Cypher index fetches share it; each supplies its entries through a {@link Source}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PhysicalOrderRidFetcher {
  /**
   * Most RIDs held at once: 8MB of positions. Not final only so a test can reach the chunked path without a million
   * records.
   */
  static int maxBufferedRids = 1 << 20;

  public enum Outcome {SCAN, PHYSICAL_ORDER, PHYSICAL_ORDER_CHUNKED}

  /**
   * The entries of one pass over the index search.
   */
  public interface Source {
    /**
     * @return the next matching entry - a RID, or anything else the caller knows how to serve as it is - or null
     * when the search is exhausted
     */
    Object next();

    /** Releases the cursors of the pass, whether or not it ran to exhaustion. */
    void close();
  }

  private final Supplier<Source>       sources;
  private final long                   scanThreshold;
  private final PhysicalOrderRidBuffer buffer = new PhysicalOrderRidBuffer();
  private       List<Object>           passThrough;
  private       Source                 chunkSource;
  private       boolean                chunkSourceExhausted;
  private       long                   matched;
  private       Outcome                outcome;
  private       WorkGuard              guard;

  /**
   * @param sources       opens a new pass over the index search: called once, and a second time only for a range too
   *                      large to buffer
   * @param scanThreshold the number of matching entries above which the search gives way to a scan
   */
  public PhysicalOrderRidFetcher(final Supplier<Source> sources, final long scanThreshold) {
    this.sources = sources;
    this.scanThreshold = scanThreshold;
  }

  /**
   * The number of matching entries above which a search over {@code bucketIds} is better served by a scan, from
   * {@link GlobalConfiguration#QUERY_INDEX_MAX_SELECTIVITY}, or -1 when the decision cannot or must not be taken: the
   * setting is 0, or some bucket keeps no record count to take the share of (counting it would be a scan of the
   * bucket, the very cost this is trying to avoid).
   */
  public static long scanThreshold(final Database database, final Iterable<Integer> bucketIds) {
    final float maxSelectivity = database.getConfiguration().getValueAsFloat(GlobalConfiguration.QUERY_INDEX_MAX_SELECTIVITY);
    if (!(maxSelectivity > 0))
      return -1;
    long records = 0;
    for (final int bucketId : bucketIds) {
      final Bucket bucket = database.getSchema().getBucketByIdIfExists(bucketId);
      if (!(bucket instanceof LocalBucket localBucket))
        return -1;
      final long count = localBucket.getCachedRecordCount();
      if (count < 0)
        return -1;
      records += count;
    }
    return Math.max(1L, (long) Math.ceil(records * (double) Math.min(1F, maxSelectivity)));
  }

  /**
   * Reads the first pass until the entries run out or exceed the threshold.
   *
   * @param guard checked periodically, since the pass can be long
   */
  public Outcome start(final WorkGuard guard) {
    this.guard = guard;
    final Source source = sources.get();
    boolean overflow = false;
    try {
      Object entry;
      while ((entry = source.next()) != null) {
        guard.checkPeriodically((int) matched);
        if (++matched > scanThreshold) {
          buffer.clear();
          passThrough = null;
          return outcome = Outcome.SCAN;
        }
        if (overflow)
          // Counting only: the range is too large to hold, it will be read again chunk by chunk
          continue;
        if (held() >= maxBufferedRids) {
          overflow = true;
          buffer.clear();
          passThrough = null;
          continue;
        }
        add(entry);
      }
    } finally {
      source.close();
    }

    if (overflow) {
      // The first pass only proved the range is below the threshold. Read it again from the start rather than
      // resuming after what was buffered, so the entries served are exactly one pass over the index.
      chunkSource = sources.get();
      fillNextChunk();
      return outcome = Outcome.PHYSICAL_ORDER_CHUNKED;
    }
    buffer.sort();
    return outcome = Outcome.PHYSICAL_ORDER;
  }

  /**
   * @return the next entry to serve - a {@link RID} in physical order, or an entry that is not a record address as it
   * came - or null when there is none left. Only after {@link #start(WorkGuard)} answered a physical order.
   */
  public Object next() {
    // The entries that are not record addresses go first, in no particular order (removeLast() is only the cheap end
    // of the list): the fetcher serves only statements whose output cannot show the order rows arrive in
    while (true) {
      if (passThrough != null && !passThrough.isEmpty())
        return passThrough.removeLast();
      if (buffer.hasNext())
        return buffer.next();
      if (chunkSource == null || chunkSourceExhausted)
        return null;
      fillNextChunk();
    }
  }

  /** The entries the first pass matched: all of them, or one more than the threshold when it answered a scan. */
  public long getMatched() {
    return matched;
  }

  public long getScanThreshold() {
    return scanThreshold;
  }

  public Outcome getOutcome() {
    return outcome;
  }

  public void close() {
    if (chunkSource != null) {
      chunkSource.close();
      chunkSource = null;
    }
  }

  private void fillNextChunk() {
    buffer.clear();
    int read = 0;
    while (held() < maxBufferedRids) {
      guard.checkPeriodically(++read);
      final Object entry = chunkSource.next();
      if (entry == null) {
        chunkSourceExhausted = true;
        chunkSource.close();
        break;
      }
      add(entry);
    }
    buffer.sort();
  }

  /** Everything held for the current chunk, record addresses and the entries served as they came alike. */
  private int held() {
    return buffer.size() + (passThrough != null ? passThrough.size() : 0);
  }

  private void add(final Object entry) {
    // Only the address is kept, bound to a database or not: the caller loads it back through the query's database,
    // which is the one the index belongs to
    if (entry instanceof RID rid && rid.getBucketId() >= 0 && rid.getPosition() >= 0)
      buffer.add(rid.getBucketId(), rid.getPosition());
    else {
      // Not a record address (an embedded result, a record not stored yet): served as it came
      if (passThrough == null)
        passThrough = new ArrayList<>();
      passThrough.add(entry);
    }
  }
}
