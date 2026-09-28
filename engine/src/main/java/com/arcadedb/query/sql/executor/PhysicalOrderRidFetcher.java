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
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;

import java.util.ArrayList;
import java.util.Iterator;
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
  // nextRecord(): the buckets of the current chunk, and the records of the one being read
  private       List<BucketSlice>      bucketSlices;
  private       int                    nextBucketSlice;
  private       Iterator<Record>       records;

  private record BucketSlice(int bucketId, long[] positions, int from, int to) {
  }

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
   * The number of matching entries above which a search over {@code bucketIds} is better served by a scan, or -1 when
   * the decision cannot or must not be taken: {@link GlobalConfiguration#QUERY_INDEX_MAX_SELECTIVITY} is 0, or some
   * bucket keeps no record count to take the share of (counting it would be a scan of the bucket, the very cost this is
   * trying to avoid).
   * <p>
   * The setting is the share of the records for a scan on one thread. A scan on several workers gives way sooner: it
   * speeds up with them, while the index entries are still read by one thread and only the records are loaded in
   * parallel. So the share is divided by {@code (1 + scanWorkers) / 2}, which is where the two meet as measured on
   * types in and out of the page cache (issue #8333): 60% of the type for a sequential scan by default, 24% on 4
   * workers, 6% on 18.
   *
   * @param scanWorkers the workers the scan would run on, 1 when it would be sequential
   */
  public static long scanThreshold(final Database database, final Iterable<Integer> bucketIds, final int scanWorkers) {
    final float maxSelectivity = database.getConfiguration().getValueAsFloat(GlobalConfiguration.QUERY_INDEX_MAX_SELECTIVITY);
    if (!(maxSelectivity > 0))
      return -1;
    final double share = Math.min(1D, maxSelectivity * 2D / (1 + Math.max(1, scanWorkers)));
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
    return Math.max(1L, (long) Math.ceil(records * share));
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
   * Serves the matching records on this thread: those of every chunk in physical order, read through their buckets
   * ({@link LocalBucket#iterator(long[], int, int)}, every page read once and its records built in batches), then the
   * entries of the chunk that are not record addresses, as they came. Only after {@link #start(WorkGuard)} answered a
   * physical order.
   *
   * @param database the database to load the records through, the one the index belongs to
   *
   * @return the next {@link Record}, or the next entry that is not a record address, or null when there is none left.
   * A record deleted since the index returned it is skipped.
   */
  public Object nextRecord(final Database database) {
    while (true) {
      if (records != null && records.hasNext())
        return records.next();
      records = null;

      if (bucketSlices == null) {
        // One slice per bucket: the bucket iterator already reads in batches
        bucketSlices = new ArrayList<>();
        buffer.slices(Integer.MAX_VALUE, (bucketId, positions, from, to) -> bucketSlices.add(new BucketSlice(bucketId, positions, from, to)));
        nextBucketSlice = 0;
      }

      if (nextBucketSlice < bucketSlices.size()) {
        final BucketSlice slice = bucketSlices.get(nextBucketSlice++);
        // A bucket dropped since the index was read holds none of the records any more
        final Bucket bucket = database.getSchema().getBucketByIdIfExists(slice.bucketId());
        if (bucket instanceof LocalBucket localBucket)
          records = localBucket.iterator(slice.positions(), slice.from(), slice.to());
        continue;
      }

      // The entries that are not record addresses, in no particular order (removeLast() is only the cheap end of the
      // list): the fetcher serves only statements whose output cannot show the order rows arrive in
      if (passThrough != null && !passThrough.isEmpty())
        return passThrough.removeLast();

      if (!nextChunk())
        return null;
    }
  }

  /**
   * Cuts the record addresses of the current chunk, in physical order, in slices of at most {@code sliceSize} positions
   * of one bucket each, for the workers of a parallel load to share instead of {@link #nextRecord(Database)} serving them on one thread.
   * The slices share the fetcher's arrays: they are valid until {@link #nextChunk()}. Only after
   * {@link #start(WorkGuard)} answered a physical order.
   *
   * @param consumer receives every slice, or null to count them only
   *
   * @return the number of slices
   */
  public int slices(final int sliceSize, final SliceConsumer consumer) {
    return buffer.slices(sliceSize, consumer);
  }

  /**
   * Hands over the entries of the current chunk that are not record addresses, which {@link #slices} leaves out: an
   * embedded result, a record not stored yet. Once: a second call answers null.
   *
   * @return the entries, or null when there is none
   */
  public List<Object> takePassThrough() {
    final List<Object> taken = passThrough != null && !passThrough.isEmpty() ? passThrough : null;
    passThrough = null;
    return taken;
  }

  /**
   * Replaces the current chunk of a range too large to hold at once with the next one, for a parallel load that takes
   * the entries by {@link #slices}. Call it only once every slice of the current chunk has been loaded: the next chunk
   * reuses their arrays.
   *
   * @return false when no entry is left
   */
  public boolean nextChunk() {
    if (chunkSource == null || chunkSourceExhausted)
      return false;
    fillNextChunk();
    bucketSlices = null;
    records = null;
    return held() > 0;
  }

  /** The record addresses the current chunk holds, what {@link #slices} cuts. */
  public int getBufferedRids() {
    return buffer.size();
  }

  /** Receives one slice of the record addresses held: the positions [from, to) of a bucket, in physical order. */
  @FunctionalInterface
  public interface SliceConsumer {
    void accept(int bucketId, long[] positions, int from, int to);
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
