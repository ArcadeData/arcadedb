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
package com.arcadedb.index.lsm;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TrackableBinary;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/** Writes an ordered key/RID stream into the compacted LSM series format used by normal compaction. */
final class LSMTreeIndexCompactedStreamWriter {
  static final int DEFAULT_MAX_DATA_PAGES_PER_SERIES = 1_024;

  private static final RID[] ROOT_SENTINEL               = new RID[] { new RID(0, 0) };
  private static final int   MAX_RIDS_PER_BOUNDED_APPEND = 256;
  private static final int   INITIAL_BLOOM_HASHES        = 1_024;

  /**
   * A series whose key hashes would need more than this much heap is written without a bloom filter: the 8 bytes per
   * key are transient, but a compaction must not trade a correct index for an optimisation. At the cap the accumulator
   * peaks at 64 MB (plus one doubling in flight), against a compaction that is already holding whole pages per input
   * cursor.
   */
  private static final int MAX_BLOOM_KEYS_PER_SERIES = 8_000_000;

  private final LSMTreeIndex          mainIndex;
  private final LSMTreeIndexMutable   mutableIndex;
  private final LSMTreeIndexCompacted compactedIndex;
  private final DatabaseInternal      database;
  private final Binary                keyValueContent = new Binary();
  private final Binary                sizingContent   = new Binary();
  private final Binary                bloomKeyContent = new Binary();

  /** Target false-positive rate of the per-series bloom filters (#5517), or 0 when the feature is off. */
  private final double bloomFalsePositiveRate;

  private long[]  bloomHashes;
  private int     bloomKeyCount;
  private long    bloomLastHash;

  private MutablePage     rootPage;
  private TrackableBinary rootPageBuffer;
  private MutablePage     lastPage;
  private TrackableBinary lastPageBuffer;
  private AtomicInteger   pageNumberInSeries;
  private Object[]        lastPageMaxKey;
  private Object[]        lastPageMaxConvertedKey;
  private List<MutablePage> newPagesInSeries;

  LSMTreeIndexCompactedStreamWriter(final LSMTreeIndex mainIndex, final LSMTreeIndexCompacted compactedIndex) {
    this.mainIndex = mainIndex;
    this.mutableIndex = mainIndex.getMutableIndex();
    this.compactedIndex = compactedIndex;
    this.database = mutableIndex.getDatabase();

    final double rate = database.getConfiguration().getValueAsFloat(GlobalConfiguration.INDEX_BLOOM_FILTER_RATE);
    this.bloomFalsePositiveRate = rate > 0 && rate < 1 ? rate : 0;
  }

  MutablePage startSeries() {
    if (rootPage != null)
      throw new IllegalStateException("A compacted series is already active for index '" + mainIndex.getName() + "'");

    bloomHashes = bloomFalsePositiveRate > 0 ? new long[INITIAL_BLOOM_HASHES] : null;
    bloomKeyCount = 0;
    bloomLastHash = 0;

    rootPage = compactedIndex.createNewPage(0);
    rootPageBuffer = rootPage.getTrackable();
    lastPage = null;
    lastPageBuffer = null;
    pageNumberInSeries = new AtomicInteger(1);
    lastPageMaxKey = null;
    lastPageMaxConvertedKey = null;
    newPagesInSeries = new ArrayList<>();
    return rootPage;
  }

  void append(final Object[] keys, final RID[] rids) throws IOException, InterruptedException {
    appendConverted(keys, compactedIndex.convertKeysForCompaction(keys), rids);
  }

  private void appendConverted(final Object[] keys, final Object[] convertedKeys, final RID[] rids)
      throws IOException, InterruptedException {
    if (rootPage == null)
      throw new IllegalStateException("No compacted series is active for index '" + mainIndex.getName() + "'");

    rememberForBloomFilter(convertedKeys);

    final List<MutablePage> newPages = compactedIndex.appendDuringCompactionConverted(keyValueContent, lastPage,
        lastPageBuffer, pageNumberInSeries, keys, convertedKeys, rids);

    if (!newPages.isEmpty()) {
      lastPage = newPages.getLast();
      lastPageBuffer = lastPage.getTrackable();

      for (final MutablePage newPage : newPages) {
        final int newPageNumber = newPage.getPageId().getPageNumber();
        final List<MutablePage> newRootPages = compactedIndex.appendDuringCompactionConverted(keyValueContent, rootPage,
            rootPageBuffer, pageNumberInSeries, keys, convertedKeys, new RID[] { new RID(0, newPageNumber) });

        LogManager.instance().log(mainIndex, Level.FINE,
            "- Creating a new entry in index '%s' root page %s->%d (entry in page=%d threadId=%d)", null, mutableIndex,
            Arrays.toString(keys), newPageNumber, mutableIndex.getCount(rootPage) - 1, Thread.currentThread().threadId());

        if (!newRootPages.isEmpty())
          throw new UnsupportedOperationException("Root index page overflow");
      }
      newPagesInSeries.addAll(newPages);
    }

    lastPageMaxKey = keys;
    lastPageMaxConvertedKey = convertedKeys;
  }

  void appendBounded(final Object[] keys, final RID[] rids, final int maxDataPagesPerSeries)
      throws IOException, InterruptedException {
    if (maxDataPagesPerSeries < 1)
      throw new IllegalArgumentException("maxDataPagesPerSeries must be at least 1");

    final Object[] convertedKeys = compactedIndex.convertKeysForCompaction(keys);
    int from = 0;
    while (from < rids.length) {
      if (rootPage == null)
        startSeries();

      final int requestedTo = Math.min(rids.length, from + MAX_RIDS_PER_BOUNDED_APPEND);
      RID[] chunk = from == 0 && requestedTo == rids.length ? rids : Arrays.copyOfRange(rids, from, requestedTo);
      final int fitting = compactedIndex.valuesFittingInEmptyLeaf(sizingContent, rootPage, keys, convertedKeys, chunk);
      if (fitting < chunk.length)
        chunk = Arrays.copyOf(chunk, fitting);
      final int requiredLeafSpace = compactedIndex.requiredSpaceForSerializedEntry(sizingContent);

      final boolean startsNewLeaf = lastPage == null
          || requiredLeafSpace > compactedIndex.availableSpaceForEntries(lastPage);
      int requiredRootSpace = compactedIndex.requiredSpaceForEntry(sizingContent, rootPage, keys, convertedKeys,
          ROOT_SENTINEL);
      if (startsNewLeaf)
        requiredRootSpace += compactedIndex.requiredSpaceForEntry(sizingContent, rootPage, keys, convertedKeys,
            new RID[] { new RID(0, compactedIndex.getTotalPages()) });

      if ((startsNewLeaf && newPagesInSeries.size() >= maxDataPagesPerSeries)
          || requiredRootSpace > compactedIndex.availableSpaceForEntries(rootPage)) {
        finishSeries();
        startSeries();
        requiredRootSpace = compactedIndex.requiredSpaceForEntry(sizingContent, rootPage, keys, convertedKeys,
            ROOT_SENTINEL)
            + compactedIndex.requiredSpaceForEntry(sizingContent, rootPage, keys, convertedKeys,
            new RID[] { new RID(0, compactedIndex.getTotalPages()) });
        if (requiredRootSpace > compactedIndex.availableSpaceForEntries(rootPage))
          throw new IndexException(
              "Root entry for key " + Arrays.toString(keys) + " does not fit in an empty compacted series");
      }

      final int pagesBefore = newPagesInSeries.size();
      appendConverted(keys, convertedKeys, chunk);
      if (newPagesInSeries.size() - pagesBefore > 1)
        throw new IndexException("A bounded compacted append unexpectedly created multiple data pages");
      from += chunk.length;
    }
  }

  void finishSeries() throws IOException, InterruptedException {
    if (rootPage == null)
      throw new IllegalStateException("No compacted series is active for index '" + mainIndex.getName() + "'");

    if (lastPageMaxKey != null) {
      final List<MutablePage> overflow = compactedIndex.appendDuringCompactionConverted(keyValueContent, rootPage,
          rootPageBuffer, pageNumberInSeries, lastPageMaxKey, lastPageMaxConvertedKey, ROOT_SENTINEL);
      if (!overflow.isEmpty())
        throw new UnsupportedOperationException("Root index page overflow");

      LogManager.instance().log(mainIndex, Level.FINE,
          "- Creating last entry in index '%s' root page %s (entriesInRootPage=%d threadId=%d)", null, mutableIndex,
          Arrays.toString(lastPageMaxKey), compactedIndex.getCount(rootPage), Thread.currentThread().threadId());
    }

    final int seriesRootPage = rootPage.getPageId().getPageNumber();
    final int seriesPages = newPagesInSeries.size();
    // Taken from the last data page, exactly as the reader will take it, so a filter can only ever answer for the
    // series it was built from - see LSMTreeIndexCompacted.seriesFingerprint.
    final int seriesFingerprint = lastPage != null ? compactedIndex.seriesFingerprint(lastPage) : 0;

    final List<MutablePage> modifiedPages = new ArrayList<>(newPagesInSeries);
    modifiedPages.add(database.getPageManager().updatePageVersion(rootPage, true));
    database.getPageManager().writePages(modifiedPages, false);

    // Only now that the series is on disk: a filter that reached the directory first would answer for pages a failed
    // write never produced.
    publishBloomFilter(seriesRootPage, seriesPages, seriesFingerprint);

    rootPage = null;
    rootPageBuffer = null;
    lastPage = null;
    lastPageBuffer = null;
    pageNumberInSeries = null;
    lastPageMaxKey = null;
    lastPageMaxConvertedKey = null;
    newPagesInSeries = null;
  }

  void finishIfActive() throws IOException, InterruptedException {
    if (rootPage != null)
      finishSeries();
  }

  int getRootEntryCount() {
    return rootPage != null ? compactedIndex.getCount(rootPage) : 0;
  }

  /**
   * Records the key hash for the bloom filter of the series being written (#5517). Called for every key BEFORE it is
   * appended, so a key can never reach a page without reaching the filter - that asymmetry is the false negative the
   * filter must never produce.
   * <p>
   * Keys arrive in ascending order and a key whose values overflow a page comes back for each chunk, so suppressing an
   * adjacent repeat is enough to count every key once and size the filter for what it really holds.
   */
  private void rememberForBloomFilter(final Object[] convertedKeys) {
    if (bloomHashes == null)
      return;

    // A key that reaches a page WITHOUT reaching the filter is a false negative, so anything that stops this key from
    // being hashed must abandon the whole series' filter - never skip the key and publish the rest. And it must not
    // propagate either: a compaction that produced correct index pages cannot be failed by an optimisation.
    final long hash;
    try {
      final Binary serialized = compactedIndex.serializeKeyForHashing(bloomKeyContent, convertedKeys);
      if (serialized == null)
        throw new IllegalStateException("the key is not hashable");
      hash = LSMTreeIndexBloomFilter.hashKey(serialized);
    } catch (final Exception e) {
      LogManager.instance().log(mainIndex, Level.WARNING,
          "Cannot hash a key of index '%s' for its bloom filter: the series is written WITHOUT one (error=%s)", null,
          mainIndex.getName(), e.toString());
      bloomHashes = null;
      return;
    }
    if (bloomKeyCount > 0 && hash == bloomLastHash)
      return;

    if (bloomKeyCount == bloomHashes.length) {
      if (bloomKeyCount >= MAX_BLOOM_KEYS_PER_SERIES) {
        LogManager.instance().log(mainIndex, Level.INFO,
            "Series of index '%s' holds more than %d keys: writing it without a bloom filter", null, mainIndex.getName(),
            MAX_BLOOM_KEYS_PER_SERIES);
        bloomHashes = null;
        return;
      }
      bloomHashes = Arrays.copyOf(bloomHashes, Math.min(MAX_BLOOM_KEYS_PER_SERIES, bloomKeyCount * 2));
    }

    bloomHashes[bloomKeyCount++] = hash;
    bloomLastHash = hash;
  }

  /**
   * Hands the accumulated hashes to the filter component, creating it on first use. A failure to write a filter is
   * logged and dropped: it costs a lookup, never a row.
   */
  private void publishBloomFilter(final int seriesRootPage, final int seriesPages, final int seriesFingerprint) {
    final long[] hashes = bloomHashes;
    final int count = bloomKeyCount;

    bloomHashes = null;
    bloomKeyCount = 0;

    if (hashes == null || count < 1 || seriesPages < 1)
      return;

    try {
      compactedIndex.getOrCreateBloomFilter()
          .publish(seriesRootPage, seriesPages, seriesFingerprint, hashes, count, bloomFalsePositiveRate);
    } catch (final Exception e) {
      LogManager.instance().log(mainIndex, Level.WARNING,
          "Cannot create the bloom filter of index '%s' (error=%s)", null, mainIndex.getName(), e.toString());
    }
  }
}
