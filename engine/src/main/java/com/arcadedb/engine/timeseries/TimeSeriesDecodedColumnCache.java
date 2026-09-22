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
package com.arcadedb.engine.timeseries;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Decoded sealed-block columns, kept so that reading the same block twice decodes it once (issue #8179).
 * <p>
 * <b>Why this is sound.</b> A sealed block is immutable: once written, its bytes are never edited in place - every
 * path that changes a block's content writes a NEW block with a NEW {@link TimeSeriesSealedStore.BlockEntry#blockId}.
 * The one path that reuses an id, {@code writeRetainedBlock}, copies the block VERBATIM, so an entry cached under
 * that id still describes the bytes the id names. That makes {@code (blockId, columnIndex, shape)} a content key
 * rather than a location key, which is what lets a cached decode survive the file rewrites that compaction,
 * downsampling and retention perform around it. The id is durable on disk since issue #8043 and identical on every
 * node holding a copy of the shard, so it survives a restart and an HA sealed-blob install too.
 * <p>
 * <b>What must not be cached through here.</b> The aggregation push-down
 * ({@code TimeSeriesSealedStore.aggregateMultiBlocks}) decodes into per-call REUSABLE buffers that it overwrites
 * block after block. Those arrays must never reach this cache: their contents change under whoever holds them, so
 * serving one from here would hand out a column belonging to a different block. That path is deliberately left
 * alone - it already allocates nothing per block - and only the four {@code BlockEntry}-keyed decode methods
 * populate this cache.
 * <p>
 * {@code downsampleBlocks} is the path to read carefully, because it both reads through this cache and rewrites the
 * blocks it read (review of PR #8194). Only its TAG columns are decoded outside the cache, through a direct
 * {@code readBytes}/{@code decodeColumn}; its timestamps and numeric columns go through {@code decompressTimestamps}
 * and {@code decompressDoubleColumn} and do populate it. That is safe rather than merely lucky: the method holds
 * {@code directoryLock.writeLock()} for its whole body, so no reader can be served from it mid-rewrite, and it ends
 * by clearing the cache. Anyone auditing this should not carry away the shorter claim that downsampling bypasses the
 * cache outright - two of its three column kinds do not.
 * <p>
 * <b>Every file rewrite clears the WHOLE cache, including entries that are still valid.</b> A block copied verbatim
 * keeps its id, so its decoded columns would still describe it correctly afterwards - yet compaction, retention and
 * downsampling drop them along with everything else. That is a deliberate trade-off and not an oversight (review of
 * PR #8194): invalidating precisely would mean diffing the directory before and after each rewrite to learn which ids
 * survived, and the cost of being imprecise is a hot block decoded once more after a maintenance cycle. Worth
 * revisiting if maintenance ever runs often enough for that re-decode to show up.
 * <p>
 * <b>Cached arrays are shared, not copied,</b> so every consumer must treat them as read-only. That holds today:
 * all of the decode call sites index, binary-search or accumulate over them and none writes an element. A future
 * consumer that needs to mutate a decoded column must copy it first.
 * <p>
 * Eviction is least-recently-used against a byte budget rather than an entry count, because a column's cost depends
 * on the block's sample count: one column of a full 65,536-sample block is half a megabyte, so an entry-counted
 * bound would be a memory bound only by accident.
 * <p>
 * <b>Why one monitor rather than a lock-free map,</b> on hot paths this engine usually keeps lock-free (review of PR
 * #8194). The budget is this cache's only safety property - it is what the configuration setting promises an operator
 * - and a running byte total that stays exact needs the charge, the map and the eviction to move together. A
 * {@code ConcurrentHashMap} would buy concurrent lookups at the price of approximating the total, which is the one
 * thing here that must not be approximate. The critical section it costs is a hash lookup plus an access-order
 * relink, with no I/O and no decode inside it, against the hundreds of microseconds of codec work a hit replaces; and
 * it is per store, hence per shard, so shards never contend with each other. Should a single hot shard at high
 * concurrency ever make this monitor the bottleneck, the shape to reach for is striping by block id rather than
 * dropping the exact accounting.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class TimeSeriesDecodedColumnCache {

  /** A column in the form its codec produces: {@code long[]}, {@code double[]} or {@code String[]}. */
  static final int SHAPE_RAW    = 0;
  /** A column boxed one value per row, as {@code decompressColumns} hands it to a row-building scan. */
  static final int SHAPE_BOXED  = 1;
  /** A numeric column widened to {@code double[]}, as the aggregation and downsampling reads want it. */
  static final int SHAPE_DOUBLE = 2;

  /**
   * The shape is part of the key rather than something converted between, so a column read both raw and boxed
   * costs two entries and the budget keeps telling the truth about the memory held. Converting instead would have
   * to reproduce {@link ColumnDefinition}'s boxing exactly, and issue #7711 is what that gets wrong.
   * <p>
   * The two shapes of one DICTIONARY column share NOTHING, not even the strings (review of PR #8194): the boxed path
   * reaches {@code DictionaryCodec.decode} through {@code decodeColumn} on its own, and that method builds a fresh
   * {@code String} per distinct value on every call. So a query touching both shapes of the same block decodes it
   * twice and is charged for both, which is what the budget should say about two independent object graphs - but it
   * is not something a reader should have to derive from two files to know.
   */
  private record CacheKey(long blockId, int columnIndex, int shape) {
  }

  /**
   * A held column and what it was charged, measured ONCE on admission. Re-deriving the charge on eviction would
   * walk the array again - {@link #holdsSharedReferences} has to look at an element to know what boxing cost - and
   * would also risk an eviction subtracting a different number than the admission added.
   */
  private record Held(Object value, long bytes) {
  }

  private final Map<CacheKey, Held> entries;
  private final long                maxBytes;
  private       long                heldBytes;
  private       long                hits;
  private       long                misses;

  TimeSeriesDecodedColumnCache(final long maxBytes) {
    this.maxBytes = maxBytes;
    // Access-ordered, so iteration yields least-recently-used first and eviction is a walk from the front.
    this.entries = new LinkedHashMap<>(16, 0.75f, true);
  }

  boolean isEnabled() {
    return maxBytes > 0;
  }

  /**
   * The cached column, or {@code null} when this block/column/shape is not held.
   * <p>
   * One short-lived key per lookup is allocated deliberately: it is a handful of bytes against a decode of
   * hundreds of kilobytes, and the alternative - packing three fields into a primitive key - can only be done by
   * hashing them together, which trades an allocation for a chance of returning another block's column.
   */
  synchronized Object get(final long blockId, final int columnIndex, final int shape) {
    if (maxBytes <= 0 || blockId == TimeSeriesSealedStore.BlockEntry.NO_BLOCK_ID)
      return null;

    final Held cached = entries.get(new CacheKey(blockId, columnIndex, shape));
    if (cached != null)
      hits++;
    else
      misses++;
    return cached != null ? cached.value() : null;
  }

  /**
   * Holds a decoded column, evicting least-recently-used entries until the budget is met again.
   * <p>
   * A column larger than the whole budget is not held at all: admitting it would evict everything else to make
   * room for a single entry that the next admission evicts in turn.
   */
  synchronized void put(final long blockId, final int columnIndex, final int shape, final Object decoded) {
    if (maxBytes <= 0 || decoded == null || blockId == TimeSeriesSealedStore.BlockEntry.NO_BLOCK_ID)
      return;

    final long size = sizeOf(decoded);
    if (size > maxBytes)
      return;

    final Held previous = entries.put(new CacheKey(blockId, columnIndex, shape), new Held(decoded, size));
    if (previous != null)
      heldBytes -= previous.bytes();
    heldBytes += size;

    final Iterator<Held> eldest = entries.values().iterator();
    while (heldBytes > maxBytes && eldest.hasNext()) {
      heldBytes -= eldest.next().bytes();
      eldest.remove();
    }
  }

  /**
   * Drops everything held.
   * <p>
   * Not needed for correctness on the paths that rewrite blocks - those mint new ids, so their entries simply stop
   * being asked for - but a store whose whole file has been replaced ({@code installSealedFile}, a directory
   * reload) would otherwise keep paying budget for columns of blocks it no longer has.
   */
  synchronized void clear() {
    entries.clear();
    heldBytes = 0;
  }

  synchronized long getHits() {
    return hits;
  }

  synchronized long getMisses() {
    return misses;
  }

  synchronized long getHeldBytes() {
    return heldBytes;
  }

  /**
   * What holding this column costs, near enough to bound memory with.
   * <p>
   * A {@code String[]} counts its references only: a DICTIONARY column decodes to the same handful of String
   * objects repeated once per row, so charging each row the string's own bytes would over-count the array by orders
   * of magnitude.
   * <p>
   * A boxed column is charged per row ONLY when boxing actually allocated per row. It usually does -
   * {@link ColumnDefinition#boxDouble} and {@link ColumnDefinition#boxRaw} produce a fresh object per value - but
   * {@link ColumnDefinition#boxString} hands a STRING column's value straight back, so the boxed shape of a
   * dictionary STRING tag holds the very references the raw shape holds and costs no more than it. That is the
   * host-style tag of the query this cache exists for, and billing it three times its retained size would evict it
   * ahead of entries that really are that large. The element type is decided by the column's declared type, so the
   * first non-null value speaks for the array.
   */
  private static long sizeOf(final Object array) {
    if (array instanceof long[] a)
      return 16L + 8L * a.length;
    if (array instanceof double[] a)
      return 16L + 8L * a.length;
    if (array instanceof String[] a)
      return 16L + 8L * a.length + distinctStringBytes(a);
    if (array instanceof Object[] a)
      return holdsSharedReferences(a)
          ? 16L + 8L * a.length + distinctStringBytes(a)
          : 16L + 24L * a.length;
    return 16L;
  }

  /**
   * Whether a boxed column's elements are references it shares rather than objects it allocated. An all-null column
   * allocated nothing either, so it answers true as well.
   * <p>
   * Two types qualify. A STRING column is handed back unchanged by {@link ColumnDefinition#boxString}, and a BOOLEAN
   * one autoboxes to {@code Boolean.TRUE}/{@code Boolean.FALSE}, which the JVM caches - so a column of either holds
   * two references per slot and nothing more (review of PR #8194). Every other boxing allocates per value.
   */
  private static boolean holdsSharedReferences(final Object[] values) {
    for (final Object value : values) {
      if (value != null)
        return value instanceof String || value instanceof Boolean;
    }
    return true;
  }

  /**
   * What the DISTINCT strings an array points at retain, on top of the slots pointing at them (code review on PR
   * #8194).
   * <p>
   * Counting the references alone would have been right if the strings outlived the array anyway, and they do not:
   * {@code DictionaryCodec.decode} builds each one with {@code new String(utf8, UTF_8)} per call, so holding the
   * array is what keeps them alive. A block may carry up to {@code DictionaryCodec.MAX_DICTIONARY_SIZE} = 65535
   * distinct values, so the gap between "65536 references" and what those references retain is megabytes on a column
   * charged half of one - and the budget is the only thing bounding this cache's footprint.
   * <p>
   * Deduplicated by IDENTITY, which is both cheaper than equality and the accurate measure: every slot of a decoded
   * dictionary column points into the same small array of distinct values, so the retained set is that array and not
   * one string per row. Sizes are deliberately generous - an object header plus two bytes per character, the
   * worst-case UTF-16 coder - because an estimate that bounds memory should err upwards.
   */
  private static long distinctStringBytes(final Object[] values) {
    final Set<String> counted = Collections.newSetFromMap(new IdentityHashMap<>());
    long bytes = 0;
    for (final Object value : values) {
      if (value instanceof String text && counted.add(text))
        bytes += 40L + 2L * text.length();
    }
    return bytes;
  }
}
