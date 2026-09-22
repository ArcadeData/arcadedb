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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

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
 * <b>Cached arrays are shared, not copied,</b> so every consumer must treat them as read-only. That holds today:
 * all of the decode call sites index, binary-search or accumulate over them and none writes an element. A future
 * consumer that needs to mutate a decoded column must copy it first.
 * <p>
 * Eviction is least-recently-used against a byte budget rather than an entry count, because a column's cost depends
 * on the block's sample count: one column of a full 65,536-sample block is half a megabyte, so an entry-counted
 * bound would be a memory bound only by accident.
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
   */
  private record CacheKey(long blockId, int columnIndex, int shape) {
  }

  private final Map<CacheKey, Object> entries;
  private final long                  maxBytes;
  private       long                  heldBytes;
  private       long                  hits;
  private       long                  misses;

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

    final Object cached = entries.get(new CacheKey(blockId, columnIndex, shape));
    if (cached != null)
      hits++;
    else
      misses++;
    return cached;
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

    final Object previous = entries.put(new CacheKey(blockId, columnIndex, shape), decoded);
    if (previous != null)
      heldBytes -= sizeOf(previous);
    heldBytes += size;

    final Iterator<Object> eldest = entries.values().iterator();
    while (heldBytes > maxBytes && eldest.hasNext()) {
      heldBytes -= sizeOf(eldest.next());
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
   * of magnitude. A boxed column, by contrast, DOES allocate one object per row, so it is charged for them.
   */
  private static long sizeOf(final Object array) {
    if (array instanceof long[] a)
      return 16L + 8L * a.length;
    if (array instanceof double[] a)
      return 16L + 8L * a.length;
    if (array instanceof String[] a)
      return 16L + 8L * a.length;
    if (array instanceof Object[] a)
      // Reference plus the boxed value it points at; the values are per row and not shared.
      return 16L + 24L * a.length;
    return 16L;
  }
}
