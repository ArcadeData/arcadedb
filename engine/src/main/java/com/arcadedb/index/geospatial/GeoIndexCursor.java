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
package com.arcadedb.index.geospatial;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.serializer.BinaryComparator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Lazy cursor over the GeoHash cells covering a search shape (#5601).
 * <p>
 * A query decomposes its shape into covering cells - a 10x10-degree box resolves into roughly 4,200 of them - and each
 * cell is answered by the underlying LSM-Tree with one prefix range scan (frontier cell) or one exact lookup (internal
 * cell). The previous implementation drained every one of those scans into a {@code LinkedHashSet} and then copied it
 * into a list of {@code IndexCursorEntry} before the caller saw the first row, so the whole candidate set was resident
 * even when the query only needed the first page of results. This cursor opens ONE underlying scan at a time, closes
 * it as soon as it is drained, and never touches a cell the consumer did not ask to reach.
 * <p>
 * Deduplication is still needed and is still a set of RIDs: the covering cells of a query are disjoint, but a single
 * record is not indexed under a single cell - a polygon or a line decomposes into many - so the same RID can be reached
 * through several of them. What the set no longer costs is a second, parallel copy of the same information: it holds
 * only what has actually been EMITTED, so a consumer that stops early (a {@code LIMIT} applied after the geo.*
 * predicate re-check) never pays for the candidates it did not read. On the legacy {@code FULL} layout, which stores
 * the whole ancestor chain of every cell, the set additionally absorbs the same record being found under a cell and
 * under one of its ancestors.
 * <p>
 * {@link #hasNext()} is exact: the lookahead needed to skip already-seen RIDs also makes it honest, so {@link #next()}
 * never answers null and throws {@link NoSuchElementException} when exhausted, per the {@link Iterator} contract. Since
 * #5635 that is the contract of every {@link IndexCursor}, {@code LSMTreeIndexCursor} included, so the cell scans this
 * one drains need no null guard.
 * <p>
 * A geospatial hit has no relevance to report - the materialising implementation stamped a constant 1 on every entry -
 * so the score is left at the interface default, the documented "scoring not supported" value. Nothing reads a score
 * off a geo cursor: only the full-text path propagates one ({@code TypeIndex.get}, {@code FetchFromIndexStep}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GeoIndexCursor implements IndexCursor {
  /** Shared: {@link #getBinaryKeyTypes()} is on the cursor surface, so it must not allocate per call. */
  private static final byte[] NO_KEY_TYPES = new byte[0];

  private final Object[]            keys;
  private final GeoCoveringCellWalk walk;
  private final CellCursorFactory   cellCursorFactory;
  private final Set<RID>            seen = new HashSet<>();
  private       IndexCursor         cellCursor;
  private       RID                 nextRID;
  private       RID                 currentRID;
  private       boolean             closed;

  /**
   * Opens the underlying scan that answers one covering cell. Kept as a factory rather than a direct call into
   * {@link LSMTreeGeoIndex} so the streaming behaviour can be exercised without a database.
   */
  @FunctionalInterface
  interface CellCursorFactory {
    IndexCursor open(String token, boolean frontier);
  }

  GeoIndexCursor(final Object[] keys, final GeoCoveringCellWalk walk, final CellCursorFactory cellCursorFactory) {
    this.keys = keys;
    this.walk = walk;
    this.cellCursorFactory = cellCursorFactory;
  }

  @Override
  public boolean hasNext() {
    if (nextRID == null)
      fetchNext();
    return nextRID != null;
  }

  @Override
  public Identifiable next() {
    if (nextRID == null) {
      fetchNext();
      if (nextRID == null)
        throw new NoSuchElementException();
    }
    currentRID = nextRID;
    nextRID = null;
    return currentRID;
  }

  /**
   * Advances to the next RID never emitted before, opening as many cell scans as it takes. Leaves {@link #nextRID}
   * null when the covering-cell walk is over.
   */
  private void fetchNext() {
    if (closed)
      return;

    while (true) {
      if (cellCursor != null) {
        while (cellCursor.hasNext()) {
          final RID rid = cellCursor.next().getIdentity();
          if (seen.add(rid)) {
            nextRID = rid;
            return;
          }
        }

        // drained: release its pages and, for a compacted series, its retire guard before opening the next cell
        cellCursor.close();
        cellCursor = null;
      }

      if (!walk.advance()) {
        // The walk is over, so nothing more can be emitted and the seen-set has no reader left. Release it here rather
        // than waiting for close(): on a wide-area query it holds one entry per candidate, and a cursor that empties
        // itself on exhaustion does not depend on every caller remembering to close a fully drained scan.
        closed = true;
        seen.clear();
        return;
      }

      cellCursor = cellCursorFactory.open(walk.getToken(), walk.isFrontier());
    }
  }

  /**
   * Number of RIDs the dedup set is currently holding - the cursor's only unbounded footprint. Package-private
   * introspection: the retained size is a property worth asserting on, since a cursor that keeps it after the walk is
   * over would hold one entry per candidate of a wide-area query for as long as the caller keeps the cursor.
   */
  int seenSize() {
    return seen.size();
  }

  @Override
  public Object[] getKeys() {
    // The cell tokens are an internal encoding of the search shape, not something a caller can use as a key, so a geo
    // cursor echoes the query keys - which is what the materialising implementation put into every IndexCursorEntry.
    return keys;
  }

  @Override
  public Identifiable getRecord() {
    return currentRID;
  }

  /**
   * A geospatial cursor exposes NO comparator, like the {@code TempIndexCursor} it replaced: its rows come from many
   * cell scans merged by a hash set, so they carry no index order a caller could compare them in. Callers that need to
   * order geo results must do so on the records, after the {@code geo.*} predicate has re-checked them.
   */
  @Override
  public BinaryComparator getComparator() {
    return null;
  }

  /** Empty for the same reason as {@link #getKeys()}: the cell tokens are not a key type a caller can interpret. */
  @Override
  public byte[] getBinaryKeyTypes() {
    return NO_KEY_TYPES;
  }

  /**
   * Unknown - it would take walking every covering cell, which is exactly what this cursor exists not to do up front.
   * <p>
   * No cardinality estimator can see this {@code -1}: the only reader of {@code estimateSize()} in the engine is
   * {@code WhereClause.estimateFromIndex}, which is gated on {@code supportsOrderedIterations()} - false for a
   * geospatial index - and reaches the cursor through {@code range()}, which this index does not implement at all.
   */
  @Override
  public long estimateSize() {
    return -1L;
  }

  @Override
  public void close() {
    closed = true;
    nextRID = null;
    if (cellCursor != null) {
      cellCursor.close();
      cellCursor = null;
    }
    seen.clear();
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return this;
  }
}
