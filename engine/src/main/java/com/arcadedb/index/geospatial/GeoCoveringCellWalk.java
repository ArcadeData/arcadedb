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

import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Pull-style walk of the GeoHash cells covering a shape, telling the caller whether each one is a FRONTIER cell - one
 * the decomposition stops at, with no deeper cell of its own below it.
 * <p>
 * {@link CellIterator} is a depth-first pre-order walk, so a cell is a frontier exactly when the cell that follows it
 * is not deeper; the last cell always is. This is derived from the traversal rather than from {@link Cell#isLeaf()} so
 * that it holds for every shape and grid, including the boundary cells at the requested detail level that are emitted
 * without the leaf flag.
 * <p>
 * The write path only ever needs to push every cell at once, but the read path has to be able to STOP - a lazy query
 * cursor opens one underlying scan per cell and must not walk the remaining cells until its consumer asks for more
 * rows (#5601). Hence a hand-rolled {@code advance()}/getter pair rather than an {@code Iterator<T>}: a wide-area query
 * resolves into several thousand cells, and a per-cell tuple object would allocate one garbage object for each.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GeoCoveringCellWalk {
  private final CellIterator cellIterator;
  /**
   * Retargeted at each cell's own bytes rather than allocating one BytesRef per cell; the token is turned into a
   * String immediately, so nothing outlives the next call.
   */
  private final BytesRef     scratch      = new BytesRef();
  /** One-cell lookahead: a cell can only be classified once the cell that FOLLOWS it is known. */
  private       String       pendingToken;
  private       int          pendingLevel = -1;
  private       String       token;
  private       boolean      frontier;

  GeoCoveringCellWalk(final CellIterator cellIterator) {
    this.cellIterator = cellIterator;
  }

  /**
   * Moves to the next covering cell.
   *
   * @return false when the walk is over; otherwise {@link #getToken()} and {@link #isFrontier()} describe the cell
   */
  boolean advance() {
    while (cellIterator.hasNext()) {
      final Cell cell = cellIterator.next();
      final int level = cell.getLevel();
      final String cellToken = cell.getTokenBytesNoLeaf(scratch).utf8ToString();

      final String emit = pendingToken;
      final boolean emitFrontier = level <= pendingLevel;

      // an empty token is the world cell: it covers everything, so it is never a useful lookup key
      pendingToken = cellToken.isEmpty() ? null : cellToken;
      pendingLevel = level;

      if (emit != null) {
        token = emit;
        frontier = emitFrontier;
        return true;
      }
    }

    if (pendingToken != null) {
      // the last cell of a pre-order walk has nothing deeper after it, so it is always a frontier
      token = pendingToken;
      frontier = true;
      pendingToken = null;
      return true;
    }

    return false;
  }

  String getToken() {
    return token;
  }

  boolean isFrontier() {
    return frontier;
  }
}
