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

/**
 * Receives the rows of a time-series scan one at a time, so a reader that only needs to fold them into an answer
 * never holds the series (issue #7354).
 * <p>
 * The alternative on the read path is {@code query()} / {@code iterateQuery()}, both of which materialise every
 * matching row into an {@code ArrayList} before the caller sees the first one - {@code query()} then sorts the
 * whole list by timestamp. That is what a query returning rows needs. A question whose answer is O(label
 * cardinality) - the distinct values of a TAG column, the label combinations a metric carries - does not: it reads
 * every row and keeps a handful of strings, so materialising the series to produce a set of five is the whole
 * cost of the call.
 * <p>
 * A visitor sees the rows of one block at a time and in whatever order the shards hold them, NOT merged by
 * timestamp: the merge is what forces every shard's rows to be resident at once, and no folding answer needs it.
 * A caller that needs timestamp order wants {@code iterateQuery()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@FunctionalInterface
public interface TimeSeriesRowVisitor {
  /**
   * @param row {@code { timestamp, col1, col2, ... }}, freshly allocated for this row and NOT reused by the scan
   *            afterwards, so a visitor may keep the array rather than copy out of it - which is what lets
   *            {@code iterateRange} be this interface with an {@code ArrayList} for a visitor. A scan that ever
   *            wanted to hand out a reused buffer would have to change this contract and that caller together.
   *
   * @return {@code false} to stop the scan, which stops it for good - no further block is read and no further
   * shard is opened. {@code true} to continue.
   *
   * @implSpec Called while the scan holds the shard's compaction read lock and the sealed store's directory read
   * lock - that is what lets the rows be produced as the file is read. Fold, do not compute: the locks are shared,
   * so no other reader is blocked, but a writer waiting for one waits for the whole scan.
   */
  boolean visit(Object[] row);
}
