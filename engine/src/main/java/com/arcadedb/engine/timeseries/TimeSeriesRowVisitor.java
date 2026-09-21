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
   * @implSpec <b>Called with NO lock held</b> (issue #7897, corrected here by issue #8052). This used to say the
   * opposite - that the scan holds the shard's compaction read lock and the sealed store's directory read lock
   * across the call - and inverting exactly that is what #7897 was: {@code TimeSeriesShard.forEachRow} and
   * {@code forEachTagCombination} release the compaction lock once they have taken their snapshot, and
   * {@code TimeSeriesSealedStore.walkBlocks} decodes a block's columns under the directory read lock, releases it,
   * and only then builds the rows and hands them over. The visitor is the caller's code and its cost is therefore
   * unbounded from the engine's side, which is precisely why no lock is held across it.
   * <p>
   * Fold, do not compute - the advice is unchanged and the reason is not. It is no longer "you are blocking a
   * writer"; it is that the walk is reading a DIRECTORY SNAPSHOT that is going stale under it. Between two blocks
   * the store may be compacted, truncated, downsampled or (on an HA follower) replaced wholesale by the leader's
   * sealed file, and every block the walk has not reached yet is re-resolved against the live directory before it
   * is read. A block that still exists is read wherever it now lives; one that a retention pass really did delete
   * resolves to nothing, is skipped, and is counted in {@code AggregationMetrics.vanishedBlocks} (issue #8043). A
   * slow visitor does not block anyone - it widens that window.
   */
  boolean visit(Object[] row);
}
