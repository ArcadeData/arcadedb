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
 * Mutable accumulator for aggregation timing breakdown.
 * <p>
 * Thread-safety contract: each shard should use its own instance for accumulation
 * (via {@code addIo()}, {@code addDecompTs()}, etc.). Only {@link #mergeFrom(AggregationMetrics)}
 * is synchronized and safe to call from multiple threads to merge per-shard results
 * into a shared instance after all futures have completed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class AggregationMetrics {

  private long ioNanos;
  private long decompTsNanos;
  private long decompValNanos;
  private long accumNanos;
  private int  fastPathBlocks;
  private int  slowPathBlocks;
  private int  skippedBlocks;
  private int  vanishedBlocks;
  private int  scannedPages;
  private int  skippedPages;
  private long materializedRows;
  private int  overflowBuckets;

  public void addIo(final long nanos) {
    ioNanos += nanos;
  }

  public void addDecompTs(final long nanos) {
    decompTsNanos += nanos;
  }

  public void addDecompVal(final long nanos) {
    decompValNanos += nanos;
  }

  public void addAccum(final long nanos) {
    accumNanos += nanos;
  }

  public void addFastPathBlock() {
    fastPathBlocks++;
  }

  public void addSlowPathBlock() {
    slowPathBlocks++;
  }

  public void addSkippedBlock() {
    skippedBlocks++;
  }

  /**
   * A block the walk held a directory entry for and could no longer find in the store (issue #8043).
   * <p>
   * NOT a variant of {@link #addSkippedBlock()}: a skipped block was examined and declined - its declared range
   * or its declared tag values ruled it out - so the rows it holds are not part of the answer. A vanished block
   * was going to be read and is no longer there, because a retention pass deleted its rows or a downsampling
   * pass replaced it with a coarser block while the walk was between two blocks. The answer is therefore SHORT,
   * and counting it is what keeps that distinguishable from "no row matched" by a caller that cares - an
   * {@code EXPORT DATABASE} above all.
   */
  public void addVanishedBlock() {
    vanishedBlocks++;
  }

  /**
   * A mutable-bucket data page whose rows were examined.
   */
  public void addScannedPage() {
    scannedPages++;
  }

  /**
   * A mutable-bucket data page discarded on its min/max timestamp header alone.
   */
  public void addSkippedPage() {
    skippedPages++;
  }

  /**
   * Rows actually turned into {@code Object[]}, i.e. the ones that survived every push-down.
   */
  public void addMaterializedRows(final int rows) {
    materializedRows += rows;
  }

  /**
   * Buckets the flat-mode result had to park in its overflow map because they fell outside the
   * pre-allocated window (issue #6937). Correct output either way - the overflow map keeps the numbers
   * right - but a non-zero value means the sizing estimate in {@code aggregateMulti()} came up short,
   * which is the thing that used to lose samples silently. Worth watching in a profile.
   */
  public void addOverflowBuckets(final int buckets) {
    overflowBuckets += buckets;
  }

  public long getIoNanos() {
    return ioNanos;
  }

  public long getDecompTsNanos() {
    return decompTsNanos;
  }

  public long getDecompValNanos() {
    return decompValNanos;
  }

  public long getAccumNanos() {
    return accumNanos;
  }

  public int getFastPathBlocks() {
    return fastPathBlocks;
  }

  public int getSlowPathBlocks() {
    return slowPathBlocks;
  }

  public int getVanishedBlocks() {
    return vanishedBlocks;
  }

  public int getSkippedBlocks() {
    return skippedBlocks;
  }

  public int getScannedPages() {
    return scannedPages;
  }

  public int getSkippedPages() {
    return skippedPages;
  }

  public long getMaterializedRows() {
    return materializedRows;
  }

  public int getOverflowBuckets() {
    return overflowBuckets;
  }

  /**
   * Merges counters from another instance (used to aggregate across shards).
   */
  public synchronized void mergeFrom(final AggregationMetrics other) {
    ioNanos += other.ioNanos;
    decompTsNanos += other.decompTsNanos;
    decompValNanos += other.decompValNanos;
    accumNanos += other.accumNanos;
    fastPathBlocks += other.fastPathBlocks;
    slowPathBlocks += other.slowPathBlocks;
    skippedBlocks += other.skippedBlocks;
    vanishedBlocks += other.vanishedBlocks;
    scannedPages += other.scannedPages;
    skippedPages += other.skippedPages;
    materializedRows += other.materializedRows;
    overflowBuckets += other.overflowBuckets;
  }

  @Override
  public String toString() {
    final long totalNanos = ioNanos + decompTsNanos + decompValNanos + accumNanos;
    return String.format(
        "AggMetrics[io=%dms decompTs=%dms decompVal=%dms accum=%dms total=%dms | blocks: fast=%d slow=%d skipped=%d"
            + " vanished=%d | pages: scanned=%d skipped=%d | rows: materialized=%d | buckets: overflow=%d]",
        ioNanos / 1_000_000, decompTsNanos / 1_000_000, decompValNanos / 1_000_000,
        accumNanos / 1_000_000, totalNanos / 1_000_000,
        fastPathBlocks, slowPathBlocks, skippedBlocks, vanishedBlocks, scannedPages, skippedPages, materializedRows,
        overflowBuckets);
  }
}
