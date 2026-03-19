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

  public int getSkippedBlocks() {
    return skippedBlocks;
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
  }

  @Override
  public String toString() {
    final long totalNanos = ioNanos + decompTsNanos + decompValNanos + accumNanos;
    return String.format(
        "AggMetrics[io=%dms decompTs=%dms decompVal=%dms accum=%dms total=%dms | blocks: fast=%d slow=%d skipped=%d]",
        ioNanos / 1_000_000, decompTsNanos / 1_000_000, decompValNanos / 1_000_000,
        accumNanos / 1_000_000, totalNanos / 1_000_000,
        fastPathBlocks, slowPathBlocks, skippedBlocks);
  }
}
