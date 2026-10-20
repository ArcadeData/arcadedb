/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Central, low-overhead reporter for <i>ghost edges</i>: dangling edge-segment pointers whose backing
 * edge record no longer exists (e.g. after incomplete HA replication or a partially rolled-back
 * transaction). Traversal and graph-algorithm code skips such edges rather than failing the whole
 * query; routing every skip through here makes the anomaly observable instead of silently swallowed.
 * <p>
 * Each skip is counted and logged at {@code FINE} (full per-occurrence trace, off by default, so there
 * is no cost on clean graphs because this is only reached when a ghost is actually encountered). In
 * addition a {@code WARNING} is emitted at most once per 60s - mirroring the engine's other throttled
 * saturation logs - so operators notice a data-integrity problem without log flooding on a heavily
 * corrupted graph.
 */
public final class GhostEdgeReporter {
  private static final long       WARNING_WINDOW_NANOS = 60_000_000_000L; // 60s
  private static final AtomicLong totalSkipped         = new AtomicLong();
  // Seeded one full window in the past (NOT Long.MIN_VALUE): the throttle compares with the overflow-safe
  // 'now - last >= WINDOW'. Long.MIN_VALUE is not a real nanoTime reading, so 'now - Long.MIN_VALUE' overflows
  // to a negative value on any JVM where System.nanoTime() is positive, and the first WARNING would never fire.
  // -WARNING_WINDOW_NANOS is within a real reading's range, so the first ghost encountered always warns.
  private static final AtomicLong lastWarningNanos     = new AtomicLong(-WARNING_WINDOW_NANOS);

  private GhostEdgeReporter() {
  }

  /**
   * Records and logs that a ghost edge was skipped during traversal.
   *
   * @param cause the {@link RecordNotFoundException} raised when the dangling edge was dereferenced
   */
  public static void reportSkipped(final RecordNotFoundException cause) {
    report(cause.getMessage());
  }

  /**
   * {@code detail} is formatted by the logger, and only if FINE is on: nothing is built per ghost for a log nobody reads, not even the
   * array of its arguments, which the level guard keeps off a graph with many ghosts.
   */
  private static void report(final Object detail) {
    final long total = totalSkipped.incrementAndGet();

    if (LogManager.instance().isLoggable(GhostEdgeReporter.class, Level.FINE))
      LogManager.instance().log(GhostEdgeReporter.class, Level.FINE,
          "Skipped ghost edge during traversal (%s); total skipped since startup=%d", detail, total);

    if (shouldEmitWarning(System.nanoTime()))
      LogManager.instance().log(GhostEdgeReporter.class, Level.WARNING,
          "Ghost (dangling) edges encountered and skipped during traversal (total skipped since startup=%d). "
              + "A dangling edge-segment pointer indicates a data-integrity anomaly, e.g. incomplete HA "
              + "replication or a partially rolled-back transaction.", total);
  }

  /**
   * Records and logs that the adjacency entry naming {@code edgeRid} was skipped, for a walk that found it out from the bucket
   * ({@link #edgeRecordExists}) and so has no {@link RecordNotFoundException} of its own to hand over.
   */
  public static void reportSkipped(final RID edgeRid) {
    // No exception and no string is built for it: both would be paid on every ghost, on the graph where there are many
    report(edgeRid);
  }

  /**
   * Whether the record of the edge named by an adjacency entry is still there, answered by the bucket without reading the record: the
   * probe of a walk that follows entries on the endpoints they hold and so never loads the edge, which is where a ghost goes unnoticed
   * (issues #10148, #10149). A lightweight edge has no record of its own, its adjacency entry is all there is of it.
   * <p>
   * Asked through the transaction, so an edge the open transaction wrote is there. The permission to read the edge's bucket is not
   * asked: the walk reads nothing of the edge and hands back only the vertex the entry names, exactly as it did before the probe, so
   * the answer reaches no one as a record; a user who may traverse the entry must not need a grant the traversal never needed.
   */
  public static boolean edgeRecordExists(final Database database, final RID edgeRid) {
    // A lightweight edge has no record, and its RID has a negative position: there is nothing to be gone, so the entry is live
    if (edgeRid == null || edgeRid.getPosition() < 0)
      return true;
    final Bucket bucket = database.getSchema().getBucketByIdIfExists(edgeRid.getBucketId());
    if (bucket == null)
      return false;
    // isRecordPresent is the same question as existsRecord without the READ_RECORD check (see above); a bucket that is not an engine
    // one has only the checked form
    return bucket instanceof LocalBucket local ? local.isRecordPresent(edgeRid) : bucket.existsRecord(edgeRid);
  }

  /**
   * {@link #edgeRecordExists} for a walk that skips a ghost: false, after reporting it, when the entry names a record that is gone.
   */
  public static boolean isLiveEntry(final Database database, final RID edgeRid) {
    if (edgeRecordExists(database, edgeRid))
      return true;
    reportSkipped(edgeRid);
    return false;
  }

  /**
   * Decides, with overflow-safe elapsed arithmetic, whether a throttled WARNING should fire at {@code now}
   * and, if so, claims the slot atomically (CAS) so concurrent skips emit at most one WARNING per window.
   * <p>
   * Package-private ({@code @VisibleForTesting}) so the throttle can be exercised directly without capturing
   * log output: the previous {@code Long.MIN_VALUE} seed made {@code now - last} overflow and suppressed the
   * WARNING forever, a bug invisible to a test that only asserts on the skip count.
   *
   * @param now a {@link System#nanoTime()} reading
   * @return {@code true} iff this caller won the slot and should log the WARNING
   */
  static boolean shouldEmitWarning(final long now) {
    final long last = lastWarningNanos.get();
    return now - last >= WARNING_WINDOW_NANOS && lastWarningNanos.compareAndSet(last, now);
  }

  /** Total ghost edges skipped since JVM startup (monotonic). */
  public static long getTotalSkipped() {
    return totalSkipped.get();
  }

  /**
   * Resets the static counters so each test starts from a clean slate.
   * <p>
   * Package-private on purpose ({@code @VisibleForTesting}): it is only reachable from
   * {@code com.arcadedb.graph.GhostEdgeReporterTest}, which lives in this same package. If that test is
   * ever moved out of {@code com.arcadedb.graph}, this method must be widened (or the test kept in-package).
   */
  static void resetForTests() {
    totalSkipped.set(0);
    lastWarningNanos.set(-WARNING_WINDOW_NANOS);
  }
}
