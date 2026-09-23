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

package com.arcadedb.containers.ha.chaos;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Runs with the workload quiesced: waits until every node reports the same row and edge counts twice in a row, scans
 * every node, re-reads the counts (a change means a late commit landed during the scan: converge and scan again within
 * the same deadline), reports keys that differ between nodes, then checks node 0 against the ledger. A 5xx while
 * scanning is a SAFETY {@code SCAN_ERROR} (the node is up but cannot serve its own data); any other scan failure is an
 * AVAILABILITY {@code SCAN}.
 */
public final class Checkpoint {
  public record Result(List<Violation> violations, long[] counts, long convergenceMillis, long durationMillis) {
  }

  private final NodeReader       reader;
  private final Ledger           ledger;
  private final InvariantChecker checker;
  private final int              nodes;
  private final Duration         convergenceTimeout;
  private final Duration         pollInterval;
  private       long[]           lastCounts;
  private       String           lastReadError;

  public Checkpoint(final NodeReader reader, final Ledger ledger, final InvariantChecker checker, final int nodes,
      final Duration convergenceTimeout, final Duration pollInterval) {
    this.reader = reader;
    this.ledger = ledger;
    this.checker = checker;
    this.nodes = nodes;
    this.convergenceTimeout = convergenceTimeout;
    this.pollInterval = pollInterval;
  }

  public Result run() throws InterruptedException {
    final long start = System.nanoTime();
    final long deadline = start + convergenceTimeout.toNanos();
    while (true) {
      long[] previous = null;
      long[] current;
      String lastError = null;
      while (true) {
        current = readCounts();
        if (current == null)
          lastError = lastReadError;
        if (current != null && converged(current) && Arrays.equals(previous, current))
          break;
        previous = current;
        if (System.nanoTime() > deadline) {
          final String message = "Nodes did not converge within " + convergenceTimeout + ": [ops, edges] per node = "
              + Arrays.toString(lastCounts) + (lastError == null ? "" : ", last read error: " + lastError);
          return new Result(List.of(new Violation(ResultKind.SAFETY, "CONVERGENCE", message, new long[0])), lastCounts,
              millisSince(start), millisSince(start));
        }
        Thread.sleep(pollInterval.toMillis());
      }
      final long convergenceMillis = millisSince(start);

      final NodeSnapshot[] snapshots = new NodeSnapshot[nodes];
      for (int i = 0; i < nodes; i++) {
        snapshots[i] = new NodeSnapshot(ledger);
        try {
          reader.scan(i, snapshots[i]);
        } catch (final HttpNodeReader.ServerErrorException e) {
          return new Result(List.of(new Violation(ResultKind.SAFETY, "SCAN_ERROR",
              "node " + i + " answered with a server error while scanning after convergence: " + e.getMessage(),
              new long[0])), current, convergenceMillis, millisSince(start));
        } catch (final IOException e) {
          return new Result(List.of(new Violation(ResultKind.AVAILABILITY, "SCAN",
              "node " + i + " could not be scanned after convergence: " + e.getMessage(), new long[0])), current,
              convergenceMillis, millisSince(start));
        }
      }

      // a legal late commit can land while the nodes are scanned one after the other: converge and scan again
      if (!Arrays.equals(readCounts(), current)) {
        if (System.nanoTime() > deadline) {
          final String message = "Nodes did not hold still for a scan within " + convergenceTimeout
              + ": [ops, edges] per node = " + Arrays.toString(lastCounts);
          return new Result(List.of(new Violation(ResultKind.SAFETY, "CONVERGENCE", message, new long[0])), lastCounts,
              convergenceMillis, millisSince(start));
        }
        continue;
      }

      final List<Violation> violations = new ArrayList<>();
      for (int i = 1; i < nodes; i++) {
        final long[] diff = snapshots[0].diff(snapshots[i], InvariantChecker.MAX_KEYS);
        if (diff.length > 0)
          violations.add(new Violation(ResultKind.SAFETY, "DIVERGENCE",
              "node " + i + " differs from node 0 in " + diff.length + (diff.length == InvariantChecker.MAX_KEYS ? "+" : "")
                  + " keys despite equal counts", diff));
      }
      violations.addAll(checker.check(snapshots[0]));
      return new Result(violations, current, convergenceMillis, millisSince(start));
    }
  }

  /**
   * @return {@code [ops, edges]} per node, or null when a node could not be read ({@link #lastCounts} then holds what
   * was read, with zeros for the unreadable nodes, and {@link #lastReadError} the error)
   */
  private long[] readCounts() {
    final long[] counts = new long[nodes * 2];
    boolean readable = true;
    for (int i = 0; i < nodes; i++)
      try {
        final long[] node = reader.counts(i);
        counts[i * 2] = node[0];
        counts[i * 2 + 1] = node[1];
      } catch (final IOException e) {
        readable = false;
        lastReadError = e.getMessage();
      }
    lastCounts = counts;
    return readable ? counts : null;
  }

  private static boolean converged(final long[] counts) {
    for (int i = 2; i < counts.length; i += 2)
      if (counts[i] != counts[0] || counts[i + 1] != counts[1])
        return false;
    return true;
  }

  private static long millisSince(final long startNanos) {
    return (System.nanoTime() - startNanos) / 1_000_000;
  }
}
