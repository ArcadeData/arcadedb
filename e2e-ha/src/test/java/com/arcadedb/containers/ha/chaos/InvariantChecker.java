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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Checks one converged node snapshot against the ledger:
 * <ul>
 *   <li>I1 every acknowledged (or once seen) write is present;</li>
 *   <li>I2 every row was produced by a recorded operation, exactly once;</li>
 *   <li>I3 no write rejected before append is present;</li>
 *   <li>I4 each UNKNOWN write is resolved to ACKED_LATE or LOST_UNKNOWN. A LOST_UNKNOWN write that appears later is a
 *       legitimate late Raft commit, re-resolved to ACKED_LATE and counted, not a violation;</li>
 *   <li>I5 a pair transaction's edge exists if and only if its vertex does.</li>
 * </ul>
 */
public final class InvariantChecker {
  public static final int MAX_KEYS = 100;

  private final Ledger ledger;
  private       long   lateCommits;

  public InvariantChecker(final Ledger ledger) {
    this.ledger = ledger;
  }

  public long lateCommits() {
    return lateCommits;
  }

  public List<Violation> check(final NodeSnapshot snapshot) {
    final Collector lost = new Collector();
    final Collector phantom = new Collector();
    final Collector resurrected = new Collector();
    final Collector atomicity = new Collector();
    final Collector inFlight = new Collector();

    for (final long key : snapshot.phantoms())
      phantom.add(key);
    for (final long key : snapshot.duplicates())
      phantom.add(key);
    for (final long key : snapshot.multiEdge())
      atomicity.add(key);

    for (int w = 0; w < ledger.writers(); w++) {
      final int size = ledger.size(w);
      for (int s = 0; s < size; s++) {
        final long key = Ledger.key(w, s);
        final boolean present = snapshot.present(w, s);
        switch (ledger.outcome(key)) {
          case Ledger.ACKED, Ledger.ACKED_LATE -> {
            if (!present)
              lost.add(key);
          }
          case Ledger.FAILED -> {
            if (present)
              resurrected.add(key);
          }
          case Ledger.UNKNOWN -> ledger.record(key, present ? Ledger.ACKED_LATE : Ledger.LOST_UNKNOWN);
          case Ledger.LOST_UNKNOWN -> {
            if (present) {
              ledger.record(key, Ledger.ACKED_LATE);
              ++lateCommits;
            }
          }
          default -> inFlight.add(key);
        }
        if (present && ledger.isPair(key) != snapshot.hasEdge(w, s))
          atomicity.add(key);
      }
    }

    final List<Violation> violations = new ArrayList<>();
    lost.report(violations, ResultKind.SAFETY, "I1", "acknowledged writes are missing");
    phantom.report(violations, ResultKind.SAFETY, "I2", "rows were not produced by any recorded operation, or appear twice");
    resurrected.report(violations, ResultKind.SAFETY, "I3", "writes rejected before append are present");
    atomicity.report(violations, ResultKind.SAFETY, "I5", "transactions were applied partially (vertex and edge disagree), "
        + "or a pair's target was not visible on the leader (stale read)");
    inFlight.report(violations, ResultKind.HARNESS, "QUIESCE", "operations were still in flight at the checkpoint");
    return violations;
  }

  private static final class Collector {
    private final long[] keys = new long[MAX_KEYS];
    private       int    kept;
    private       long   count;

    void add(final long key) {
      ++count;
      if (kept < MAX_KEYS)
        keys[kept++] = key;
    }

    void report(final List<Violation> out, final ResultKind kind, final String invariant, final String what) {
      if (count > 0)
        out.add(new Violation(kind, invariant, count + " " + what, Arrays.copyOf(keys, kept)));
    }
  }
}
