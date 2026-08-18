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

import java.util.List;

/**
 * What a TimeSeries integrity check was asked to do, and what it found (issue #6360).
 * <p>
 * The two records travel together because every level of the check - the mutable bucket, the sealed store, the
 * shard, the engine - answers the same shape, so {@code DatabaseChecker} folds them all the same way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TimeSeriesIntegrity {
  private TimeSeriesIntegrity() {
  }

  /**
   * The two knobs the SQL statement turns.
   * <p>
   * <b>{@code deep}</b> is {@code CHECK DATABASE ... DEEP}. Without it the check reads every byte of the sealed
   * store to verify each block's CRC32, plus everything the headers and directories assert about themselves; with
   * it, it additionally DECODES every sealed block and reconciles it against the three things read paths take on
   * trust and nothing else verifies: that a block's timestamps are sorted (the range iterator binary-searches
   * them), that its declared per-column min/max/sum are what its values actually add up to (the aggregation
   * push-down answers from them without decompressing), and that its declared distinct tag values cover the
   * values it holds (block-level tag pruning SKIPS a block on them). A block can pass its CRC and still be wrong
   * about all three, and each of those makes a query return a wrong answer silently rather than fail.
   * <p>
   * <b>{@code fix}</b> is {@code CHECK DATABASE ... FIX}, and for TimeSeries it is deliberately narrow: it repairs
   * DERIVED bookkeeping - counters and headers recomputable from the data they describe - and never touches a
   * sample. The full argument is on {@code DatabaseChecker.checkTimeSeries}.
   */
  public record Options(boolean deep, boolean fix) {
    /** The default: the whole default tier, changing nothing. */
    public static final Options REPORT_ONLY = new Options(false, false);
    private static final Options DEEP        = new Options(true, false);
    private static final Options FIX         = new Options(false, true);
    private static final Options DEEP_FIX    = new Options(true, true);

    /**
     * The four combinations by name, so no caller writes two positional booleans.
     * <p>
     * A record is positional and the compiler accepts {@code new Options(fix, deep)} as readily as the right order,
     * which on a REPAIR command is a swap with consequences and no symptom: a run asked to decode would silently
     * rewrite headers, and a run asked to repair would silently decline to. Naming the combinations removes the
     * only place the mistake could be made.
     */
    public static Options of(final boolean deep, final boolean fix) {
      if (deep)
        return fix ? DEEP_FIX : DEEP;
      return fix ? FIX : REPORT_ONLY;
    }

    /** Decode every sealed block and reconcile it against its own metadata, changing nothing. */
    public static Options deepOnly() {
      return DEEP;
    }

    /** Repair the derived bookkeeping, without the decoding tier. */
    public static Options fixOnly() {
      return FIX;
    }

    /** Both tiers: decode everything, and repair what is derived. */
    public static Options deepAndFix() {
      return DEEP_FIX;
    }
  }

  /**
   * One line per problem and one per repair actually applied, both empty on a healthy store.
   * <p>
   * They are separate lists rather than one because they answer different questions: {@code problems} is what an
   * operator has to act on and {@code repairs} is what the run already acted on. A repair also leaves its problem
   * in place - the finding is what the run was FOR, and hiding it because it was fixed would make a clean report
   * indistinguishable from a report of damage that was silently corrected.
   */
  public record Outcome(List<String> problems, List<String> repairs) {
    private static final Outcome CLEAN = new Outcome(List.of(), List.of());

    public static Outcome clean() {
      return CLEAN;
    }
  }
}
