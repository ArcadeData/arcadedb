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
package com.arcadedb.index.columnar.spike;

import com.arcadedb.utility.StallAwareStopwatch;

/**
 * Spike-only nanosecond stopwatch with the same stall discount as {@link StallAwareStopwatch}, without its millisecond flooring.
 */
final class SpikeClock {
  private final long beginNanos = System.nanoTime();
  private final long beginStall = StallAwareStopwatch.jvmStallNanos();
  private       long endNanos;
  private       long endStall;

  static SpikeClock start() {
    return new SpikeClock();
  }

  /** Stops the clock and returns the stall-free elapsed nanoseconds. */
  long stop() {
    endNanos = System.nanoTime();
    endStall = StallAwareStopwatch.jvmStallNanos();
    return effectiveNanos();
  }

  long effectiveNanos() {
    return Math.max(0L, (endNanos - beginNanos) - (endStall - beginStall));
  }

  long stallNanos() {
    return endStall - beginStall;
  }

  static String ms(final double nanos) {
    return String.format("%.3f", nanos / 1_000_000.0);
  }
}
