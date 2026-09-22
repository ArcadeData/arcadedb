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
package com.arcadedb.engine;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * Decides at which level a file reports that it reopened a channel a thread interrupt closed (issues #7768, #4930,
 * #8139). Reopen-and-retry clears the interrupt flag around the retry and then RESTORES it, deliberately, so the
 * cancellation stays observable to the caller - which means the next interruptible operation on the reopened channel
 * is closed again at once. A commit does one or two channel operations, but a recovery scan does one per read, and
 * logging every reopen at {@code SEVERE} turned a single interrupted scan into an unbounded flood.
 * <p>
 * The first reopen is still {@code SEVERE}, which is what made the failure visible in the first place, and so is the
 * first one after {@link #SEVERE_INTERVAL_NANOS} of quiet, so a later, unrelated interrupt on a long-lived file is not
 * hidden behind an old one. Every reopen in between is logged at {@code FINE}, and the next {@code SEVERE} line says how
 * many it folded. {@link #getTotal()} lets a scan report its own count once when it finishes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ChannelReopenLog {
  static final long SEVERE_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

  private static final long NEVER = Long.MIN_VALUE;

  private final LongSupplier nanoClock;
  private final AtomicLong   total           = new AtomicLong();
  private final AtomicLong   sinceLastSevere = new AtomicLong();
  private final AtomicLong   lastSevereNanos = new AtomicLong(NEVER);

  ChannelReopenLog() {
    this(System::nanoTime);
  }

  ChannelReopenLog(final LongSupplier nanoClock) {
    this.nanoClock = nanoClock;
  }

  /**
   * Records one reopen.
   *
   * @return the number of earlier reopens folded into this report when it must be logged at {@link Level#SEVERE}
   * (0 for the first one), or -1 when it must be logged at {@link Level#FINE}
   */
  long record() {
    total.incrementAndGet();
    final long now = nanoClock.getAsLong();
    final long last = lastSevereNanos.get();
    if ((last == NEVER || now - last >= SEVERE_INTERVAL_NANOS) && lastSevereNanos.compareAndSet(last, now))
      return sinceLastSevere.getAndSet(0);
    sinceLastSevere.incrementAndGet();
    return -1;
  }

  /** Every reopen recorded since this file was opened. */
  long getTotal() {
    return total.get();
  }
}
