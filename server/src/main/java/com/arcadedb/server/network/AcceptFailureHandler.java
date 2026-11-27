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
package com.arcadedb.server.network;

import com.arcadedb.log.LogManager;

import java.util.logging.Level;

/**
 * What the accept loop of a wire-protocol listener does with a failure of the work it commits to one accepted connection.
 * <p>
 * An {@link Exception} is logged and the loop goes on. An {@link Error} belongs to the connection that met it as much as an
 * exception does, and must not end the accept thread either, since nothing starts it again. It is logged at most once per
 * {@link #LOG_INTERVAL_MS}, saying how many were held back, and the loop pauses {@link #PAUSE_MS} so that a condition that
 * lasts is not retried in a hot loop.
 * <p>
 * A persistent {@link Error}, an {@link OutOfMemoryError} included, therefore keeps the loop alive on purpose: the listener
 * goes on accepting and dropping connections at the bounded rate above, and serves again by itself once the condition
 * clears.
 * <p>
 * One instance per listener, used by its accept thread only.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class AcceptFailureHandler {
  /** The pause after an error, so that a failure that persists is retried at a bounded rate. */
  public static final long PAUSE_MS        = 100;
  /** How often a listener that keeps failing says so. */
  static final        long LOG_INTERVAL_MS = 10_000;

  private final String protocol;
  private       long   lastLogTime;
  private       long   heldBack;

  public AcceptFailureHandler(final String protocol) {
    this.protocol = protocol;
  }

  /**
   * Reports {@code failure}, which ended the work of the current iteration of the accept loop, and for an {@link Error}
   * pauses before the next one. Never throws: a failure to report must not end the loop either.
   *
   * @param message what the loop was doing, in the words the listener already uses for an exception
   */
  public void handle(final Object source, final String message, final Throwable failure) {
    try {
      if (failure instanceof Error)
        logError(source, message, failure);
      else
        LogManager.instance().log(source, Level.WARNING, message, failure);
    } catch (final Throwable e) {
      // IGNORE IT: THE LOOP MUST GO ON
    }

    if (failure instanceof Error)
      try {
        Thread.sleep(PAUSE_MS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
  }

  private void logError(final Object source, final String message, final Throwable failure) {
    final long now = System.currentTimeMillis();
    if (lastLogTime != 0 && now - lastLogTime < LOG_INTERVAL_MS) {
      ++heldBack;
      return;
    }

    LogManager.instance().log(source, Level.WARNING, "%s: %s (%d similar errors not logged since the last one)", failure,
        protocol, message, heldBack);
    lastLogTime = now;
    heldBack = 0;
  }

  /** How many errors were not logged since the last one that was. */
  long getHeldBack() {
    return heldBack;
  }
}
