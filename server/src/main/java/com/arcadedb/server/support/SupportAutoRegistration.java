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
package com.arcadedb.server.support;

import com.arcadedb.log.LogManager;

import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * Registers a server that holds a support key as an installation of the key's workspace, without anybody opening Studio: once a
 * little after start (so HA and the diagnostics are ready), again after a failure with a growing wait, and then every day while
 * the server runs. It is a daemon thread that never blocks or fails the start of the server and never logs the key (a failure is
 * described by its code and the portal's message, which never carries the key).
 * <p>
 * A refused key or any other definitive answer of the portal ends the loop for this run after one warning: asking again every
 * day would only repeat the refusal. A lapsed plan is tried again at the next interval, as it can be renewed.
 */
final class SupportAutoRegistration implements AutoCloseable {
  static final long DAY_MS = 24L * 60L * 60L * 1000L;

  /** When to try: the wait after start, the waits after consecutive failures (the last one repeats), the wait between successes. */
  record Timing(long initialDelayMs, long[] backoffMs, long intervalMs) {
    static Timing defaults() {
      return new Timing(30_000L, new long[] { 60_000L, 5L * 60_000L, 30L * 60_000L }, DAY_MS);
    }
  }

  /** The failures worth trying again soon: the portal or the way to it, not the answer to this server. */
  private static final Set<String> RETRYABLE = Set.of("portal_unreachable", "portal_error", "rate_limited", "internal_error");

  private final Timing          timing;
  private final BooleanSupplier enabled;
  private final BooleanSupplier registered;
  private final Supplier<String> action;
  private final LongSupplier    lastRegisteredAt;
  private final Consumer<String> warning;
  private final Object          monitor = new Object();
  private volatile boolean      stopped;
  private Thread                thread;

  SupportAutoRegistration(final Timing timing, final BooleanSupplier enabled, final BooleanSupplier registered,
      final Supplier<String> action, final LongSupplier lastRegisteredAt, final Consumer<String> warning) {
    this.timing = timing;
    this.enabled = enabled;
    this.registered = registered;
    this.action = action;
    this.lastRegisteredAt = lastRegisteredAt;
    this.warning = warning;
  }

  /** Starts the loop once; a second call does nothing. */
  synchronized void start() {
    if (thread != null || stopped)
      return;
    thread = new Thread(this::run, "ArcadeDB-SupportAutoRegistration");
    thread.setDaemon(true);
    thread.setPriority(Thread.MIN_PRIORITY);
    thread.start();
  }

  boolean isRunning() {
    final Thread t;
    synchronized (this) {
      t = thread;
    }
    return t != null && t.isAlive();
  }

  @Override
  public void close() {
    final Thread t;
    synchronized (this) {
      stopped = true;
      t = thread;
    }
    synchronized (monitor) {
      monitor.notifyAll();
    }
    if (t != null) {
      t.interrupt();
      try {
        t.join(2000L);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void run() {
    try {
      pause(timing.initialDelayMs());
      int failures = 0;
      while (!stopped) {
        long wait = timing.intervalMs();
        try {
          if (enabled.getAsBoolean() && registered.getAsBoolean()) {
            final long sinceLast = System.currentTimeMillis() - lastRegisteredAt.getAsLong();
            if (lastRegisteredAt.getAsLong() > 0L && sinceLast >= 0L && sinceLast < timing.intervalMs())
              // Studio or the connect flow registered this server a moment ago: wait out the rest of the interval
              wait = timing.intervalMs() - sinceLast;
            else {
              action.get();
              failures = 0;
            }
          }
        } catch (final SupportException e) {
          if (RETRYABLE.contains(e.getCode())) {
            wait = timing.backoffMs()[Math.min(failures, timing.backoffMs().length - 1)];
            failures++;
          } else if ("support_not_active".equals(e.getCode())) {
            warning.accept("The support plan of this workspace is not active, so this server was not registered as an installation: "
                + "it is tried again at the next interval");
          } else {
            warning.accept("This server could not be registered as an installation in the support portal (" + e.getCode() + "): "
                + describe(e) + ". It will not be tried again until the next start");
            return;
          }
        } catch (final RuntimeException e) {
          // not ready yet (diagnostics, HA) or a bug: never fatal, never loud
          wait = timing.backoffMs()[Math.min(failures, timing.backoffMs().length - 1)];
          failures++;
        }
        pause(wait);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final Throwable t) {
      LogManager.instance().log(this, Level.FINE, "Automatic registration in the support portal stopped: %s", t.getClass().getSimpleName());
    }
  }

  private static String describe(final SupportException e) {
    return "invalid_key".equals(e.getCode()) ? "the support key is not valid: check arcadedb.support.clientKey, or connect the server again"
        : e.getMessage();
  }

  private void pause(final long ms) throws InterruptedException {
    final long end = System.currentTimeMillis() + ms;
    synchronized (monitor) {
      long left = ms;
      while (!stopped && left > 0L) {
        monitor.wait(left);
        left = end - System.currentTimeMillis();
      }
    }
    if (stopped)
      throw new InterruptedException();
  }
}
