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
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import org.apache.ratis.server.RaftServer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * Closes a Ratis server with a bound (issue #9561). {@code RaftServer.close()} has none: a log worker that never drains,
 * a gRPC termination that never completes or the #9556 cache-eviction deadlock each turn it into a call that never
 * returns, and the caller then hangs with it - {@code RaftHAServer.stop()} and the JVM shutdown hook behind it, or the
 * in-place restart and the recovery lock it holds, so the health monitor can never retry.
 * <p>
 * The close runs on its own daemon thread. When it does not finish within the bound, the caller gets that thread back
 * instead of waiting for it, and the stack it is stuck in is logged at SEVERE. The thread is not interrupted: an
 * interrupt is what cut a Ratis gRPC shutdown short in #8898, and a close that finishes late is still a close that
 * releases what it holds. Until it does, the server's storage lock and gRPC ports may still be held, which is why the
 * in-place restart refuses to start a second server while the thread is alive.
 */
final class RatisServerCloser {

  /** Body of a close, so the bound can be tested without a Ratis server. */
  @FunctionalInterface
  interface CloseAction {
    void close() throws IOException;
  }

  private RatisServerCloser() {
  }

  /**
   * Stops the Raft log cache-eviction threads of {@code server} (issue #9556), then closes it, waiting at most
   * {@code timeoutMs} for both.
   *
   * @return null when the close finished in time; otherwise the thread still running it
   * @throws IOException the failure of a close that finished in time
   */
  static Thread close(final RaftServer server, final long timeoutMs) throws IOException {
    return close("server " + server.getId(), () -> {
      RaftLogCacheEviction.stopBeforeClose(server);
      server.close();
    }, timeoutMs);
  }

  /**
   * Runs {@code action} on a daemon thread and waits at most {@code timeoutMs} for it (0 or negative: without a bound).
   * An interrupt of the caller ends the wait early, keeps the caller's interrupt flag set and returns the thread like a
   * timeout does.
   *
   * @return null when the action finished in time; otherwise the thread still running it
   * @throws IOException the failure of an action that finished in time
   */
  static Thread close(final String what, final CloseAction action, final long timeoutMs) throws IOException {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread closer = new Thread(() -> {
      try {
        action.close();
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, "arcadedb-ratis-close-" + what.replace(' ', '-'));
    closer.setDaemon(true);
    closer.start();

    boolean interrupted = false;
    try {
      if (timeoutMs > 0)
        closer.join(timeoutMs);
      else
        closer.join(); // 0 or negative: no bound, the behavior before issue #9561
    } catch (final InterruptedException e) {
      interrupted = true;
      Thread.currentThread().interrupt();
    }

    if (closer.isAlive()) {
      if (interrupted)
        LogManager.instance().log(RatisServerCloser.class, Level.WARNING,
            "Interrupted while waiting for the close of the Ratis %s; it keeps running on thread %s", what, closer.getName());
      else
        LogManager.instance().log(RatisServerCloser.class, Level.SEVERE,
            "The close of the Ratis %s did not finish within %dms; it keeps running on thread %s, and the storage lock and "
                + "gRPC ports it holds may stay held until it does (issue #9561). Stuck at:%n%s", what, timeoutMs,
            closer.getName(), stackOf(closer));
      return closer;
    }

    final Throwable t = failure.get();
    if (t instanceof IOException e)
      throw e;
    if (t instanceof RuntimeException e)
      throw e;
    if (t instanceof Error e)
      throw e;
    if (t != null)
      throw new IOException("Error closing the Ratis " + what, t);
    return null;
  }

  static String stackOf(final Thread thread) {
    final StringBuilder sb = new StringBuilder();
    for (final StackTraceElement frame : thread.getStackTrace())
      sb.append("\tat ").append(frame).append(System.lineSeparator());
    return sb.toString();
  }
}
