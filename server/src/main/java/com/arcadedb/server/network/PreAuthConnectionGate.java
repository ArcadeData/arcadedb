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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;

import io.micrometer.core.instrument.Metrics;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Caps how many connections a wire-protocol listener may hold in the phase before authentication (issue
 * #6412).
 * <p>
 * Each of the three binary protocol listeners (Postgres, Redis, BOLT) commits one operating-system thread
 * and one file descriptor per accepted socket, before anyone has proved who they are. Bounding how <i>long</i>
 * a connection may stay in that phase - the handshake read timeout of #6377, #5912 and #5978 - only sets the
 * rate at which those are reclaimed; it does not bound how <i>many</i> exist at once, so a client that opens
 * connections faster than the timeout reaps them still drives thread and descriptor count arbitrarily high.
 * This is the other half: past the cap, the socket is closed immediately and the accept loop moves on.
 * <p>
 * Two deliberate properties:
 * <ul>
 * <li><b>The refusal is a close, not a message.</b> Writing a protocol-level "too many connections" error
 * would be friendlier, but it would be a write performed on the accept thread, and a peer that never reads
 * its socket can stall that write - which would hand an attacker the whole listener rather than one
 * connection. Nothing that a remote peer can slow down belongs on the accept path.</li>
 * <li><b>A permit is released once, when the connection first authenticates</b> (or when it dies before
 * doing so). Protocols that can return to an unauthenticated state - BOLT's LOGOFF, Redis re-AUTH - do not
 * take a permit again: the cap exists to bound connections that have not yet proved anything, and a
 * connection that has authenticated once is no longer one of those.</li>
 * </ul>
 * Each listener owns its own gate rather than sharing one server-wide, so a flood against one protocol
 * cannot use up the budget that lets clients of another protocol log in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PreAuthConnectionGate {
  /** How often a listener that is refusing connections says so, so that a flood cannot flood the log too. */
  private static final long LOG_INTERVAL_MS = 10_000;

  private final String       protocol;
  private final int          maxConnections;
  private final Semaphore    permits;
  private final AtomicLong   refused         = new AtomicLong();
  private final AtomicLong   lastLogTime     = new AtomicLong();

  /**
   * A held permit. Releasing it is idempotent, so the connection can hand it back at the point it
   * authenticates and again when it terminates without the count going wrong.
   */
  public static class Ticket {
    private final PreAuthConnectionGate gate;
    private final AtomicBoolean         released = new AtomicBoolean();

    private Ticket(final PreAuthConnectionGate gate) {
      this.gate = gate;
    }

    public void release() {
      if (released.compareAndSet(false, true) && gate.permits != null)
        gate.permits.release();
    }
  }

  public PreAuthConnectionGate(final String protocol) {
    this(protocol, GlobalConfiguration.NETWORK_MAX_PREAUTH_CONNECTIONS.getValueAsInteger());
  }

  public PreAuthConnectionGate(final String protocol, final int maxConnections) {
    this.protocol = protocol;
    this.maxConnections = maxConnections;
    this.permits = maxConnections > 0 ? new Semaphore(maxConnections) : null;
  }

  /**
   * Takes a permit for a freshly accepted connection, or returns null when the listener is already holding
   * as many unauthenticated connections as it is allowed to. A null answer means the caller must close the
   * socket and carry on accepting.
   */
  public Ticket accept() {
    if (permits == null)
      return new Ticket(this);

    if (!permits.tryAcquire()) {
      refused.incrementAndGet();
      Metrics.counter(protocol.toLowerCase() + ".connection.refused").increment();
      return null;
    }

    return new Ticket(this);
  }

  /**
   * Logs a refusal, at most once per {@link #LOG_INTERVAL_MS}: the message is worth having, but under the
   * flood it describes it would otherwise be written thousands of times a second.
   */
  public void logRefusal(final Object source, final Object remoteAddress) {
    final long now = System.currentTimeMillis();
    final long last = lastLogTime.get();
    if (now - last < LOG_INTERVAL_MS && last != 0)
      return;
    if (!lastLogTime.compareAndSet(last, now))
      return;

    LogManager.instance().log(source, Level.WARNING,
        "%s: refused connection from %s, already holding the maximum of %d connections that have not authenticated "
            + "(%d refused so far; raise %s to allow more)", protocol, remoteAddress, maxConnections, refused.get(),
        GlobalConfiguration.NETWORK_MAX_PREAUTH_CONNECTIONS.getKey());
  }

  /** Connections currently accepted but not yet authenticated. */
  public int getPending() {
    return permits == null ? 0 : maxConnections - permits.availablePermits();
  }

  /** How many connections have been refused because the cap was already reached. */
  public long getRefused() {
    return refused.get();
  }

  public int getMaxConnections() {
    return maxConnections;
  }
}
