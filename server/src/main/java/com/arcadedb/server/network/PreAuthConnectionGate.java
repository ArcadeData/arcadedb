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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;

import io.micrometer.core.instrument.Metrics;

import java.io.IOException;
import java.net.Socket;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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
 * <p>
 * The gate also owns the two properties every pre-authentication phase must have whatever the protocol, so that a new
 * listener inherits them by taking a ticket rather than by re-implementing them in its codec:
 * <ul>
 * <li><b>A deadline</b> ({@link GlobalConfiguration#NETWORK_PREAUTH_TIMEOUT}), counted from the accept. The handshake read
 * timeout only closes a connection that sends nothing: one that sends a few bytes now and then - a command it is refused
 * for, a frame answered with an error - is never idle, and could hold its permit, and so a share of the cap, for ever:
 * enough of them lock every other client out of the listener. A ticket handed the socket ({@link #accept(Socket)}) is
 * closed by the reaper when the deadline passes and it has not been released: the blocked read on the connection's own
 * thread then fails, and the connection ends as one that hung up. The reaper is one JVM-wide daemon thread, created on
 * first use, that walks the outstanding tickets once a second; closing a plain socket never blocks, so no peer can stall
 * it, and a listener speaking TLS hands over the raw socket the TLS one is layered on.</li>
 * <li><b>A bounded log</b> ({@link Ticket#logProtocolError}): what an unauthenticated peer sends is under its control, so a
 * record per refused frame, or even per connection, lets it write the server log at the rate it can open connections, and
 * rotate the history out. At most one record per {@link #LOG_INTERVAL_MS} per listener, saying how many it held back.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PreAuthConnectionGate {
  /** How often a listener that is refusing connections says so, so that a flood cannot flood the log too. */
  private static final long LOG_INTERVAL_MS  = 10_000;
  /** How often the reaper walks the outstanding tickets: the granularity of the pre-authentication deadline. */
  private static final long REAPER_PERIOD_MS = 1_000;

  /** The tickets of every gate of the JVM that carry a deadline and are not released yet: what the reaper walks. */
  private static final Set<Ticket> TRACKED = ConcurrentHashMap.newKeySet();

  private final String     protocol;
  private final int        maxConnections;
  private final long       preAuthTimeoutMs;
  private final Semaphore  permits;
  private final AtomicLong refused             = new AtomicLong();
  private final AtomicLong lastLogTime         = new AtomicLong();
  private final AtomicLong expired             = new AtomicLong();
  private final AtomicLong lastExpiredLogTime  = new AtomicLong();
  private final AtomicLong protocolErrors      = new AtomicLong();
  private final AtomicLong suppressedErrors    = new AtomicLong();
  private final AtomicLong lastProtocolLogTime = new AtomicLong();

  /**
   * A held permit. Releasing it is idempotent, so the connection can hand it back at the point it
   * authenticates and again when it terminates without the count going wrong.
   */
  public static class Ticket {
    private final PreAuthConnectionGate gate;
    private final AtomicBoolean         released = new AtomicBoolean();
    private final Socket                socket;
    private final long                  deadlineNanos;

    private Ticket(final PreAuthConnectionGate gate, final Socket socket) {
      this.gate = gate;
      this.socket = socket;
      this.deadlineNanos = socket != null && gate.preAuthTimeoutMs > 0 ?
          System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(gate.preAuthTimeoutMs) :
          0L;
    }

    public void release() {
      if (released.compareAndSet(false, true)) {
        if (deadlineNanos != 0L)
          TRACKED.remove(this);
        if (gate.permits != null)
          gate.permits.release();
      }
    }

    /** Whether the connection is past the pre-authentication phase: authenticated once, or gone. */
    public boolean isReleased() {
      return released.get();
    }

    /**
     * Logs a protocol error of the connection holding this ticket: before it authenticates through the listener's bounded
     * log (see the class javadoc), after it as a WARNING, as any other error of an authenticated client. {@code message} is
     * a {@link String#format} pattern.
     */
    public void logProtocolError(final Object source, final String message, final Object... args) {
      if (released.get())
        LogManager.instance().log(source, Level.WARNING, message, args);
      else
        gate.logPreAuthProtocolError(source, message, args);
    }

    private void expire() {
      // release() decides the race with the connection authenticating at the same moment: only one of the two wins it
      if (!released.compareAndSet(false, true))
        return;
      TRACKED.remove(this);
      if (gate.permits != null)
        gate.permits.release();

      gate.expired.incrementAndGet();
      Metrics.counter(gate.protocol.toLowerCase(Locale.ROOT) + ".connection.preauth.expired").increment();
      gate.logExpiry(socket.getRemoteSocketAddress());
      try {
        socket.close();
      } catch (final IOException e) {
        // already gone
      }
    }
  }

  /** The JVM-wide reaper thread, created by the first ticket that carries a deadline (holder idiom). */
  private static final class Reaper {
    private static final ScheduledThreadPoolExecutor EXECUTOR = create();

    private static ScheduledThreadPoolExecutor create() {
      final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, r -> {
        final Thread t = new Thread(r, "ArcadeDB Pre-Auth Reaper");
        t.setDaemon(true);
        return t;
      });
      executor.scheduleWithFixedDelay(Reaper::reap, REAPER_PERIOD_MS, REAPER_PERIOD_MS, TimeUnit.MILLISECONDS);
      return executor;
    }

    private static void reap() {
      final long now = System.nanoTime();
      for (final Ticket ticket : TRACKED)
        if (now - ticket.deadlineNanos >= 0)
          try {
            ticket.expire();
          } catch (final RuntimeException e) {
            // never let one ticket end the periodic task, which would silently stop every deadline of the JVM
            LogManager.instance().log(PreAuthConnectionGate.class, Level.FINE, "Error on closing an expired connection", e);
          }
    }

    static void start() {
      // initializing the holder is what creates the thread, once
      EXECUTOR.getPoolSize();
    }
  }

  /**
   * The cap is SCOPE.SERVER, so it is read from the SERVER's own configuration and not off the
   * {@link GlobalConfiguration} enum, which only a system property or an environment variable ever writes: taking
   * it from the enum meant a server configuration file or a {@code SET SERVER SETTING} that raised or lowered the
   * cap was silently ignored, and every listener ran on the compiled-in default (issue #7233). The pre-authentication
   * deadline is read the same way.
   */
  public PreAuthConnectionGate(final String protocol, final ContextConfiguration configuration) {
    this(protocol, configuration.getValueAsInteger(GlobalConfiguration.NETWORK_MAX_PREAUTH_CONNECTIONS),
        configuration.getValueAsLong(GlobalConfiguration.NETWORK_PREAUTH_TIMEOUT));
  }

  public PreAuthConnectionGate(final String protocol, final int maxConnections) {
    this(protocol, maxConnections, 0L);
  }

  public PreAuthConnectionGate(final String protocol, final int maxConnections, final long preAuthTimeoutMs) {
    this.protocol = protocol;
    this.maxConnections = maxConnections;
    this.preAuthTimeoutMs = Math.max(preAuthTimeoutMs, 0L);
    this.permits = maxConnections > 0 ? new Semaphore(maxConnections) : null;
  }

  /**
   * Takes a permit for a freshly accepted connection, or returns null when the listener is already holding
   * as many unauthenticated connections as it is allowed to. A null answer means the caller must close the
   * socket and carry on accepting. The ticket carries no deadline: {@link #accept(Socket)} is the form a listener uses.
   */
  public Ticket accept() {
    return accept(null);
  }

  /**
   * Same as {@link #accept()}, for the accepted {@code socket}: when the gate has a pre-authentication deadline, the socket is
   * closed once it passes with the ticket still held.
   */
  public Ticket accept(final Socket socket) {
    if (permits != null && !permits.tryAcquire()) {
      refused.incrementAndGet();
      Metrics.counter(protocol.toLowerCase(Locale.ROOT) + ".connection.refused").increment();
      return null;
    }

    final Ticket ticket = new Ticket(this, socket);
    if (ticket.deadlineNanos != 0L) {
      Reaper.start();
      TRACKED.add(ticket);
    }
    return ticket;
  }

  /**
   * Logs a refusal, at most once per {@link #LOG_INTERVAL_MS}: the message is worth having, but under the
   * flood it describes it would otherwise be written thousands of times a second.
   */
  public void logRefusal(final Object source, final Object remoteAddress) {
    if (!acquireLogSlot(lastLogTime))
      return;

    LogManager.instance().log(source, Level.WARNING,
        "%s: refused connection from %s, already holding the maximum of %d connections that have not authenticated "
            + "(%d refused so far; raise %s to allow more)", protocol, remoteAddress, maxConnections, refused.get(),
        GlobalConfiguration.NETWORK_MAX_PREAUTH_CONNECTIONS.getKey());
  }

  /** Logs a connection closed for not authenticating in time, at most once per {@link #LOG_INTERVAL_MS}. */
  private void logExpiry(final Object remoteAddress) {
    if (!acquireLogSlot(lastExpiredLogTime))
      return;

    LogManager.instance().log(this, Level.WARNING,
        "%s: closed connection from %s, not authenticated within %d ms (%d closed so far; see %s)", protocol, remoteAddress,
        preAuthTimeoutMs, expired.get(), GlobalConfiguration.NETWORK_PREAUTH_TIMEOUT.getKey());
  }

  private void logPreAuthProtocolError(final Object source, final String message, final Object... args) {
    protocolErrors.incrementAndGet();
    if (!acquireLogSlot(lastProtocolLogTime)) {
      suppressedErrors.incrementAndGet();
      return;
    }

    final long held = suppressedErrors.getAndSet(0);
    final String text = args == null || args.length == 0 ? message : String.format(message, args);
    LogManager.instance().log(source, Level.WARNING, "%s (connection not authenticated; %d similar errors not logged since the last one)",
        text, held);
  }

  /** Whether the caller may write the record a throttled log is about, claiming the slot of this interval if so. */
  private static boolean acquireLogSlot(final AtomicLong lastTime) {
    final long now = System.currentTimeMillis();
    final long last = lastTime.get();
    if (last != 0 && now - last < LOG_INTERVAL_MS)
      return false;
    return lastTime.compareAndSet(last, now);
  }

  /** Connections currently accepted but not yet authenticated. */
  public int getPending() {
    return permits == null ? 0 : maxConnections - permits.availablePermits();
  }

  /** How many connections have been refused because the cap was already reached. */
  public long getRefused() {
    return refused.get();
  }

  /** How many connections were closed for not authenticating within the pre-authentication deadline. */
  public long getExpired() {
    return expired.get();
  }

  /** How many protocol errors connections that had not authenticated reported through {@link Ticket#logProtocolError}. */
  public long getPreAuthProtocolErrors() {
    return protocolErrors.get();
  }

  public long getPreAuthTimeoutMs() {
    return preAuthTimeoutMs;
  }

  public int getMaxConnections() {
    return maxConnections;
  }
}
