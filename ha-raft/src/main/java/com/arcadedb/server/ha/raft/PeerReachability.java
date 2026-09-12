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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The pre-flight reachability probe an add-peer request runs before asking Raft to change the
 * configuration (issue #7514).
 * <p>
 * Ratis does not commit a {@code Mode.ADD} until the new peer has caught up, so an address nothing is
 * listening on cannot succeed - it can only consume the membership-change retry budget and then report
 * the failure as a serialized {@code SetConfigurationRequest}. A TCP connect answers the one question
 * that decides it, in milliseconds - name resolution included, which is why the dial runs on a throwaway
 * daemon thread the caller stops waiting on (see {@link #bounded}).
 * <p>
 * <b>It is deliberately one-sided.</b> A refused or timed-out connection is proof that the membership
 * change cannot commit, and that is the only case this reports. A successful connection proves only that
 * something is listening on the port - not that it is an ArcadeDB server, not that it shares this
 * cluster's name or token, not that it is ready - so it reports nothing and the request proceeds exactly
 * as it did before. That asymmetry is what makes the probe safe to put in front of every entry point:
 * it can turn a slow failure into a fast one, and it cannot turn a success into a failure.
 * <p>
 * A Raft-level handshake (the {@code GroupInfo} RPC {@link KubernetesAutoJoin} uses for the mirror-image
 * problem of inserting THIS node into a remote group) would answer more of those questions. It also needs
 * a client built from the server's transport {@code Parameters} so it speaks TLS exactly when the cluster
 * does, which is a larger surface than the operator mistake this exists for - naming a server that is not
 * up yet.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
final class PeerReachability {

  private PeerReachability() {
  }

  /**
   * Why an add of {@code address} must be refused before any Raft configuration change is issued, or
   * {@code null} when it must not be. The whole decision, so that every branch of it is one call away
   * from a test rather than buried in a private method of a class that needs a live cluster to build.
   * <p>
   * Two of the three branches are skips, and both are deliberate:
   * <ul>
   *   <li>{@code alreadyAMember} - {@code RaftClusterManager.buildAddArgs} reads the same configuration
   *       and treats this case as a no-op success, which is what makes a repeated {@code connect cluster}
   *       idempotent. Probing here would refuse an add that does nothing, for any committed member that
   *       happens to be down;</li>
   *   <li>{@code probeTimeoutMs <= 0} - the operator's escape hatch
   *       ({@code arcadedb.ha.addPeerProbeTimeout=0}), which restores the behaviour that was there before
   *       the probe existed. It exists because the probe's one failure mode is a network on which a TCP
   *       handshake legitimately takes longer than the budget.</li>
   * </ul>
   */
  static String addRefusalReason(final boolean alreadyAMember, final long probeTimeoutMs, final String address) {
    if (alreadyAMember || probeTimeoutMs <= 0)
      return null;
    return unreachableReason(address, probeTimeoutMs);
  }

  /**
   * Why {@code address} could not be reached, or {@code null} when it could - and also {@code null} when
   * there is nothing to dial, so a caller cannot mistake "not probed" for "unreachable".
   * <p>
   * Not probed means: a blank address, or one whose trailing {@code :port} is absent or not a port number.
   * {@link RaftPeerAddressResolver} always produces {@code host:port}, so this is the defensive half of the
   * contract rather than a path an operator can reach through the server list.
   *
   * @param address   a Raft peer address in {@code host:port} form
   * @param timeoutMs how long to wait for the TCP handshake; values above {@link Integer#MAX_VALUE} are
   *                  clamped, and a non-positive value is the caller's to filter out before calling
   *
   * @return a short human-readable reason, or {@code null} when the peer answered or was not probed
   */
  static String unreachableReason(final String address, final long timeoutMs) {
    if (address == null)
      return null;

    final int colonIdx = address.lastIndexOf(':');
    if (colonIdx <= 0 || colonIdx == address.length() - 1)
      return null;

    final int port;
    try {
      port = Integer.parseInt(address.substring(colonIdx + 1));
    } catch (final NumberFormatException e) {
      return null;
    }
    if (port <= 0 || port > 65535)
      return null;

    final String host = address.substring(0, colonIdx);
    return bounded(host, port, timeoutMs);
  }

  /**
   * Runs {@link #dial} on a throwaway daemon thread and gives up on it after {@code timeoutMs}.
   * <p>
   * The wrapper is not ceremony, it is the only way the budget covers the whole probe. {@code new
   * InetSocketAddress(host, port)} resolves the name <b>eagerly, in its constructor</b>, before
   * {@code Socket.connect}'s timeout argument applies to anything - so a resolver that is slow or
   * unreachable blocks for however long the platform's resolver takes, and the setting would bound only
   * the TCP handshake after it. That would reintroduce, through DNS, the held-worker-thread problem issue
   * #7514 exists to remove.
   * <p>
   * A thread rather than a pool because this is an operator-triggered admin request (add peer, connect
   * cluster), not a hot path: there is nothing to amortise and a pool would add a lifecycle to own. The
   * abandoned thread is a daemon, so it cannot hold the JVM open; {@code cancel(true)} interrupts a
   * blocking connect, and a name resolution already in progress is NOT interruptible on the JDK's
   * resolver - that thread simply ends when the resolver gives up. What is guaranteed is what matters
   * here: the caller is released within the budget.
   */
  private static String bounded(final String host, final int port, final long timeoutMs) {
    final int connectTimeoutMs = (int) Math.min(timeoutMs, Integer.MAX_VALUE);
    final FutureTask<String> probe = new FutureTask<>(() -> dial(host, port, connectTimeoutMs));
    final Thread worker = new Thread(probe, "arcadedb-peer-probe-" + host + "-" + port);
    worker.setDaemon(true);
    worker.start();

    try {
      return probe.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (final TimeoutException e) {
      probe.cancel(true);
      return "no answer within " + timeoutMs + " ms, name resolution included";
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      probe.cancel(true);
      // Not "unreachable": this thread was interrupted (shutdown), which says nothing about the peer.
      // Reporting a reason here would refuse an add on the strength of our own shutdown.
      return null;
    } catch (final ExecutionException e) {
      // dial() converts every IOException into a reason, so reaching here means an unchecked failure -
      // report it rather than swallowing it into a false "reachable".
      final Throwable cause = e.getCause() != null ? e.getCause() : e;
      return cause.getMessage() != null && !cause.getMessage().isBlank() ? cause.getMessage() : cause.toString();
    }
  }

  /** The blocking half: resolve, connect, and turn any failure into a reason. Never throws. */
  private static String dial(final String host, final int port, final int connectTimeoutMs) {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
      return null;
    } catch (final SocketTimeoutException e) {
      // Distinguished from the refusal below because the two point an operator at different things: a
      // refusal means the host is up and the process is not, a timeout means the host or the route is not
      // answering at all (or the handshake is slower than the budget, which is what the setting is for).
      return "no answer within " + connectTimeoutMs + " ms";
    } catch (final IOException e) {
      return e.getMessage() != null && !e.getMessage().isBlank() ? e.getMessage() : e.toString();
    }
  }
}
