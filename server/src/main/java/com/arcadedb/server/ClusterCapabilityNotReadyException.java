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
package com.arcadedb.server;

import java.util.List;

/**
 * An operation was refused because a member of the cluster has not proved it can decode the replicated entry the
 * operation would be written as (issue #7511).
 * <p>
 * Raised by the HA plugin before anything is submitted: the point of the refusal is that nothing reaches the Raft
 * log, because a committed entry a peer cannot decode halts that peer rather than being skipped (the #4798 rule,
 * enforced in {@code ArcadeStateMachine}). The caller's state is therefore untouched and the request can simply be
 * reissued once the lagging node is upgraded or reachable again.
 * <p>
 * <b>Its parent is what gives it a status on both transports.</b> {@code ServerControlPlane.OperationNotAvailableException}
 * already maps to gRPC {@code FAILED_PRECONDITION}, which is exactly what this is - a precondition of the cluster,
 * not a fault of the request - and {@code AbstractServerHttpHandler} answers this subtype HTTP {@code 409 Conflict}.
 * Not a 5xx: the request is well formed and authorized, and a client or load balancer must not read it as a server
 * fault worth retrying blindly. Not a 400 either: nothing about the request needs changing.
 *
 * <h2>Why the peers are carried as fields and not only in the message</h2>
 *
 * The peers that withheld the answer are the only actionable half - "the cluster is not ready" sends an operator
 * nowhere, "peer arcadedb2 has not advertised security-groups-entry" sends them to the node that has not finished
 * upgrading. In {@code production} mode {@code AbstractServerHttpHandler.buildErrorBody} conceals {@code detail},
 * where the message goes, so a refusal that carried its peers only in prose arrived at the client as a bare
 * "Cluster is not ready for this operation" (PR #7555 review). {@link #toExceptionArgs()} is the bounded,
 * non-sensitive rendering that rides {@code exceptionArgs} instead, which every mode emits - the same split
 * {@code ResultSetTooLargeException} makes between the sentence and the number a caller has to stay under.
 * <p>
 * Bounded, and deliberately NOT carrying the per-peer reasons. A reason is free-form text built from a probe
 * failure and can contain a host, a port or a JDK exception message; that is precisely the class of content
 * {@code detail} exists to conceal outside development. The peer ids are cluster member names the caller - who has
 * just passed a root-only check - can already read from {@code GET /api/v1/cluster}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ClusterCapabilityNotReadyException extends ServerControlPlane.OperationNotAvailableException {

  /**
   * How many peer names {@link #toExceptionArgs()} spells out before summarising the rest. The field is a wire
   * contract read by clients, so it stays bounded however large the cluster is; five is enough to act on, and an
   * operator who needs the sixth reads the log or {@code GET /api/v1/cluster}.
   */
  private static final int MAX_PEERS_IN_ARGS = 5;

  private final String       capability;
  private final List<String> missingPeers;

  /**
   * @param message      the full refusal, including each peer's reason. Goes to {@code detail}, which production
   *                     mode conceals.
   * @param capability   the capability token the peers did not advertise.
   * @param missingPeers the peers that did not advertise it, in configuration order. Copied and frozen.
   */
  public ClusterCapabilityNotReadyException(final String message, final String capability,
      final List<String> missingPeers) {
    super(message);
    this.capability = capability;
    this.missingPeers = missingPeers != null ? List.copyOf(missingPeers) : List.of();
  }

  /** The capability token the peers of {@link #getMissingPeers()} have not advertised. */
  public String getCapability() {
    return capability;
  }

  /** The peers that have not advertised {@link #getCapability()}, in configuration order. Never {@code null}. */
  public List<String> getMissingPeers() {
    return missingPeers;
  }

  /**
   * The refusal's actionable half as the {@code exceptionArgs} wire field: {@code <capability>|peer,peer[,+N more]}.
   * <p>
   * Pipe-separated like {@code DuplicatedKeyException}'s, so a client that already splits that field needs no new
   * parsing rule, and bounded by {@link #MAX_PEERS_IN_ARGS} so a large cluster cannot turn an error body into a
   * peer dump.
   */
  public String toExceptionArgs() {
    final StringBuilder args = new StringBuilder(64);
    if (capability != null)
      args.append(capability);
    args.append('|');
    for (int i = 0; i < missingPeers.size() && i < MAX_PEERS_IN_ARGS; i++) {
      if (i > 0)
        args.append(',');
      args.append(missingPeers.get(i));
    }
    if (missingPeers.size() > MAX_PEERS_IN_ARGS)
      args.append(",+").append(missingPeers.size() - MAX_PEERS_IN_ARGS).append(" more");
    return args.toString();
  }
}
