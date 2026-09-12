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
 * <p>
 * The message carries the peers that withheld the answer and why each is unknown, because that is the only
 * actionable half - "the cluster is not ready" sends an operator nowhere, "peer arcadedb2 answered 404 to the
 * capability route" sends them to the node that has not finished upgrading.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ClusterCapabilityNotReadyException extends ServerControlPlane.OperationNotAvailableException {
  public ClusterCapabilityNotReadyException(final String message) {
    super(message);
  }
}
