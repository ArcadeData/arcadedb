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
package com.arcadedb.server.grpc;

import io.grpc.Metadata;

/**
 * Trailers a follower puts on an RPC it refuses because the work may only run on the cluster leader, so a caller
 * can redirect itself instead of parsing prose out of the error description (issue #6091). They live in the module
 * that owns the protocol definition rather than on either side of it, so the server writing them and the client
 * reading them cannot drift apart.
 * <p>
 * The gRPC address is the one to dial for the RPC that was just refused; it is present only when the cluster can
 * resolve it, which needs either a {@code grpc:} field in the object form of {@code arcadedb.ha.serverList} or a
 * homogeneous deployment where the derive-from-local-port fallback is correct. The HTTP address is always
 * resolvable and is kept as the fallback a human reads - it is not an address this RPC can be retried on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class LeaderRedirectProtocol {

  /** The leader's client-reachable gRPC address ({@code host:port}), when the cluster can resolve one. */
  public static final Metadata.Key<String> LEADER_GRPC_ADDRESS = Metadata.Key.of("arcadedb-leader-grpc-address",
      Metadata.ASCII_STRING_MARSHALLER);

  /** The leader's HTTP address ({@code host:port}). Always known when a leader is; never dialable over gRPC. */
  public static final Metadata.Key<String> LEADER_HTTP_ADDRESS = Metadata.Key.of("arcadedb-leader-http-address",
      Metadata.ASCII_STRING_MARSHALLER);

  private LeaderRedirectProtocol() {
  }
}
