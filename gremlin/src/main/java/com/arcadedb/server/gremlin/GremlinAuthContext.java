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
package com.arcadedb.server.gremlin;

import com.arcadedb.server.security.ServerSecurityUser;

/**
 * Carries the authenticated Gremlin principal from the Netty request thread (where authentication and
 * authorization run) to the Gremlin execution-pool thread (where the traversal actually touches the
 * database). {@link ArcadeGremlinAuthorizer} publishes the resolved user here; the
 * {@link GremlinPrincipalPropagatingExecutorService} captures it at submit time and re-binds it on the
 * worker thread so the engine's per-database and per-type ACLs enforce for Gremlin callers
 * (GHSA-c287-v325-j5jx).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GremlinAuthContext {
  private static final ThreadLocal<ServerSecurityUser> CURRENT = new ThreadLocal<>();
  private static final ThreadLocal<Client>             CLIENT  = new ThreadLocal<>();

  /**
   * The connection a request arrived on, as {@code list queries} names it beside the user (issue #9962): a Gremlin Server
   * typically runs with one configured credential, so the user alone does not tell two clients apart.
   *
   * @param connectionId the Netty channel id
   * @param address      the peer address of the channel
   */
  public record Client(String connectionId, String address) {
  }

  private GremlinAuthContext() {
  }

  /**
   * Publishes the connection of the request being read on this Netty thread, for the execution pool to capture at submit
   * time like the user; {@code null} clears it. TinkerPop's {@code RequestMessage} carries no channel, so it is published
   * by the pipeline (see {@link ConcealingWebSocketChannelizer}), around the read that submits the evaluation.
   */
  static void setClient(final Client client) {
    if (client != null)
      CLIENT.set(client);
    else
      CLIENT.remove();
  }

  static Client getClient() {
    return CLIENT.get();
  }

  public static void set(final ServerSecurityUser user) {
    CURRENT.set(user);
  }

  public static ServerSecurityUser get() {
    return CURRENT.get();
  }

  public static void clear() {
    CURRENT.remove();
  }
}
