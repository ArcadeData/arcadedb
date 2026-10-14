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

import com.arcadedb.query.RunningQuery;
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
  private static final ThreadLocal<ServerSecurityUser>  CURRENT = new ThreadLocal<>();
  private static final ThreadLocal<RunningQuery.Client> CLIENT  = new ThreadLocal<>();

  private GremlinAuthContext() {
  }

  /**
   * Publishes the connection a request arrived on (channel id and peer), as {@code list queries} names it beside the user
   * (issue #9962): a Gremlin Server typically runs with one configured credential, so the user alone does not tell two
   * clients apart. {@code null} clears it. The one holder of it on both threads: the pipeline publishes it on the Netty
   * thread around the read that submits the evaluation (see {@link ConcealingWebSocketChannelizer}), since TinkerPop's
   * {@code RequestMessage} carries no channel, and {@link GremlinPrincipalPropagatingExecutorService} carries it to the
   * pool thread that evaluates the request, where {@link ArcadeGraphManager} reads it.
   */
  static void setClient(final RunningQuery.Client client) {
    if (client != null)
      CLIENT.set(client);
    else
      CLIENT.remove();
  }

  static RunningQuery.Client getClient() {
    return CLIENT.get();
  }

  /** A read of the pipeline, which may throw whatever the handlers after it throw. */
  @FunctionalInterface
  interface Read {
    void run() throws Exception;
  }

  /**
   * Runs {@code read} with {@code client} published, and clears it afterwards whatever the read does: the Netty thread
   * goes on to serve other channels, which must not inherit it.
   */
  static void publishing(final RunningQuery.Client client, final Read read) throws Exception {
    setClient(client);
    try {
      read.run();
    } finally {
      setClient(null);
    }
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
