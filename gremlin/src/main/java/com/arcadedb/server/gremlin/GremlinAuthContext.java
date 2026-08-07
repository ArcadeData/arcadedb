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

  private GremlinAuthContext() {
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
