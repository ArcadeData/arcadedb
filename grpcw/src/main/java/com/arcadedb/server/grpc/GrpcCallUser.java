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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * The user a long-lived unit of work of a gRPC call - an open client transaction, a client stream - was authenticated as.
 * <p>
 * The interceptor authenticates a call once, when it starts, and a unit of work that outlives the request that began it
 * (a transaction whose statements run on its own thread, a stream whose chunks arrive for as long as the client sends)
 * holds what that start resolved. {@code ServerSecurity.updateUser} publishes a NEW user object for every change, so what
 * was held is a snapshot: the grants of the user at that moment, and a name that may no longer exist. {@link #live()}
 * resolves the user as it stands now, the way the other wire protocols do for each request
 * ({@code ServerSecurity.revalidate}): a user that was dropped, or whose password changed since, is refused, and anything
 * else answers the current object, whose database grants are the ones in force.
 * <p>
 * One lookup in the users map per call: a unit of work asks it before each statement or chunk, not per row.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class GrpcCallUser {
  private final ServerSecurity     security;
  private final ServerSecurityUser held;

  private GrpcCallUser(final ServerSecurity security, final ServerSecurityUser held) {
    this.security = security;
    this.held = held;
  }

  /**
   * The user {@code userName} authenticates as on {@code server}, or {@code null} when the server authenticates nobody
   * (no users configured, or no server): there is nothing to re-check then.
   *
   * @throws NoLongerValid when the server has users and none is named {@code userName}
   */
  static GrpcCallUser of(final ArcadeDBServer server, final String userName) {
    final ServerSecurity security = server != null ? server.getSecurity() : null;
    if (security == null || userName == null || security.getUsers() == null || security.getUsers().isEmpty())
      return null;

    final ServerSecurityUser user = security.getUser(userName);
    if (user == null)
      throw new NoLongerValid();
    return new GrpcCallUser(security, user);
  }

  /**
   * The user as it stands now.
   *
   * @throws NoLongerValid when the user was dropped, or its password changed, since the work began
   */
  ServerSecurityUser live() {
    try {
      return security.revalidate(held);
    } catch (final ServerSecurityException e) {
      throw new NoLongerValid();
    }
  }

  /** The user's rights on {@code db} as they stand now. */
  SecurityDatabaseUser databaseUser(final Database db) {
    return live().getDatabaseUser(db);
  }

  /**
   * The user's rights on {@code db} as they stand now, applied to the calling thread when it holds a context for the
   * database: the engine's permission checks read the user the thread's context carries, which is the one the call bound on
   * the thread it started on.
   *
   * @return the rights, whether the thread held a context for them or not
   *
   * @throws NoLongerValid when the user was dropped, or its password changed, since the work began
   */
  SecurityDatabaseUser bindTo(final Database db) {
    final SecurityDatabaseUser current = databaseUser(db);
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(db.getDatabasePath());
    if (context != null)
      context.setCurrentUser(current);
    return current;
  }

  /** Whether {@link #live()} would answer. */
  boolean isValid() {
    try {
      live();
      return true;
    } catch (final NoLongerValid e) {
      return false;
    }
  }

  /**
   * The refusal of a unit of work whose user can no longer act. A type of its own, so a handler can tell it from any other
   * refusal and end the work where a failed statement would only be reported.
   */
  static final class NoLongerValid extends StatusRuntimeException {
    NoLongerValid() {
      super(Status.UNAUTHENTICATED.withDescription("User/Password not valid"));
    }
  }
}
