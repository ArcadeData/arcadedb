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

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The Gremlin server executes traversals on a shared thread pool, decoupled from the Netty thread that
 * authenticated the request, and it never hands the authenticated principal to the graph layer. Without
 * a bound principal, the engine's permission gates ({@code checkPermissionsOnDatabase} /
 * {@code checkPermissionsOnFile}) early-return "allow", so per-database and per-type ACLs are silently
 * disabled for the Gremlin wire protocol (GHSA-c287-v325-j5jx).
 * <p>
 * This wrapper is installed as the Gremlin execution pool (the single {@code ExecutorService} used by
 * both the bytecode and the script/eval paths). At submit time - still on the Netty request thread - it
 * captures the principal published by {@link ArcadeGremlinAuthorizer} in {@link GremlinAuthContext}; on
 * the worker thread it binds that principal into {@link DatabaseContext} for every open database before
 * the task runs and clears it afterwards, so the engine enforces the same per-user ACLs it enforces for
 * the HTTP and BOLT transports. Binding the deny-all sentinel returned by
 * {@code getDatabaseUser(unauthorized-db)} also backstops the alias-based check for script requests that
 * reference a database directly through a global binding.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GremlinPrincipalPropagatingExecutorService extends AbstractExecutorService {
  static final String GREMLIN_PROTOCOL = "gremlin";

  /** The principal of the request a pool thread runs, and the databases it is bound on: one per thread, as one request is. */
  private record RequestPrincipal(ServerSecurityUser user, List<DatabaseInternal> bound) {
  }

  private static final ThreadLocal<RequestPrincipal> REQUEST = new ThreadLocal<>();

  private final ExecutorService delegate;
  private final ArcadeDBServer  server;

  public GremlinPrincipalPropagatingExecutorService(final ExecutorService delegate, final ArcadeDBServer server) {
    this.delegate = delegate;
    this.server = server;
  }

  @Override
  public void execute(final Runnable command) {
    // Captured on the Netty request thread, where ArcadeGremlinAuthorizer has just published the user.
    final ServerSecurityUser user = GremlinAuthContext.get();
    // The connection, published on the same thread by the channelizer for the request being read (issue #9962)
    final RunningQuery.Client client = GremlinAuthContext.getClient();
    delegate.execute(() -> {
      final List<DatabaseInternal> bound = user != null ? bindPrincipal(user) : Collections.emptyList();
      if (user != null)
        REQUEST.set(new RequestPrincipal(user, bound));
      // Tagged as the client request it is (issue #8363): under HA, RaftReplicatedDatabase refuses a CLIENT request
      // on a database whose directory is being replaced from the leader's snapshot and tells a client from the
      // engine by this tag, so an untagged traversal read as engine work and was served from the discarded copy.
      // Cleared afterwards because the pool's threads are reused.
      ProtocolContext.set(GREMLIN_PROTOCOL);
      // Who the registry entry of the request belongs to, for its listing and its visibility (issue #9689)
      ArcadeGraphManager.setRequestUser(user != null ? user.getName() : null);
      // The one holder of the client on both threads: what was there before is put back, so a pool that ever ran the task on
      // the submitting Netty thread would not take the client away from the rest of that read
      final RunningQuery.Client previousClient = GremlinAuthContext.getClient();
      GremlinAuthContext.setClient(client);
      try {
        command.run();
      } finally {
        ArcadeGraphManager.setRequestUser(null);
        GremlinAuthContext.setClient(previousClient);
        // A request whose evaluation ended without reporting its end to ArcadeGraphManager must not keep its admission
        // slot (issue #9518): the pool's threads are reused, and a slot left on one would also be taken for the slot of an
        // enclosing query by the next request it runs
        ArcadeGraphManager.releaseAdmission();
        ProtocolContext.clear();
        REQUEST.remove();
        unbindPrincipal(bound);
      }
    });
  }

  /**
   * Binds the principal of the request this thread runs onto {@code database}, which the request has just opened (issue #11068):
   * {@link #bindPrincipal} binds the databases open when the request starts, and a traversal source naming a database that is
   * not open - closed for idleness, or not opened at startup - opens it while the request runs. Without this the traversal would
   * run on it with no user bound, which the engine's permission gates read as "no check". A no-op outside a request, or for a
   * database already bound; unbound with the others when the request ends.
   */
  static void bindRequestPrincipal(final DatabaseInternal database) {
    final RequestPrincipal request = REQUEST.get();
    if (request == null)
      return;
    for (final DatabaseInternal bound : request.bound)
      if (bound == database)
        return;
    DatabaseContext.INSTANCE.init(database).setCurrentUser(request.user.getDatabaseUser(database));
    request.bound.add(database);
  }

  private List<DatabaseInternal> bindPrincipal(final ServerSecurityUser user) {
    final List<DatabaseInternal> bound = new ArrayList<>();
    // getDatabaseNames() returns only already-open databases, so this never opens dormant ones: one the request opens is bound
    // when it is opened (bindRequestPrincipal, issue #11068)
    for (final String databaseName : server.getDatabaseNames()) {
      try {
        final DatabaseInternal database = (DatabaseInternal) server.getDatabaseForClient(databaseName);
        // LOAD-BEARING: the engine's permission gates (GremlinHostAccessGuard, checkPermissionsOnDatabase) allow a thread
        // with no bound user, so any other pool that runs Gremlin must bind the principal exactly like this.
        DatabaseContext.INSTANCE.init(database).setCurrentUser(user.getDatabaseUser(database));
        bound.add(database);
      } catch (final Exception e) {
        // A database may have been dropped/closed concurrently; skip it - a missing binding cannot widen
        // access: a traversal that opens a database binds the principal on it (bindRequestPrincipal)
      }
    }
    return bound;
  }

  private void unbindPrincipal(final List<DatabaseInternal> bound) {
    for (final DatabaseInternal database : bound) {
      final DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
      if (ctx != null)
        ctx.setCurrentUser(null);
    }
  }

  @Override
  public void shutdown() {
    delegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
    return delegate.awaitTermination(timeout, unit);
  }
}
