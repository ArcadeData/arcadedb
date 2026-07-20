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
    delegate.execute(() -> {
      final List<DatabaseInternal> bound = user != null ? bindPrincipal(user) : Collections.emptyList();
      try {
        command.run();
      } finally {
        unbindPrincipal(bound);
      }
    });
  }

  private List<DatabaseInternal> bindPrincipal(final ServerSecurityUser user) {
    final List<DatabaseInternal> bound = new ArrayList<>();
    // getDatabaseNames() returns only already-open databases, so this never opens dormant ones.
    for (final String databaseName : server.getDatabaseNames()) {
      try {
        final DatabaseInternal database = (DatabaseInternal) server.getDatabase(databaseName);
        DatabaseContext.INSTANCE.init(database).setCurrentUser(user.getDatabaseUser(database));
        bound.add(database);
      } catch (final Exception e) {
        // A database may have been dropped/closed concurrently; skip it - a missing binding cannot widen
        // access because the traversal can only touch databases that are open.
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
