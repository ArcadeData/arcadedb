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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.HAReplicatedDatabase;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionException;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;

import java.util.Deque;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public abstract class DatabaseAbstractHandler extends AbstractServerHttpHandler {
  private static final HttpString SESSION_ID_HEADER = new HttpString(HttpSessionManager.ARCADEDB_SESSION_ID);

  protected DatabaseAbstractHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  protected abstract ExecutionResponse execute(HttpServerExchange exchange, ServerSecurityUser user, Database database,
      JSONObject payload) throws Exception;

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload)
      throws Exception {
    final DatabaseInternal database;
    HttpSession activeSession = null;
    boolean atomicTransaction = false;

    DatabaseContext.DatabaseContextTL current;

    if (requiresDatabase()) {
      final Deque<String> databaseName = exchange.getQueryParameters().get("database");
      if (databaseName.isEmpty())
        return new ExecutionResponse(400, "{ \"error\" : \"Database parameter is null\"}");

      if (user != null && !user.canAccessToDatabase(databaseName.getFirst()))
        throw new SecurityException(
            "User '" + user.getName() + "' is not allowed to access database '" + databaseName.getFirst() + "'");

      database = httpServer.getServer().getDatabase(databaseName.getFirst(), false, false);

      current = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
      if (current != null && !current.transactions.isEmpty() && current.transactions.get(0).isActive()) {
        LogManager.instance().log(this, Level.WARNING, "Found a pending transaction from a previous operation. Rolling it back...");
        cleanTL(database, current);
      }

      activeSession = setTransactionInThreadLocal(exchange, database, user);

      current = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
      if (current == null)
        // INITIALIZE THE DATABASE CONTEXT
        current = DatabaseContext.INSTANCE.init(database);

      final SecurityDatabaseUser currentUser = current.getCurrentUser();
      if (currentUser == null || !currentUser.equals(user.getDatabaseUser(database)))
        current.setCurrentUser(user != null ? user.getDatabaseUser(database) : null);

      // A session request attaches its QuerySession in setTransactionInThreadLocal above. A non-session
      // request must never inherit a stale session from a thread-context reused on this pooled thread,
      // which would leak another user's SESSION SET parameters into this request.
      if (activeSession == null)
        current.setQuerySession(null);

      // Warn if autoCommit parameter conflicts with session
      if (activeSession != null && payload != null && payload.has("autoCommit")) {
        final boolean explicitAutoCommit = payload.getBoolean("autoCommit");
        if (explicitAutoCommit) {
          LogManager.instance().log(this, Level.WARNING,
            "autoCommit parameter 'true' ignored: session transaction is already active (session ID: %s)",
            activeSession.id);
        }
      }

      final Boolean explicitAutoCommit = extractAutoCommitParameter(payload);
      atomicTransaction = determineAtomicTransaction(explicitAutoCommit, activeSession);

    } else
      database = null;

    final int retries = payload != null && !payload.isNull("retries") ? payload.getInt("retries") : 1;

    // Resolve HA database for read consistency (may be wrapped inside ServerDatabase).
    final HAReplicatedDatabase haDbForRead = resolveHAReplicatedDatabase(database);

    final AtomicReference<ExecutionResponse> response = new AtomicReference<>();
    try {
      // Set read consistency context for HA follower reads.
      // Must be inside the try block so the finally always clears the ThreadLocal.
      if (haDbForRead != null) {
        final HeaderValues readConsistencyHeader = exchange.getRequestHeaders().get("X-ArcadeDB-Read-Consistency");
        // X-ArcadeDB-Read-After is the canonical request-side bookmark header (since v26.4.1).
        // X-ArcadeDB-Commit-Index is accepted only as a legacy fallback; it is the response echo header.
        HeaderValues bookmarkHeader = exchange.getRequestHeaders().get("X-ArcadeDB-Read-After");
        if (bookmarkHeader == null || bookmarkHeader.isEmpty())
          bookmarkHeader = exchange.getRequestHeaders().get("X-ArcadeDB-Commit-Index");

        final String consistencyStr = readConsistencyHeader != null && !readConsistencyHeader.isEmpty()
            ? readConsistencyHeader.getFirst()
            : database.getConfiguration().getValueAsString(GlobalConfiguration.HA_READ_CONSISTENCY);

        final long bookmarkIndex;
        try {
          bookmarkIndex = parseReadBookmark(bookmarkHeader != null && !bookmarkHeader.isEmpty() ? bookmarkHeader.getFirst() : null);
        } catch (final IllegalArgumentException e) {
          // Malformed client-supplied bookmark header: surface as HTTP 400 instead of a 500. The raw value is
          // not echoed back to avoid reflecting hostile input.
          return new ExecutionResponse(400, "{ \"error\" : \"Invalid read-consistency bookmark header\" }");
        }

        try {
          final Database.READ_CONSISTENCY consistency = Database.READ_CONSISTENCY.valueOf(consistencyStr.toUpperCase(Locale.ROOT));
          haDbForRead.setReadConsistencyContext(consistency, bookmarkIndex);
        } catch (final IllegalArgumentException ignored) {
          // Invalid consistency level, skip
        }
      }
      boolean finalAtomicTransaction = atomicTransaction;
      if (activeSession != null) {
        // EXECUTE THE CODE LOCKING THE CURRENT SESSION. THIS AVOIDS USING THE SAME SESSION FROM MULTIPLE THREADS AT THE SAME TIME
        activeSession.execute(user, () -> {
          if (finalAtomicTransaction)
            executeInTransaction(exchange, user, database, payload, response, retries);
          else
            response.set(execute(exchange, user, database, payload));
          return null;
        });
      } else {
        if (finalAtomicTransaction)
          executeInTransaction(exchange, user, database, payload, response, retries);
        else
          response.set(execute(exchange, user, database, payload));
      }

      if (database != null && atomicTransaction && database.isTransactionActive())
        // STARTED ATOMIC TRANSACTION, COMMIT
        database.commit();

      // Emit bookmark header for read-your-writes consistency
      emitCommitIndexBookmark(exchange, haDbForRead);

    } finally {
      // Clear read consistency context
      if (haDbForRead != null)
        haDbForRead.clearReadConsistencyContext();

      if (activeSession != null)
        // DETACH CURRENT CONTEXT/TRANSACTIONS FROM CURRENT THREAD
        DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
      else if (database != null) {
        try {
          if (!atomicTransaction)
            // NO TRANSACTION, ROLLBACK TO MAKE SURE ANY PENDING OPERATION IS REMOVED
            database.rollbackAllNested();
        } finally {
          // DO NOT CLEAN THE CURRENT SESSION BECAUSE IT COULD HAVE AN OPEN TX
          cleanTL(database, null);
        }
      }
    }

    return response.get();
  }

  /**
   * Runs {@link #execute(HttpServerExchange, ServerSecurityUser, Database, JSONObject)} inside the auto-commit
   * transaction, retrying it up to {@code retries} times.
   * <p>
   * A {@link RuntimeException} is rethrown <b>unchanged</b>. Wrapping it - which this block used to do for every
   * exception alike - broke the two contracts the wrapper itself depends on (issue #6201):
   * <ul>
   * <li>{@code LocalDatabase.transaction(...)} decides what to retry by type
   * ({@code catch (NeedRetryException | DuplicatedKeyException)}). A conflict raised while {@code execute()} runs -
   * a {@code save()} losing an MVCC race, an index update hitting a concurrent key - arrived there already wrapped,
   * matched neither arm and propagated on the first attempt, so {@code retries} had no effect on anything but a
   * conflict detected by the wrapper's own {@code commit()}.</li>
   * <li>The HTTP error mapping in {@link AbstractServerHttpHandler} keys on the exception type, so a retryable
   * conflict was answered as an opaque 500 "Error on transaction commit" rather than the documented 503, and a
   * client whose retry policy keys on 503 gave up on a write that would have succeeded on retry.</li>
   * </ul>
   * Only a CHECKED exception still needs the wrapper, because {@code TransactionScope.execute()} declares none;
   * every ArcadeDB exception is unchecked and therefore reaches both dispatchers as itself.
   * <p>
   * Package-private for direct unit testing.
   */
  void executeInTransaction(final HttpServerExchange exchange, final ServerSecurityUser user,
      final DatabaseInternal database, final JSONObject payload, final AtomicReference<ExecutionResponse> response,
      final int retries) {
    database.transaction(() -> {
      try {
        response.set(execute(exchange, user, database, payload));
      } catch (final RuntimeException e) {
        throw e;
      } catch (final Exception e) {
        throw new TransactionException("Error on executing command", e);
      }
    }, false, retries);
  }

  /**
   * Parses a client-supplied HA read-consistency bookmark header (X-ArcadeDB-Read-After or the legacy
   * X-ArcadeDB-Commit-Index). Returns {@code -1} when the header is absent/blank, and throws
   * {@link IllegalArgumentException} on a non-numeric value so the caller can answer HTTP 400 rather than
   * letting a raw {@link NumberFormatException} bubble up to a 500. Package-private for direct unit testing.
   */
  static long parseReadBookmark(final String raw) {
    if (raw == null || raw.isBlank())
      return -1;
    try {
      return Long.parseLong(raw.trim());
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Invalid read-consistency bookmark header value");
    }
  }

  private void cleanTL(final Database database, DatabaseContext.DatabaseContextTL current) {
    if (current == null)
      current = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());

    if (current != null) {
      TransactionContext tx;
      while ((tx = current.popIfNotLastTransaction()) != null) {
        if (tx.isActive())
          tx.rollback();
        else
          break;
      }

      DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
    }
  }

  protected boolean requiresDatabase() {
    return true;
  }

  protected boolean requiresTransaction() {
    return true;
  }

  /**
   * Extracts the autoCommit parameter from the request payload.
   *
   * @param payload The request payload (may be null)
   * @return null if not specified, true to force atomic transaction, false to disable transaction
   */
  protected Boolean extractAutoCommitParameter(final JSONObject payload) {
    if (payload == null)
      return null;

    if (payload.has("autoCommit"))
      return payload.getBoolean("autoCommit");

    return null;
  }

  /**
   * Determines whether to use an atomic transaction based on the explicit parameter,
   * session state, and handler's default transaction requirement.
   *
   * @param explicitAutoCommit null (auto), true (force), or false (disable)
   * @param activeSession The active session if any
   * @return true if atomic transaction should be used
   */
  protected boolean determineAtomicTransaction(final Boolean explicitAutoCommit, final HttpSession activeSession) {
    // If there's an active session, never use atomic transaction (session manages the transaction)
    if (activeSession != null)
      return false;

    // Explicit parameter takes precedence
    if (explicitAutoCommit != null)
      return explicitAutoCommit;

    // Fall back to handler's default behavior
    return requiresTransaction();
  }

  /**
   * Unregisters the server-side session referenced by the request's session-id header, if any - but only when
   * it is owned by {@code user}. Called by the {@code /commit} and {@code /rollback} endpoints so a stale
   * session id is no longer resolvable. The ownership gate (via {@link HttpSessionManager#getSessionById})
   * ensures a request carrying another principal's session id cannot evict (and orphan) that session; it is
   * also a no-op when the id is already gone (e.g. an idempotent retry).
   */
  protected void removeSession(final HttpServerExchange exchange, final ServerSecurityUser user) {
    final HeaderValues sessionId = exchange.getRequestHeaders().get(HttpSessionManager.ARCADEDB_SESSION_ID);
    if (sessionId != null && !sessionId.isEmpty()) {
      final HttpSession session = httpServer.getSessionManager().getSessionById(user, sessionId.getFirst());
      if (session != null)
        httpServer.getSessionManager().removeSession(session.id);
    }
  }

  protected HttpSession setTransactionInThreadLocal(final HttpServerExchange exchange, final Database database,
      final ServerSecurityUser user) {
    final HeaderValues sessionId = exchange.getRequestHeaders().get(HttpSessionManager.ARCADEDB_SESSION_ID);
    if (sessionId != null && !sessionId.isEmpty()) {
      // LOOK UP FOR THE SESSION ID
      final HttpSession session = httpServer.getSessionManager().getSessionById(user, sessionId.get(0));
      if (session == null) {
        // The session id is not resolvable (committed/rolled back, expired, or owned by another principal).
        // A write-capable handler MUST reject it: falling through session-less would run the command in an
        // implicit auto-committing transaction while the client believes it is inside a transaction it can
        // still roll back. Read-only handlers (GET /query) and the transaction endpoints
        // (/begin, /commit, /rollback) instead degrade to a session-less request, keeping read-after-commit
        // and idempotent retries of commit/rollback working.
        if (requiresTransaction())
          throw new HttpSessionException("Remote transaction '" + sessionId.getFirst() + "' not found or expired");

        return null;
      }

      // FORCE THE RESET OF TL
      final DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.init((DatabaseInternal) database,
          session.transaction);
      // Attach the session to the thread context so the engine can reach it for GQL SESSION statements and
      // session-parameter merging.
      ctx.setQuerySession(session);
      exchange.getResponseHeaders().put(SESSION_ID_HEADER, session.id);

      return session;
    }
    return null;
  }
}
