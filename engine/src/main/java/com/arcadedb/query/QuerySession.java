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
package com.arcadedb.query;

import java.util.HashMap;
import java.util.Map;

/**
 * A stateful client session that survives across individual commands. ISO GQL Session Management statements
 * ({@code SESSION SET}, {@code SESSION RESET}, {@code SESSION CLOSE}) operate on the session bound to the
 * current thread.
 * <p>
 * Sessions are owned by the server layer (e.g. the HTTP {@code arcadedb-session-id} session), which the
 * engine module cannot reference directly. The owner attaches its session to the per-thread
 * {@link com.arcadedb.database.DatabaseContext.DatabaseContextTL#setQuerySession} alongside the transaction;
 * the engine reads it back from that same thread context. When none is attached (embedded use, no server
 * session) Session Management statements report an actionable error rather than silently doing nothing.
 * <p>
 * Session parameters are a GQL concept: they are merged into a query's parameters only on the OpenCypher
 * engine path (any language served by the OpenCypher engine), not for other query languages.
 * <p>
 * Session state is node-local and is <b>not</b> replicated across an HA cluster. Because Session Management
 * statements are non-idempotent they route to the leader, but the parameters live on the connection / HTTP
 * session that issued the {@code SESSION SET}. In an HA setup without session affinity, a {@code SESSION SET}
 * on one node followed by a query routed to another node will not see the parameter.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface QuerySession {
  /**
   * Binds {@code name} to {@code value} as a session parameter. Subsequent commands run within this session
   * see it as a query parameter (e.g. {@code $name}) unless the command supplies its own value for that name.
   */
  void setParameter(String name, Object value);

  /**
   * Returns the live, read-only view of the session parameters (never {@code null}).
   */
  Map<String, Object> getParameters();

  /**
   * Clears all session parameters (the {@code SESSION RESET} effect).
   */
  void reset();

  /**
   * Releases this session's resources (the {@code SESSION CLOSE} effect). The exact effect is intentionally
   * <b>transport-specific</b>: the HTTP session also rolls back its open transaction and invalidates its id so
   * later references fail, whereas a connection-scoped owner (Bolt) only clears the session parameters and
   * leaves the connection and its transaction live. A client must therefore not assume {@code SESSION CLOSE}
   * rolls back a transaction on every transport. Over the <b>HTTP</b> transport specifically, the transaction
   * is finalized here, so callers must not issue further operations on that HTTP session within the same
   * request after closing it; on a connection-scoped transport (Bolt) further operations remain valid.
   */
  void close();

  /**
   * Merges session parameters under request-supplied parameters, with request parameters taking precedence.
   * Returns {@code requestParams} unchanged (no allocation) when there are no session parameters - the common
   * case. Single source of truth for the session-parameter merge semantics.
   */
  static Map<String, Object> mergeParameters(final Map<String, Object> sessionParams, final Map<String, Object> requestParams) {
    if (sessionParams == null || sessionParams.isEmpty())
      return requestParams;
    // Sized for both maps up front so putAll never resizes the table (request params win on a name clash).
    final int size = sessionParams.size() + (requestParams != null ? requestParams.size() : 0);
    final Map<String, Object> merged = HashMap.newHashMap(size);
    merged.putAll(sessionParams);
    if (requestParams != null)
      merged.putAll(requestParams);
    return merged;
  }
}
