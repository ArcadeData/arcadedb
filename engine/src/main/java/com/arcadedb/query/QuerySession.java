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

import java.util.Map;

/**
 * A stateful client session that survives across individual commands. ISO GQL (issue #4141 section 2)
 * Session Management statements ({@code SESSION SET}, {@code SESSION RESET}, {@code SESSION CLOSE})
 * operate on the session bound to the current thread.
 * <p>
 * Sessions are owned by the server layer (e.g. the HTTP {@code arcadedb-session-id} session), which the
 * engine module cannot reference directly. The owner therefore binds its session to {@link #bind} for the
 * duration of a command and clears it with {@link #unbind} afterwards; the engine reaches it through
 * {@link #current}. When nothing is bound (embedded use, no server session) {@link #current} returns
 * {@code null} and Session Management statements report an actionable error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface QuerySession {
  ThreadLocal<QuerySession> CURRENT = new ThreadLocal<>();

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
   * Closes the session: releases its resources (e.g. rolls back its open transaction) and invalidates it so
   * later references to its id fail (the {@code SESSION CLOSE} effect).
   */
  void close();

  /**
   * Returns the session bound to the current thread, or {@code null} when none is bound (embedded use).
   */
  static QuerySession current() {
    return CURRENT.get();
  }

  /**
   * Binds {@code session} to the current thread. The owner must call {@link #unbind} when the command finishes.
   */
  static void bind(final QuerySession session) {
    CURRENT.set(session);
  }

  /**
   * Clears the session bound to the current thread.
   */
  static void unbind() {
    CURRENT.remove();
  }
}
