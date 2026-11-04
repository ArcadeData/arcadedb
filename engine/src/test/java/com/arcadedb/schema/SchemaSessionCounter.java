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
package com.arcadedb.schema;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

/**
 * Counts the outermost schema recording sessions a piece of work opens, which is the number of Raft {@code SCHEMA_ENTRY}
 * the same code would publish under HA, without a Raft server.
 * <p>
 * The Raft entry boundary is the outermost {@code recordFileChanges} call on the thread: {@code RaftReplicatedDatabase} delegates
 * a call made while a session is open on the thread, and publishes once per call made while none is. The counter interposes on
 * {@code LocalDatabase.setWrappedDatabaseInstance}, the seam the HA server uses to install that wrapper, and counts only the calls
 * made while no session is open (issues #6990, #10147).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SchemaSessionCounter {
  private final DatabaseInternal delegate;
  private final DatabaseInternal proxy;
  private       int              depth     = 0;
  private       int              outermost = 0;

  private SchemaSessionCounter(final DatabaseInternal delegate) {
    this.delegate = delegate;
    this.proxy = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (p, method, args) -> {
          final boolean session = "recordFileChanges".equals(method.getName());
          if (session) {
            if (depth == 0)
              ++outermost;
            ++depth;
          }
          try {
            return method.invoke(this.delegate, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          } finally {
            if (session)
              --depth;
          }
        });
  }

  /** What {@link #count} saw, available even when the work threw: the only way to measure work that fails part way. */
  static final class Count {
    private int sessions = -1;

    int sessions() {
      return sessions;
    }
  }

  /**
   * Runs {@code work} with the counting database installed and returns how many outermost sessions it opened. A failure of
   * {@code work} propagates; {@code seen} still holds the count.
   */
  static int count(final LocalDatabase database, final Count seen, final Runnable work) {
    final DatabaseInternal previous = database.getWrappedDatabaseInstance();
    final SchemaSessionCounter counter = new SchemaSessionCounter(database);
    database.setWrappedDatabaseInstance(counter.proxy);
    seen.sessions = -1;
    try {
      work.run();
    } finally {
      database.setWrappedDatabaseInstance(previous);
      seen.sessions = counter.outermost;
    }
    return counter.outermost;
  }

  static int count(final LocalDatabase database, final Runnable work) {
    return count(database, new Count(), work);
  }
}
