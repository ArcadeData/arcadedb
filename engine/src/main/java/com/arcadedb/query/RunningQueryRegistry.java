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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The statements in progress, by id (issue #9680). A server owns one; a protocol opens an entry around each statement
 * it runs with {@link #register} and closes it when the request is over, whatever its outcome.
 * <p>
 * Costs one map insertion, one removal and one small object per request, and nothing at all on the engine's hot
 * paths, which only ever read the flag of the entry they were handed. Listing scans the map, whose size is the number
 * of statements running at that instant.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RunningQueryRegistry {
  private static final String ID_PREFIX = "q";

  private final AtomicLong                            lastId  = new AtomicLong();
  private final ConcurrentHashMap<Long, RunningQuery> running = new ConcurrentHashMap<>();

  /**
   * Opens the entry of a statement about to run on the calling thread and publishes it there
   * ({@link RunningQuery#current()}). The caller closes it on the same thread, in a {@code finally} block.
   *
   * @param database  name of the database the statement runs against, or {@code null}
   * @param user      name of the user running it
   * @param protocol  how it reached the server, e.g. {@code http}
   * @param sessionId the transaction session it runs in, or {@code null}
   * @param tag       the opaque label the client gave it, or {@code null}
   */
  public RunningQuery register(final String database, final String user, final String protocol, final String sessionId,
      final String tag) {
    final RunningQuery query = new RunningQuery(this, lastId.incrementAndGet(), database, user, protocol, sessionId, tag);
    running.put(query.getNumericId(), query);
    return query;
  }

  /** The running statement with this id ({@code q12}, or just {@code 12}), or {@code null} if none is running. */
  public RunningQuery get(final String id) {
    if (id == null)
      return null;
    String value = id.trim();
    if (value.regionMatches(true, 0, ID_PREFIX, 0, ID_PREFIX.length()))
      value = value.substring(ID_PREFIX.length());
    try {
      return running.get(Long.parseLong(value));
    } catch (final NumberFormatException e) {
      return null;
    }
  }

  /** A snapshot of the statements running now, oldest first. */
  public List<RunningQuery> getRunning() {
    final List<RunningQuery> list = new ArrayList<>(running.values());
    list.sort((a, b) -> Long.compare(a.getNumericId(), b.getNumericId()));
    return list;
  }

  public int size() {
    return running.size();
  }

  void unregister(final RunningQuery query) {
    running.remove(query.getNumericId(), query);
  }
}
