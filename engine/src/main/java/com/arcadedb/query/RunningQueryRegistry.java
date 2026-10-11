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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

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
  private final AtomicLong                            lastId      = new AtomicLong();
  // Random per registry: part of every id this registry hands out, so an id never resolves on a node that did not issue it
  private final String                                instanceTag = Integer.toHexString(ThreadLocalRandom.current().nextInt() | 0x10000000);
  private final ConcurrentHashMap<Long, RunningQuery> running = new ConcurrentHashMap<>();
  // Who sees and stops every statement rather than only their own: nobody until the server says who administers it
  private volatile Predicate<String>                  administrator = user -> false;

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
    final RunningQuery query = open(database, user, protocol, sessionId, tag);
    query.publish();
    return query;
  }

  /**
   * Opens the entry of a statement without publishing it on the calling thread, for a statement whose work runs across
   * several messages or on another thread (issue #9689): the protocol {@link RunningQuery#bind() binds} it wherever the
   * work runs and closes it once the statement is over. Same parameters as {@link #register}.
   */
  public RunningQuery open(final String database, final String user, final String protocol, final String sessionId,
      final String tag) {
    final RunningQuery query = new RunningQuery(this, lastId.incrementAndGet(), database, user, protocol, sessionId, tag);
    running.put(query.getNumericId(), query);
    return query;
  }

  /** Who sees and stops every statement running here; anybody else sees and stops only their own. */
  public void setAdministrator(final Predicate<String> administrator) {
    this.administrator = administrator != null ? administrator : user -> false;
  }

  /**
   * Whether {@code viewer} may see and stop {@code query}: the administrator may see every statement, anybody else
   * only their own. Every surface that lists or terminates statements asks this, so they cannot disagree.
   */
  public boolean isVisible(final String viewer, final RunningQuery query) {
    return query != null && isVisible(viewer, query.getUser());
  }

  /**
   * The same rule for whatever else belongs to a user and may be stopped - a transaction session, a protocol connection
   * with or without a statement running: whether {@code viewer} may see and stop what {@code owner} runs.
   */
  public boolean isVisible(final String viewer, final String owner) {
    return viewer != null && (administrator.test(viewer) || viewer.equals(owner));
  }

  /**
   * The running statement with this id ({@code q<n>-<instance>}, see {@link RunningQuery#getId()}), or {@code null} if none
   * is running here. An id issued by another registry - another node, or this one before a restart - is not found.
   */
  public RunningQuery get(final String id) {
    if (id == null)
      return null;
    final String value = id.trim();
    final int dash = value.lastIndexOf('-');
    if (dash < 2 || (value.charAt(0) != 'q' && value.charAt(0) != 'Q') || !instanceTag.equalsIgnoreCase(value.substring(dash + 1)))
      return null;
    try {
      return running.get(Long.parseLong(value.substring(1, dash)));
    } catch (final NumberFormatException e) {
      return null;
    }
  }

  /**
   * The running statement another server forwarded here as its statement {@code id} (see
   * {@link RunningQuery#getForwardedFrom()}), or {@code null}.
   */
  public RunningQuery getForwardedFrom(final String id) {
    if (id == null)
      return null;
    final String value = id.trim();
    for (final RunningQuery query : running.values())
      if (value.equalsIgnoreCase(query.getForwardedFrom()))
        return query;
    return null;
  }

  /** The tag that sets this registry's ids apart from any other's. */
  public String getInstanceTag() {
    return instanceTag;
  }

  /** A snapshot of the statements running now, oldest first. */
  public List<RunningQuery> getRunning() {
    final List<RunningQuery> list = new ArrayList<>(running.values());
    list.sort(Comparator.comparingLong(RunningQuery::getNumericId));
    return list;
  }

  public int size() {
    return running.size();
  }

  void unregister(final RunningQuery query) {
    running.remove(query.getNumericId(), query);
  }
}
