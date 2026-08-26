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
package com.arcadedb.server.http.ws;

import com.arcadedb.database.Document;
import io.undertow.websockets.core.WebSocketChannel;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class EventWatcherSubscription {
  private final String                             database;
  private final WebSocketChannel                   channel;
  private final Map<String, Set<ChangeEvent.TYPE>> typeSubscriptions = new ConcurrentHashMap<>();
  /**
   * Bytes handed to Undertow for this channel that it has not reported as sent yet (issue #6762). Change events are
   * fanned out fire-and-forget, so without this a subscriber that never reads accumulates frames in the server's
   * send buffer indefinitely: the producer-side queue is bounded, but a slow consumer is charged to the server's
   * heap rather than to its own.
   */
  private final AtomicLong                         pendingBytes      = new AtomicLong();

  private final static Set<ChangeEvent.TYPE> allTypes = Arrays.stream(ChangeEvent.TYPE.values()).collect(Collectors.toSet());

  public EventWatcherSubscription(final String database, final WebSocketChannel channel) {
    this.database = database;
    this.channel = channel;
  }

  public void close() {
    if (channel != null)
      try {
        channel.close();
      } catch (final IOException e) {
        // IGNORE THIS
      }

    typeSubscriptions.clear();
  }

  public void add(final String type, final Set<ChangeEvent.TYPE> changeTypes) {
    final var key = type == null ? "*" : type; // ConcurrentHashMap can't have null keys, so use * for "all types."
    // The set values are read by the watcher thread (isMatch) while Undertow IO threads mutate them here, so they must
    // be thread-safe: a plain HashSet resize during addAll could make a concurrent contains() misread or throw.
    typeSubscriptions.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).addAll(changeTypes == null ? allTypes : changeTypes);
  }

  public WebSocketChannel getChannel() {
    return channel;
  }

  /**
   * Charges {@code bytes} to this subscriber's outstanding-send budget.
   *
   * @param maxPendingBytes the budget, or {@code 0} (or less) to disable the cap
   *
   * @return {@code false} when the send would push the subscriber past its budget, in which case nothing is charged
   *         and the caller must evict it instead of sending
   */
  public boolean reservePending(final int bytes, final long maxPendingBytes) {
    final long pending = pendingBytes.addAndGet(bytes);
    if (maxPendingBytes <= 0 || pending <= maxPendingBytes)
      return true;
    pendingBytes.addAndGet(-bytes);
    return false;
  }

  /** Releases a reservation made by {@link #reservePending(int, long)} once Undertow reports the frame done. */
  public void releasePending(final int bytes) {
    pendingBytes.addAndGet(-bytes);
  }

  // @VisibleForTesting
  public long getPendingBytes() {
    return pendingBytes.get();
  }

  public String getDatabase() {
    return database;
  }

  public boolean isMatch(final ChangeEvent event) {
    // Only documents, vertices and edges are part of the change stream. Internal records (e.g. edge segments) are not
    // Documents and must never be published: calling asDocument() on them used to crash the watcher thread (issue #4479).
    if (!(event.getRecord() instanceof Document document))
      return false;

    final var databaseEventTypes = typeSubscriptions.get("*");
    // Use Document.getTypeName() directly: Vertex and Edge both extend Document, while asDocument() deliberately throws for them.
    final var typeEventTypes = typeSubscriptions.get(document.getTypeName());
    // first, see if the type matches on the "database" sub, then the type specific sub
    return (databaseEventTypes != null && databaseEventTypes.contains(event.getType())) || (typeEventTypes != null && typeEventTypes.contains(event.getType()));
  }

  @Override
  public String toString() {
    return "EventWatcherSubscription{" + "database='" + database + '\'' + ", typeSubscriptions=" + typeSubscriptions + '}';
  }
}
