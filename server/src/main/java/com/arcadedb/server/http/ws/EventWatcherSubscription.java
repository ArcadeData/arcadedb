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

import io.undertow.websockets.core.WebSocketChannel;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class EventWatcherSubscription {
  private final String                             database;
  private final WebSocketChannel                   channel;
  private final Map<String, Set<ChangeEvent.TYPE>> typeSubscriptions = new ConcurrentHashMap<>();

  private final static Set<ChangeEvent.TYPE> allTypes = Arrays.stream(ChangeEvent.TYPE.values()).collect(Collectors.toSet());

  public EventWatcherSubscription(final String database, final WebSocketChannel channel) {
    this.database = database;
    this.channel = channel;
  }

  public void close() {
    if (channel != null)
      try {
        channel.close();
      } catch (IOException e) {
        // IGNORE THIS
      }

    typeSubscriptions.clear();
  }

  public void add(final String type, final Set<ChangeEvent.TYPE> changeTypes) {
    final var key = type == null ? "*" : type; // ConcurrentHashMap can't have null keys, so use * for "all types."
    typeSubscriptions.computeIfAbsent(key, k -> new HashSet<>()).addAll(changeTypes == null ? allTypes : changeTypes);
  }

  public String getDatabase() {
    return database;
  }

  public WebSocketChannel getChannel() {
    return channel;
  }

  public boolean isMatch(final ChangeEvent event) {
    final var databaseEventTypes = typeSubscriptions.get("*");
    final var typeEventTypes = typeSubscriptions.get(event.getRecord().asDocument().getTypeName());
    // first, see if the type matches on the "database" sub, then the type specific sub
    return (databaseEventTypes != null && databaseEventTypes.contains(event.getType())) || (typeEventTypes != null && typeEventTypes.contains(event.getType()));
  }

  @Override
  public String toString() {
    return "EventWatcherSubscription{" + "database='" + database + '\'' + ", typeSubscriptions=" + typeSubscriptions + '}';
  }
}
