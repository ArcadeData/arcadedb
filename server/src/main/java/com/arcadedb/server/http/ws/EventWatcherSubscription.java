package com.arcadedb.server.http.ws;

import com.arcadedb.log.LogManager;
import io.undertow.websockets.core.WebSocketChannel;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.util.stream.*;

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
    LogManager.instance().log(this, Level.INFO, "EventWatcherSubscription close database %s, typeSubscriptions=%s", null, database, typeSubscriptions);

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
