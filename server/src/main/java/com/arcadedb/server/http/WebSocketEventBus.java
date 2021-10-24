package com.arcadedb.server.http;

import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ChangeEvent;
import com.arcadedb.utility.Pair;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

public class WebSocketEventBus {
  private final ConcurrentLinkedQueue<ChangeEvent> events           = new ConcurrentLinkedQueue<>();
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, Set<Pair<UUID, WebSocketChannel>>>>
                                                   subscribers      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Pair<WebSocketEventListener, ScheduledExecutorService>>
                                                   databaseWatchers = new ConcurrentHashMap<>();
  private final ArcadeDBServer                     arcadeServer;

  public static final String CHANNEL_ID = "ID";

  public WebSocketEventBus(final ArcadeDBServer server) {
    this.arcadeServer = server;
  }

  public void push(ChangeEvent event) {
    this.events.add(event);
  }

  public void subscribe(String database, String type, WebSocketChannel channel) {
    this.getSubscriberSet(database, type).add(new Pair<>((UUID) channel.getAttribute(CHANNEL_ID), channel));
    if (!this.databaseWatchers.containsKey(database)) this.startDatabaseWatcher(database);
  }

  public void unsubscribe(String database, String type, UUID id) {
    this.getSubscriberSet(database, type).removeIf(pair -> pair.getFirst() == id);
  }

  private void startDatabaseWatcher(String database) {
    WebSocketEventListener listener = new WebSocketEventListener(this, database);
    this.arcadeServer.getDatabase(database).getEvents()
        .registerListener((AfterRecordCreateListener) listener)
        .registerListener((AfterRecordUpdateListener) listener)
        .registerListener((AfterRecordDeleteListener) listener);

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    executor.scheduleAtFixedRate(() -> {
      ChangeEvent event;
      while ((event = events.poll()) != null) {
        var databaseWatchers = this.getSubscriberSet(database, "*");
        var typeWatchers = this.getSubscriberSet(database, event.getRecord().asDocument().getTypeName());
        HashSet<Pair<UUID, WebSocketChannel>> matchingSubscribers = new HashSet<>() {{
          addAll(databaseWatchers);
          addAll(typeWatchers);
        }};

        var json = event.toJSON();
        if (subscribers.isEmpty()) {
          this.stopDatabaseWatcher(database);
        } else {
          matchingSubscribers.forEach(pair -> WebSockets.sendText(json, pair.getSecond(), null));
        }
      }
    }, 0, 500, TimeUnit.MILLISECONDS);

    this.databaseWatchers.put(database, new Pair<>(listener, executor));
  }

  private void stopDatabaseWatcher(String database) {
    var pair = this.databaseWatchers.get(database);
    this.arcadeServer.getDatabase(database).getEvents()
        .unregisterListener((AfterRecordCreateListener) pair.getFirst())
        .unregisterListener((AfterRecordUpdateListener) pair.getFirst())
        .unregisterListener((AfterRecordDeleteListener) pair.getFirst());

    pair.getSecond().shutdown();
    this.databaseWatchers.remove(database);
  }

  private Set<Pair<UUID, WebSocketChannel>> getSubscriberSet(String database, String typeFilter) {
    var type = typeFilter == null || typeFilter.trim().isEmpty() ? "*" : typeFilter;
    if (!this.subscribers.containsKey(database)) this.subscribers.put(database, new ConcurrentHashMap<>());
    if (!this.subscribers.get(database).containsKey(type)) this.subscribers.get(database).put(type, new HashSet<>());
    return this.subscribers.get(database).get(type);
  }

  public void unsubscribeAll(UUID id) {
    this.subscribers.values().forEach(types -> types.values().forEach(sets -> sets.removeIf(pair -> pair.getFirst() == id)));
  }
}

