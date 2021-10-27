package com.arcadedb.server.http.ws;

import io.undertow.websockets.core.WebSocketChannel;

import java.util.*;

public class EventWatcherSubscription {
  private final String                database;
  private final String                type;
  private final Set<ChangeEvent.TYPE> changeTypes;
  private final WebSocketChannel      channel;

  public EventWatcherSubscription(final String database, final String type, final List<Object> changeTypes, final WebSocketChannel channel) {
    this.database = database;
    this.type = type;
    if (changeTypes != null) {
      this.changeTypes = new HashSet<>();
      changeTypes.forEach(changeType -> this.changeTypes.add(ChangeEvent.TYPE.valueOf(changeType.toString().toUpperCase())));
    } else {
      this.changeTypes = null;
    }
    this.channel = channel;
  }

  public String getDatabase() {
    return database;
  }

  public String getType() {
    return type;
  }

  public Set<ChangeEvent.TYPE> getChangeTypes() {
    return changeTypes;
  }

  public WebSocketChannel getChannel() {
    return channel;
  }

  public boolean isMatch(final ChangeEvent event) {
    return (this.changeTypes == null || this.changeTypes.contains(event.getType())) && (this.type == null || this.type.equals(
        event.getRecord().asDocument().getTypeName()));
  }
}
