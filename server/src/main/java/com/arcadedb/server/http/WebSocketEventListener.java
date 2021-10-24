package com.arcadedb.server.http;

import com.arcadedb.database.Record;
import com.arcadedb.event.*;
import com.arcadedb.server.ChangeEvent;

public class WebSocketEventListener implements AfterRecordCreateListener, AfterRecordUpdateListener, AfterRecordDeleteListener {

  private final WebSocketEventBus eventBus;
  private final String            database;

  public WebSocketEventListener(final WebSocketEventBus eventBus, final String database) {
    this.eventBus = eventBus;
    this.database = database;
  }

  @Override
  public void onAfterCreate(Record record) {
    this.eventBus.push(new ChangeEvent(ChangeEvent.TYPE.CREATE, record, database));
  }

  @Override
  public void onAfterUpdate(Record record) {
    this.eventBus.push(new ChangeEvent(ChangeEvent.TYPE.UPDATE, record, database));
  }

  @Override
  public void onAfterDelete(Record record) {
    this.eventBus.push(new ChangeEvent(ChangeEvent.TYPE.DELETE, record, database));
  }
}
