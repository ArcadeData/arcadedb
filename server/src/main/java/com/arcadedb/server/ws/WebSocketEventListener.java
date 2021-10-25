package com.arcadedb.server.ws;

import com.arcadedb.database.Record;
import com.arcadedb.event.*;
import com.arcadedb.server.ChangeEvent;

public class WebSocketEventListener implements AfterRecordCreateListener, AfterRecordUpdateListener, AfterRecordDeleteListener {

  private final WebSocketEventBus eventBus;

  public WebSocketEventListener(final WebSocketEventBus eventBus) {
    this.eventBus = eventBus;
  }

  @Override
  public void onAfterCreate(Record record) {
    this.eventBus.push(new ChangeEvent(ChangeEvent.TYPE.CREATE, record));
  }

  @Override
  public void onAfterUpdate(Record record) {
    this.eventBus.push(new ChangeEvent(ChangeEvent.TYPE.UPDATE, record));
  }

  @Override
  public void onAfterDelete(Record record) {
    this.eventBus.push(new ChangeEvent(ChangeEvent.TYPE.DELETE, record));
  }
}
