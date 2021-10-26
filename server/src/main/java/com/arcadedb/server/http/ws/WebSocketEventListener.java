package com.arcadedb.server.http.ws;

import com.arcadedb.database.Record;
import com.arcadedb.event.*;
import com.arcadedb.server.ChangeEvent;

public class WebSocketEventListener implements AfterRecordCreateListener, AfterRecordUpdateListener, AfterRecordDeleteListener {

  private final DatabaseEventWatcherThread watcherThread;

  public WebSocketEventListener(final DatabaseEventWatcherThread watcherThread) {
    this.watcherThread = watcherThread;
  }

  @Override
  public void onAfterCreate(Record record) {
    this.watcherThread.push(new ChangeEvent(ChangeEvent.TYPE.CREATE, record));
  }

  @Override
  public void onAfterUpdate(Record record) {
    this.watcherThread.push(new ChangeEvent(ChangeEvent.TYPE.UPDATE, record));
  }

  @Override
  public void onAfterDelete(Record record) {
    this.watcherThread.push(new ChangeEvent(ChangeEvent.TYPE.DELETE, record));
  }
}
