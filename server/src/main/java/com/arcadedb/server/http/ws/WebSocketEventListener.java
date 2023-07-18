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

import com.arcadedb.database.Record;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;

/**
 * The onAfterRead has not been implemented because it could dramatically slow down the entire database.
 */
public class WebSocketEventListener implements AfterRecordCreateListener, AfterRecordUpdateListener, AfterRecordDeleteListener {

  private final DatabaseEventWatcherThread watcherThread;

  public WebSocketEventListener(final DatabaseEventWatcherThread watcherThread) {
    this.watcherThread = watcherThread;
  }

  @Override
  public void onAfterCreate(final Record record) {
    this.watcherThread.push(new ChangeEvent(ChangeEvent.TYPE.CREATE, record));
  }

  @Override
  public void onAfterUpdate(final Record record) {
    this.watcherThread.push(new ChangeEvent(ChangeEvent.TYPE.UPDATE, record));
  }

  @Override
  public void onAfterDelete(final Record record) {
    this.watcherThread.push(new ChangeEvent(ChangeEvent.TYPE.DELETE, record));
  }
}
