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
package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordEventsRegistry;
import com.arcadedb.log.LogManager;

import java.util.logging.*;

public class DatabaseAsyncUpdateRecord implements DatabaseAsyncTask {
  public final Record                record;
  public final UpdatedRecordCallback onOkCallback;
  public final ErrorCallback         onErrorCallback;

  public DatabaseAsyncUpdateRecord(final Record record, final UpdatedRecordCallback callback, final ErrorCallback onErrorCallback) {
    this.record = record;
    this.onOkCallback = callback;
    this.onErrorCallback = onErrorCallback;
  }

  @Override
  public void execute(DatabaseAsyncExecutorImpl.AsyncThread async, DatabaseInternal database) {
    try {
      // INVOKE EVENT CALLBACKS
      if (!((RecordEventsRegistry) database.getEvents()).onBeforeUpdate(record))
        return;
      if (record instanceof Document)
        if (!((RecordEventsRegistry) ((Document) record).getType().getEvents()).onBeforeUpdate(record))
          return;

      database.updateRecordNoLock(record);

      // INVOKE EVENT CALLBACKS
      ((RecordEventsRegistry) database.getEvents()).onAfterUpdate(record);
      if (record instanceof Document)
        ((RecordEventsRegistry) ((Document) record).getType().getEvents()).onAfterUpdate(record);

      if (onOkCallback != null)
        onOkCallback.call(record);

    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on executing async update record operation (threadId=%d)", e, Thread.currentThread().getId());

      async.onError(e);

      if (onErrorCallback != null)
        onErrorCallback.call(e);
    }
  }

  @Override
  public String toString() {
    return "UpdateRecord(" + record + ")";
  }
}
