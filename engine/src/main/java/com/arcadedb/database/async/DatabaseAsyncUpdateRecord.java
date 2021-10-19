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
 */
package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseEventsRegistry;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Record;
import com.arcadedb.log.LogManager;

import java.util.logging.*;

public class DatabaseAsyncUpdateRecord extends DatabaseAsyncAbstractTask {
  public final Record                record;
  public final UpdatedRecordCallback callback;

  public DatabaseAsyncUpdateRecord(final Record record, final UpdatedRecordCallback callback) {
    this.record = record;
    this.callback = callback;
  }

  @Override
  public void execute(DatabaseAsyncExecutorImpl.AsyncThread async, DatabaseInternal database) {
    try {
      if (!((DatabaseEventsRegistry) database.getEvents()).onBeforeUpdate(record))
        return;

      database.updateRecordNoLock(record);

      ((DatabaseEventsRegistry) database.getEvents()).onAfterUpdate(record);

      if (callback != null)
        callback.call(record);

    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on executing async update operation (threadId=%d)", e, Thread.currentThread().getId());

      async.onError(e);
    }
  }

  @Override
  public String toString() {
    return "UpdateRecord(" + record + ")";
  }
}
