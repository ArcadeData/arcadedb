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
import com.arcadedb.database.Record;
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

/**
 * Asynchronous delete of one record.
 * <p>
 * The before/after-delete listeners are NOT dispatched here: {@code deleteRecordNoLock} owns that dispatch, exactly
 * as it does for the synchronous {@code deleteRecord()}. This task used to dispatch them as well, so every listener
 * fired twice per asynchronous delete - a counter double-counted, an audit log got two entries, a cascade ran twice
 * (issue #7003). A veto is learnt from the delegate's return value instead, so a vetoed delete still skips the
 * success callback as it always did.
 */
public class DatabaseAsyncDeleteRecord implements DatabaseAsyncTask {
  public final Record                record;
  public final DeletedRecordCallback onOkCallback;
  public final ErrorCallback         onErrorCallback;

  public DatabaseAsyncDeleteRecord(final Record record, final DeletedRecordCallback callback, final ErrorCallback onErrorCallback) {
    this.record = record;
    this.onOkCallback = callback;
    this.onErrorCallback = onErrorCallback;
  }

  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    try {
      if (!database.deleteRecordNoLock(record))
        // VETOED BY A BEFORE-DELETE LISTENER
        return;

      if (onOkCallback != null)
        onOkCallback.call(record);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on executing async delete record operation (threadId=%d)", e, Thread.currentThread().getId());

      if (database.isTransactionActive()) {
        try {
          database.rollback();
        } catch (final Exception re) {
          LogManager.instance().log(this, Level.WARNING, "Error on rolling back active transaction", re);
        }
      }

      async.onError(e);

      if (onErrorCallback != null)
        onErrorCallback.call(e);
    }
  }

  @Override
  public String toString() {
    return "DeleteRecord(" + record + ")";
  }
}
