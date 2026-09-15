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
import com.arcadedb.engine.Bucket;
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

public class DatabaseAsyncCreateRecord implements DatabaseAsyncTask {
  public final Record            record;
  public final Bucket            bucket;
  public final NewRecordCallback onOkCallback;
  public final ErrorCallback     onErrorCallback;

  public DatabaseAsyncCreateRecord(final Record record, final Bucket bucket, final NewRecordCallback callback,
      final ErrorCallback onErrorCallback) {
    this.record = record;
    this.bucket = bucket;
    this.onOkCallback = callback;
    this.onErrorCallback = onErrorCallback;
  }

  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    try {
      database.createRecordNoLock(record, bucket.getName(), onOkCallback == null);

      if (onOkCallback != null)
        onOkCallback.call(record);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on executing async create record operation (threadId=%d)", e,
          Thread.currentThread().threadId());

      if (database.isTransactionActive()) {
        try {
          database.rollback();
        } catch (final Exception re) {
          LogManager.instance().log(this, Level.WARNING, "Error on rolling back active transaction", re);
        }
      }
      // #7615 (claude-review): unconditional, matching commitBatch()/closeTransactionBoundaryIfDurabilityPolicyChanged()
      // - not nested in the isTransactionActive() branch above, so a hypothetical failure that already left the
      // transaction inactive before this catch runs (not via the rollback() right above) still notifies every
      // sibling command buffered earlier in this same batch, each of which already fired its own onComplete and
      // is about to have that write silently discarded too. A no-op when both lists are already empty.
      async.notifyPendingBatchCommandsAndAbandon(e);

      async.onError(e);

      if (onErrorCallback != null)
        onErrorCallback.call(e);
    }
  }

  @Override
  public void notifyBatchAbandoned(final Throwable cause) {
    if (onErrorCallback != null) {
      try {
        onErrorCallback.call(cause);
      } catch (final Throwable callbackError) {
        // Never let the callback's own failure escape onto the caller (issue #7615): it would abort
        // notifyPendingBatchCommandsAndAbandon()'s loop over the rest of the abandoned batch, and replace
        // the real conflict with this one on its way out of commitBatch().
        LogManager.instance().log(this, Level.WARNING, "Error on invoking the error callback of %s", callbackError, this);
      }
    }
  }

  @Override
  public String toString() {
    return "CreateRecord(" + record + ")";
  }
}
