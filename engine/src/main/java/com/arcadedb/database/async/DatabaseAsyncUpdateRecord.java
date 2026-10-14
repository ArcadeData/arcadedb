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
 * Asynchronous update of one record. Mirrors the preconditions of the synchronous {@code updateRecord()}: the record
 * must be persistent and, when it is a document, it must satisfy its schema. The validation used to be missing here,
 * so an asynchronous update persisted a record violating a MANDATORY/NOTNULL constraint with no error anywhere - not
 * to the caller, not to the error callback (issue #7002). A violation now takes the same route as any other failure of
 * this task: the worker's transaction is rolled back and the error callback is invoked.
 * <p>
 * That validation, the listeners and the {@code updateRecord} statistic now all live in
 * {@link DatabaseInternal#updateRecordWithEventsNoLock(Record, boolean)}, which the synchronous update shares: this task
 * re-implemented two of the three and missed the statistic (issue #9987).
 */
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
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    try {
      // The preconditions, the validation, the before/after-update listeners and the statistic are the database's,
      // shared with the synchronous update; a veto skips the success callback, as for the delete (issue #9987)
      if (!database.updateRecordWithEventsNoLock(record, onOkCallback == null))
        return;

      if (onOkCallback != null)
        onOkCallback.call(record);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on executing async update record operation (threadId=%d)", e, Thread.currentThread().threadId());

      if (database.isTransactionActive()) {
        try {
          database.rollback();
        } catch (final Exception re) {
          LogManager.instance().log(this, Level.WARNING, "Error on rolling back active transaction", re);
        }
      }
      // #7615 (code review): unconditional, matching commitBatch()/closeTransactionBoundaryIfDurabilityPolicyChanged()
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
    return "UpdateRecord(" + record + ")";
  }
}
