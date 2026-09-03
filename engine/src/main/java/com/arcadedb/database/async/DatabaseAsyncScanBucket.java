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
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.log.LogManager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public class DatabaseAsyncScanBucket implements DatabaseAsyncTask {
  public final CountDownLatch             semaphore;
  public final DocumentCallback           userCallback;
  public final ErrorRecordCallback        errorRecordCallback;
  public final Bucket                     bucket;
  // #6467: shared by every DatabaseAsyncScanBucket task of one scanType() call, so it can rethrow a bucket-level
  // failure after every bucket has been drained instead of silently returning a partial scan. Null when a caller
  // constructs the task directly without going through scanType() and does not care about that (kept optional
  // rather than required, since scanning a single bucket in isolation needs nothing beyond what execute() itself
  // already reports through the worker's own rollback/logging/onError handling).
  public final AtomicReference<Throwable> firstError;

  public DatabaseAsyncScanBucket(final CountDownLatch semaphore, final DocumentCallback userCallback, final ErrorRecordCallback errorRecordCallback,
      final Bucket bucket, final AtomicReference<Throwable> firstError) {
    this.semaphore = semaphore;
    this.userCallback = userCallback;
    this.errorRecordCallback = errorRecordCallback;
    this.bucket = bucket;
    this.firstError = firstError;
  }

  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    try {
      bucket.scan((rid, view) -> {
        // isAborting(), not isShutdown() (issue #7004): a worker retired by a shrinking setParallelLevel() carries the
        // shutdown flag while it drains its queue, and this scan is part of that drain. Bailing on it truncated the
        // scan after its first record while scanType() still reported success to the caller.
        if (async.isAborting())
          return false;

        final Record record = database.getRecordFactory()
            .newImmutableRecord(database, database.getSchema().getType(database.getSchema().getTypeNameByBucketId(rid.getBucketId())), rid, view, null);

        return userCallback.onRecord((Document) record);
      }, errorRecordCallback);
    } catch (final RuntimeException | Error e) {
      // #6467: a bucket-level failure (I/O, corruption) used to be swallowed here - routed only to the executor-wide
      // onErrorCallback (typically unset) while completed() still counted this bucket down, so scanType() returned
      // as if every bucket had been scanned. Captured here so scanType() can rethrow it once every bucket is done,
      // then rethrown so the worker's own rollback/logging/onError handling in executeTask() still runs unchanged.
      if (firstError != null && !firstError.compareAndSet(null, e))
        // A DIFFERENT bucket's failure already won the race to be the one scanType() rethrows: this one would
        // otherwise vanish with no trace at all, even though it could be a distinct root cause (e.g. one bucket
        // hitting an I/O error and another independently corrupted).
        LogManager.instance().log(this, Level.SEVERE,
            "Bucket '%s' failed during a parallel scan, but another bucket's failure will be the one reported to the caller",
            e, bucket.getName());
      throw e;
    }
  }

  @Override
  public void completed() {
    // UNLOCK THE CALLER THREAD. The worker invokes completed() after execute() but also when the
    // task is dropped during shutdown (#4954), so scanType() never hangs on the latch.
    semaphore.countDown();
  }

  @Override
  public String toString() {
    return "ScanBucket(" + bucket + ")";
  }
}
