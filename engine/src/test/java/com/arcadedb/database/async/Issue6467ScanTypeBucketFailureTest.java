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

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.RawRecordCallback;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.DatabaseOperationException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #6467: {@link DatabaseAsyncExecutorImpl#scanType} must not return successfully when one of
 * its per-bucket tasks fails with a bucket-level (as opposed to per-record) exception - that used to be swallowed,
 * routed only to the (typically unset) executor-wide error callback while the per-bucket latch still counted down,
 * so the caller saw a silently partial scan.
 */
class Issue6467ScanTypeBucketFailureTest extends TestHelper {

  private static final String TYPE = "Issue6467Item";

  /**
   * Direct unit test of the fix's mechanism: a bucket whose {@code scan()} throws must have that exception captured
   * into the shared holder AND rethrown (so the existing per-task rollback/logging in the worker still runs
   * unchanged), and {@code completed()} must still fire so a caller blocked in {@code scanType()} is never left
   * hanging on a failed bucket.
   */
  @Test
  void failingBucketScanIsCapturedRethrownAndStillCompletes() {
    final RuntimeException injected = new RuntimeException("simulated bucket I/O failure");

    final Bucket brokenBucket = new Bucket() {
      @Override
      public RID createRecord(final Record record, final boolean discardRecordAfter) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void updateRecord(final Record record, final boolean discardRecordAfter) {
        throw new UnsupportedOperationException();
      }

      @Override
      public com.arcadedb.database.Binary getRecord(final RID rid) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean existsRecord(final RID rid) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void deleteRecord(final RID rid) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void deleteRecord(final RID rid, final boolean force) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void scan(final RawRecordCallback callback, final ErrorRecordCallback errorRecordCallback) {
        throw injected;
      }

      @Override
      public Iterator<Record> iterator() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Iterator<Record> inverseIterator() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long count() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getFileId() {
        return 0;
      }

      @Override
      public String getName() {
        return "brokenBucket";
      }
    };

    final AtomicReference<Throwable> firstError = new AtomicReference<>();
    final CountDownLatch             latch      = new CountDownLatch(1);
    final DatabaseAsyncScanBucket task = new DatabaseAsyncScanBucket(latch, record -> true, null, brokenBucket, firstError);

    assertThatThrownBy(() -> task.execute(null, null)).isSameAs(injected);
    assertThat(firstError.get()).isSameAs(injected);

    task.completed();
    assertThat(latch.getCount()).isZero();
  }

  /**
   * End-to-end: {@code LocalBucket.scan()} always rethrows a {@link DatabaseIsReadOnlyException} raised by the
   * user's per-record callback regardless of whether an {@code errorRecordCallback} is set - a deliberate
   * abort-the-whole-scan signal, and the real, reachable analogue of "a bucket's scan throws (I/O / corruption)"
   * the issue describes, as opposed to an ordinary per-record error (which already correctly routes through
   * {@code errorRecordCallback} and is not what this test is about). Two buckets are used so the fix's per-bucket
   * capture is proven to still let the OTHER bucket be scanned to completion.
   */
  @Test
  void scanTypePropagatesABucketLevelFailureInsteadOfReturningAPartialScan() {
    final List<RID> allIds = new ArrayList<>();
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE, 2);
      for (int i = 0; i < 20; i++)
        allIds.add(database.newDocument(TYPE).set("id", i).save().getIdentity());
    });

    final Set<RID>            scannedIds     = ConcurrentHashMap.newKeySet();
    final AtomicInteger       scanned        = new AtomicInteger();
    final AtomicReference<Integer> failingBucketId = new AtomicReference<>();

    assertThatThrownBy(() -> database.async().scanType(TYPE, true, record -> {
      scannedIds.add(record.getIdentity());
      if (scanned.incrementAndGet() == 5) {
        failingBucketId.set(record.getIdentity().getBucketId());
        throw new DatabaseIsReadOnlyException("simulated bucket-level scan failure");
      }
      return true;
    })).isInstanceOf(DatabaseOperationException.class);

    // Every record that lives in a DIFFERENT bucket than the one that failed must have been scanned: that bucket's
    // task is unaffected by the other one's exception and must still drain to completion, rather than the whole
    // scan silently stopping partway through (which is the exact bug #6467 is about).
    final List<RID> otherBucketIds = allIds.stream().filter(rid -> rid.getBucketId() != failingBucketId.get()).toList();
    assertThat(otherBucketIds).isNotEmpty();
    assertThat(scannedIds).containsAll(otherBucketIds);
  }
}
