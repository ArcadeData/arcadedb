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
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.database;

import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.IntHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Explicit lock on a transaction to lock buckets and types in pessimistic way. This avoids the retry mechanism of default implicit locking.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class LocalTransactionExplicitLock implements TransactionExplicitLock {
  // Bounded internal retry budget for the snapshot-vs-lock race against compaction-induced
  // file migration. Safe to retry here because no transaction modifications exist when lock()
  // runs, so each attempt is cheap (no rollback, no page reload). Converges in microseconds
  // instead of burning user-level COMMIT RETRY attempts that incur full BEGIN+...+COMMIT cost.
  // Sized for the worst legitimate case (concurrent compactions across all indexes of a
  // multi-indexed type); persistent retry exhaustion past this signals a real concurrency bug.
  private static final int MAX_INTERNAL_RETRIES = 10;

  private final TransactionContext transactionContext;
  private final IntHashSet         filesToLock = new IntHashSet();
  private final List<String>       typeNames   = new ArrayList<>();
  private final List<String>       bucketNames = new ArrayList<>();

  public LocalTransactionExplicitLock(final TransactionContext transactionContext) {
    this.transactionContext = transactionContext;
  }

  @Override
  public LocalTransactionExplicitLock bucket(final String bucketName) {
    bucketNames.add(bucketName);
    collectBucketFileIds(bucketName);
    return this;
  }

  @Override
  public LocalTransactionExplicitLock type(final String typeName) {
    typeNames.add(typeName);
    collectTypeFileIds(typeName);
    return this;
  }

  @Override
  public void lock() {
    ConcurrentModificationException last = null;
    for (int attempt = 0; attempt < MAX_INTERNAL_RETRIES; attempt++) {
      try {
        transactionContext.explicitLock(filesToLock);
        return;
      } catch (final ConcurrentModificationException e) {
        // A compaction migrated one of the snapshotted file IDs between collection and lock check.
        // Re-resolve and try again; the new file ID is visible via mutable.getFileId() after splitIndex swap.
        last = e;
        LogManager.instance().log(this, Level.WARNING,
            "Retrying explicit lock acquisition after compaction-induced file migration (attempt %d/%d, threadId=%d): %s",
            attempt + 1, MAX_INTERNAL_RETRIES, Thread.currentThread().threadId(), e.getMessage());
        refreshFileIds();
      }
    }
    throw new ConcurrentModificationException(
        "Exhausted " + MAX_INTERNAL_RETRIES + " internal retries acquiring explicit lock after compaction-induced file migration. Last error: "
            + last.getMessage());
  }

  private void refreshFileIds() {
    filesToLock.clear();
    for (final String b : bucketNames)
      collectBucketFileIds(b);
    for (final String t : typeNames)
      collectTypeFileIds(t);
  }

  private void collectBucketFileIds(final String bucketName) {
    final Bucket bucket = transactionContext.getDatabase().getSchema().getBucketByName(bucketName);
    addNonNegative(bucket.getFileId());
    final DocumentType associatedType = transactionContext.getDatabase().getSchema().getInvolvedTypeByBucketId(bucket.getFileId());
    if (associatedType != null)
      for (final var typeIndex : associatedType.getAllIndexes(true))
        for (final IndexInternal idx : typeIndex.getIndexesOnBuckets())
          addIndexFileIds(idx);
  }

  private void collectTypeFileIds(final String typeName) {
    final DocumentType type = transactionContext.getDatabase().getSchema().getType(typeName);

    // Lock all indexes for this type
    for (final var typeIndex : type.getAllIndexes(true))
      for (final IndexInternal idx : typeIndex.getIndexesOnBuckets())
        addIndexFileIds(idx);

    // Lock all currently involved buckets for this type
    for (final Bucket b : type.getInvolvedBuckets())
      addNonNegative(b.getFileId());

    // COMPREHENSIVE BUCKET LOCKING: Also lock all polymorphic buckets (includes subtypes)
    // This helps handle cases where records might be created in buckets not initially involved
    for (final Bucket b : type.getBuckets(true))
      addNonNegative(b.getFileId());

    LogManager.instance().log(this, Level.FINE,
      "Explicit lock for type '%s' will lock %d bucket files (threadId=%d)",
      typeName, filesToLock.size(), Thread.currentThread().threadId());
  }

  private void addNonNegative(final int fileId) {
    if (fileId >= 0)
      filesToLock.add(fileId);
  }

  /**
   * Locks EVERY component file of an index (getFileIds, plural), not just the mutable one (getFileId). A
   * multi-file index - an LSM-Tree with a compacted sub-index, or a vector index with its companion graph
   * file - has its whole file set added to the commit-time lock-coverage check by
   * {@code TransactionIndexContext.addFilesToLock}. Locking only the mutable component leaves the sibling
   * files uncovered, so the coverage check rejects the transaction with a non-retryable
   * "not all the modified resources were locked" error. Keep this symmetric with addFilesToLock.
   */
  private void addIndexFileIds(final IndexInternal idx) {
    for (final int fileId : idx.getFileIds())
      addNonNegative(fileId);
  }
}
