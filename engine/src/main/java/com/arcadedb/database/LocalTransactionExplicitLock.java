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
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;

import java.util.*;
import java.util.logging.Level;

/**
 * Explicit lock on a transaction to lock buckets and types in pessimistic way. This avoids the retry mechanism of default implicit locking.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LocalTransactionExplicitLock implements TransactionExplicitLock {
  private final TransactionContext transactionContext;
  private final Set<Integer>       filesToLock = new HashSet<>();

  public LocalTransactionExplicitLock(final TransactionContext transactionContext) {
    this.transactionContext = transactionContext;
  }

  @Override
  public LocalTransactionExplicitLock bucket(final String bucketName) {
    final Bucket bucket = transactionContext.getDatabase().getSchema().getBucketByName(bucketName);
    filesToLock.add(bucket.getFileId());
    final DocumentType associatedType = transactionContext.getDatabase().getSchema().getInvolvedTypeByBucketId(bucket.getFileId());
    if (associatedType != null)
      filesToLock.addAll(associatedType.getAllIndexes(true).stream()
          .flatMap(i -> Arrays.stream(i.getIndexesOnBuckets()))
          .map(b -> b.getFileId())
          .toList());

    filesToLock.removeIf((f) -> f < 0); // Remove negative file IDs (e.g., for virtual buckets)
    return this;
  }

  @Override
  public LocalTransactionExplicitLock type(final String typeName) {
    final DocumentType type = transactionContext.getDatabase().getSchema().getType(typeName);

    // Lock all indexes for this type
    filesToLock.addAll(type.getAllIndexes(true).stream()
        .flatMap(i -> Arrays.stream(i.getIndexesOnBuckets()))
        .map(b -> b.getFileId())
        .toList());

    // Lock all currently involved buckets for this type
    filesToLock.addAll(type.getInvolvedBuckets().stream().map(b -> b.getFileId()).toList());

    // COMPREHENSIVE BUCKET LOCKING: Also lock all polymorphic buckets (includes subtypes)
    // This helps handle cases where records might be created in buckets not initially involved
    filesToLock.addAll(type.getBuckets(true).stream().map(b -> b.getFileId()).toList());

    filesToLock.removeIf((f) -> f < 0); // Remove negative file IDs (e.g., for virtual buckets)

    LogManager.instance().log(this, Level.FINE,
      "Explicit lock for type '%s' will lock %d bucket files (threadId=%d)",
      typeName, filesToLock.size(), Thread.currentThread().getId());

    return this;
  }

  @Override
  public void lock() {
    transactionContext.explicitLock(filesToLock);
  }
}
