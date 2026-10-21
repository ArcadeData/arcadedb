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
package com.arcadedb.schema;

import com.arcadedb.database.CommittedReadScope;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.DocumentIndexer;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.utility.IntHashSet;

import java.util.List;

/**
 * The second half of an index build that joins the caller's transaction (issue #10490). The first half indexed the COMMITTED
 * state of the bucket in a transaction of its own, so the entries of every committed record outlive a rollback of the caller.
 * What that scan cannot see is what the caller has written and not committed, and this reconciles it: for each record the
 * caller created, updated or deleted in the bucket, the entries are queued on the CALLER's transaction, where they commit, or
 * roll back, with the records they describe.
 * <p>
 * Per record the committed version is read with the caller's transaction set aside and the caller's own version with it back,
 * and the two decide what is queued:
 * <ul>
 *   <li>none committed, one pending: a create, the entries of the pending record are added;</li>
 *   <li>one committed, none pending: a delete, the entries of the committed record are removed;</li>
 *   <li>both: an update, the entries are diffed by the same logic a live update uses, so a key that did not change is not
 *   queued twice;</li>
 *   <li>neither: created and deleted by the caller, nothing to do.</li>
 * </ul>
 * Deciding from the two versions instead of from which of create / update / delete was queued keeps it right for a record
 * created and then updated, deleted and then created again in the same slot, and every other sequence.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class IndexBuildCallerChanges {
  private IndexBuildCallerChanges() {
  }

  /**
   * Queues on the caller's transaction the entries of {@code index} for what that transaction changed in the bucket the index
   * covers. Must be called with the caller's transaction active and after the committed state has been indexed.
   * <p>
   * The progress callback hears of the records the committed scan could not have counted, the ones the caller created, and
   * continues the count from {@code indexedSoFar}. An updated record was counted with its committed version, and a deleted one
   * was too: it was indexed and then taken out.
   */
  static void apply(final DatabaseInternal database, final IndexInternal index, final Index.BuildIndexCallback callback,
      final long indexedSoFar) {
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx == null || !tx.isActive())
      return;

    final IntHashSet bucketIds = new IntHashSet();
    bucketIds.add(index.getAssociatedBucketId());
    final List<RID> written = tx.getWrittenRecordIdsIn(bucketIds);
    if (written.isEmpty())
      return;

    final DocumentIndexer indexer = database.getIndexer();
    final List<IndexInternal> thisIndex = List.of(index);
    long indexed = indexedSoFar;

    // In chunks, so the committed versions held in memory at once are bounded by the chunk and not by the size of the caller's
    // transaction: a DDL statement run inside a large uncommitted transaction would otherwise hold one document per write
    for (int from = 0; from < written.size(); from += IndexBuilder.BUILD_BATCH_SIZE) {
      final int to = Math.min(from + IndexBuilder.BUILD_BATCH_SIZE, written.size());

      // The committed version of each record, read with the caller's transaction set aside, so what the caller wrote cannot
      // masquerade as committed
      final Document[] committed = new Document[to - from];
      try (final CommittedReadScope committedOnly = CommittedReadScope.open(database)) {
        for (int i = from; i < to; i++)
          committed[i - from] = readDocument(database, null, written.get(i));
      }

      for (int i = from; i < to; i++) {
        final RID rid = written.get(i);
        final Document before = committed[i - from];
        final Document pending = readDocument(database, tx, rid);

        if (before == null) {
          if (pending != null) {
            indexer.addToIndex(index, rid, pending);
            if (callback != null)
              callback.onDocumentIndexed(pending, ++indexed);
          }
        } else if (pending == null)
          indexer.removeFromIndex(index, rid, before);
        else
          indexer.updateDocument(before, pending, thisIndex);

        // A record the caller wrote is updated again from here on against what the indexes hold for it now, not against what the
        // committed buffer says: the next update must remove the entry queued above, not the one the buffer implies
        if (pending != null)
          indexer.snapshotAfterBuild(tx, pending, index);
      }
    }
  }

  /**
   * The document at {@code rid} as the calling thread sees it, or null when there is none. With {@code tx} given the
   * transaction's own written copy comes first: an update is parked on it until the commit and the page still holds the old
   * content.
   */
  private static Document readDocument(final DatabaseInternal database, final TransactionContext tx, final RID rid) {
    if (tx != null) {
      final Record written = tx.getWrittenRecord(rid);
      if (written instanceof Document document)
        return document;
    }
    try {
      return database.lookupByRID(rid, true) instanceof Document document ? document : null;
    } catch (final RecordNotFoundException e) {
      // Only the absence of the record is read as "none": a record the caller created has no committed version, and one it
      // deleted has no pending one. Any other failure of the read is not caught and fails the build, which drops the index
      return null;
    }
  }
}
