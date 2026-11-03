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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.CommittedReadScope;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.async.AsyncQuiesce;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexBulkLoader;
import com.arcadedb.log.LogManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Rebuilds an LSM_TREE index IN PLACE, one bucket index at a time, while the database keeps serving every other type and the
 * reads of the rebuilt one.
 * <p>
 * The drop-and-create rebuild ({@code REBUILD INDEX} before this class) held the database write lock for the whole build,
 * so a query on any type waited for it, and dropped the index before building the new one, so a process killed in between
 * reopened with no index at all: for a UNIQUE index, no constraint either. Here every bucket index keeps its identity and is
 * given new content the way a full compaction gives it: its records are scanned and sorted ({@link LSMTreeIndexBulkLoader}),
 * written into a new compacted file, and that file is swapped in under the lock of the index with an empty mutable file in
 * front. Until the swap the index answers from its previous files; a crash before it leaves them in place (the new compacted
 * file is an orphan the next open drops), and a crash after it reopens on the new ones, because the swap saves the schema
 * before the previous files are dropped. Old content or new content, per bucket, never none.
 * <p>
 * What waits, and for how long: only the writes to the bucket being rebuilt, for the time of that bucket. The commit locks of
 * the bucket and of its index files are held from the scan to the swap, so a write to that bucket waits for them (and gives up
 * with a retryable {@code LockTimeoutException} after {@code arcadedb.commitLockTimeout}, as it does behind any commit), while
 * writes to the other buckets of the type are indexed by whichever bucket index they find, old or rebuilt, both complete. That
 * is the trade chosen over capturing the writes that land during the scan: the entries of a write that commits mid-scan would
 * have to be told apart from the ones the scan saw, and getting that wrong does not throw, it answers the wrong records. The
 * database read lock is held for the same span, which keeps a close or a schema change from removing the files under the
 * build and blocks nothing else.
 * <p>
 * A key two buckets share cannot be seen by a per-bucket sort, so a UNIQUE index of more than one bucket is walked once more at
 * the end, without any lock, and a key two live records share is refused as the drop-and-create rebuild refused it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class InPlaceIndexRebuild {
  private InPlaceIndexRebuild() {
  }

  /**
   * Why {@code index} cannot be rebuilt in place, or null when it can. The rebuild then drops and re-creates it as before.
   */
  public static String refusal(final DatabaseInternal database, final Index index) {
    if (database.isReplicated() || database.getWrappedDatabaseInstance().isReplicated())
      // the swap writes its pages outside any transaction, so no replica would receive them
      return "the database is replicated";

    final Schema schema = database.getSchema();
    final List<Bucket> buckets = new ArrayList<>();
    if (index instanceof TypeIndex typeIndex) {
      if (typeIndex.isDropped())
        return "the index was dropped";
      final IndexInternal[] onBuckets = typeIndex.getIndexesOnBuckets();
      if (onBuckets.length == 0)
        return "the index has no bucket index";
      for (final IndexInternal onBucket : onBuckets) {
        if (!isPlainLSMTree(onBucket))
          return "the index is not an LSM_TREE index";
        final Bucket bucket = bucketOrNull(schema, onBucket.getAssociatedBucketId());
        if (bucket == null)
          return "the bucket of the bucket index '" + onBucket.getName() + "' is not found";
        buckets.add(bucket);
      }
      if (typeIndex.getTypeName() == null || !schema.existsType(typeIndex.getTypeName()))
        return "the type of the index is not found";
      buckets.addAll(schema.getType(typeIndex.getTypeName()).getBuckets(false));
    } else if (isPlainLSMTree(index) && ((IndexInternal) index).getTypeIndex() != null
        && bucketOrNull(schema, index.getAssociatedBucketId()) != null)
      buckets.add(schema.getBucketById(index.getAssociatedBucketId()));
    else
      return "the index is not an LSM_TREE bucket index of a type";

    if (IndexBuilder.anyCallerTransactionHasChangesIn(database, buckets))
      // the scan reads the committed records, and those changes would be in neither the scan nor the index
      return "a transaction has pending changes to the indexed buckets";
    return null;
  }

  /**
   * Rebuilds {@code index}, a type index or one of its bucket indexes, in place. A bucket of the type that has no bucket index (an
   * index left incomplete) is given one first.
   *
   * @return the index, which keeps its name and identity
   */
  public static Index rebuild(final DatabaseInternal database, final Index index, final Index.BuildIndexCallback callback,
      final int maxAttempts) {
    final AtomicLong indexed = new AtomicLong();
    int rebuiltBuckets = 0;
    // The rebuild reads committed records and writes outside any transaction: the caller's, typically the auto-commit one of
    // an HTTP command, is set aside and given back untouched (refusal() made sure it holds nothing for these buckets)
    // The swap of every bucket index deletes its previous files: one that cannot be deleted refuses the rebuild before anything is
    // changed (issue #10421), rather than leaving a file behind once the new content is in place
    final List<Integer> filesToReplace = new ArrayList<>();
    if (index instanceof TypeIndex typeIndex)
      for (final IndexInternal onBucket : typeIndex.getIndexesOnBuckets())
        filesToReplace.addAll(onBucket.getFileIds());
    else
      filesToReplace.addAll(((IndexInternal) index).getFileIds());
    database.getSchema().getEmbedded().requireFilesCanBeDeleted("rebuild the index '" + index.getName() + "'", filesToReplace);

    try (final CommittedReadScope outsideCallerTransaction = CommittedReadScope.open(database)) {
      if (index instanceof TypeIndex typeIndex) {
        createMissingBucketIndexes(database, typeIndex, maxAttempts);
        // The bucket indexes created above first: the type index is reported as not valid until they are filled
        final IndexInternal[] onBuckets = awaitingFillFirst(typeIndex.getIndexesOnBuckets());
        for (int i = 0; i < onBuckets.length; i++) {
          final LSMTreeIndex onBucket = (LSMTreeIndex) onBuckets[i];
          rebuildBucketIndex(database, onBucket, callback, indexed, maxAttempts);
          if (onBucket.isAwaitingFill()) {
            onBucket.setAwaitingFill(false);
            typeIndex.refreshCompleteness();
            // persisted at once: a stop from here on reopens with this bucket index counted
            database.getSchema().getEmbedded().saveConfiguration();
          }
          ++rebuiltBuckets;
          LogManager.instance().log(InPlaceIndexRebuild.class, Level.INFO, "Rebuilt index '%s': %d of %d bucket indexes (%,d entries so far)",
              typeIndex.getName(), i + 1, onBuckets.length, indexed.get());
        }
        if (typeIndex.isUnique() && onBuckets.length > 1)
          verifyUniqueAcrossBuckets(database, typeIndex, maxAttempts);
      } else
        rebuildBucketIndex(database, (LSMTreeIndex) index, callback, indexed, maxAttempts);
    } catch (final NeedRetryException e) {
      // The same contract as the drop-and-create rebuild (#6040): REBUILD INDEX either succeeds or says why it did not. Here
      // nothing is ever dropped, so the index is whole whichever bucket the attempts ran out on
      throw new CommandExecutionException("Cannot rebuild index '" + index.getName() + "' after " + maxAttempts
          + " attempts: the index lock could not be acquired (error: " + e.getMessage() + "). " + (rebuiltBuckets == 0 ?
          "The index itself is unchanged" :
          "The index is complete: the " + rebuiltBuckets + " bucket indexes rebuilt so far hold their new entries, the others their previous ones"),
          e);
    }
    return index;
  }

  /**
   * The check {@link #rebuild} runs at the end of a UNIQUE type index: a key two records of different buckets share. Exposed for
   * {@code REBUILD INDEX *}, which rebuilds bucket indexes one by one and runs it once per type index afterwards.
   */
  public static void verifyUniqueAcrossBuckets(final DatabaseInternal database, final TypeIndex typeIndex, final int maxAttempts) {
    try (final CommittedReadScope outsideCallerTransaction = CommittedReadScope.open(database)) {
      TypeIndexBuilder.verifyUniqueKeys(database, typeIndex, maxAttempts, true);
    }
  }

  private static Bucket bucketOrNull(final Schema schema, final int bucketId) {
    if (bucketId < 0)
      return null;
    try {
      return schema.getBucketById(bucketId);
    } catch (final SchemaException e) {
      return null;
    }
  }

  private static boolean isPlainLSMTree(final Index index) {
    // a wrapper (full-text, geospatial, sparse vector) keeps more than the LSM entries of its records, and is rebuilt by its own builder
    return index != null && index.getClass() == LSMTreeIndex.class && index.getType() == Schema.INDEX_TYPE.LSM_TREE;
  }

  /**
   * Gives a bucket index to every bucket of the type that has none, the state a rebuild interrupted by an older version could
   * leave behind. Built as the bucket index of a bucket added to the type is, under the schema lock: a repair, not the common path.
   */
  private static void createMissingBucketIndexes(final DatabaseInternal database, final TypeIndex typeIndex, final int maxAttempts) {
    final LocalSchema schema = database.getSchema().getEmbedded();
    final LocalDocumentType type = schema.getType(typeIndex.getTypeName());
    final List<Bucket> missing = typeIndex.getBucketsWithoutIndex();
    if (missing.isEmpty())
      return;

    LogManager.instance().log(InPlaceIndexRebuild.class, Level.WARNING,
        "Index '%s' has no bucket index for buckets %s: creating them, then filling them with the others", typeIndex.getName(), missing);
    // EMPTY, under the schema lock for the time of creating the files only: they are filled afterwards one bucket at a time like the
    // other bucket indexes, without the database write lock. Until then each one is AWAITING FILL, flagged in memory and in the
    // schema entry written by this very schema change: the type index does not count it (it stays reported as not valid, so no
    // planner uses it), and a stop before the fill reopens without it, in the incomplete state the rebuild started from
    try (final AsyncQuiesce asyncPaused = database.quiesceAsync()) {
      schema.recordFileChanges(() -> {
        for (final Bucket bucket : missing)
          database.transaction(() -> {
            final Index created = schema.createBucketIndex(type, typeIndex.getKeyTypes(), bucket, type.getName(), typeIndex.getType(),
                typeIndex.isUnique(), typeIndex.getPageSizeForNewFile(), typeIndex.getNullStrategy(), null,
                typeIndex.getPropertyNames().toArray(new String[0]), typeIndex, IndexBuilder.BUILD_BATCH_SIZE,
                typeIndex.getMetadataForNewFile(), false);
            if (!(created instanceof LSMTreeIndex lsm))
              throw new IndexException("Cannot repair index '" + typeIndex.getName() + "': the bucket index created for bucket '"
                  + bucket.getName() + "' is not an LSM_TREE index");
            // set before the transaction ends, which is when the schema entry is written
            lsm.setAwaitingFill(true);
            // created parked UNAVAILABLE; readable while empty, as the type index does not count it anyway
            lsm.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.UNAVAILABLE }, IndexInternal.INDEX_STATUS.AVAILABLE);
            typeIndex.refreshCompleteness();
          }, false, maxAttempts, null, null);
        return null;
      });
    }
  }

  private static IndexInternal[] awaitingFillFirst(final IndexInternal[] onBuckets) {
    final IndexInternal[] ordered = new IndexInternal[onBuckets.length];
    int next = 0;
    for (final IndexInternal onBucket : onBuckets)
      if (onBucket instanceof LSMTreeIndex lsm && lsm.isAwaitingFill())
        ordered[next++] = onBucket;
    for (final IndexInternal onBucket : onBuckets)
      if (!(onBucket instanceof LSMTreeIndex lsm && lsm.isAwaitingFill()))
        ordered[next++] = onBucket;
    return ordered;
  }

  private static void rebuildBucketIndex(final DatabaseInternal database, final LSMTreeIndex index,
      final Index.BuildIndexCallback callback, final AtomicLong indexed, final int maxAttempts) {
    for (int attempt = 1; ; attempt++) {
      final long indexedBefore = indexed.get();
      try {
        rebuildBucketIndexOnce(database, index, callback, indexed);
        return;
      } catch (final NeedRetryException e) {
        // nothing was swapped: the index still has its previous content, and the count of this attempt is taken back
        indexed.set(indexedBefore);
        if (attempt >= maxAttempts)
          throw e;
        try {
          Thread.sleep(200L + 200L * attempt);
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
  }

  private static void rebuildBucketIndexOnce(final DatabaseInternal database, final LSMTreeIndex index,
      final Index.BuildIndexCallback callback, final AtomicLong indexed) {
    if (database.getPageManager().isPageFlushingSuspended(database))
      throw new NeedRetryException("Cannot rebuild the index '" + index.getName() + "' while page flushing is suspended (backup in progress?)");

    final LocalSchema schema = database.getSchema().getEmbedded();
    final Bucket bucket = schema.getBucketById(index.getAssociatedBucketId());

    // Taken before the read lock and the commit locks, the order a schema frame takes it in: parking the workers waits for the
    // batch each one holds to commit, and that commit may need the bucket's commit lock
    try (final AsyncQuiesce asyncPaused = database.quiesceAsync()) {
      database.executeInReadLock(() -> {
        // Marked as compacting for the duration: a compaction of the same index (scheduled or explicit) and a DROP INDEX are
        // refused meanwhile instead of racing the swap
        if (!index.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.AVAILABLE,
            IndexInternal.INDEX_STATUS.COMPACTION_SCHEDULED }, IndexInternal.INDEX_STATUS.COMPACTION_IN_PROGRESS))
          throw new NeedRetryException("Cannot rebuild the index '" + index.getName() + "' while it is being compacted");
        try {
          final List<Integer> fileIds = new ArrayList<>(index.getFileIds());
          fileIds.add(bucket.getFileId());
          if (schema.getTypeByBucketId(bucket.getFileId()) instanceof LocalDocumentType owner) {
            final Integer externalBucketId = owner.getExternalBucketIdFor(bucket.getFileId());
            if (externalBucketId != null)
              fileIds.add(externalBucketId);
          }

          // Requested by the THREAD, not by a transaction: the swap (LSMTreeIndex.splitIndex) and the retirement of the previous
          // compacted file ask for these locks again as the thread, and find them already held
          final long timeout = database.getConfiguration().getValueAsLong(GlobalConfiguration.COMMIT_LOCK_TIMEOUT);
          final List<Integer> locked = database.getTransactionManager().tryLockFiles(fileIds, timeout, Thread.currentThread());
          try {
            scanAndSwap(database, schema, index, bucket, callback, indexed);
          } finally {
            database.getTransactionManager().unlockFilesInOrder(locked, Thread.currentThread());
          }
        } finally {
          index.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.COMPACTION_IN_PROGRESS },
              IndexInternal.INDEX_STATUS.AVAILABLE);
        }
        return null;
      });
    }
  }

  private static void scanAndSwap(final DatabaseInternal database, final LocalSchema schema, final LSMTreeIndex index,
      final Bucket bucket, final Index.BuildIndexCallback callback, final AtomicLong indexed) {
    final List<String> propertyNames = index.getPropertyNames();
    // Only the spill workspace is left to clean after a crash: see SortedIndexBuildRecoveryMarker.create(..., inPlace)
    final SortedIndexBuildRecoveryMarker marker = SortedIndexBuildRecoveryMarker.create(database, index.getTypeName(), propertyNames,
        null, true);
    try {
      try (final LSMTreeIndexBulkLoader loader = new LSMTreeIndexBulkLoader(database, index.getName(), 0L, null,
          TypeIndexBuilder.DEFAULT_SORTED_BUILD_MERGE_FAN_IN, marker.getSpillWorkspace(), 1)) {
        loader.replaceEntriesOf(index);

        database.transaction(() -> database.scanBucket(bucket.getName(), record -> {
          final Document document = (Document) record;
          loader.add(index, document);
          final long total = indexed.incrementAndGet();
          if (callback != null)
            callback.onDocumentIndexed(document, total);
          return true;
        }, (rid, exception) -> {
          if (exception instanceof RuntimeException runtimeException)
            throw runtimeException;
          throw new IndexException("Error reading record " + rid + " while rebuilding index '" + index.getName() + "'", exception);
        }), false, 1, null, null);

        loader.writeCompacted();
      }
    } finally {
      try {
        marker.clear();
      } catch (final RuntimeException e) {
        LogManager.instance().log(InPlaceIndexRebuild.class, Level.WARNING, "Cannot remove the rebuild workspace of index '%s'", e,
            index.getName());
      }
    }
  }
}
