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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.async.AsyncQuiesce;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexBuildTestHooks;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.index.geospatial.LSMTreeGeoIndex;
import com.arcadedb.index.hash.HashIndex;
import com.arcadedb.index.hash.HashIndexBucket;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.index.lsm.LSMTreeIndexBulkLoader;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * What {@link InPlaceIndexRebuild} needs to know about one index kind: whether a bucket index of that kind can be rebuilt in place,
 * how its new content is built from the records of its bucket and swapped in, and what is left to do once every bucket index of a
 * type index was rebuilt. The locking, the retries, the repair of missing bucket indexes and the order of the buckets are the same
 * for every kind and stay in {@link InPlaceIndexRebuild}: while {@link #scanAndSwap} runs, the caller holds the database read lock
 * and the commit locks of the bucket, of its external bucket and of the files {@link IndexInternal#getFileIds()} lists, and the
 * bucket index is marked {@code COMPACTION_IN_PROGRESS}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
abstract class InPlaceRebuildStrategy {

  /** The strategy that rebuilds {@code index}, a bucket index, in place, or null when its kind is always dropped and re-created. */
  static InPlaceRebuildStrategy of(final Index index) {
    if (index == null)
      return null;
    if (index.getClass() == LSMTreeIndex.class && index.getType() == Schema.INDEX_TYPE.LSM_TREE)
      return LSMTreeStrategy.INSTANCE;
    if (index.getClass() == LSMTreeGeoIndex.class && index.getType() == Schema.INDEX_TYPE.GEOSPATIAL)
      return GeospatialStrategy.INSTANCE;
    if (index.getClass() == LSMTreeFullTextIndex.class && index.getType() == Schema.INDEX_TYPE.FULL_TEXT)
      return FullTextStrategy.INSTANCE;
    if (index.getClass() == HashIndex.class && index.getType() == Schema.INDEX_TYPE.HASH)
      return HashStrategy.INSTANCE;
    if (index.getClass() == LSMVectorIndex.class && index.getType() == Schema.INDEX_TYPE.LSM_VECTOR)
      return VectorStrategy.INSTANCE;
    return null;
  }

  /** The name of the kind, for the messages. */
  abstract String kindName();

  /**
   * Why {@code index}, a bucket index of this kind, still has to be dropped and re-created, or null when it can be rebuilt in place.
   * A REBUILD that is meant to change what the index stores (a layout upgrade) is the typical reason.
   */
  String refusal(final IndexInternal index) {
    return null;
  }

  /**
   * Builds the new content of {@code index} from the records of {@code bucket} and swaps it in. A crash at any point must leave the
   * index with its previous content or its new one.
   */
  abstract void scanAndSwap(DatabaseInternal database, IndexInternal index, Bucket bucket, Index.BuildIndexCallback callback,
      AtomicLong indexed);

  /** Runs once {@code index}, a bucket index, was rebuilt, outside the locks of {@link #scanAndSwap}. */
  void afterBucketIndexRebuilt(final DatabaseInternal database, final IndexInternal index) {
  }

  /**
   * Runs once a type index had its bucket indexes rebuilt, outside the locks of {@link #scanAndSwap}.
   *
   * @param bucketIndexesFilled some bucket indexes were created for buckets that had none and filled: the records of those buckets
   *                            were not indexed before
   */
  void afterTypeIndexRebuilt(final DatabaseInternal database, final TypeIndex typeIndex, final boolean bucketIndexesFilled) {
  }

  /** Refuses a key two records of different buckets share, for a UNIQUE type index of more than one bucket. */
  void verifyUniqueAcrossBuckets(final DatabaseInternal database, final TypeIndex typeIndex, final int maxAttempts) {
    TypeIndexBuilder.verifyUniqueKeys(database, typeIndex, maxAttempts, true);
  }

  /**
   * The kinds that store their entries in an LSM tree: the records are scanned and their entries sorted by
   * {@link LSMTreeIndexBulkLoader}, written into a new compacted file and swapped in as a full compaction swaps its result, with an
   * empty mutable file in front. A kind tells only how a record turns into entries ({@link #addEntries}).
   */
  abstract static class LSMTreeBacked extends InPlaceRebuildStrategy {
    /** The LSM tree the entries of {@code index} are stored in. */
    abstract LSMTreeIndex lsmTreeOf(IndexInternal index);

    /** Gives the loader the entries {@code record} has in {@code index}, exactly the ones a write of the record puts. */
    abstract void addEntries(DatabaseInternal database, LSMTreeIndexBulkLoader loader, IndexInternal index, LSMTreeIndex lsmTree,
        Document record);

    @Override
    void scanAndSwap(final DatabaseInternal database, final IndexInternal index, final Bucket bucket,
        final Index.BuildIndexCallback callback, final AtomicLong indexed) {
      final LSMTreeIndex lsmTree = lsmTreeOf(index);
      final List<String> propertyNames = index.getPropertyNames();
      // Only the spill workspace is left to clean after a crash: see SortedIndexBuildRecoveryMarker.create(..., inPlace)
      final SortedIndexBuildRecoveryMarker marker = SortedIndexBuildRecoveryMarker.create(database, index.getTypeName(),
          propertyNames, null, true);
      try {
        try (final LSMTreeIndexBulkLoader loader = new LSMTreeIndexBulkLoader(database, index.getName(), 0L, null,
            TypeIndexBuilder.DEFAULT_SORTED_BUILD_MERGE_FAN_IN, marker.getSpillWorkspace(), 1)) {
          loader.replaceEntriesOf(lsmTree);

          database.transaction(() -> database.scanBucket(bucket.getName(), record -> {
            final Document document = (Document) record;
            addEntries(database, loader, index, lsmTree, document);
            final long total = indexed.incrementAndGet();
            IndexBuildTestHooks.recordIndexed(index, document.getIdentity());
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
          LogManager.instance().log(InPlaceRebuildStrategy.class, Level.WARNING, "Cannot remove the rebuild workspace of index '%s'", e,
              index.getName());
        }
      }
    }
  }

  /** A plain LSM_TREE index: the entries are the keys of the indexed properties. */
  static final class LSMTreeStrategy extends LSMTreeBacked {
    static final LSMTreeStrategy INSTANCE = new LSMTreeStrategy();

    @Override
    String kindName() {
      return "LSM_TREE";
    }

    @Override
    LSMTreeIndex lsmTreeOf(final IndexInternal index) {
      return (LSMTreeIndex) index;
    }

    @Override
    void addEntries(final DatabaseInternal database, final LSMTreeIndexBulkLoader loader, final IndexInternal index,
        final LSMTreeIndex lsmTree, final Document record) {
      loader.add(lsmTree, record);
    }
  }

  /**
   * A GEOSPATIAL index: the entries are the GeoHash cells of the shape, tokenized with the precision, the cell layout and the planar
   * setting of the index ({@link LSMTreeGeoIndex#forEachBulkEntry}). An index a REBUILD INDEX is meant to move to the current layout
   * keeps the drop-and-create, which builds it with the current settings.
   */
  static final class GeospatialStrategy extends LSMTreeBacked {
    static final GeospatialStrategy INSTANCE = new GeospatialStrategy();

    @Override
    String kindName() {
      return "GEOSPATIAL";
    }

    @Override
    String refusal(final IndexInternal index) {
      return ((LSMTreeGeoIndex) index).isUpgradedByRebuild() ?
          "the geospatial index '" + index.getName() + "' is in a legacy layout, which REBUILD INDEX upgrades by creating it again" :
          null;
    }

    @Override
    LSMTreeIndex lsmTreeOf(final IndexInternal index) {
      return LocalSchema.lsmTreeIndexOf(index);
    }

    @Override
    void addEntries(final DatabaseInternal database, final LSMTreeIndexBulkLoader loader, final IndexInternal index,
        final LSMTreeIndex lsmTree, final Document record) {
      final LSMTreeGeoIndex geoIndex = (LSMTreeGeoIndex) index;
      final RID rid = record.getIdentity();
      database.getIndexer().forEachIndexKey(geoIndex, record,
          keys -> geoIndex.forEachBulkEntry(keys, rid, (entryKeys, value) -> loader.addEntry(lsmTree, entryKeys, value)));
    }
  }

  /**
   * A FULL_TEXT index: the entries are the tokens of the analyzer of the index, field-prefixed and unprefixed on a multi-property
   * index, and on a BM25 index every posting carries its term frequency and the length of its document
   * ({@link LSMTreeFullTextIndex#forEachBulkEntry}), which the spill runs and the compacted file keep.
   * <p>
   * The BM25 corpus counters are type-wide, shared by every bucket index, and a rebuild of the postings does not change the corpus: they
   * are kept as they are when they are trustworthy, and recomputed once, at the end, when they are not or when some bucket was not
   * indexed before.
   */
  static final class FullTextStrategy extends LSMTreeBacked {
    static final FullTextStrategy INSTANCE = new FullTextStrategy();

    @Override
    String kindName() {
      return "FULL_TEXT";
    }

    @Override
    String refusal(final IndexInternal index) {
      final LSMTreeFullTextIndex fullText = (LSMTreeFullTextIndex) index;
      final FullTextIndexMetadata metadata = fullText.getFullTextMetadata();
      // the postings are stored with or without their statistics by the setting of the file: a similarity changed since needs new files
      if (metadata != null && metadata.isBM25() != fullText.isBM25())
        return "the similarity of the full-text index '" + index.getName() + "' changed since its postings were written";
      return null;
    }

    @Override
    LSMTreeIndex lsmTreeOf(final IndexInternal index) {
      return LocalSchema.lsmTreeIndexOf(index);
    }

    @Override
    void addEntries(final DatabaseInternal database, final LSMTreeIndexBulkLoader loader, final IndexInternal index,
        final LSMTreeIndex lsmTree, final Document record) {
      final LSMTreeFullTextIndex fullText = (LSMTreeFullTextIndex) index;
      final RID rid = record.getIdentity();
      database.getIndexer().forEachIndexKey(fullText, record,
          keys -> fullText.forEachBulkEntry(keys, rid, (entryKeys, value) -> loader.addEntry(lsmTree, entryKeys, value)));
    }

    @Override
    void afterTypeIndexRebuilt(final DatabaseInternal database, final TypeIndex typeIndex, final boolean bucketIndexesFilled) {
      final IndexInternal[] onBuckets = typeIndex.getIndexesOnBuckets();
      if (onBuckets.length == 0 || !(onBuckets[0] instanceof LSMTreeFullTextIndex fullText) || !fullText.isBM25())
        return;
      final FullTextIndexMetadata metadata = fullText.getFullTextMetadata();
      if (metadata == null)
        return;
      if (!bucketIndexesFilled && metadata.isCountersValid() && !metadata.isRescanRequired() && !metadata.isMeasuredWithAnotherAnalyzer())
        return;
      // Once for the type, as REBUILD INDEX ... WITH statsOnly = true does: the scan reads every bucket the counters count, with the
      // async writers parked so none of their batches lands half counted
      try (final AsyncQuiesce asyncPaused = database.quiesceAsync()) {
        fullText.recomputeBM25Counters();
      }
    }
  }

  /**
   * A HASH index: one file per bucket index, every page of it written through transactions. The new content is written into a new
   * file in the current layout (a shadow, {@link HashIndex#createShadowBucket}) by scanning the bucket in transactions of
   * {@link IndexBuilder#BUILD_BATCH_SIZE} records, and swapped in for the current file by {@link HashIndex#replaceBucket}, whose schema
   * save is the commit point. The index keeps its object and its name; an index in a legacy layout comes out in the current one.
   */
  static final class HashStrategy extends InPlaceRebuildStrategy {
    static final HashStrategy INSTANCE = new HashStrategy();

    @Override
    String kindName() {
      return "HASH";
    }

    /** A file whose metadata page is damaged cannot tell the key types to rebuild with: it is created again from its definition. */
    @Override
    String refusal(final IndexInternal index) {
      final List<String> problems = index.checkIntegrity();
      return problems.isEmpty() ? null : "the file of the hash index '" + index.getName() + "' is damaged: " + problems.getFirst();
    }

    @Override
    void scanAndSwap(final DatabaseInternal database, final IndexInternal index, final Bucket bucket,
        final Index.BuildIndexCallback callback, final AtomicLong indexed) {
      final HashIndex hashIndex = (HashIndex) index;
      final HashIndexBucket[] shadow = new HashIndexBucket[1];
      boolean swapped = false;
      try {
        database.transaction(() -> shadow[0] = hashIndex.createShadowBucket(bucket.getName() + "_" + System.nanoTime()), false, 1,
            null, null);

        final long[] inBatch = new long[1];
        database.begin();
        try {
          database.scanBucket(bucket.getName(), record -> {
            final Document document = (Document) record;
            final RID rid = document.getIdentity();
            database.getIndexer().forEachIndexKey(hashIndex, document, keys -> hashIndex.putIntoShadow(shadow[0], keys, rid));
            final long total = indexed.incrementAndGet();
            IndexBuildTestHooks.recordIndexed(index, rid);
            if (callback != null)
              callback.onDocumentIndexed(document, total);
            IndexInternal.commitBuildBatch(database, ++inBatch[0], IndexBuilder.BUILD_BATCH_SIZE, false);
            return true;
          }, (rid, exception) -> {
            if (exception instanceof RuntimeException runtimeException)
              throw runtimeException;
            throw new IndexException("Error reading record " + rid + " while rebuilding index '" + index.getName() + "'", exception);
          });
          database.commit();
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }

        hashIndex.replaceBucket(shadow[0]);
        swapped = true;
      } finally {
        if (!swapped && shadow[0] != null)
          hashIndex.dropShadowBucket(shadow[0]);
      }
    }

    /**
     * A hash index has no order to walk, so each key of a record is looked up in the bucket indexes after the one of its record: a
     * key another bucket also holds is confirmed against every bucket index before it is refused, since a commit can move a key
     * between two records while the walk runs.
     */
    @Override
    void verifyUniqueAcrossBuckets(final DatabaseInternal database, final TypeIndex typeIndex, final int maxAttempts) {
      final IndexInternal[] onBuckets = typeIndex.getIndexesOnBuckets();
      database.transaction(() -> {
        for (int i = 0; i < onBuckets.length - 1; i++) {
          final int from = i;
          final HashIndex hashIndex = (HashIndex) onBuckets[i];
          database.scanBucket(database.getSchema().getBucketById(hashIndex.getAssociatedBucketId()).getName(), record -> {
            final RID rid = record.getIdentity();
            database.getIndexer().forEachIndexKey(hashIndex, (Document) record, keys -> {
              if (LSMTreeIndexAbstract.isKeyNull(keys))
                return;
              for (int j = from + 1; j < onBuckets.length; j++) {
                final RID other = firstHolder(onBuckets[j], keys);
                if (other != null && !other.equals(rid) && isHeldByMoreThanOneRecord(onBuckets, keys))
                  throw new DuplicatedKeyException(typeIndex.getName(), Arrays.toString(keys), other);
              }
            });
            return true;
          }, (rid, exception) -> {
            if (exception instanceof RuntimeException runtimeException)
              throw runtimeException;
            throw new IndexException("Error reading record " + rid + " while checking index '" + typeIndex.getName() + "'", exception);
          });
        }
      }, false, maxAttempts, null, null);
    }

    private static RID firstHolder(final IndexInternal index, final Object[] keys) {
      final IndexCursor cursor = index.get(keys, 1);
      while (cursor.hasNext()) {
        final Identifiable next = cursor.next();
        if (next != null)
          return next.getIdentity();
      }
      return null;
    }

    private static boolean isHeldByMoreThanOneRecord(final IndexInternal[] onBuckets, final Object[] keys) {
      RID first = null;
      for (final IndexInternal onBucket : onBuckets) {
        final IndexCursor cursor = onBucket.get(keys);
        while (cursor.hasNext()) {
          final Identifiable next = cursor.next();
          if (next == null)
            continue;
          if (first == null)
            first = next.getIdentity();
          else if (!first.equals(next.getIdentity()))
            return true;
        }
      }
      return false;
    }
  }

  /**
   * An LSM_VECTOR index is not built again into new files: it is made to hold exactly what a build from the records puts
   * ({@link LSMVectorIndex#reconcileWithBucket}), so the records it already holds right stay in its resident graph and keep being
   * found, and its file is then compacted, which writes it again and builds a fresh graph while the resident one keeps answering
   * ({@link LSMVectorIndex#compactAfterRebuild}). An index whose vector ids cannot be trusted, or that was never built to the end, is
   * created again.
   */
  static final class VectorStrategy extends InPlaceRebuildStrategy {
    static final VectorStrategy INSTANCE = new VectorStrategy();

    @Override
    String kindName() {
      return "LSM_VECTOR";
    }

    @Override
    String refusal(final IndexInternal index) {
      return ((LSMVectorIndex) index).inPlaceRebuildRefusal();
    }

    @Override
    void scanAndSwap(final DatabaseInternal database, final IndexInternal index, final Bucket bucket,
        final Index.BuildIndexCallback callback, final AtomicLong indexed) {
      final long changed = ((LSMVectorIndex) index).reconcileWithBucket(IndexBuilder.BUILD_BATCH_SIZE, (document, total) -> {
        final long all = indexed.incrementAndGet();
        if (callback != null)
          callback.onDocumentIndexed(document, all);
      });
      LogManager.instance().log(InPlaceRebuildStrategy.class, Level.INFO, "Rebuilt vector index '%s' in place: %,d records written again",
          index.getName(), changed);
    }

    @Override
    void afterBucketIndexRebuilt(final DatabaseInternal database, final IndexInternal index) {
      try {
        ((LSMVectorIndex) index).compactAfterRebuild();
      } catch (final IOException | RuntimeException e) {
        // the index holds every record: only the space of the replaced entries and the fresh graph wait for the next compaction
        LogManager.instance().log(InPlaceRebuildStrategy.class, Level.WARNING,
            "Vector index '%s' was rebuilt, but its compaction did not run: %s", index.getName(), e.getMessage());
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    void verifyUniqueAcrossBuckets(final DatabaseInternal database, final TypeIndex typeIndex, final int maxAttempts) {
      // a vector index is never unique
    }
  }
}
