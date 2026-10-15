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
package com.arcadedb.index;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.IndexCursorCollection;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * It represent an index on a type. It's backed by one or multiple underlying indexes, one per bucket. By using multiple buckets, the read/write operation can
 * work concurrently and lock-free.
 *
 * @author Luca Garulli
 */
public class TypeIndex implements RangeIndex, IndexInternal {
  private       String              logicName;
  // Copy-on-write: a query plans and reads while DDL on the same type adds, removes or clears sub-indexes
  private final List<IndexInternal> indexesOnBuckets = new CopyOnWriteArrayList<>();
  private final DocumentType        type;
  private volatile boolean          valid            = true;
  // Raised by drop() before it takes the first sub-index away, while valid is still true: a reader can tell that what it read
  // through the index may be missing the sub-indexes already gone (#9651)
  private volatile boolean          dropping         = false;
  // Moved around every change of indexesOnBuckets, odd while one is under way: what getForQuery() checks before and after a
  // read to know nobody changed the sub-indexes under it, without copying the list on a hot path (#9811). The changes come from
  // DDL, which runs under the schema's write lock, so one writer at a time; the before/after comparison alone would still catch
  // overlapping writers, only the "odd means under way" shortcut relies on there being one
  private final AtomicLong          subIndexChanges  = new AtomicLong();
  private       IndexInternal       associatedIndex;
  private       IndexMetadata       metadata;

  // #8153: CACHED BY keyedCursor(), SEE THERE
  private BinaryComparator comparator;

  public TypeIndex(final String logicName, final DocumentType type) {
    this.logicName = logicName;
    this.type = type;
  }

  @Override
  public long countEntries() {
    checkIsValid();
    long total = 0;
    for (final IndexInternal index : indexesOnBuckets)
      total += index.countEntries();
    return total;
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder) {
    checkIsValid();
    if (!supportsOrderedIterations())
      throw new UnsupportedOperationException("Index '" + getName() + "' does not support ordered iterations");

    return new MultiIndexCursor(subIndexesToRead(), ascendingOrder, -1);
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys, final boolean inclusive) {
    checkIsValid();
    if (!supportsOrderedIterations())
      throw new UnsupportedOperationException("Index '" + getName() + "' does not support ordered iterations");

    return new MultiIndexCursor(subIndexesToRead(), fromKeys, ascendingOrder, inclusive, -1);
  }

  /**
   * The sub-indexes a cursor reads, as one snapshot. The live list is copy-on-write, so a loop over it already walks a snapshot,
   * but a cursor that asked the list again afterwards (for its first element's key types) read it as a concurrent drop left it:
   * emptied, which raised a {@link NoSuchElementException} that a Gremlin traversal takes for the end of the stream,
   * answering an empty result as a success (#9651). An index with no sub-index left is being dropped, and says so the way
   * every reader already expects.
   */
  private List<IndexInternal> subIndexesToRead() {
    final List<IndexInternal> snapshot = List.copyOf(indexesOnBuckets);
    if (snapshot.isEmpty())
      throw new IndexException("Index '" + getName() + "' is not valid. Probably has been drop or rebuilt");
    return snapshot;
  }

  @Override
  public IndexCursor range(final boolean ascending, final Object[] beginKeys, final boolean beginKeysInclusive,
      final Object[] endKeys, final boolean endKeysInclusive) {
    checkIsValid();
    if (!supportsOrderedIterations())
      throw new UnsupportedOperationException("Index '" + getName() + "' does not support ordered iterations");

    final List<IndexInternal> subIndexes = subIndexesToRead();
    final List<IndexCursor> cursors = new ArrayList<>(subIndexes.size());
    try {
      for (final Index index : subIndexes)
        cursors.add(((RangeIndex) index).range(ascending, beginKeys, beginKeysInclusive, endKeys, endKeysInclusive));

      return new MultiIndexCursor(cursors, -1, ascending);
    } catch (final RuntimeException e) {
      // #5662: nothing owns the per-bucket cursors until the MultiIndexCursor is built, so a failure partway through
      // would abandon the ones already opened. The null guard matters: MultiIndexCursor keeps THIS list rather than
      // copying it, and a constructor that fails has already closed the children and nulled their slots - without the
      // guard the cleanup would throw a NullPointerException over the exception that is being propagated.
      for (final IndexCursor cursor : cursors)
        if (cursor != null)
          cursor.close();
      throw e;
    }
  }

  /**
   * {@link #get(Object[])} for a reader that answers a client with what it finds - a protocol's key lookup, a query - rather
   * than for index maintenance (issue #9811). It answers only a lookup that read the whole index, and raises the
   * {@link IndexException} every guarded reader already treats as "read again" ({@link #isStaleRead}) otherwise:
   * <ul>
   * <li>before the read, an index not {@link #isReadyForQueries() ready}: one being created or rebuilt holds the entries of
   * the buckets built so far only, one being dropped is losing its sub-indexes;</li>
   * <li>after the read, an index that is no longer the one it read (dropped, or its sub-indexes changed): a drop takes the sub-indexes
   * away one at a time while the index still answers as valid, so a lookup in that window comes back short - empty, for a
   * key that exists - without any error;</li>
   * <li>during the read, the page manager's "File with id n was not found" of a sub-index a drop deleted under it.</li>
   * </ul>
   * {@link #get(Object[])} itself stays raw: the uniqueness check at commit reads through it, and a commit must not fail
   * because an index it does not even need any more is being dropped. The check after the read is sound because every arm of
   * {@code get} collects its entries before it returns. It costs two reads of a counter, and no copy of the sub-index list.
   */
  public IndexCursor getForQuery(final Object[] keys) {
    if (!isReadyForQueries())
      throw new IndexException("Index '" + getName() + "' is not ready for queries: it is being built, rebuilt or dropped");

    final long changesBefore = subIndexChanges.get();
    if ((changesBefore & 1) != 0)
      throw new IndexException("Index '" + getName() + "' is being changed by a concurrent DDL");
    final IndexCursor cursor;
    try {
      cursor = get(keys);
    } catch (final IndexException e) {
      throw e;
    } catch (final RuntimeException e) {
      if (isStaleRead(this, e))
        throw new IndexException("Index '" + getName() + "' was dropped or rebuilt while it was read", e);
      throw e;
    }

    if (!valid || dropping || subIndexChanges.get() != changesBefore) {
      cursor.close();
      throw new IndexException("Index '" + getName() + "' was dropped or rebuilt while it was read");
    }
    return cursor;
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    checkIsValid();

    // Check if this is a full-text index to preserve scores
    final IndexInternal first = firstOrNull();
    final boolean isFullText = first != null && first.getType() == Schema.INDEX_TYPE.FULL_TEXT;

    if (isFullText) {
      if (first instanceof final LSMTreeFullTextIndex fullTextIndex && fullTextIndex.isBM25())
        return FullTextSearch.searchSimple(this, keys, -1);

      // For full-text indexes, collect entries with scores
      final List<IndexCursorEntry> entries = new ArrayList<>();

      for (final Index index : getIndexesByKeys(keys)) {
        try (final IndexCursor cursor = index.get(keys, -1)) {
          while (cursor.hasNext()) {
            final Identifiable record = cursor.next();
            final int score = cursor.getScore();
            entries.add(new IndexCursorEntry(keys, record, score));
          }
        }
      }

      // Sort by score descending
      if (entries.size() > 1)
        entries.sort((o1, o2) -> Integer.compare(o2.score, o1.score));

      return new TempIndexCursor(entries);
    } else {
      // For non-full-text indexes, use the original implementation
      Set<Identifiable> result = null;

      for (final Index index : getIndexesByKeys(keys)) {
        // an all-null key is exempt from uniqueness, so a unique index holds an entry per record that has it (issue #9237)
        final boolean unique = index.isUnique() && !LSMTreeIndexAbstract.isKeyNull(keys);

        // #5662: try-with-resources - the unique branch returns from inside the loop, abandoning the cursor partway
        try (final IndexCursor cursor = index.get(keys, unique ? 1 : -1)) {
          while (cursor.hasNext()) {
            if (unique) {
              result = Set.of(cursor.next());
              return keyedCursor(result, keys);
            }

            if (result == null)
              result = new HashSet<>();
            result.add(cursor.next());
          }
        }
      }
      return keyedCursor(result != null ? result : Collections.emptyList(), keys);
    }
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    checkIsValid();

    // Check if this is a full-text index to preserve scores
    final IndexInternal first = firstOrNull();
    final boolean isFullText = first != null && first.getType() == Schema.INDEX_TYPE.FULL_TEXT;

    if (isFullText) {
      if (first instanceof final LSMTreeFullTextIndex fullTextIndex && fullTextIndex.isBM25())
        return FullTextSearch.searchSimple(this, keys, limit);

      // For full-text indexes, collect entries with scores
      final List<IndexCursorEntry> entries = new ArrayList<>();

      for (final Index index : getIndexesByKeys(keys)) {
        // #5662: try-with-resources - the limit breaks out of the loop, abandoning the cursor partway
        try (final IndexCursor cursor = index.get(keys, limit > -1 ? limit - entries.size() : -1)) {
          while (cursor.hasNext()) {
            final Identifiable record = cursor.next();
            final int score = cursor.getScore();
            entries.add(new IndexCursorEntry(keys, record, score));

            if (limit > -1 && entries.size() >= limit)
              break;
          }
        }
        if (limit > -1 && entries.size() >= limit)
          break;
      }

      // Sort by score descending
      if (entries.size() > 1)
        entries.sort((o1, o2) -> Integer.compare(o2.score, o1.score));

      // Apply limit after sorting
      if (limit > -1 && entries.size() > limit)
        return new TempIndexCursor(entries.subList(0, limit));

      return new TempIndexCursor(entries);
    } else {
      // For non-full-text indexes, use the original implementation.
      // An index whose results are APPROXIMATE (a spatial grid: a cell hit is a candidate, not a match) cannot have a
      // row limit applied to its output - truncating candidates before the caller's predicate re-checks them silently
      // drops rows that would have survived it. Such an index is asked for, and returns, everything.
      final int effectiveLimit = isResultApproximate() ? -1 : limit;
      if (effectiveLimit == 0)
        // THE LOOP BELOW ADDS AN ENTRY BEFORE IT CHECKS THE BOUND, AND A CHILD ASKED FOR 0 ROWS MAY STILL ANSWER ONE
        return keyedCursor(Collections.emptyList(), keys);

      Set<Identifiable> result = null;

      for (final Index index : getIndexesByKeys(keys)) {
        // #5662: try-with-resources - the limit returns from inside the loop, abandoning the cursor partway
        // ONLY THE ROWS STILL MISSING ARE ASKED OF THIS BUCKET: THE OLD size - limit WAS NEGATIVE, I.E. UNLIMITED
        try (final IndexCursor cursor = index.get(keys,
            effectiveLimit > -1 ? effectiveLimit - (result != null ? result.size() : 0) : -1)) {
          while (cursor.hasNext()) {
            if (result == null)
              result = effectiveLimit > -1 ? new HashSet<>(effectiveLimit) : new HashSet<>();

            result.add(cursor.next());

            if (effectiveLimit > -1 && result.size() >= effectiveLimit)
              return keyedCursor(result, keys);
          }
        }
      }
      return keyedCursor(result != null ? result : Collections.emptyList(), keys);
    }
  }

  /** The entries of the set, at most {@code limit} of them when the limit is set (-1 for none) */
  public static Set<IndexCursorEntry> cappedTo(final Set<IndexCursorEntry> entries, final int limit) {
    if (limit < 0 || entries.size() <= limit)
      return entries;
    final Set<IndexCursorEntry> capped = new HashSet<>(limit * 2);
    for (final IndexCursorEntry entry : entries) {
      if (capped.size() >= limit)
        break;
      capped.add(entry);
    }
    return capped;
  }

  /**
   * #8153: an equality lookup answers with the key it was asked for and the key types and comparator of this index, like
   * every range cursor over it, so a {@link MultiIndexCursor} merging both kinds can compare them.
   */
  private IndexCursor keyedCursor(final Collection<Identifiable> result, final Object[] keys) {
    final IndexInternal first = firstOrNull();
    if (first == null)
      return new IndexCursorCollection(result);
    BinaryComparator cmp = comparator;
    if (cmp == null)
      // RESOLVED ONCE: THE SERIALIZER, AND SO ITS COMPARATOR, LIVES AS LONG AS THE DATABASE. A RACE ONLY RESOLVES IT TWICE
      comparator = cmp = ((DatabaseInternal) type.getSchema().getEmbedded().getDatabase()).getSerializer().getComparator();
    return new IndexCursorCollection(result, keys, first.getBinaryKeyTypes(), cmp);
  }

  @Override
  public void put(final Object[] keys, final RID[] rid) {
    throw new UnsupportedOperationException("put");
  }

  @Override
  public void remove(final Object[] keys) {
    checkIsValid();
    for (final Index index : getIndexesByKeys(keys))
      index.remove(keys);
  }

  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    checkIsValid();
    for (final Index index : getIndexesByKeys(keys))
      index.remove(keys, rid);
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {
    checkIsValid();
    boolean result = false;
    for (final IndexInternal index : indexesOnBuckets)
      if (index.compact())
        result = true;
    return result;
  }

  @Override
  public boolean isCompacting() {
    checkIsValid();
    for (final IndexInternal index : indexesOnBuckets)
      if (index.isCompacting())
        return true;
    return false;
  }

  @Override
  public boolean scheduleCompaction() {
    checkIsValid();
    for (final IndexInternal index : indexesOnBuckets)
      if (!index.scheduleCompaction())
        return false;

    return true;
  }

  @Override
  public String getMostRecentFileName() {
    return getFirstUnderlyingIndex().getMostRecentFileName();
  }

  @Override
  public Schema.INDEX_TYPE getType() {
    checkIsValid();
    final IndexInternal first = firstOrNull();
    return first == null ? null : first.getType();
  }

  @Override
  public String getTypeName() {
    return type.getName();
  }

  @Override
  public List<String> getPropertyNames() {
    checkIsValid();
    return getFirstUnderlyingIndex().getPropertyNames();
  }

  @Override
  public void flush() {
    checkIsValid();
    for (final IndexInternal index : indexesOnBuckets)
      index.flush();
  }

  @Override
  public void close() {
    checkIsValid();
    for (final IndexInternal index : indexesOnBuckets)
      index.close();
  }

  @Override
  public JSONObject toJSON() {
    return getFirstUnderlyingIndex().toJSON();
  }

  @Override
  public String getUpgradeWarning() {
    // A definition-derived warning is the same on every bucket sub-index, but a PHYSICAL one - a key order that no
    // longer matches the comparator (#5802) - is raised per bucket, and asking only the first one would report a type
    // index healthy while another of its buckets needs a rebuild. The first answer found wins: they read alike, and
    // the remedy is one REBUILD INDEX over the whole logical index either way.
    for (final IndexInternal index : indexesOnBuckets) {
      final String warning = index.getUpgradeWarning();
      if (warning != null)
        return warning;
    }
    return null;
  }

  @Override
  public boolean isResultApproximate() {
    // Same definition across every bucket sub-index, so the first one answers for all of them.
    final IndexInternal first = firstOrNull();
    return first != null && first.isResultApproximate();
  }

  @Override
  public void drop() {
    if (!valid)
      // Already dropped. The schema's leaf-drop path (LocalSchema#dropIndex) auto-removes a wrapper
      // whose last bucket child has just been removed (#4179). When a caller still holds a reference
      // to a wrapper that has since been auto-removed and explicitly calls drop(), there is nothing
      // left to clean up - the underlying bucket indexes were already dropped through the leaf path.
      // Throwing here would surface a confusing "is not valid" error for an idempotent operation.
      return;

    dropping = true;
    final List<IndexInternal> acquired = new ArrayList<>(indexesOnBuckets.size());
    try {
      for (final IndexInternal index : new ArrayList<>(indexesOnBuckets))
        if (index.setStatus(new INDEX_STATUS[] { INDEX_STATUS.AVAILABLE, INDEX_STATUS.UNAVAILABLE }, INDEX_STATUS.UNAVAILABLE))
          acquired.add(index);
        else
          throw new NeedRetryException(
              "Cannot drop index '" + getName() + "' because one or more underlying files are not available");
    } catch (final RuntimeException | Error e) {
      // NOTHING WAS TAKEN AWAY: RESET THE ACQUIRED STATUSES AND THE FLAG, THE INDEX ANSWERS QUERIES AGAIN
      for (final IndexInternal i : acquired)
        i.setStatus(new INDEX_STATUS[] { INDEX_STATUS.UNAVAILABLE }, INDEX_STATUS.AVAILABLE);
      dropping = false;
      throw e;
    }

    // Order: sub-indexes dropped, then valid = false, then the list cleared. The wrapper must stay valid during the first step
    // (LocalSchema#dropIndex resolves and unregisters the wrapper through its sub-indexes), so until valid = false a reader
    // sees a valid wrapper with some sub-indexes missing: it usually gets an IndexException or a null type, which the callers
    // treat as "not a candidate". A query that already chose this index and reads it in that instant can miss the rows of the
    // sub-indexes already dropped: the index is going away, and that query was planned before the drop completed.
    // A failure from here on leaves `dropping` raised on purpose: some sub-indexes may be gone, and an index missing part of
    // its entries must not answer a query as if it were whole. The drop is retried, and only a completed one clears the
    // wrapper (valid = false)
    try {
      for (final Index index : new ArrayList<>(indexesOnBuckets))
        type.getSchema().dropIndex(index.getName());
    } catch (final RuntimeException | Error e) {
      LogManager.instance().log(this, Level.WARNING,
          "Drop of index '%s' failed after removing part of its sub-indexes: it stays unusable by queries until the drop is run again",
          e, getName());
      throw e;
    }

    valid = false;
    subIndexChanges.incrementAndGet();
    try {
      indexesOnBuckets.clear();
    } finally {
      subIndexChanges.incrementAndGet();
    }
  }

  @Override
  public String getName() {
    return logicName;
  }

  @Override
  public Map<String, Long> getStats() {
    checkIsValid();
    final Map<String, Long> stats = new HashMap<>();
    for (final Index index : indexesOnBuckets)
      stats.putAll(((IndexInternal) index).getStats());
    return stats;
  }

  @Override
  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    checkIsValid();
    return getFirstUnderlyingIndex().getNullStrategy();
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    checkIsValid();
    // each bucket index enforces its own strategy, and the record lands in the bucket chosen by the bucket selection strategy
    for (final IndexInternal index : indexesOnBuckets)
      index.setNullStrategy(nullStrategy);
  }

  @Override
  public boolean isUnique() {
    checkIsValid();
    return getFirstUnderlyingIndex().isUnique();
  }

  /**
   * {@link #isUnique()} for a caller that walks the indexes of a type while a concurrent DDL runs (issue #8859): an index being
   * created has no sub-index yet and one being dropped is invalid, and both make {@link #isUnique()} throw. Such an index has no
   * uniqueness to enforce for the caller, so the answer is false. Never throws.
   */
  public boolean isUniqueIfPresent() {
    if (!valid)
      return false;
    final IndexInternal first = firstOrNull();
    if (first == null)
      return false;
    try {
      return first.isUnique();
    } catch (final IndexException e) {
      return false;
    }
  }

  @Override
  public boolean supportsOrderedIterations() {
    checkIsValid();
    return getFirstUnderlyingIndex().supportsOrderedIterations();
  }

  @Override
  public boolean isAutomatic() {
    return true;
  }

  @Override
  public int getPageSize() {
    checkIsValid();
    return getFirstUnderlyingIndex().getPageSize();
  }

  @Override
  public int getPageSizeForNewFile() {
    checkIsValid();
    // Same definition across every bucket sub-index, so the first one answers for all of them.
    return getFirstUnderlyingIndex().getPageSizeForNewFile();
  }

  @Override
  public long build(final int buildIndexBatchSize, final boolean sharesCallerTransaction,
      final BuildIndexCallback callback) {
    checkIsValid();
    long total = 0;
    for (final IndexInternal index : indexesOnBuckets)
      total += index.build(buildIndexBatchSize, sharesCallerTransaction, callback);
    return total;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof TypeIndex))
      return false;

    final TypeIndex index2 = (TypeIndex) obj;

    if (!BinaryComparator.equalsString(getName(), index2.getName()))
      return false;

    final List<String> index1Properties = getPropertyNames();
    final List<String> index2Properties = index2.getPropertyNames();

    if (index1Properties.size() != index2Properties.size())
      return false;

    for (int p = 0; p < index1Properties.size(); ++p) {
      if (!index1Properties.get(p).equals(index2Properties.get(p)))
        return false;
    }

    if (indexesOnBuckets.size() != index2.indexesOnBuckets.size())
      return false;

    for (int i = 0; i < indexesOnBuckets.size(); ++i) {
      final Index bIdx1 = indexesOnBuckets.get(i);

      boolean found = false;
      for (int j = 0; j < index2.indexesOnBuckets.size(); j++) {
        final Index bIdx2 = index2.indexesOnBuckets.get(j);
        if (bIdx2.getName().equals(bIdx1.getName())) {
          found = true;
          if (bIdx1.getAssociatedBucketId() != bIdx2.getAssociatedBucketId())
            return false;

          break;
        }
      }

      if (!found)
        return false;
    }

    return true;
  }

  /**
   * Internal Only. Retrieved the underlying indexes.
   */
  public List<IndexInternal> getSubIndexes() {
    return indexesOnBuckets;
  }

  @Override
  public int hashCode() {
    return logicName.hashCode();
  }

  @Override
  public String toString() {
    return logicName;
  }

  @Override
  public void setMetadata(IndexMetadata metadata) {
    throw new UnsupportedOperationException("setMetadata");
  }

  @Override
  public void setMetadata(final JSONObject indexJSON) {
    throw new UnsupportedOperationException("applyMetadataFromSchema");
  }

  @Override
  public boolean setStatus(final INDEX_STATUS[] expectedStatuses, final INDEX_STATUS newStatus) {
    return false;
  }

  @Override
  public int getFileId() {
    return -1;
  }

  @Override
  public PaginatedComponent getComponent() {
    throw new UnsupportedOperationException("getPaginatedComponent");
  }

  @Override
  public Type[] getKeyTypes() {
    checkIsValid();
    return getFirstUnderlyingIndex().getKeyTypes();
  }

  /** Every bucket index of this type index converts keys alike: they share the definition. */
  @Override
  public Object[] convertLookupKeys(final Object[] keys) {
    checkIsValid();
    return getFirstUnderlyingIndex().convertLookupKeys(keys);
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    checkIsValid();
    return getFirstUnderlyingIndex().getBinaryKeyTypes();
  }

  @Override
  public List<Integer> getFileIds() {
    final List<Integer> ids = new ArrayList<>(indexesOnBuckets.size() * 2);
    for (final IndexInternal idx : indexesOnBuckets)
      ids.addAll(idx.getFileIds());
    return ids;
  }

  @Override
  public void setTypeIndex(final TypeIndex typeIndex) {
    throw new UnsupportedOperationException("setTypeIndex");
  }

  @Override
  public TypeIndex getTypeIndex() {
    return null;
  }

  @Override
  public IndexInternal getAssociatedIndex() {
    return associatedIndex;
  }

  @Override
  public void updateTypeName(final String newTypeName) {
    logicName = getTypeName() + getPropertyNames().toString().replace(" ", "");
    for (IndexInternal idx : indexesOnBuckets)
      idx.updateTypeName(newTypeName);
  }

  public void setAssociatedIndex(final IndexInternal associatedIndex) {
    this.associatedIndex = associatedIndex;
  }

  @Override
  public int getAssociatedBucketId() {
    return -1;
  }

  public void addIndexOnBucket(final IndexInternal index) {
    checkIsValid();
    if (index instanceof TypeIndex)
      throw new IllegalArgumentException("Invalid subIndex " + index);

    subIndexChanges.incrementAndGet();
    try {
      indexesOnBuckets.add(index);
    } finally {
      subIndexChanges.incrementAndGet();
    }
    index.setTypeIndex(this);
  }

  public void removeIndexOnBucket(final IndexInternal index) {
    checkIsValid();
    if (index instanceof TypeIndex)
      throw new IllegalArgumentException("Invalid subIndex " + index);

    subIndexChanges.incrementAndGet();
    try {
      indexesOnBuckets.remove(index);
    } finally {
      subIndexChanges.incrementAndGet();
    }
    index.setTypeIndex(null);
  }

  public IndexInternal[] getIndexesOnBuckets() {
    return indexesOnBuckets.toArray(new IndexInternal[indexesOnBuckets.size()]);
  }

  @Override
  public boolean recomputeStatistics() {
    // BM25 corpus counters are shared type-wide (the same metadata object backs every bucket sub-index), so recomputing on a
    // single bucket sub-index repairs them for the whole type; this avoids N redundant full-type scans. Returning on the first
    // success is therefore correct: every bucket sub-index of a given type shares one similarity and one metadata object, so
    // they cannot disagree on whether statistics exist.
    for (final IndexInternal idx : indexesOnBuckets)
      if (idx.recomputeStatistics())
        return true;
    return false;
  }

  public int countIndexesOnBuckets() {
    return indexesOnBuckets.size();
  }

  public List<? extends Index> getIndexesByKeys(final Object[] keys) {
    // For full-text indexes, always search all buckets regardless of bucket selection strategy.
    // Full-text queries contain search terms/phrases, not document property values, so bucket
    // selection based on hashing those keys would incorrectly query only one bucket.
    final IndexInternal first = firstOrNull();
    final boolean isFullText = first != null && first.getType() == Schema.INDEX_TYPE.FULL_TEXT;

    if (isFullText) {
      // Full-text searches must scan all buckets to ensure complete results
      return indexesOnBuckets;
    }

    // Partition mapping is stale: some records may sit outside the bucket their key hashes to
    // (issue #832 / #4087). Pruning to the single hash-target sub-index would miss them, so fan out
    // across every sub-index until a `REBUILD TYPE ... WITH repartition = true` relocates the records
    // and clears the flag. Mirrors the cluster-scan planner's gate in SelectExecutionPlanner.
    if (type instanceof LocalDocumentType ldt && ldt.isNeedsRepartition()) {
      ldt.warnIfNeedsRepartition();
      return indexesOnBuckets;
    }

    // Pass the properties these keys belong to, NOT just the values: a record is placed by hashing the type's
    // PARTITION properties, so pruning to the hash of an arbitrary key set lands on an unrelated bucket and the
    // record is silently missed (issue #5589). The strategy compares the two and returns -1 when they differ,
    // which falls through to the full fan-out below.
    final List<String> propNames = getPropertyNames();

    final boolean async =
        DatabaseContext.INSTANCE.getContext(type.getSchema().getEmbedded().getDatabase().getDatabasePath()).perThreadBucketSelection;

    final int bucketIndex = type.getBucketIndexByKeys(propNames, keys, async);

    if (bucketIndex > -1) {
      // USE THE SHARDED INDEX
      List<IndexInternal> polymorphicIndexesOnKeys = type.getPolymorphicBucketIndexByBucketId(
          type.getBuckets(false).get(bucketIndex).getFileId(), propNames);

      final List<DocumentType> subTypes = type.getSubTypes();
      if (!subTypes.isEmpty()) {
        // MODIFIABLE COPY
        polymorphicIndexesOnKeys = new ArrayList<>(polymorphicIndexesOnKeys);

        for (final DocumentType s : subTypes) {
          // `bucketIndex` is only meaningful modulo the bucket count it was computed against (the
          // parent's). A subtype is free to declare a different bucket count, so reusing it here can
          // index past the end of the subtype's own bucket list (issue #5645). Each subtype has to
          // re-derive its own placement through its own strategy and its own modulus instead.
          final int subBucketIndex = s.getBucketIndexByKeys(propNames, keys, async);
          if (subBucketIndex > -1) {
            final List<IndexInternal> subIndexes = s.getPolymorphicBucketIndexByBucketId(
                s.getBuckets(false).get(subBucketIndex).getFileId(), propNames);
            polymorphicIndexesOnKeys.addAll(subIndexes);
          } else {
            // This subtype cannot prune (not partitioned, or its strategy declines): fall back to
            // its own full bucket list rather than skip it or guess with the parent's bucket index.
            for (final Bucket subBucket : s.getBuckets(false))
              polymorphicIndexesOnKeys.addAll(s.getPolymorphicBucketIndexByBucketId(subBucket.getFileId(), propNames));
          }
        }
      }

      return polymorphicIndexesOnKeys;
    }

    // SEARCH ON ALL THE UNDERLYING INDEXES
    return indexesOnBuckets;
  }

  @Override
  public boolean isValid() {
    return valid;
  }

  /**
   * True once a drop of this index has started, even while it still answers as valid: {@link #drop()} takes the sub-indexes away
   * first and invalidates the index last, so a reader in between can read part of the index, or none of it, without an error.
   */
  public boolean isDropping() {
    return dropping;
  }

  /**
   * True when a query can use this index: it was not dropped or rebuilt and its per-bucket sub-indexes exist. DDL runs
   * concurrently with queries: an index being created is registered before its sub-indexes exist (no type yet) and one being
   * dropped stays registered for a while after it was invalidated. Best effort, the index can still go away right after the
   * call, so a caller that reads the metadata of an index must also be ready for an {@link IndexException}. Never throws.
   * <p>
   * An index still being populated by {@code TypeIndexBuilder} is not ready either (issue #9331). The answer is for the
   * planners and lookups: writes, uniqueness checks and index maintenance do not consult it.
   */
  public boolean isReadyForQueries() {
    if (!valid || dropping)
      return false;
    try {
      final IndexInternal first = firstOrNull();
      if (first == null || first.getType() == null)
        return false;
      // Complete: while TypeIndexBuilder populates it, the index holds the entries of the buckets built so far only (#9331)
      return !(type instanceof LocalDocumentType local) || !local.isIndexUnderConstruction(first.getPropertyNames());
    } catch (final IndexException e) {
      LogManager.instance().log(this, Level.FINE, "Index '%s' is not ready for queries: %s", null, getName(), e.getMessage());
      return false;
    }
  }

  /**
   * Whether {@code e} is the page manager's answer for the file of an index deleted under a lookup or an open cursor ("File with
   * id n was not found", see {@code FileManager#getFile}). The one place that knows the wording, for the readers that turn it
   * into a stale-index signal when the index is no longer valid (issue #9331).
   */
  public static boolean isFileNotFound(final IllegalArgumentException e) {
    final String message = e.getMessage();
    return message != null && message.startsWith("File with id");
  }

  /**
   * Whether a reader that read through this index since it took {@code snapshot} ({@link #getIndexesOnBuckets()}) read all of
   * it: the index is still valid, no drop has started, and it has the very sub-indexes it had then. A drop takes the
   * sub-indexes away one at a time while the index still answers as valid, so a scan running in that window can come back short
   * without any error (see {@link #drop()}); a reader that must not answer short checks this once the scan is over, and reads
   * again without the index when it is false (#9651).
   */
  public boolean isUnchangedSince(final IndexInternal[] snapshot) {
    if (!valid || dropping)
      return false;
    final IndexInternal[] current = getIndexesOnBuckets();
    if (current.length != snapshot.length)
      return false;
    for (int i = 0; i < current.length; i++)
      if (current[i] != snapshot[i])
        return false;
    return true;
  }

  /**
   * Whether {@code e}, raised by a read of {@code index}, is how the read fails when a concurrent DDL dropped or rebuilt the
   * index under it: an {@link IndexException}, or the page manager's "File with id n was not found" ({@link #isFileNotFound})
   * once the index is no longer valid or a drop of it is under way. A reader that has not returned anything yet can then read without the index (a scan,
   * or another index on the same properties) instead of failing (issues #9331, #9651). Any other exception is the caller's.
   */
  public static boolean isStaleRead(final Index index, final RuntimeException e) {
    if (e instanceof IndexException)
      return true;
    // a drop under way removes the sub-index files while the index still answers as valid
    return e instanceof IllegalArgumentException iae && isFileNotFound(iae) && !(index instanceof TypeIndex typeIndex
        && typeIndex.isValid() && !typeIndex.dropping);
  }

  /**
   * Snapshot of the indexes of {@code indexes} that a query can use, see {@link #isReadyForQueries()}. The collection a type
   * hands out can be a live view of its schema map, so it is copied once rather than walked while a concurrent DDL changes it.
   * Best effort: the caller still reads an index's metadata under a try/catch of {@link IndexException}.
   */
  public static List<TypeIndex> filterReadyForQueries(final Collection<TypeIndex> indexes) {
    final List<TypeIndex> result = new ArrayList<>(indexes.size());
    for (final TypeIndex index : indexes) {
      if (index.isReadyForQueries())
        result.add(index);
    }
    return result;
  }

  /**
   * The property names of this index when it can answer an exact key lookup (not a FULL_TEXT, vector or geospatial one, which
   * answer by token or similarity), null when it cannot, when it is still being populated, or when it went away while being
   * read: a concurrent DDL can drop or rebuild an index right after {@link #isReadyForQueries()} said yes (issue #8918).
   */
  public List<String> getPropertyNamesIfExactKeyLookup() {
    try {
      final Schema.INDEX_TYPE indexType = getType();
      return indexType != null && indexType.isExactKeyLookup() && isReadyForQueries() ? getPropertyNames() : null;
    } catch (final IndexException e) {
      return null;
    }
  }

  private void checkIsValid() {
    if (!valid)
      throw new IndexException("Index '" + getName() + "' is not valid. Probably has been drop or rebuilt");
  }

  private IndexInternal getFirstUnderlyingIndex() {
    checkIsValid();
    final IndexInternal first = firstOrNull();
    if (first == null)
      throw new IndexException("Index '" + getName() + "' is not valid. Probably has been drop or rebuilt");
    return first;
  }

  /** The first sub-index, or null when none (yet, or any more): a single read, so it cannot race with a concurrent clear(). */
  private IndexInternal firstOrNull() {
    // A single read of the backing array (CopyOnWriteArrayList#get), so it cannot race with a concurrent clear(), and it
    // allocates nothing when the list is populated: the exception below only happens on the rare empty state
    try {
      return indexesOnBuckets.get(0);
    } catch (final IndexOutOfBoundsException e) {
      return null;
    }
  }

  public IndexMetadata getMetadata() {
    // Return stored metadata if available, otherwise delegate to first underlying bucket index
    if (metadata != null)
      return metadata;
    final IndexInternal first = firstOrNull();
    return first == null ? null : first.getMetadata();
  }

  @Override
  public IndexMetadata getMetadataForNewFile() {
    // Same precedence as getMetadata(), but the delegate answers with the type-specific configuration a wrapper index
    // keeps outside the underlying LSM-Tree. Same definition across every bucket sub-index, so the first one answers
    // for all of them.
    if (metadata != null)
      return metadata;
    final IndexInternal first = firstOrNull();
    return first == null ? null : first.getMetadataForNewFile();
  }
}
