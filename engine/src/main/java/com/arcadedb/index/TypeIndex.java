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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.IndexCursorCollection;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EmbeddedSchema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.json.JSONObject;

import java.io.*;
import java.util.*;

/**
 * It represent an index on a type. It's backed by one or multiple underlying indexes, one per bucket. By using multiple buckets, the read/write operation can
 * work concurrently and lock-free.
 *
 * @author Luca Garulli
 */
public class TypeIndex implements RangeIndex, IndexInternal {
  private final String              logicName;
  private final List<IndexInternal> indexesOnBuckets = new ArrayList<>();
  private final DocumentType        type;
  private       boolean             valid            = true;
  private       IndexInternal       associatedIndex;

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

    return new MultiIndexCursor(indexesOnBuckets, ascendingOrder, -1);
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys, final boolean inclusive) {
    checkIsValid();
    if (!supportsOrderedIterations())
      throw new UnsupportedOperationException("Index '" + getName() + "' does not support ordered iterations");

    return new MultiIndexCursor(indexesOnBuckets, fromKeys, ascendingOrder, inclusive, -1);
  }

  @Override
  public IndexCursor range(final boolean ascending, final Object[] beginKeys, final boolean beginKeysInclusive,
      final Object[] endKeys, final boolean endKeysInclusive) {
    checkIsValid();
    if (!supportsOrderedIterations())
      throw new UnsupportedOperationException("Index '" + getName() + "' does not support ordered iterations");

    final List<IndexCursor> cursors = new ArrayList<>(indexesOnBuckets.size());
    for (final Index index : indexesOnBuckets)
      cursors.add(((RangeIndex) index).range(ascending, beginKeys, beginKeysInclusive, endKeys, endKeysInclusive));

    return new MultiIndexCursor(cursors, -1, ascending);
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    checkIsValid();
    Set<Identifiable> result = null;

    for (final Index index : getIndexesByKeys(keys)) {
      final boolean unique = index.isUnique();

      final IndexCursor cursor = index.get(keys, unique ? 1 : -1);
      while (cursor.hasNext()) {
        if (unique) {
          result = Collections.singleton(cursor.next());
          return new IndexCursorCollection(result);
        }

        if (result == null)
          result = new HashSet<>();
        result.add(cursor.next());
      }
    }
    return new IndexCursorCollection(result != null ? result : Collections.emptyList());
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    checkIsValid();
    Set<Identifiable> result = null;

    for (final Index index : getIndexesByKeys(keys)) {
      final IndexCursor cursor = index.get(keys, limit > -1 ? (result != null ? result.size() : 0) - limit : -1);
      while (cursor.hasNext()) {
        if (result == null)
          result = new HashSet<>(limit);

        result.add(cursor.next());

        if (limit > -1 && result.size() >= limit)
          return new IndexCursorCollection(result);
      }
    }
    return new IndexCursorCollection(result != null ? result : Collections.emptyList());
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
    return indexesOnBuckets.get(0).getMostRecentFileName();
  }

  @Override
  public EmbeddedSchema.INDEX_TYPE getType() {
    checkIsValid();
    if (indexesOnBuckets.isEmpty())
      return null;
    return indexesOnBuckets.get(0).getType();
  }

  @Override
  public String getTypeName() {
    return type.getName();
  }

  @Override
  public List<String> getPropertyNames() {
    checkIsValid();
    return indexesOnBuckets.get(0).getPropertyNames();
  }

  @Override
  public void close() {
    checkIsValid();
    for (final IndexInternal index : indexesOnBuckets)
      index.close();
  }

  @Override
  public JSONObject toJSON() {
    return indexesOnBuckets.get(0).toJSON();
  }

  @Override
  public void drop() {
    checkIsValid();

    final List<IndexInternal> acquired = new ArrayList<>(indexesOnBuckets.size());
    for (final IndexInternal index : new ArrayList<>(indexesOnBuckets))
      if (index.setStatus(new INDEX_STATUS[] { INDEX_STATUS.AVAILABLE, INDEX_STATUS.UNAVAILABLE }, INDEX_STATUS.UNAVAILABLE))
        acquired.add(index);
      else {
        // NOT AVAILABLE, RESET ACQUIRED STATUSES
        for (IndexInternal i : acquired)
          i.setStatus(new INDEX_STATUS[] { INDEX_STATUS.UNAVAILABLE }, INDEX_STATUS.AVAILABLE);
        throw new NeedRetryException(
            "Cannot drop index '" + getName() + "' because one or more underlying files are not available");
      }

    for (final Index index : new ArrayList<>(indexesOnBuckets))
      type.getSchema().dropIndex(index.getName());

    indexesOnBuckets.clear();

    valid = false;
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
    return indexesOnBuckets.get(0).getNullStrategy();
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    checkIsValid();
    indexesOnBuckets.get(0).setNullStrategy(nullStrategy);
  }

  @Override
  public boolean isUnique() {
    checkIsValid();
    return indexesOnBuckets.get(0).isUnique();
  }

  @Override
  public boolean supportsOrderedIterations() {
    checkIsValid();
    return indexesOnBuckets.get(0).supportsOrderedIterations();
  }

  @Override
  public boolean isAutomatic() {
    return true;
  }

  @Override
  public int getPageSize() {
    checkIsValid();
    return indexesOnBuckets.get(0).getPageSize();
  }

  @Override
  public long build(final int buildIndexBatchSize, final BuildIndexCallback callback) {
    checkIsValid();
    long total = 0;
    for (final IndexInternal index : indexesOnBuckets)
      total += index.build(buildIndexBatchSize, callback);
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
  public void setMetadata(final String name, final String[] propertyNames, final int associatedBucketId) {
    throw new UnsupportedOperationException("setMetadata");
  }

  @Override
  public boolean setStatus(INDEX_STATUS[] expectedStatuses, INDEX_STATUS newStatus) {
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
    return indexesOnBuckets.get(0).getKeyTypes();
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    checkIsValid();
    return indexesOnBuckets.get(0).getBinaryKeyTypes();
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

    indexesOnBuckets.add(index);
    index.setTypeIndex(this);
  }

  public void removeIndexOnBucket(final IndexInternal index) {
    checkIsValid();
    if (index instanceof TypeIndex)
      throw new IllegalArgumentException("Invalid subIndex " + index);

    indexesOnBuckets.remove(index);
    index.setTypeIndex(null);
  }

  public IndexInternal[] getIndexesOnBuckets() {
    return indexesOnBuckets.toArray(new IndexInternal[indexesOnBuckets.size()]);
  }

  public List<? extends Index> getIndexesByKeys(final Object[] keys) {
    final int bucketIndex = type.getBucketIndexByKeys(keys,
        DatabaseContext.INSTANCE.getContext((type.getSchema().getEmbedded().getDatabase()).getDatabasePath()).asyncMode);

    if (bucketIndex > -1) {
      // USE THE SHARDED INDEX
      final List<String> propNames = getPropertyNames();

      List<IndexInternal> polymorphicIndexesOnKeys = type.getPolymorphicBucketIndexByBucketId(
          type.getBuckets(false).get(bucketIndex).getFileId(), propNames);

      final List<DocumentType> subTypes = type.getSubTypes();
      if (!subTypes.isEmpty()) {
        // MODIFIABLE COPY
        polymorphicIndexesOnKeys = new ArrayList<>(polymorphicIndexesOnKeys);

        for (DocumentType s : subTypes) {
          final List<IndexInternal> subIndexes = s.getPolymorphicBucketIndexByBucketId(
              s.getBuckets(false).get(bucketIndex).getFileId(), propNames);
          polymorphicIndexesOnKeys.addAll(subIndexes);

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

  private void checkIsValid() {
    if (!valid)
      throw new IndexException("Index '" + getName() + "' is not valid. Probably has been drop or rebuilt");
  }
}
