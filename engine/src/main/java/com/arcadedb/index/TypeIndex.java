/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.index;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.IndexCursorCollection;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EmbeddedSchema;

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

  public TypeIndex(final String logicName, final DocumentType type) {
    this.logicName = logicName;
    this.type = type;
  }

  @Override
  public long countEntries() {
    long total = 0;
    for (IndexInternal index : indexesOnBuckets)
      total += index.countEntries();
    return total;
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder) {
    if (!supportsOrderedIterations())
      throw new UnsupportedOperationException("Index '" + getName() + "' does not support ordered iterations");

    return new MultiIndexCursor(indexesOnBuckets, ascendingOrder, -1);
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys, final boolean inclusive) {
    if (!supportsOrderedIterations())
      throw new UnsupportedOperationException("Index '" + getName() + "' does not support ordered iterations");

    return new MultiIndexCursor(indexesOnBuckets, fromKeys, ascendingOrder, inclusive, -1);
  }

  @Override
  public IndexCursor range(final boolean ascending, final Object[] beginKeys, final boolean beginKeysInclusive, final Object[] endKeys,
      boolean endKeysInclusive) {
    if (!supportsOrderedIterations())
      throw new UnsupportedOperationException("Index '" + getName() + "' does not support ordered iterations");

    final List<IndexCursor> cursors = new ArrayList<>(indexesOnBuckets.size());
    for (Index index : indexesOnBuckets)
      cursors.add(((RangeIndex) index).range(ascending, beginKeys, beginKeysInclusive, endKeys, endKeysInclusive));

    return new MultiIndexCursor(cursors, -1, ascending);
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    Set<Identifiable> result = null;

    for (Index index : getIndexesByKeys(keys)) {
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
    Set<Identifiable> result = null;

    for (Index index : getIndexesByKeys(keys)) {
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
    for (Index index : getIndexesByKeys(keys))
      index.remove(keys);
  }

  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    for (Index index : getIndexesByKeys(keys))
      index.remove(keys, rid);
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {
    boolean result = false;
    for (IndexInternal index : indexesOnBuckets)
      if (index.compact())
        result = true;
    return result;
  }

  @Override
  public boolean isCompacting() {
    for (Index index : indexesOnBuckets)
      if (index.isCompacting())
        return true;
    return false;
  }

  @Override
  public boolean scheduleCompaction() {
    for (Index index : indexesOnBuckets)
      if (!index.scheduleCompaction())
        return false;

    return true;
  }

  @Override
  public EmbeddedSchema.INDEX_TYPE getType() {
    if (indexesOnBuckets.isEmpty())
      return null;
    return indexesOnBuckets.get(0).getType();
  }

  @Override
  public String getTypeName() {
    if (indexesOnBuckets.isEmpty())
      return null;

    return indexesOnBuckets.get(0).getTypeName();
  }

  @Override
  public List<String> getPropertyNames() {
    return indexesOnBuckets.get(0).getPropertyNames();
  }

  @Override
  public void close() {
    for (IndexInternal index : indexesOnBuckets)
      index.close();
  }

  @Override
  public void drop() {
    final DocumentType t = type.getSchema().getType(getTypeName());
    t.removeIndexInternal(this);

    for (Index index : new ArrayList<>(indexesOnBuckets))
      type.getSchema().dropIndex(index.getName());
    indexesOnBuckets.clear();
  }

  @Override
  public String getName() {
    return logicName;
  }

  @Override
  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
    for (Index index : indexesOnBuckets)
      stats.putAll(((IndexInternal) index).getStats());
    return stats;
  }

  @Override
  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return indexesOnBuckets.get(0).getNullStrategy();
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    indexesOnBuckets.get(0).setNullStrategy(nullStrategy);
  }

  @Override
  public boolean isUnique() {
    return indexesOnBuckets.get(0).isUnique();
  }

  @Override
  public boolean supportsOrderedIterations() {
    return indexesOnBuckets.get(0).supportsOrderedIterations();
  }

  @Override
  public boolean isAutomatic() {
    return true;
  }

  @Override
  public long build(final BuildIndexCallback callback) {
    long total = 0;
    for (Index index : indexesOnBuckets)
      total += index.build(callback);
    return total;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof TypeIndex))
      return false;

    final TypeIndex index2 = (TypeIndex) obj;

    if (!getName().equals(index2.getName()))
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
      final Index bIdx2 = index2.indexesOnBuckets.get(i);

      if (bIdx1.getAssociatedBucketId() != bIdx2.getAssociatedBucketId())
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
  public int getFileId() {
    return -1;
  }

  @Override
  public PaginatedComponent getPaginatedComponent() {
    throw new UnsupportedOperationException("getPaginatedComponent");
  }

  @Override
  public byte[] getKeyTypes() {
    return indexesOnBuckets.get(0).getKeyTypes();
  }

  @Override
  public int getAssociatedBucketId() {
    return -1;
  }

  public void addIndexOnBucket(final IndexInternal index) {
    if (index instanceof TypeIndex)
      throw new IllegalArgumentException("Invalid subIndex " + index);

    indexesOnBuckets.add(index);
  }

  public void removeIndexOnBucket(final IndexInternal index) {
    if (index instanceof TypeIndex)
      throw new IllegalArgumentException("Invalid subIndex " + index);

    indexesOnBuckets.remove(index);
  }

  public IndexInternal[] getIndexesOnBuckets() {
    return indexesOnBuckets.toArray(new IndexInternal[indexesOnBuckets.size()]);
  }

  private List<? extends Index> getIndexesByKeys(final Object[] keys) {
    final int bucketIndex = type.getBucketIndexByKeys(keys,
        DatabaseContext.INSTANCE.getContext((type.getSchema().getEmbedded().getDatabase()).getDatabasePath()).asyncMode);

    if (bucketIndex > -1)
      // USE THE SHARDED INDEX
      return type.getPolymorphicBucketIndexByBucketId(type.getBuckets(false).get(bucketIndex).getId());

    // SEARCH ON ALL THE UNDERLYING INDEXES
    return indexesOnBuckets;
  }
}
