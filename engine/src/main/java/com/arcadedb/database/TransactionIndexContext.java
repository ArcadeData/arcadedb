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

package com.arcadedb.database;

import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.BinaryComparator;

import java.util.*;
import java.util.logging.Level;

public class TransactionIndexContext {
  private final DatabaseInternal                                   database;
  private       Map<String, TreeMap<ComparableKey, Set<IndexKey>>> indexEntries = new LinkedHashMap<>(); // MOST COMMON USE CASE INSERTION IS ORDERED, USE AN ORDERED MAP TO OPTIMIZE THE INDEX

  public static class IndexKey {
    public final boolean  addOperation;
    public final Object[] keyValues;
    public final RID      rid;

    public IndexKey(final boolean addOperation, final Object[] keyValues, final RID rid) {
      this.addOperation = addOperation;
      this.keyValues = keyValues;
      this.rid = rid;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (!(o instanceof IndexKey))
        return false;
      final IndexKey indexKey = (IndexKey) o;
      return Arrays.equals(keyValues, indexKey.keyValues) && Objects.equals(rid, indexKey.rid);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(rid);
      result = 31 * result + Arrays.hashCode(keyValues);
      return result;
    }

    @Override
    public String toString() {
      return "IndexKey(" + (addOperation ? "add " : "remove ") + Arrays.toString(keyValues) + ")";
    }
  }

  public static class ComparableKey implements Comparable<ComparableKey> {
    public final Object[] values;

    public ComparableKey(final Object[] values) {
      this.values = values;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      final ComparableKey that = (ComparableKey) o;
      return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }

    @Override
    public int compareTo(final ComparableKey that) {
      for (int i = 0; i < values.length; i++) {
        final Object v1 = values[i];
        final Object v2 = that.values[i];

        int cmp = 0;
        if (v1 == v2) {
        } else if (v1 == null) {
          return 1;
        } else if (v2 == null) {
          return -1;
        } else if (v1 instanceof List && v2 instanceof List) {
          List l1 = (List) v1;
          List l2 = (List) v2;

          for (int j = 0; j < l1.size(); j++) {
            cmp = BinaryComparator.compareTo(l1.get(j), l2.get(j));
            if (cmp != 0)
              return cmp;
          }

        } else if (v1 instanceof List) {
          List l1 = (List) v1;
          for (int j = 0; j < l1.size(); j++) {
            cmp = j > 0 ? 1 : BinaryComparator.compareTo(l1.get(j), v2);
            if (cmp != 0)
              return cmp;
          }
        } else if (v2 instanceof List) {
          List l2 = (List) v2;
          for (int j = 0; j < l2.size(); j++) {
            cmp = j > 0 ? -1 : BinaryComparator.compareTo(v1, l2.get(j));
            if (cmp != 0)
              return cmp;
          }
        } else
          cmp = BinaryComparator.compareTo(v1, v2);

        if (cmp != 0)
          return cmp;
      }
      return 0;
    }
  }

  public TransactionIndexContext(final DatabaseInternal database) {
    this.database = database;
  }

  public void removeIndex(final String indexName) {
    indexEntries.remove(indexName);
  }

  public int getTotalEntries() {
    int total = 0;
    for (Map<ComparableKey, Set<IndexKey>> entry : indexEntries.values()) {
      total += entry.values().size();
    }
    return total;
  }

  public int getTotalEntriesByIndex(final String indexName) {
    final Map<ComparableKey, Set<IndexKey>> entries = indexEntries.get(indexName);
    if (entries == null)
      return 0;
    return entries.size();
  }

  public void commit() {
    checkUniqueIndexKeys();

    for (Map.Entry<String, TreeMap<ComparableKey, Set<IndexKey>>> entry : indexEntries.entrySet()) {
      final Index index = database.getSchema().getIndexByName(entry.getKey());
      final Map<ComparableKey, Set<IndexKey>> keys = entry.getValue();

      for (Map.Entry<ComparableKey, Set<IndexKey>> keyValueEntries : keys.entrySet()) {
        final Set<IndexKey> values = keyValueEntries.getValue();

        if (values.size() > 1) {
          // BATCH MODE. USE SET TO SKIP DUPLICATES
          final Set<RID> rids2Insert = new LinkedHashSet<>(values.size());

          for (IndexKey key : values) {
            if (key.addOperation)
              rids2Insert.add(key.rid);
            else
              index.remove(key.keyValues, key.rid);
          }

          if (!rids2Insert.isEmpty()) {
            final RID[] rids = new RID[rids2Insert.size()];
            rids2Insert.toArray(rids);
            index.put(keyValueEntries.getKey().values, rids);
          }

        } else {
          for (IndexKey key : values) {
            if (key.addOperation)
              index.put(key.keyValues, new RID[] { key.rid });
            else
              index.remove(key.keyValues, key.rid);
          }
        }
      }
    }

    indexEntries.clear();
  }

  public void addFilesToLock(Set<Integer> modifiedFiles) {
    final Schema schema = database.getSchema();

    final Set<Index> lockedIndexes = new HashSet<>();

    for (String indexName : indexEntries.keySet()) {
      final IndexInternal index = (IndexInternal) schema.getIndexByName(indexName);

      if (lockedIndexes.contains(index))
        continue;

      lockedIndexes.add(index);

      modifiedFiles.add(index.getFileId());

      if (index.isUnique()) {
        // LOCK ALL THE FILES IMPACTED BY THE INDEX KEYS TO CHECK FOR UNIQUE CONSTRAINT
        final DocumentType type = schema.getType(index.getTypeName());
        final List<Bucket> buckets = type.getBuckets(false);
        for (Bucket b : buckets)
          modifiedFiles.add(b.getId());

        for (Index typeIndex : type.getAllIndexes(true))
          for (Index idx : ((TypeIndex) typeIndex).getIndexesOnBuckets())
            modifiedFiles.add(((IndexInternal) idx).getFileId());
      } else
        modifiedFiles.add(index.getAssociatedBucketId());
    }
  }

  public Map<String, TreeMap<ComparableKey, Set<IndexKey>>> toMap() {
    return indexEntries;
  }

  public void setKeys(final Map<String, TreeMap<ComparableKey, Set<IndexKey>>> keysTx) {
    indexEntries = keysTx;
  }

  public boolean isEmpty() {
    return indexEntries.isEmpty();
  }

  public void addIndexKeyLock(final String indexName, final boolean addOperation, final Object[] keysValues, final RID rid) {
    TreeMap<ComparableKey, Set<IndexKey>> keys = indexEntries.get(indexName);

    final ComparableKey k = new ComparableKey(keysValues);
    final IndexKey v = new IndexKey(addOperation, keysValues, rid);

    Set<IndexKey> values;
    if (keys == null) {
      keys = new TreeMap<>(); // ORDERD TO KEEP INSERTION ORDER
      indexEntries.put(indexName, keys);

      values = new HashSet<>();
      keys.put(k, values);
    } else {
      values = keys.get(k);
      if (values == null) {
        values = new HashSet<>();
        keys.put(k, values);
      } else {
        // CHECK FOR REMOVING ENTRIES WITH THE SAME KEY IN TX CONTEXT
        values.remove(v);
      }
    }

    values.add(v);
  }

  public void reset() {
    indexEntries.clear();
  }

  public TreeMap<ComparableKey, Set<IndexKey>> getIndexKeys(final String indexName) {
    return indexEntries.get(indexName);
  }

  /**
   * Called at commit time in the middle of the lock to avoid concurrent insertion of the same key.
   */
  private void checkUniqueIndexKeys(final Index index, final IndexKey key) {
    if (!key.addOperation)
      return;

    if (LSMTreeIndexAbstract.isKeyNull(key.keyValues))
      // NULL KEYS ARE NO INDEXED, SO HAS NO SENSE TO CHECK FOR DUPLICATES
      return;

    final DocumentType type = database.getSchema().getType(index.getTypeName());

    // CHECK UNIQUENESS ACROSS ALL THE INDEXES FOR ALL THE BUCKETS
    final TypeIndex idx = type.getPolymorphicIndexByProperties(index.getPropertyNames());
    if (idx != null) {
      final IndexCursor found = idx.get(key.keyValues, 2);
      if (found.hasNext()) {
        final Identifiable firstEntry = found.next();
        if (found.size() > 1 || (found.size() == 1 && !firstEntry.equals(key.rid))) {
          try {
//            database.lookupByRID(firstEntry.getIdentity(), true);
            // NO EXCEPTION = FOUND
            throw new DuplicatedKeyException(idx.getName(), Arrays.toString(key.keyValues), firstEntry.getIdentity());

          } catch (RecordNotFoundException e) {
            // INDEX DIRTY, THE RECORD WA DELETED, REMOVE THE ENTRY IN THE INDEX TO FIX IT
            LogManager.instance()
                .log(this, Level.WARNING, "Found entry in index '%s' with key %s pointing to the deleted record %s. Overriding it.", null, idx.getName(),
                    Arrays.toString(key.keyValues), firstEntry.getIdentity());

            idx.remove(key.keyValues);
          }
        }
      }
    }

  }

  private void checkUniqueIndexKeys() {
    for (Map.Entry<String, TreeMap<ComparableKey, Set<IndexKey>>> indexEntry : indexEntries.entrySet()) {
      final Index index = database.getSchema().getIndexByName(indexEntry.getKey());
      if (index.isUnique()) {
        final Map<ComparableKey, Set<IndexKey>> entries = indexEntry.getValue();
        for (Set<IndexKey> keys : entries.values())
          for (IndexKey entry : keys)
            checkUniqueIndexKeys(index, entry);
      }
    }
  }
}
