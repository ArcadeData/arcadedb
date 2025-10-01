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
package com.arcadedb.database;

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
import com.arcadedb.utility.CollectionUtils;

import java.util.*;
import java.util.logging.*;

public class TransactionIndexContext {
  private final DatabaseInternal                                             database;
  private       Map<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> indexEntries = new LinkedHashMap<>(); // MOST COMMON USE CASE INSERTION IS ORDERED, USE AN ORDERED MAP TO OPTIMIZE THE INDEX

  public static class IndexKey {
    public final boolean           unique;
    public final Object[]          keyValues;
    public final RID               rid;
    public       IndexKeyOperation operation;

    public enum IndexKeyOperation {
      REMOVE, ADD, REPLACE // @compatibility < 25.3.2: 0 = REMOVE, 1 = ADD. 2 = REPLACE introduced with 25.3.2
    }

    public IndexKey(final boolean unique, final IndexKeyOperation operation, final Object[] keyValues, final RID rid) {
      this.unique = unique;
      this.operation = operation;
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
      if (unique)
        return Arrays.equals(keyValues, indexKey.keyValues);
      return Objects.equals(rid, indexKey.rid) && Arrays.equals(keyValues, indexKey.keyValues);
    }

    @Override
    public int hashCode() {
      if (unique)
        return Objects.hash(Arrays.hashCode(keyValues));
      return Objects.hash(rid, Arrays.hashCode(keyValues));
    }

    @Override
    public String toString() {
      return "IndexKey(" + operation + Arrays.toString(keyValues) + ")";
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
        } else if (v1 instanceof List list && v2 instanceof List list1) {

          return CollectionUtils.compare(list, list1);

        } else if (v1 instanceof List l1) {
          for (int j = 0; j < l1.size(); j++) {
            cmp = j > 0 ? 1 : BinaryComparator.compareTo(l1.get(j), v2);
            if (cmp != 0)
              return cmp;
          }
        } else if (v2 instanceof List l2) {
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
    for (final Map<ComparableKey, Map<IndexKey, IndexKey>> entry : indexEntries.values()) {
      total += entry.values().size();
    }
    return total;
  }

  public int getTotalEntriesByIndex(final String indexName) {
    final Map<ComparableKey, Map<IndexKey, IndexKey>> entries = indexEntries.get(indexName);
    if (entries == null)
      return 0;
    return entries.size();
  }

  public void commit() {
    checkUniqueIndexKeys();

    for (final Map.Entry<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> entry : indexEntries.entrySet()) {
      final Index index = database.getSchema().getIndexByName(entry.getKey());
      final Map<ComparableKey, Map<IndexKey, IndexKey>> keys = entry.getValue();

      for (final Map.Entry<ComparableKey, Map<IndexKey, IndexKey>> keyValueEntries : keys.entrySet()) {
        final Collection<IndexKey> values = keyValueEntries.getValue().values();
        for (final IndexKey key : values) {
          if (key.operation == IndexKey.IndexKeyOperation.REMOVE)
            index.remove(key.keyValues, key.rid);
        }
      }
    }

    for (final Map.Entry<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> entry : indexEntries.entrySet()) {
      final Index index = database.getSchema().getIndexByName(entry.getKey());
      final Map<ComparableKey, Map<IndexKey, IndexKey>> keys = entry.getValue();

      for (final Map.Entry<ComparableKey, Map<IndexKey, IndexKey>> keyValueEntries : keys.entrySet()) {
        final Collection<IndexKey> values = keyValueEntries.getValue().values();

        if (values.size() > 1) {
          // BATCH MODE. USE SET TO SKIP DUPLICATES
          final Set<RID> rids2Insert = new LinkedHashSet<>(values.size());

          for (final IndexKey key : values) {
            if (key.operation == IndexKey.IndexKeyOperation.ADD ||
                key.operation == IndexKey.IndexKeyOperation.REPLACE)
              rids2Insert.add(key.rid);
          }

          if (!rids2Insert.isEmpty()) {
            final RID[] rids = new RID[rids2Insert.size()];
            rids2Insert.toArray(rids);
            index.put(keyValueEntries.getKey().values, rids);
          }

        } else {
          for (final IndexKey key : values) {
            if (key.operation == IndexKey.IndexKeyOperation.ADD ||
                key.operation == IndexKey.IndexKeyOperation.REPLACE)
              index.put(key.keyValues, new RID[] { key.rid });
          }
        }
      }
    }

    indexEntries.clear();
  }

  public void addFilesToLock(final Set<Integer> modifiedFiles) {
    final Schema schema = database.getSchema();

    final Set<Index> lockedIndexes = new HashSet<>(indexEntries.size());

    for (final String indexName : indexEntries.keySet()) {
      final IndexInternal index = (IndexInternal) schema.getIndexByName(indexName);

      if (!lockedIndexes.add(index))
        // ALREADY IN THE SET
        continue;

      modifiedFiles.add(index.getFileId());

      if (index.isUnique()) {
        // LOCK ALL THE FILES IMPACTED BY THE INDEX KEYS TO CHECK FOR UNIQUE CONSTRAINT
        // TODO: OPTIMIZE LOCKING IF STRATEGY IS PARTITIONED: LOCK ONLY THE RELEVANT INDEX
        final DocumentType type = schema.getType(index.getTypeName());
        modifiedFiles.addAll(type.getBucketIds(false));

        for (final TypeIndex typeIndex : type.getAllIndexes(true))
          for (final IndexInternal idx : typeIndex.getIndexesOnBuckets())
            modifiedFiles.add(idx.getFileId());
      } else
        modifiedFiles.add(index.getAssociatedBucketId());
    }
  }

  public Map<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> toMap() {
    return indexEntries;
  }

  public void setKeys(final Map<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> keysTx) {
    indexEntries = keysTx;
  }

  public boolean isEmpty() {
    return indexEntries.isEmpty();
  }

  public void addIndexKeyLock(final IndexInternal index, IndexKey.IndexKeyOperation operation, final Object[] keysValues,
      final RID rid) {
    if (index.getNullStrategy() == LSMTreeIndexAbstract.NULL_STRATEGY.SKIP && LSMTreeIndexAbstract.isKeyNull(keysValues))
      // NULL VALUES AND SKIP NUL VALUES
      return;

    final String indexName = index.getName();

    TreeMap<ComparableKey, Map<IndexKey, IndexKey>> keys = indexEntries.get(indexName);

    final ComparableKey k = new ComparableKey(keysValues);
    final IndexKey v = new IndexKey(index.isUnique(), operation, keysValues, rid);

    Map<IndexKey, IndexKey> values;
    if (keys == null) {
      keys = new TreeMap<>(); // ORDERED TO KEEP INSERTION ORDER
      indexEntries.put(indexName, keys);

      values = new HashMap<>();
      keys.put(k, values);
    } else {
      values = keys.get(k);
      if (values == null) {
        values = new HashMap<>();
        keys.put(k, values);
      } else {
        if (v.operation == IndexKey.IndexKeyOperation.ADD) {
          if (index.isUnique()) {
            // CHECK IMMEDIATELY (INSTEAD OF AT COMMIT TIME) FOR DUPLICATED KEY IN CASE 2 ENTRIES WITH THE SAME KEY ARE SAVED IN TX.
            final IndexKey entry = values.get(v);
            if (entry != null && entry.operation == IndexKey.IndexKeyOperation.ADD && !entry.rid.equals(rid))
              throw new DuplicatedKeyException(indexName, Arrays.toString(keysValues), entry.rid);

            // REPLACE EXISTENT WITH THIS
            v.operation = IndexKey.IndexKeyOperation.REPLACE;
          }
        }
      }
    }

    if (index.isUnique() &&
        (v.operation == IndexKey.IndexKeyOperation.ADD || v.operation == IndexKey.IndexKeyOperation.REPLACE)) {
      // CHECK FOR UNIQUE ON OTHER SUB-INDEXES
      final TypeIndex typeIndex = index.getTypeIndex();
      if (typeIndex != null) {
        for (final Index idx : typeIndex.getIndexesByKeys(keysValues)) {
          final TreeMap<ComparableKey, Map<IndexKey, IndexKey>> entries = indexEntries.get(idx.getName());
          if (entries != null) {
            final Map<IndexKey, IndexKey> otherIndexValues = entries.get(k);
            if (otherIndexValues != null)
              for (final IndexKey e : otherIndexValues.values()) {
                if (e.operation == IndexKey.IndexKeyOperation.ADD && !e.rid.equals(rid))
                  throw new DuplicatedKeyException(indexName, Arrays.toString(keysValues), e.rid);
                // REPLACE EXISTENT WITH THIS
                v.operation = IndexKey.IndexKeyOperation.REPLACE;
              }
          }
        }
      }
    }

    values.put(v, v);
  }

  public void reset() {
    indexEntries.clear();
  }

  public TreeMap<ComparableKey, Map<IndexKey, IndexKey>> getIndexKeys(final String indexName) {
    return indexEntries.get(indexName);
  }

  /**
   * Called at commit time in the middle of the lock to avoid concurrent insertion of the same key.
   */
  private void checkUniqueIndexKeys(final Index index, final IndexKey key, final RID deleted) {
    final DocumentType type = database.getSchema().getType(index.getTypeName());

    // CHECK UNIQUENESS ACROSS ALL THE INDEXES FOR ALL THE BUCKETS
    final TypeIndex idx = type.getPolymorphicIndexByProperties(index.getPropertyNames());
    if (idx != null) {
      final IndexCursor found = idx.get(key.keyValues, 2);
      if (found.hasNext()) {
        final Identifiable firstEntry = found.next();
        int totalEntries = 1;
        if (found.hasNext())
          ++totalEntries;

        if (found.hasNext() || (totalEntries == 1 && !firstEntry.equals(key.rid))) {
          if (firstEntry.equals(deleted))
            // DELETED IN TX
            return;

          try {
//            database.lookupByRID(firstEntry.getIdentity(), true);
            // NO EXCEPTION = FOUND
            throw new DuplicatedKeyException(idx.getName(), Arrays.toString(key.keyValues), firstEntry.getIdentity());

          } catch (final RecordNotFoundException e) {
            // INDEX DIRTY, THE RECORD WA DELETED, REMOVE THE ENTRY IN THE INDEX TO FIX IT
            LogManager.instance()
                .log(this, Level.WARNING, "Found entry in index '%s' with key %s pointing to the deleted record %s. Overriding it.",
                    idx.getName(),
                    Arrays.toString(key.keyValues), firstEntry.getIdentity());

            idx.remove(key.keyValues);
          }
        }
      }
    }

  }

  /**
   * Checks unique indexes integrity. Since a type index is composed by multiple bucket indexes, the deleted keys are first collected across all the indexes.
   */
  private void checkUniqueIndexKeys() {
    final Map<TypeIndex, Map<ComparableKey, RID>> deletedKeys = getTxDeletedEntries();

    for (final Map.Entry<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> indexEntries : indexEntries.entrySet()) {
      final IndexInternal index = (IndexInternal) database.getSchema().getIndexByName(indexEntries.getKey());
      if (index.isUnique()) {
        final TypeIndex typeIndex = index.getTypeIndex();

        final Map<ComparableKey, Map<IndexKey, IndexKey>> txEntriesPerIndex = indexEntries.getValue();
        for (final Map.Entry<ComparableKey, Map<IndexKey, IndexKey>> txEntriesPerKey : txEntriesPerIndex.entrySet()) {
          final Map<IndexKey, IndexKey> valuesPerKey = txEntriesPerKey.getValue();

          for (final IndexKey entry : valuesPerKey.values()) {
            if (entry.operation == IndexKey.IndexKeyOperation.ADD || entry.operation == IndexKey.IndexKeyOperation.REPLACE) {
              final Map<ComparableKey, RID> deletedEntries = deletedKeys.get(typeIndex);
              final RID deleted = deletedEntries != null ? deletedEntries.get(new ComparableKey(entry.keyValues)) : null;
              checkUniqueIndexKeys(index, entry, deleted);
            }
          }
        }
      }
    }
  }

  private Map<TypeIndex, Map<ComparableKey, RID>> getTxDeletedEntries() {
    // GET ANY DELETED OPERATION FIRST
    final Map<TypeIndex, Map<ComparableKey, RID>> deletedKeys = new HashMap<>();

    for (final Map.Entry<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> indexEntries : indexEntries.entrySet()) {
      final IndexInternal index = (IndexInternal) database.getSchema().getIndexByName(indexEntries.getKey());
      if (index.isUnique()) {
        final Map<ComparableKey, Map<IndexKey, IndexKey>> txEntriesPerIndex = indexEntries.getValue();
        for (final Map.Entry<ComparableKey, Map<IndexKey, IndexKey>> txEntriesPerKey : txEntriesPerIndex.entrySet()) {
          final Map<IndexKey, IndexKey> valuesPerKey = txEntriesPerKey.getValue();

          for (final IndexKey entry : valuesPerKey.values()) {
            if (entry.operation == IndexKey.IndexKeyOperation.REMOVE ||
                entry.operation == IndexKey.IndexKeyOperation.REPLACE) {
              final TypeIndex typeIndex = index.getTypeIndex();
              final Map<ComparableKey, RID> entries = deletedKeys.computeIfAbsent(typeIndex, k -> new HashMap<>());
              entries.put(new ComparableKey(entry.keyValues), entry.rid);
            }
          }
        }
      }
    }
    return deletedKeys;
  }
}
