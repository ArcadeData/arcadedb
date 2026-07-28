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

import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.utility.IntHashSet;

import java.util.*;
import java.util.logging.Level;

public class TransactionIndexContext {
  private final DatabaseInternal                                             database;
  private       Map<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> indexEntries = new LinkedHashMap<>(); // MOST COMMON USE CASE INSERTION IS ORDERED, USE AN ORDERED MAP TO OPTIMIZE THE INDEX
  /**
   * Append-only lane for indexes that declare {@link IndexInternal#isTransactionKeyOrderRequired()}
   * {@code false} (issue #5411). Entries replay at commit in the exact order they were queued, so
   * the last operation on a given key wins with no key-ordered bookkeeping: no {@code ComparableKey}
   * comparison chain, no per-key value map. Used by {@code LSM_SPARSE_VECTOR}, whose single record
   * queues one entry per non-zero dimension.
   */
  private final Map<String, List<IndexKey>>                                  unorderedEntries = new LinkedHashMap<>();

  public static class IndexKey {
    public final boolean           unique;
    public final Object[]          keyValues;
    public final RID               rid;
    public       RID               oldRid; // for REPLACE created from same-bucket REMOVE→ADD: the old RID being replaced
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
      if (!(o instanceof IndexKey indexKey))
        return false;
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

  /**
   * #4947: navigation key for TreeMap positioning with PARTIAL (prefix) keys. ComparableKey.compareTo
   * returns 0 for any entry sharing the prefix, so ceiling/floor/higher/lower land on whichever
   * prefix-equal node the tree walk reaches first - the MIDDLE of the run - silently skipping the other
   * entries of the run during in-tx iteration. A biased key never compares equal: bias -1 sorts before the
   * whole prefix run (ceiling finds the run's FIRST entry), bias +1 after it (higher skips the whole run;
   * floor finds its LAST entry). For full-length keys the bias degenerates to the exact same navigation the
   * raw key gives, so callers can use it unconditionally.
   */
  private static class NavigationKey extends ComparableKey {
    private final int bias;

    private NavigationKey(final Object[] values, final int bias) {
      super(values);
      this.bias = bias;
    }

    @Override
    public int compareTo(final ComparableKey that) {
      final int cmp = super.compareTo(that);
      return cmp != 0 ? cmp : bias;
    }
  }

  /** Navigation key sorting BEFORE every entry whose key starts with {@code keys} (see {@link NavigationKey}). */
  public static ComparableKey lowNavigationKey(final Object[] keys) {
    return new NavigationKey(keys, -1);
  }

  /** Navigation key sorting AFTER every entry whose key starts with {@code keys} (see {@link NavigationKey}). */
  public static ComparableKey highNavigationKey(final Object[] keys) {
    return new NavigationKey(keys, 1);
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
          // #4947: nulls sort LOW, matching BinaryComparator.compare and LSMTreeIndexAbstract.compareKey.
          // The tx overlay used nulls-HIGH here, so with NULL_STRATEGY.INDEX and composite keys containing
          // nulls, the cursor's TreeMap navigation (ceiling/floor/higher/lower) disagreed with the disk
          // merge order: uncommitted entries could be skipped or emitted out of order during in-tx iteration.
          return -1;
        } else if (v2 == null) {
          return 1;
        } else if (v1 instanceof List<?> list && v2 instanceof List<?> list1) {

          return CollectionUtils.compare(list, list1);

        } else if (v1 instanceof List<?> l1) {
          for (int j = 0; j < l1.size(); j++) {
            cmp = j > 0 ? 1 : BinaryComparator.compareTo(l1.get(j), v2);
            if (cmp != 0)
              return cmp;
          }
        } else if (v2 instanceof List<?> l2) {
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
    unorderedEntries.remove(indexName);
  }

  public int getTotalEntries() {
    int total = 0;
    for (final Map<ComparableKey, Map<IndexKey, IndexKey>> entry : indexEntries.values()) {
      total += entry.values().stream().mapToInt(Map::size).sum();
    }
    for (final List<IndexKey> entry : unorderedEntries.values())
      total += entry.size();
    return total;
  }

  public int getTotalEntriesByIndex(final String indexName) {
    final List<IndexKey> unordered = unorderedEntries.get(indexName);
    if (unordered != null)
      return unordered.size();
    final Map<ComparableKey, Map<IndexKey, IndexKey>> entries = indexEntries.get(indexName);
    if (entries == null)
      return 0;
    return entries.size();
  }

  public void commit() {
    // REMOVE ENTRIES FOR INDEXES DROPPED DURING THE TRANSACTION (e.g. TYPE DROP)
    indexEntries.keySet().removeIf(indexName -> !database.getSchema().existsIndex(indexName));
    unorderedEntries.keySet().removeIf(indexName -> !database.getSchema().existsIndex(indexName));

    checkUniqueIndexKeys();

    // APPEND-ONLY LANE FIRST: ITS ENTRIES CARRY NO UNIQUENESS CONSTRAINT AND REPLAY STRICTLY IN
    // INSERTION ORDER, SO A REMOVE FOLLOWED BY AN ADD ON THE SAME KEY ENDS WITH THE ADD (AND VICE
    // VERSA) WITHOUT THE TWO-PHASE REMOVE-THEN-ADD SPLIT THE ORDERED LANE NEEDS FOR ITS DEDUP MAP.
    for (final Map.Entry<String, List<IndexKey>> entry : unorderedEntries.entrySet()) {
      final IndexInternal index = (IndexInternal) database.getSchema().getIndexByName(entry.getKey());
      final List<IndexKey> keys = entry.getValue();
      for (int i = 0; i < keys.size(); i++) {
        final IndexKey key = keys.get(i);
        if (key.operation == IndexKey.IndexKeyOperation.REMOVE)
          index.removeReplay(key.keyValues, key.rid);
        else
          index.putReplay(key.keyValues, new RID[] { key.rid });
      }
    }
    unorderedEntries.clear();

    for (final Map.Entry<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> entry : indexEntries.entrySet()) {
      final IndexInternal index = (IndexInternal) database.getSchema().getIndexByName(entry.getKey());
      final Map<ComparableKey, Map<IndexKey, IndexKey>> keys = entry.getValue();

      for (final Map.Entry<ComparableKey, Map<IndexKey, IndexKey>> keyValueEntries : keys.entrySet()) {
        final Collection<IndexKey> values = keyValueEntries.getValue().values();
        for (final IndexKey key : values) {
          if (key.operation == IndexKey.IndexKeyOperation.REMOVE)
            index.removeReplay(key.keyValues, key.rid);
          else if (key.operation == IndexKey.IndexKeyOperation.REPLACE && key.oldRid != null)
            // REMOVE THE OLD RID THAT WAS REPLACED BY A NEW ONE IN THE SAME BUCKET
            index.removeReplay(key.keyValues, key.oldRid);
        }
      }
    }

    for (final Map.Entry<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> entry : indexEntries.entrySet()) {
      final IndexInternal index = (IndexInternal) database.getSchema().getIndexByName(entry.getKey());
      final Map<ComparableKey, Map<IndexKey, IndexKey>> keys = entry.getValue();

      // Batch optimization for vector indexes (issue #3864): collect all ADD operations
      // and process them in a single putBatch call with one lock acquisition
      if (index instanceof LSMVectorIndex vectorIndex && keys.size() > 1) {
        final List<Object[]> batchKeys = new ArrayList<>(keys.size());
        final List<RID> batchRids = new ArrayList<>(keys.size());

        for (final Map.Entry<ComparableKey, Map<IndexKey, IndexKey>> keyValueEntries : keys.entrySet()) {
          for (final IndexKey key : keyValueEntries.getValue().values()) {
            if (key.operation == IndexKey.IndexKeyOperation.ADD ||
                key.operation == IndexKey.IndexKeyOperation.REPLACE) {
              batchKeys.add(key.keyValues);
              batchRids.add(key.rid);
            }
          }
        }

        if (!batchKeys.isEmpty())
          vectorIndex.putBatch(batchKeys, batchRids);

        continue;
      }

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
            index.putReplay(keyValueEntries.getKey().values, rids);
          }

        } else {
          for (final IndexKey key : values) {
            if (key.operation == IndexKey.IndexKeyOperation.ADD ||
                key.operation == IndexKey.IndexKeyOperation.REPLACE)
              index.putReplay(key.keyValues, new RID[] { key.rid });
          }
        }
      }
    }

    indexEntries.clear();
  }

  public void addFilesToLock(final IntHashSet modifiedFiles) {
    final Schema schema = database.getSchema();

    final Set<Index> lockedIndexes = new HashSet<>(indexEntries.size() + unorderedEntries.size());

    final List<String> indexNames = new ArrayList<>(indexEntries.size() + unorderedEntries.size());
    indexNames.addAll(indexEntries.keySet());
    indexNames.addAll(unorderedEntries.keySet());

    for (final String indexName : indexNames) {
      if (!schema.existsIndex(indexName))
        // INDEX WAS DROPPED DURING THE TRANSACTION (e.g. TYPE DROP), SKIP IT
        continue;

      final IndexInternal index = (IndexInternal) schema.getIndexByName(indexName);

      if (!lockedIndexes.add(index))
        // ALREADY IN THE SET
        continue;

      // getFileIds (plural), not getFileId: a multi-file index (e.g. LSMVectorIndex with its companion
      // graph file) writes pages into ALL its component files during the transaction; omitting one lets its
      // pages pass the commit version checks without the file lock held (#4937).
      for (final int indexFileId : index.getFileIds())
        modifiedFiles.add(indexFileId);

      // Lock only the data bucket this index entry belongs to (not all buckets of the type).
      // Guard against -1, returned by composite (TypeIndex) or metadata-less indexes, which is not a valid file id.
      final int associatedBucketId = index.getAssociatedBucketId();

      if (index.isUnique()) {
        // Cross-bucket uniqueness is serialised through the per-bucket index file locks below.
        if (associatedBucketId >= 0)
          modifiedFiles.add(associatedBucketId);

        // Only the UNIQUE indexes of the type need the all-buckets fan-out (#5499). A unique index is
        // partitioned by the record's bucket, so a colliding key can sit in ANY bucket's sub-index and
        // checkUniqueIndexKeys has to read the whole polymorphic TypeIndex - exactly the set locked here -
        // for the check to be atomic against a concurrent inserter. A NOTUNIQUE sibling enforces no such
        // cross-bucket invariant and is never read by that check; the only sub-index it can WRITE is its
        // own, and that one is already covered by the getFileIds() call above, which runs for every index
        // this transaction registered an entry for. Locking the siblings too multiplied the per-commit lock
        // set by the number of indexes on the type - 6 indexes x 32 buckets = 192 exclusive locks to insert
        // one edge in the report that surfaced this - and serialised every writer against every other one
        // regardless of which keys or buckets they touched.
        final DocumentType type = schema.getType(index.getTypeName());
        for (final TypeIndex typeIndex : type.getAllIndexes(true))
          if (typeIndex.isUnique())
            for (final IndexInternal idx : typeIndex.getIndexesOnBuckets())
              modifiedFiles.add(idx.getFileId());
      } else if (associatedBucketId >= 0)
        modifiedFiles.add(associatedBucketId);
    }
  }

  public Map<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> toMap() {
    return indexEntries;
  }

  public void setKeys(final Map<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> keysTx) {
    indexEntries = keysTx;
  }

  public boolean isEmpty() {
    return indexEntries.isEmpty() && unorderedEntries.isEmpty();
  }

  public void addIndexKeyLock(final IndexInternal index, IndexKey.IndexKeyOperation operation, final Object[] keysValues,
      final RID rid) {
    if (index.getNullStrategy() == LSMTreeIndexAbstract.NULL_STRATEGY.SKIP && LSMTreeIndexAbstract.isKeyNull(keysValues))
      // NULL VALUES AND SKIP NUL VALUES
      return;

    final String indexName = index.getName();

    if (!index.isTransactionKeyOrderRequired()) {
      // APPEND-ONLY LANE: NO KEY ORDERING, NO PER-KEY DEDUP. REPLAY ORDER == INSERTION ORDER.
      List<IndexKey> lane = unorderedEntries.get(indexName);
      if (lane == null) {
        // Checked once per index per transaction, not per entry: duplicated-key detection reads the
        // key-ordered map back, so an index that skips it cannot enforce uniqueness.
        if (index.isUnique())
          throw new IllegalStateException("Unique index '" + indexName
              + "' cannot opt out of the key-ordered transaction map: duplicated-key detection reads it back");
        lane = new ArrayList<>();
        unorderedEntries.put(indexName, lane);
      }
      lane.add(new IndexKey(false, operation, keysValues, rid));
      return;
    }

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
          if (index.isUnique() && !LSMTreeIndexAbstract.isKeyNull(keysValues)) {
            // CHECK IMMEDIATELY (INSTEAD OF AT COMMIT TIME) FOR DUPLICATED KEY IN CASE 2 ENTRIES WITH THE SAME KEY ARE SAVED IN TX.
            // Skip duplicate check for NULL keys - SQL standard: NULL != NULL (multiple NULLs allowed in unique index)
            final IndexKey entry = values.get(v);
            if (entry != null && entry.operation == IndexKey.IndexKeyOperation.ADD && !entry.rid.equals(rid))
              throw new DuplicatedKeyException(indexName, Arrays.toString(keysValues), entry.rid);

            // REPLACE EXISTENT WITH THIS
            v.operation = IndexKey.IndexKeyOperation.REPLACE;
            if (entry != null) {
              if (entry.operation == IndexKey.IndexKeyOperation.REMOVE)
                // SAVE THE OLD RID SO IT CAN BE PROPERLY REMOVED FROM THE PERSISTED INDEX AT COMMIT TIME
                v.oldRid = entry.rid;
              else if (entry.operation == IndexKey.IndexKeyOperation.REPLACE)
                // PROPAGATE THE OLD RID FROM THE PREVIOUS REPLACE OPERATION (e.g. REMOVE → ADD → ADD)
                v.oldRid = entry.oldRid;
            }
          }
        }
      }
    }

    if (index.isUnique() && !LSMTreeIndexAbstract.isKeyNull(keysValues) &&
        (v.operation == IndexKey.IndexKeyOperation.ADD || v.operation == IndexKey.IndexKeyOperation.REPLACE)) {
      // CHECK FOR UNIQUE ON OTHER SUB-INDEXES
      // Skip duplicate check for NULL keys - SQL standard: NULL != NULL (multiple NULLs allowed in unique index)
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
    unorderedEntries.clear();
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
            // PROBE THE EXISTING RECORD: IF IT LOADS, THE KEY IS A REAL DUPLICATE; OTHERWISE THE INDEX ENTRY IS DANGLING (e.g. it
            // points to a record or bucket that no longer exists - see issue #4501) AND MUST BE REPAIRED INSTEAD OF FAILING THE WRITE.
            database.lookupByRID(firstEntry.getIdentity(), true);
            // NO EXCEPTION = RECORD EXISTS = REAL DUPLICATED KEY
            throw new DuplicatedKeyException(idx.getName(), Arrays.toString(key.keyValues), firstEntry.getIdentity());

          } catch (final RecordNotFoundException e) {
            // #5279: "not found" is only evidence of a dirty index when the record is missing from the COMMITTED
            // state too. This transaction reads through its OWN image of the record's page, which can be older
            // than the committed one - since concurrent inserts into the same bucket now take different slots of
            // the same page, a transaction that already modified that page cannot see a record a concurrent
            // transaction committed into it. Repairing then would silently delete a HEALTHY index entry and let a
            // duplicate key through: the key is really taken, so fail like any other duplicate.
            if (existsInCommittedState(firstEntry.getIdentity()))
              throw new DuplicatedKeyException(idx.getName(), Arrays.toString(key.keyValues), firstEntry.getIdentity());

            // INDEX DIRTY: THE RECORD WAS DELETED OR ITS BUCKET IS GONE, REMOVE THE DANGLING ENTRY TO FIX THE INDEX
            LogManager.instance()
                .log(this, Level.WARNING,
                    "Found entry in index '%s' with key %s pointing to the missing record %s. Removing the dangling entry to repair the index.",
                    idx.getName(), Arrays.toString(key.keyValues), firstEntry.getIdentity());

            idx.remove(key.keyValues, firstEntry.getIdentity());
          }
        }
      }
    }

  }

  /**
   * Tells whether a record still lives at {@code rid} in the CURRENT COMMITTED state, ignoring what this
   * transaction's own (possibly older) image of that page shows. Used to tell a genuinely dangling index entry from
   * one this transaction simply cannot see yet (#5279).
   */
  private boolean existsInCommittedState(final RID rid) {
    return database.getSchema().getFileByIdIfExists(rid.getBucketId()) instanceof LocalBucket bucket//
        && bucket.existsRecordInCommittedPage(rid);
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
              // Skip uniqueness check for NULL keys - SQL standard: NULL != NULL (multiple NULLs allowed in unique index)
              if (LSMTreeIndexAbstract.isKeyNull(entry.keyValues))
                continue;
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

          for (final Map.Entry<IndexKey, IndexKey> entry : valuesPerKey.entrySet()) {
            if (entry.getValue().operation == IndexKey.IndexKeyOperation.REMOVE ||
                entry.getValue().operation == IndexKey.IndexKeyOperation.REPLACE) {
              final TypeIndex typeIndex = index.getTypeIndex();
              final Map<ComparableKey, RID> entries = deletedKeys.computeIfAbsent(typeIndex, k -> new HashMap<>());

              final ComparableKey key = new ComparableKey(entry.getValue().keyValues);
              final RID existent = entries.get(key);
              if (existent == null || entry.getValue().operation == IndexKey.IndexKeyOperation.REMOVE) {
                // MULTIPLE OPERATIONS ON THE SAME KEY (DIFFERENT BUCKETS), PREFER THE REMOVE ONE.
                // For REPLACE entries that originated from a same-bucket REMOVE→ADD merge, use the oldRid (the actual deleted RID).
                final RID deletedRid = entry.getValue().operation == IndexKey.IndexKeyOperation.REPLACE && entry.getValue().oldRid != null
                    ? entry.getValue().oldRid
                    : entry.getKey().rid;
                entries.put(key, deletedRid);
              }
            }
          }
        }
      }
    }
    return deletedKeys;
  }
}
