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
import com.arcadedb.index.IndexKeyEquality;
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
  /**
   * The index each lane above belongs to, remembered when the lane is opened rather than resolved back from its
   * name at commit time (issue #6105).
   * <p>
   * A lane is keyed by the name the index answered to when its first entry was queued, and an {@code LSM_VECTOR}
   * index renames itself when it is compacted - it is named after the component file it holds, and a compaction
   * swaps that file in. That compaction runs on the async executor, so it can land between an entry being queued
   * and this transaction committing. Resolving the lane by its name would then find nothing, and {@link #commit()}
   * would discard it under the rule that drops the lanes of indexes dropped mid-transaction (TYPE DROP): the record
   * is written, the index entry is silently lost.
   * <p>
   * Holding the reference lets the lane ask the index what it is called now instead of assuming it is still called
   * what it was called then. Everything else is unchanged: the current name goes to the same schema lookup as
   * before, so a renamed index is kept, a genuinely dropped one is still discarded, and the schema stays the sole
   * authority on which object that name resolves to (see {@link #laneIndexName} for why that distinction matters).
   */
  private final Map<String, IndexInternal>                                   indexPerLane     = new HashMap<>();

  /**
   * Monotonic write-order stamp handed to every entry {@link #addIndexKeyLock} queues, so an entry can be compared
   * with another by WHEN it was written and not only by what key it carries. See {@link IndexKey#sequence}.
   */
  private       int                                                          sequence;

  /**
   * The lanes of one index, reachable by the index ITSELF rather than by any of the names it has answered to
   * (issue #7967).
   * <p>
   * {@link #getIndexKeyLanes} used to answer by walking {@link #indexEntries} and asking {@link #indexPerLane} who
   * owned each lane. That is O(distinct indexes this transaction has touched) - and it is paid on EVERY dense
   * vector search issued from a transaction that has written to some OTHER index, for a search whose own index was
   * never written to and whose honest answer is "nothing". Keeping the answer here makes that the one hash lookup
   * it should always have been, and the lanes of an index that HAS been written to are found the same way.
   * <p>
   * An {@link IdentityHashMap} and not a {@link HashMap}: the question is which lanes belong to THIS object, which
   * is exactly the comparison the walk it replaces made ({@code owner != index}), and an index is free to define
   * equality however it likes without that becoming an aliasing bug here.
   */
  private final IdentityHashMap<IndexInternal, List<TreeMap<ComparableKey, Map<IndexKey, IndexKey>>>> orderedLanesPerIndex = new IdentityHashMap<>();

  /**
   * Whether any lane in {@link #indexEntries} has no owner recorded in {@link #indexPerLane}, which is only ever
   * true for lanes restored wholesale by {@link #setKeys}. Those cannot be resolved by identity - there is no
   * reference to resolve - so while one is present {@link #getIndexKeyLanes} falls back to the by-name walk rather
   * than answering from {@link #orderedLanesPerIndex} and silently missing it.
   */
  private       boolean                                                      lanesWithoutOwner;

  /**
   * How many times the lanes have CHANGED, in any way that could change what a reader of them would see: an entry
   * queued, an entry taken back, a lane dropped, the whole set replaced (issue #7967).
   * <p>
   * The stamp a cached read-your-own-writes view is kept under. A view built while this read {@code v} describes
   * the lanes exactly as long as it still reads {@code v}, so a cache keyed on it cannot go stale - which matters
   * because a stale one is not a slow search, it is a wrong answer. Deliberately bumped by the REMOVAL paths too,
   * where nothing is queued and {@link #sequence} therefore does not move.
   */
  private       long                                                         laneVersion;

  /** Per-index cached read view, valid only while {@link #laneVersion} still reads {@link #cachedViewVersion}. */
  private       IdentityHashMap<IndexInternal, Object>                       cachedViews;
  private       long                                                         cachedViewVersion = -1L;

  /** The cached stand-in for "this transaction has written nothing to this index". See {@link #cacheIndexView}. */
  public static final Object NO_VIEW = new Object();

  /**
   * The journal of what one record's indexing added, so it can be taken back exactly (issue #7467).
   * <p>
   * {@code LocalDatabase.createRecordNoLock} writes the record body, assigns its identity and increments the
   * bucket delta BEFORE {@code DocumentIndexer.createDocument} runs the unique check, so a
   * {@link DuplicatedKeyException} raised there used to leave the record in the transaction with no index entry
   * to find it by. Every caller that tallies the failure and carries on - the {@code /ws} insert session, the
   * gRPC stream, an HTTP batch, a SQL script with its own error handling - then committed a record that the
   * index denies and the acknowledgement denies. Undoing the record body is only half of the retraction: the
   * indexes BEFORE the one that refused already hold an entry for it, and those have to go back too.
   * <p>
   * Reverse order, and restoring the DISPLACED entry rather than simply dropping ours, is what makes it exact: a
   * unique index keys its per-key map on the key alone, so an {@code ADD} can overwrite an earlier {@code REMOVE}
   * (a delete-then-reinsert of the same key in one transaction) and the {@code REPLACE}/{@code oldRid} that
   * records it. Dropping ours would take the earlier operation with it and leave a stale index entry behind.
   * <p>
   * The list is REUSED across records - only its size is reset - so an insert-heavy workload allocates the
   * holders once rather than once per record. Nothing is recorded while disarmed, which is every path but the one
   * that is about to have to undo.
   */
  private final List<RecordUndoEntry>                                        recordUndo       = new ArrayList<>();
  /** How many of {@link #recordUndo} belong to the record being indexed right now; -1 when disarmed. */
  private       int                                                          recordUndoSize   = -1;

  /**
   * One journalled index-map mutation. Mutable and reused: {@link #recordUndo} hands the same holders back out
   * for the next record rather than allocating a new one per index key.
   */
  private static final class RecordUndoEntry {
    /** The append-only lane the entry was appended to, or {@code null} when this is an ordered-lane entry. */
    private List<IndexKey>                                  lane;
    /** The lane this entry belongs to, so an undo that empties the lane can drop it. */
    private String                                          indexName;
    /** Ordered lane: the per-index key map, the per-key value map, and what our put displaced from it. */
    private TreeMap<ComparableKey, Map<IndexKey, IndexKey>> keys;
    private ComparableKey                                   key;
    private Map<IndexKey, IndexKey>                         values;
    private IndexKey                                        added;
    private IndexKey                                        displaced;

    private void ordered(final String indexName, final TreeMap<ComparableKey, Map<IndexKey, IndexKey>> keys,
        final ComparableKey key, final Map<IndexKey, IndexKey> values, final IndexKey added, final IndexKey displaced) {
      this.lane = null;
      this.indexName = indexName;
      this.keys = keys;
      this.key = key;
      this.values = values;
      this.added = added;
      this.displaced = displaced;
    }

    private void appended(final String indexName, final List<IndexKey> lane) {
      this.lane = lane;
      this.indexName = indexName;
      this.keys = null;
      this.key = null;
      this.values = null;
      this.added = null;
      this.displaced = null;
    }

    /** Drops the references the holder is keeping alive, so a reused journal pins nothing between records. */
    private void clear() {
      this.lane = null;
      this.indexName = null;
      this.keys = null;
      this.key = null;
      this.values = null;
      this.added = null;
      this.displaced = null;
    }
  }

  public static class IndexKey {
    public final boolean           unique;
    public final Object[]          keyValues;
    public final RID               rid;
    /**
     * Where this entry sits in the transaction's WRITE order, counted by {@link #sequence} (issue #7971).
     * <p>
     * The ordered lane replays its entries in {@code ComparableKey} order, which for most indexes is exactly what
     * the receiving structure wants. It is not what a structure that holds ONE value per RID wants: a dense vector
     * index that is handed two embeddings of the same record - two different keys, so nothing dedups them - would
     * otherwise keep whichever key sorted last, i.e. a hash of the vector's contents rather than the rewrite the
     * application actually ran last. Carrying the write order on the entry lets {@link #commit()} pick the last
     * write for a RID instead of the last key.
     * <p>
     * Zero on every entry a lane restored wholesale by {@link #setKeys} carries, which records no write order. The
     * per-RID pick then falls back to the first entry in key order - exactly what that path did before this field
     * existed, so it is left no worse than it was rather than silently given a wrong answer.
     * <p>
     * <b>Deliberately NOT part of {@link #equals}/{@link #hashCode}</b>, and it must stay that way. The per-key map
     * in {@code addIndexKeyLock} identifies an entry by its key (and, on a non-unique index, its RID) so that a
     * later operation on the same key REPLACES the earlier one - which is what collapses a {@code REMOVE} onto the
     * {@code ADD} it retires. Including the write order in equality would make every entry distinct, the map would
     * accumulate one per write instead of one per key, and the dedup this class is built around would quietly stop
     * happening (PR #8001 review).
     */
    public final int               sequence;
    public       RID               oldRid; // for REPLACE created from same-bucket REMOVE→ADD: the old RID being replaced
    public       IndexKeyOperation operation;

    public enum IndexKeyOperation {
      REMOVE, ADD, REPLACE // @compatibility < 25.3.2: 0 = REMOVE, 1 = ADD. 2 = REPLACE introduced with 25.3.2
    }

    public IndexKey(final boolean unique, final IndexKeyOperation operation, final Object[] keyValues, final RID rid) {
      this(unique, operation, keyValues, rid, 0);
    }

    public IndexKey(final boolean unique, final IndexKeyOperation operation, final Object[] keyValues, final RID rid,
        final int sequence) {
      this.unique = unique;
      this.operation = operation;
      this.keyValues = keyValues;
      this.rid = rid;
      this.sequence = sequence;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (!(o instanceof IndexKey indexKey))
        return false;
      if (unique)
        return IndexKeyEquality.sameTuple(keyValues, indexKey.keyValues);
      return Objects.equals(rid, indexKey.rid) && IndexKeyEquality.sameTuple(keyValues, indexKey.keyValues);
    }

    @Override
    public int hashCode() {
      if (unique)
        return Objects.hash(IndexKeyEquality.hashTuple(keyValues));
      return Objects.hash(rid, IndexKeyEquality.hashTuple(keyValues));
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
      return IndexKeyEquality.sameTuple(values, that.values);
    }

    @Override
    public int hashCode() {
      return IndexKeyEquality.hashTuple(values);
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
    final TreeMap<ComparableKey, Map<IndexKey, IndexKey>> ordered = indexEntries.remove(indexName);
    unorderedEntries.remove(indexName);
    final IndexInternal owner = indexPerLane.remove(indexName);
    if (ordered != null)
      forgetOrderedLane(owner, ordered);
    ++laneVersion;
  }

  /** Drops one ordered lane from the identity view, and the index's entry with its last lane. */
  // The reference comparison below is the POINT of this method, not an oversight: see the block comment on it.
  @SuppressWarnings("PMD.CompareObjectsWithEquals")
  private void forgetOrderedLane(final IndexInternal owner,
      final TreeMap<ComparableKey, Map<IndexKey, IndexKey>> lane) {
    if (owner == null) {
      // A lane restored by setKeys: it was never in the identity view, and its absence is what lanesWithoutOwner
      // already accounts for. Recomputed rather than cleared, because another such lane may still be present.
      lanesWithoutOwner = indexEntries.size() > countOwnedOrderedLanes();
      return;
    }
    final List<TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> lanes = orderedLanesPerIndex.get(owner);
    if (lanes == null)
      return;

    // BY IDENTITY, never List.remove(Object) (PR #8001 review). TreeMap inherits equals() from AbstractMap, so it
    // compares by CONTENT - and two empty ones are always equal. An index can own more than one lane (a compaction
    // that renames it mid-transaction opens a second one under the new name), and the lane reaching here has just
    // been emptied, so a content-equality removal could evict the OTHER lane of the same index instead. That lane
    // would still be in indexEntries and still be replayed by commit(), but no longer reachable through the
    // identity fast path: a read-your-own-writes search would silently answer with part of the transaction's own
    // writes missing. Which is the failure mode this map is an IdentityHashMap to avoid in the first place.
    for (int i = 0; i < lanes.size(); i++)
      if (lanes.get(i) == lane) {
        lanes.remove(i);
        break;
      }

    if (lanes.isEmpty())
      orderedLanesPerIndex.remove(owner);
  }

  private int countOwnedOrderedLanes() {
    int owned = 0;
    for (final List<TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> lanes : orderedLanesPerIndex.values())
      owned += lanes.size();
    return owned;
  }

  /**
   * The name a lane's index answers to NOW: the current name of the reference captured when the lane was opened,
   * falling back to the lane's own key for lanes restored wholesale by {@link #setKeys}, which carry no reference.
   * See {@link #indexPerLane}.
   * <p>
   * The captured reference is used to find the NAME, never as the index to operate on. Those are not always the same
   * object: a wrapper index queues its entries under the index it wraps - {@code LSMTreeFullTextIndex} tokenizes text
   * and calls through to its {@code LSMTreeIndex}, which is what reaches {@code addIndexKeyLock} - while the schema
   * registers the WRAPPER under that same name. Replaying through the captured inner index instead of the registered
   * wrapper would quietly change which implementation of {@code putReplay}/{@code removeReplay},
   * {@code getAssociatedBucketId} and {@code getFileIds} the commit uses. So this fix moves only the one thing that
   * was wrong - the name being looked up - and leaves the schema the sole authority on what that name resolves to.
   * <p>
   * That also means an index dropped and re-created under the same name inside one transaction resolves to the new
   * one, exactly as it did before this fix: no behaviour of that (already ill-defined) sequence is changed here.
   * <p>
   * The {@code setKeys} fallback is not a residual hole either. {@code setKeys} is only used by
   * {@code TransactionContext.commitFromReplica}, and a replica never renames a vector index behind its own back:
   * {@code LSMVectorIndex.isCompactionAllowedOnThisNode} refuses to schedule a compaction on anything but a standalone
   * database or the current leader, and an explicit {@code COMPACT INDEX} is a DDL statement a follower forwards to
   * the leader. A follower's copy is renamed only when it adopts the component the leader shipped it, which arrives
   * through the schema update rather than concurrently with a replicated commit it is already applying.
   */
  private String laneIndexName(final String laneName) {
    final IndexInternal index = indexPerLane.get(laneName);
    return index != null ? index.getName() : laneName;
  }

  /** The index a lane belongs to, as the schema currently resolves it. See {@link #laneIndexName}. */
  private IndexInternal resolveIndex(final String laneName) {
    return (IndexInternal) database.getSchema().getIndexByName(laneIndexName(laneName));
  }

  /**
   * Whether the index a lane belongs to still exists in the schema - asked of the name the index answers to NOW, so
   * an index that renamed itself since the lane was opened is kept (issue #6105) while one dropped mid-transaction
   * (TYPE DROP) is still reported as gone.
   */
  private boolean laneIndexStillExists(final String laneName) {
    return database.getSchema().existsIndex(laneIndexName(laneName));
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

  /**
   * Looks a lane up by its KEY, which is the name the index answered to when the lane was opened - not necessarily
   * the name it answers to now. Correct for every caller today: only {@code LSMVectorIndex} renames itself and it
   * never reaches here (the read-your-own-writes callers are {@code HashIndex}, {@code LSMTreeIndex} and
   * {@code LSMTreeIndexCursor}, none of which rename), and each of them passes its OWN unchanging name. An index
   * type that gains a rename - i.e. one that starts calling {@code LocalSchema.indexRenamed} - has to reach its
   * lane through {@link #laneIndexName} instead, or it reproduces issue #6105 here.
   */
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
    indexEntries.keySet().removeIf(indexName -> !laneIndexStillExists(indexName));
    unorderedEntries.keySet().removeIf(indexName -> !laneIndexStillExists(indexName));

    checkUniqueIndexKeys();

    // APPEND-ONLY LANE FIRST: ITS ENTRIES CARRY NO UNIQUENESS CONSTRAINT AND REPLAY STRICTLY IN
    // INSERTION ORDER, SO A REMOVE FOLLOWED BY AN ADD ON THE SAME KEY ENDS WITH THE ADD (AND VICE
    // VERSA) WITHOUT THE TWO-PHASE REMOVE-THEN-ADD SPLIT THE ORDERED LANE NEEDS FOR ITS DEDUP MAP.
    for (final Map.Entry<String, List<IndexKey>> entry : unorderedEntries.entrySet()) {
      final IndexInternal index = resolveIndex(entry.getKey());
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
      final IndexInternal index = resolveIndex(entry.getKey());
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

    // SECOND PASS over indexEntries, and it handles only the ADDs. Every REMOVE - for a vector index as much as for
    // any other - was already replayed through index.removeReplay() by the pass above, which is why the vector
    // batch below can skip a RID whose last entry is a REMOVE without leaving it untombstoned: the tombstone has
    // happened, and skipping only avoids adding it straight back (PR #8001 review asked for this to be said here
    // rather than only at the skip itself).
    //
    // Per dense vector index, the winning entry per RID across ALL of that index's lanes. Identity-keyed for the
    // same reason the lane map is: which index a batch belongs to is a question about the object.
    final Map<LSMVectorIndex, Map<RID, IndexKey>> vectorBatches = new IdentityHashMap<>();

    for (final Map.Entry<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> entry : indexEntries.entrySet()) {
      final IndexInternal index = resolveIndex(entry.getKey());
      final Map<ComparableKey, Map<IndexKey, IndexKey>> keys = entry.getValue();

      // Batch optimization for vector indexes (issue #3864): collect all ADD operations
      // and process them in a single putBatch call with one lock acquisition.
      //
      // Accumulated ACROSS LANES and flushed after this loop, not per lane (PR #8001 review). One index can own
      // more than one lane - it renames itself when a compaction swaps in the component file it is named after, and
      // the next write opens a lane under the new name (issue #6105) - so a record rewritten either side of that
      // rename has an entry in each. Deduplicating per lane would then hand putBatch one winner from each, which is
      // the very thing this dedup exists to prevent: the record indexed under two of the embeddings it held.
      if (index instanceof LSMVectorIndex vectorIndex) {
        // merge, not putAll: a later lane's entry only wins if it was written later, which is the whole point.
        final Map<RID, IndexKey> batch = vectorBatches.computeIfAbsent(vectorIndex, i -> new LinkedHashMap<>());
        for (final Map.Entry<RID, IndexKey> winner : lastWritePerRidOf(keys).entrySet())
          batch.merge(winner.getKey(), winner.getValue(),
              (previous, current) -> current.sequence > previous.sequence ? current : previous);
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

    // ONE embedding per record, and the LAST one written is the one the record holds (issue #7971).
    //
    // A dense vector index keeps a single live vector per RID - remove() tombstones every vector id the RID
    // resolves to - so several ADDs for one RID are not several entries to insert, they are one entry written
    // several times inside this transaction. Replaying them all persists a vector id per rewrite, leaves the
    // record indexed under every embedding it ever held during the transaction, and lets the ComparableVector
    // order of the TreeMap being walked - a hash of the vector's contents - decide which of them a search ranks it
    // by. The write-order stamp is what turns that back into "the last write wins": the survivor is chosen by WHEN
    // it was queued, not by where its key sorted, nor by which lane it landed in.
    // Through putBatch unconditionally, including a lane carrying a single entry - which took putReplay directly
    // before the cross-lane accumulation above made that distinction unrepresentable. Measured rather than assumed
    // (PR #8001 review asked for it): 20k single-row transactions against a 128-dimension index run in 684 ms
    // through putBatch and 685 ms through a putReplay fast path, i.e. the two ArrayLists and the map entry cost
    // nothing measurable next to the page write and WAL append each row already pays for.
    for (final Map.Entry<LSMVectorIndex, Map<RID, IndexKey>> batch : vectorBatches.entrySet()) {
      final Map<RID, IndexKey> winners = batch.getValue();
      if (winners.isEmpty())
        continue;

      final List<Object[]> batchKeys = new ArrayList<>(winners.size());
      final List<RID> batchRids = new ArrayList<>(winners.size());
      for (final IndexKey key : winners.values()) {
        if (key.operation == IndexKey.IndexKeyOperation.REMOVE)
          // The transaction's last word on this RID was a removal, and the pass above has already replayed it.
          // Re-adding it here is what made a record deleted after being added come back (PR #8001 review).
          continue;
        batchKeys.add(key.keyValues);
        batchRids.add(key.rid);
      }
      if (batchKeys.isEmpty())
        continue;
      batch.getKey().putBatch(batchKeys, batchRids);
    }

    indexEntries.clear();
    indexPerLane.clear();
    orderedLanesPerIndex.clear();
    lanesWithoutOwner = false;
    sequence = 0;
    ++laneVersion;
    if (cachedViews != null)
      cachedViews.clear();
  }

  /**
   * The LAST entry written for each RID within one lane, whatever it was. Merged across the lanes of one index by
   * {@link #commit()}, which is where "last write wins" actually has to hold.
   * <p>
   * {@code REMOVE} entries are weighed in, not filtered out (PR #8001 review). The commit replays every
   * {@code REMOVE} before any {@code ADD}, so a removal cancels an addition only by DISPLACING it in the per-key
   * map - which requires the two to share a {@code ComparableKey}. They usually do, because
   * {@code LSMVectorIndex.removalKey()} queues the vector being retired. When it cannot - the caller had no usable
   * old value and the removal rides the placeholder - they do not, and a pass that considered only
   * {@code ADD}/{@code REPLACE} would re-add a RID the transaction had deleted. Keeping the last entry of ANY kind
   * and letting the caller drop a RID whose last word was a removal answers that without depending on the keys
   * lining up.
   */
  private static Map<RID, IndexKey> lastWritePerRidOf(final Map<ComparableKey, Map<IndexKey, IndexKey>> keys) {
    final Map<RID, IndexKey> winners = new LinkedHashMap<>(keys.size());
    for (final Map.Entry<ComparableKey, Map<IndexKey, IndexKey>> keyValueEntries : keys.entrySet())
      for (final IndexKey key : keyValueEntries.getValue().values())
        winners.merge(key.rid, key, (previous, current) -> current.sequence > previous.sequence ? current : previous);
    return winners;
  }

  public void addFilesToLock(final IntHashSet modifiedFiles) {
    final Schema schema = database.getSchema();

    final Set<Index> lockedIndexes = new HashSet<>(indexEntries.size() + unorderedEntries.size());

    final List<String> indexNames = new ArrayList<>(indexEntries.size() + unorderedEntries.size());
    indexNames.addAll(indexEntries.keySet());
    indexNames.addAll(unorderedEntries.keySet());

    for (final String indexName : indexNames) {
      if (!laneIndexStillExists(indexName))
        // INDEX WAS DROPPED DURING THE TRANSACTION (e.g. TYPE DROP), SKIP IT
        continue;

      final IndexInternal index = resolveIndex(indexName);

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
        //
        // A MANUAL index has no type to fan out over (#5765): it is the only index holding its keys, and its own
        // files are already in the set above. Reading getTypeName() as a type name threw a NullPointerException
        // here, so a unique manual index could not commit a single entry.
        final String typeName = index.getTypeName();
        if (typeName != null) {
          final DocumentType type = schema.getType(typeName);
          for (final TypeIndex typeIndex : type.getAllIndexes(true))
            if (typeIndex.isUnique())
              for (final IndexInternal idx : typeIndex.getIndexesOnBuckets())
                modifiedFiles.add(idx.getFileId());
        }
      } else if (associatedBucketId >= 0)
        modifiedFiles.add(associatedBucketId);
    }
  }

  public Map<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> toMap() {
    return indexEntries;
  }

  public void setKeys(final Map<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> keysTx) {
    indexEntries = keysTx;
    // These lanes carry no index reference, so they cannot be resolved by identity: getIndexKeyLanes falls back to
    // the by-name walk while any of them is present (issue #7967).
    orderedLanesPerIndex.clear();
    lanesWithoutOwner = !keysTx.isEmpty();
    ++laneVersion;
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
        // Recorded once per lane, at creation, not once per key: the lane belongs to THIS index whatever it ends up
        // being called by commit time (issue #6105).
        indexPerLane.put(indexName, index);
      }
      lane.add(new IndexKey(false, operation, keysValues, rid, sequence++));
      ++laneVersion;
      journalAppend(indexName, lane);
      return;
    }

    TreeMap<ComparableKey, Map<IndexKey, IndexKey>> keys = indexEntries.get(indexName);

    final ComparableKey k = new ComparableKey(keysValues);
    final IndexKey v = new IndexKey(index.isUnique(), operation, keysValues, rid, sequence++);

    Map<IndexKey, IndexKey> values;
    if (keys == null) {
      keys = new TreeMap<>(); // ORDERED TO KEEP INSERTION ORDER
      indexEntries.put(indexName, keys);
      // See the sibling call in the append-only branch above (issue #6105).
      indexPerLane.put(indexName, index);
      // ...and the same lane reachable by the index itself, which is how a reader finds it in O(1) (issue #7967).
      orderedLanesPerIndex.computeIfAbsent(index, i -> new ArrayList<>(2)).add(keys);

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

    final IndexKey displaced = values.put(v, v);
    ++laneVersion;
    journalPut(indexName, keys, k, values, v, displaced);
  }

  /**
   * Starts journalling what the next {@code addIndexKeyLock} calls add, so {@link #undoRecordChanges()} can take
   * them back. Always paired with {@link #disarmRecordUndo()} in a {@code finally}: see {@link #recordUndo}.
   * <p>
   * Not re-entrant, and does not need to be. The one caller indexes ONE record between the two calls, on the
   * thread the transaction is bound to, and nothing it invokes indexes another.
   */
  public void armRecordUndo() {
    recordUndoSize = 0;
  }

  /** Stops journalling and drops what was journalled. Idempotent. */
  public void disarmRecordUndo() {
    recordUndoSize = -1;
  }

  /**
   * Takes back every index entry journalled since {@link #armRecordUndo()}, restoring the map to the state it was
   * in - the displaced entries included - and leaving no empty lane behind, so {@code isEmpty()} and
   * {@code addFilesToLock} answer as though the record had never been indexed.
   */
  public void undoRecordChanges() {
    for (int i = recordUndoSize - 1; i >= 0; i--) {
      final RecordUndoEntry undo = recordUndo.get(i);

      if (undo.lane != null) {
        // Append-only lane: our entry is the one we appended, and nothing was displaced to restore.
        undo.lane.remove(undo.lane.size() - 1);
        if (undo.lane.isEmpty()) {
          unorderedEntries.remove(undo.indexName);
          indexPerLane.remove(undo.indexName);
        }
        continue;

      }

      if (undo.displaced != null)
        undo.values.put(undo.displaced, undo.displaced);
      else {
        undo.values.remove(undo.added);
        if (undo.values.isEmpty()) {
          undo.keys.remove(undo.key);
          if (undo.keys.isEmpty()) {
            indexEntries.remove(undo.indexName);
            forgetOrderedLane(indexPerLane.remove(undo.indexName), undo.keys);
          }
        }
      }
    }
    recordUndoSize = 0;
    // Entries went away, so any view cached over them describes lanes that no longer exist. sequence does not move
    // on this path - nothing was queued - which is exactly why the cache is keyed on laneVersion and not on it.
    ++laneVersion;
  }

  private void journalAppend(final String indexName, final List<IndexKey> lane) {
    if (recordUndoSize >= 0)
      nextUndoEntry().appended(indexName, lane);
  }

  private void journalPut(final String indexName, final TreeMap<ComparableKey, Map<IndexKey, IndexKey>> keys,
      final ComparableKey key, final Map<IndexKey, IndexKey> values, final IndexKey added, final IndexKey displaced) {
    if (recordUndoSize >= 0)
      nextUndoEntry().ordered(indexName, keys, key, values, added, displaced);
  }

  /** The next holder of the reused journal, growing the list only the first time that depth is reached. */
  private RecordUndoEntry nextUndoEntry() {
    if (recordUndoSize == recordUndo.size())
      recordUndo.add(new RecordUndoEntry());
    return recordUndo.get(recordUndoSize++);
  }

  public void reset() {
    indexEntries.clear();
    unorderedEntries.clear();
    indexPerLane.clear();
    orderedLanesPerIndex.clear();
    lanesWithoutOwner = false;
    sequence = 0;
    ++laneVersion;
    if (cachedViews != null)
      cachedViews.clear();
    // The holders stay - they are the reusable journal - but not the maps and keys they point at, which the
    // three clears above have just made garbage.
    for (int i = 0; i < recordUndo.size(); i++)
      recordUndo.get(i).clear();
    recordUndoSize = -1;
  }

  /** Looks a lane up by its KEY, with the same caveat as {@link #getTotalEntriesByIndex}. */
  public TreeMap<ComparableKey, Map<IndexKey, IndexKey>> getIndexKeys(final String indexName) {
    return indexEntries.get(indexName);
  }

  /**
   * Every lane belonging to {@code index}, in the order {@link #commit()} replays them - regardless of the names
   * those lanes were opened under (issue #7378).
   * <p>
   * A lane is keyed by the name the index answered to when its FIRST entry was queued, and an {@code LSM_VECTOR}
   * index renames itself when a compaction swaps in the component file it is named after. That compaction runs on
   * the async executor, so it can land mid-transaction - which is issue #6105 on the write side, and is why
   * {@link #indexPerLane} remembers the index rather than the name.
   * <p>
   * It also means one index can own MORE THAN ONE lane. {@code addIndexKeyLock} opens a fresh lane whenever
   * {@code index.getName()} is a name this transaction has not seen, so a transaction that writes, is renamed
   * under it, and writes again leaves two lanes for one index - one under each name. {@link #commit()} replays
   * both, because it resolves every lane through {@link #laneIndexName}. A reader that stopped at the first
   * match would see only one of them and would answer a search with part of the transaction's own writes
   * missing, which is precisely the defect the overlay in {@code LSMVectorIndex} exists to close. So this returns
   * all of them.
   * <p>
   * The walk is over {@link #indexEntries} and not over {@code indexPerLane}: the former is a
   * {@link LinkedHashMap} in lane-creation order, which is the order {@code commit()} replays in and therefore
   * the order a reader has to merge in for "the last write to a RID wins" to mean the same thing on both sides.
   * {@code indexPerLane} is a {@link HashMap} and its iteration order says nothing.
   * <p>
   * Cost is one hash lookup per lane this transaction has opened - O(1) for the ordinary transaction that has
   * touched one index, O(distinct indexes touched) for one that has touched several, and it is paid even when the
   * answer is empty. See issue #7967.
   *
   * @return the lanes, in replay order; empty when this transaction has queued nothing for {@code index}
   */
  public List<TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> getIndexKeyLanes(final IndexInternal index) {
    if (!lanesWithoutOwner) {
      // One hash lookup, whatever this transaction has written to and however many names this index has answered
      // to (issue #7967). Registered lane by lane as they are opened, so the answer is the identical set the walk
      // below produces - and the empty answer, which is what every search from a transaction that never touched
      // this index gets, costs the same lookup rather than a scan of every other index's lanes.
      final List<TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> lanes = orderedLanesPerIndex.get(index);
      return lanes == null ? List.of() : lanes;
    }

    // A lane restored wholesale by setKeys is present: it carries no index reference, so identity cannot find it
    // and the by-name rule laneIndexName applies is the only one that can. Unreachable today - setKeys is only
    // used by commitFromReplica, whose status is never BEGUN - but the two must not drift.
    List<TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> lanes = null;

    for (final Map.Entry<String, TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> lane : indexEntries.entrySet()) {
      final IndexInternal owner = indexPerLane.get(lane.getKey());
      if (owner == null ? !lane.getKey().equals(index.getName()) : owner != index)
        continue;

      if (lanes == null)
        lanes = new ArrayList<>(2);
      lanes.add(lane.getValue());
    }

    return lanes == null ? List.of() : lanes;
  }

  /**
   * The read-your-own-writes view this transaction last built for {@code index}, or {@code null} when it has none
   * or the lanes have changed since (issue #7967).
   * <p>
   * Building that view costs one pass over everything the transaction has queued for the index plus a conversion
   * per pending row, and the search paths rebuilt it from scratch on EVERY search - so a transaction that ingests
   * and queries in turn paid its whole write set again per query. The view is a pure function of the lanes, so it
   * stays valid for exactly as long as they do not change, and {@link #laneVersion} moves on every change that
   * could alter it: an entry queued, an entry taken back, a lane dropped, the whole set replaced. A view returned
   * here is therefore the same object the caller would have rebuilt, never an older one.
   * <p>
   * Typed as {@code Object} on purpose. What the view IS belongs to the index that builds it - this class holds
   * it for the lifetime of a version and looks at nothing inside it - and the alternative is to widen an
   * index-internal type into this package to name it.
   */
  public Object cachedIndexView(final IndexInternal index) {
    if (cachedViews == null || cachedViewVersion != laneVersion)
      return null;
    return cachedViews.get(index);
  }

  /**
   * Remembers {@code view} as {@code index}'s read-your-own-writes view of the lanes AS THEY STAND NOW. Discarded
   * by the next change to them. See {@link #cachedIndexView}.
   * <p>
   * A {@code null} view - "this transaction has written nothing to this index" - is worth caching too, and is in
   * fact the answer most worth caching: it is the one a search from a transaction busy with OTHER indexes gets,
   * over and over. {@link #NO_VIEW} stands in for it, because a null value in the map is indistinguishable from an
   * absent key.
   */
  public void cacheIndexView(final IndexInternal index, final Object view) {
    if (cachedViews == null)
      cachedViews = new IdentityHashMap<>();
    else if (cachedViewVersion != laneVersion)
      cachedViews.clear();
    cachedViewVersion = laneVersion;
    cachedViews.put(index, view == null ? NO_VIEW : view);
  }

  /**
   * The append-only lane of an index that opted out of the key-ordered map, in the order its entries were queued -
   * which is the order {@link #commit()} replays them in, and therefore the order anything reading its own writes
   * back has to apply them in (issue #7966).
   * <p>
   * Returned live rather than copied: the caller is the transaction's own thread, the only thread that can append
   * to it, and a sparse-vector search resolves this once per query over a lane that holds one entry per non-zero
   * dimension written. Same lookup-by-KEY caveat as {@link #getTotalEntriesByIndex}, and for the same reason it
   * does not bite: {@code LSM_SPARSE_VECTOR} is the only index on this lane and it never renames itself.
   *
   * @return the lane, or {@code null} when this transaction has queued nothing for that index
   */
  public List<IndexKey> getUnorderedIndexKeys(final String indexName) {
    return unorderedEntries.get(indexName);
  }

  /**
   * Called at commit time in the middle of the lock to avoid concurrent insertion of the same key.
   */
  private void checkUniqueIndexKeys(final Index index, final IndexKey key, final RID deleted) {
    // CHECK UNIQUENESS ACROSS ALL THE INDEXES FOR ALL THE BUCKETS.
    // A MANUAL index has no type (#5765): nothing else indexes its keys, so it IS the whole search space and the
    // polymorphic lookup below has nothing to widen it to. Resolving getTypeName() as a type name threw a
    // NullPointerException here, which is where a unique manual index failed to commit its first entry.
    final Index idx;
    if (index.getTypeName() == null)
      idx = index;
    else
      idx = database.getSchema().getType(index.getTypeName()).getPolymorphicIndexByProperties(index.getPropertyNames());

    if (idx != null) {
      // #5662: try-with-resources - the cursor stops after at most two entries, so it is never drained
      try (final IndexCursor found = idx.get(key.keyValues, 2)) {
        if (!found.hasNext())
          return;

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
      final IndexInternal index = resolveIndex(indexEntries.getKey());
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
      final IndexInternal index = resolveIndex(indexEntries.getKey());
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
