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
package com.arcadedb.query.facet;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarProjection;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The dictionary ordinals of the buckets (and subtypes) of one declaring type, merged into one dense ordinal space per property, so a
 * facet over several buckets counts in one array (Lucene's {@code OrdinalMap}). Dictionaries are per bucket: the same value has
 * another ordinal in each bucket, and a compaction replaces a bucket's dictionary by a new instance with new ordinals.
 * <p>
 * {@link #of} answers the map of one query, which never changes while the query uses it:
 * <ul>
 *   <li>one snapshot: an {@link IdentityOrdinalMap}, the bucket's own ordinals (no array, no hash);</li>
 *   <li>several: a {@link MergedOrdinalMap} over a cached entry per (declaring type, property) and database. Merged ordinals are given
 *   by value in first-seen order and never reassigned while the entry lives; per bucket the entry holds the dictionary instance it
 *   translated and an {@code int[]} from bucket ordinal to merged ordinal, extended with the new tail when the dictionary grew
 *   (append-only) and rebuilt when the bucket's current dictionary is another instance (a compaction), which bumps
 *   {@link #version()}. Arrays are published copy-on-write, so a concurrent query keeps those it read. A snapshot holding a dictionary
 *   that is neither the cached one nor its bucket's current one (a compaction raced the acquire) gets an array of its own, uncached
 *   and charged to {@link FacetMemoryBudget}. A user who may not read every bucket of the declaring type gets an uncached entry of
 *   the query's own, so the merge holds the values of the buckets it reads only.</li>
 * </ul>
 * A value no bucket references any more keeps its merged ordinal until the entry is dropped: the entries live in the database's
 * {@link ColumnarIndexManager#ordinalMapEntry}, cleared on any columnar schema event (they are rare: a projection or a column
 * created, dropped, renamed or retyped) and when the database closes. So an entry holds
 * at most the distinct values times the buckets, never anything proportional to the records.
 * <p>
 * {@link #close()} releases the memory a private array reserved; idempotent.
 */
public abstract class CrossBucketOrdinalMap implements AutoCloseable {
  /** Every version of every entry is drawn here, so a version never repeats, not even after an entry was dropped and recreated. */
  private static final AtomicLong VERSIONS = new AtomicLong();

  CrossBucketOrdinalMap() {
  }

  /**
   * The map of a query over the snapshots of the declaring type's buckets.
   *
   * @param typeName  the declaring type: the type of the COLUMNAR index on the property (one per hierarchy)
   * @param property  a dictionary-encoded column (ORDINAL or ORDINAL_LIST) every snapshot publishes
   * @param snapshots at least one, each of a distinct bucket and holding a dictionary of the property
   *
   * @return the map, or null when the memory budget refused the private array of an older dictionary generation
   */
  public static CrossBucketOrdinalMap of(final DatabaseInternal db, final String typeName, final String property,
      final ColumnarSnapshot[] snapshots) {
    final ColumnarIndexManager manager = ((LocalDatabase) db.getEmbedded()).getColumnarIndexManager();
    // the cached entry merges every bucket any query read: its size would count the values of buckets this user may not read
    final Entry entry = readsEveryBucket(db, typeName) ? entry(manager, typeName, property) : new Entry();
    for (final ColumnarSnapshot snapshot : snapshots)
      if (snapshot.dictionary(property) == null)
        throw new IllegalArgumentException("The snapshot of bucket " + snapshot.bucketId() + " has no dictionary of '" + property + "'");
    if (snapshots.length == 1)
      return entry.identity(snapshots[0].bucketId(), snapshots[0].dictionary(property));
    return entry.merged(manager, property, snapshots);
  }

  /** The merged ordinal of the bucket's ordinal; -1 for a bucket or ordinal outside the snapshots the map was taken for. */
  public abstract int merged(int bucketId, int bucketOrdinal);

  /** The value of a merged ordinal below {@link #size()}. */
  public abstract Object value(int mergedOrdinal);

  /** The number of merged ordinals: a counter indexed by merged ordinal has this many slots. */
  public abstract int size();

  /** The merged ordinal of a value, -1 when no snapshot's dictionary holds it (a value only a patched record has). */
  public abstract int mergedOf(Object value);

  /** Changes whenever a bucket array is rebuilt; consumers caching per-ordinal state (plan 5) key on it. */
  public abstract long version();

  /**
   * The bucket's ordinal to merged ordinal translation, indexed by bucket ordinal, for a collector's per-row loop; null when the
   * bucket ordinals are the merged ordinals (the identity map). Shared, never write it.
   */
  abstract int[] bucketArray(int bucketId);

  @Override
  public void close() {
  }

  /**
   * Whether the current user may read every bucket of the declaring type and its subtypes. When not, the query merges its own snapshots
   * in an uncached entry: its counter size and PROFILE cardinality then depend on the dictionaries of the buckets it reads only, never
   * on values only hidden records hold, nor on what another user counted before.
   */
  private static boolean readsEveryBucket(final DatabaseInternal db, final String typeName) {
    final SecurityDatabaseUser user = SecurityHelper.currentUser(db);
    if (user == null)
      return true;
    for (final Bucket bucket : db.getSchema().getType(typeName).getBuckets(true))
      if (!user.requestAccessOnFile(bucket.getFileId(), SecurityDatabaseUser.ACCESS.READ_RECORD))
        return false;
    return true;
  }

  /** The database's entry of the (declaring type, property), held by its {@link ColumnarIndexManager#ordinalMapEntry}. */
  private static Entry entry(final ColumnarIndexManager manager, final String typeName, final String property) {
    return (Entry) manager.ordinalMapEntry(new Key(typeName, property), k -> new Entry());
  }

  private static Object normalize(final Object value) {
    return FacetValueComparator.normalize(value);
  }

  private record Key(String typeName, String property) {
  }

  /** The bucket ordinals are the merged ordinals: a type read through one bucket. */
  static final class IdentityOrdinalMap extends CrossBucketOrdinalMap {
    private final GlobalDictionary dictionary;
    private final int              size;
    private final long             version;

    private IdentityOrdinalMap(final GlobalDictionary dictionary, final long version) {
      this.dictionary = dictionary;
      // read once: the writer keeps appending to the dictionary, the snapshot's ordinals are all below what is read here
      this.size = dictionary.size();
      this.version = version;
    }

    @Override
    public int merged(final int bucketId, final int bucketOrdinal) {
      return bucketOrdinal;
    }

    @Override
    public Object value(final int mergedOrdinal) {
      return dictionary.valueOf(mergedOrdinal);
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public int mergedOf(final Object value) {
      final int ordinal = dictionary.ordinalOf(normalize(value));
      return ordinal < size ? ordinal : -1;
    }

    @Override
    public long version() {
      return version;
    }

    /** The dictionary the ordinals are of, for a collector ranking by {@link GlobalDictionary#compare}. */
    GlobalDictionary dictionary() {
      return dictionary;
    }

    @Override
    int[] bucketArray(final int bucketId) {
      return null;
    }
  }

  /** The map of one query over several buckets: the entry's arrays as they were when the query took it, plus its private arrays. */
  static final class MergedOrdinalMap extends CrossBucketOrdinalMap {
    private final Entry                         entry;
    private final int[]                         bucketIds;
    private final int[][]                       toMerged;
    private final GlobalDictionary[]            dictionaries;
    private final Object[]                      values;
    private final int                           size;
    private final long                          version;
    private final FacetMemoryBudget.Reservation reservation;

    private MergedOrdinalMap(final Entry entry, final int[] bucketIds, final int[][] toMerged, final GlobalDictionary[] dictionaries,
        final Object[] values, final int size, final long version, final FacetMemoryBudget.Reservation reservation) {
      this.entry = entry;
      this.bucketIds = bucketIds;
      this.toMerged = toMerged;
      this.dictionaries = dictionaries;
      this.values = values;
      this.size = size;
      this.version = version;
      this.reservation = reservation;
    }

    @Override
    public int merged(final int bucketId, final int bucketOrdinal) {
      final int[] array = bucketArray(bucketId);
      return array != null && bucketOrdinal >= 0 && bucketOrdinal < array.length ? array[bucketOrdinal] : -1;
    }

    @Override
    public Object value(final int mergedOrdinal) {
      if (mergedOrdinal < 0 || mergedOrdinal >= size)
        throw new IndexOutOfBoundsException("Merged ordinal " + mergedOrdinal + " out of bounds for size " + size);
      return values[mergedOrdinal];
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public int mergedOf(final Object value) {
      final Object normalized = normalize(value);
      for (final int bucketId : bucketIds) {
        final int ordinal = dictionaries[bucketId].ordinalOf(normalized);
        final int[] array = toMerged[bucketId];
        if (ordinal >= 0 && ordinal < array.length)
          return array[ordinal];
      }
      return -1;
    }

    @Override
    public long version() {
      return version;
    }

    @Override
    int[] bucketArray(final int bucketId) {
      return bucketId >= 0 && bucketId < toMerged.length ? toMerged[bucketId] : null;
    }

    @Override
    public void close() {
      if (reservation != null)
        reservation.close();
    }

    /** The cache entry the map was taken from (tests: the same entry serves later queries). */
    Entry entry() {
      return entry;
    }

    /** The bytes the private arrays of the map reserved, 0 when every array is the cached one. */
    long privateBytes() {
      return reservation == null ? 0 : reservation.bytes();
    }
  }

  /** The published arrays of an entry, indexed by bucket id; immutable once published. */
  private record State(GlobalDictionary[] dictionaries, int[][] toMerged, Object[] values, int size, long version) {
    /** A state with an array of its own: the values array is appended to in place, it is never shared between entries. */
    static State empty() {
      return new State(new GlobalDictionary[0], new int[0][], new Object[16], 0, 0);
    }

    boolean covers(final int bucketId, final GlobalDictionary dictionary, final int dictionarySize) {
      return bucketId < dictionaries.length && dictionaries[bucketId] == dictionary && toMerged[bucketId].length >= dictionarySize;
    }
  }

  /** The cached map of one (declaring type, property) of a database. */
  static final class Entry {
    /** Only read and written under the entry's lock. */
    private final HashMap<Object, Integer> mergedByValue = new HashMap<>();
    private volatile State                 state         = State.empty();
    /** By bucket id: the dictionary the identity map of a query reading that bucket alone delegated to last, and its version. */
    private volatile Identity[]            identities    = new Identity[0];

    private record Identity(GlobalDictionary dictionary, long version) {
    }

    /** Keyed per bucket, so queries reading one bucket alone, then another, do not bump each other's version. */
    private IdentityOrdinalMap identity(final int bucketId, final GlobalDictionary dictionary) {
      Identity[] current = identities;
      Identity identity = bucketId < current.length ? current[bucketId] : null;
      if (identity == null || identity.dictionary != dictionary)
        synchronized (this) {
          current = identities;
          identity = bucketId < current.length ? current[bucketId] : null;
          if (identity == null || identity.dictionary != dictionary) {
            final Identity[] next = Arrays.copyOf(current, Math.max(current.length, bucketId + 1));
            identity = new Identity(dictionary, VERSIONS.incrementAndGet());
            next[bucketId] = identity;
            identities = next;
          }
        }
      return new IdentityOrdinalMap(dictionary, identity.version);
    }

    private MergedOrdinalMap merged(final ColumnarIndexManager manager, final String property, final ColumnarSnapshot[] snapshots) {
      final int[] sizes = new int[snapshots.length];
      int maxBucketId = 0;
      for (int i = 0; i < snapshots.length; i++) {
        // read once: the snapshot's ordinals are all below this size, later appends are not the query's
        sizes[i] = snapshots[i].dictionary(property).size();
        maxBucketId = Math.max(maxBucketId, snapshots[i].bucketId());
      }

      State current = state;
      if (!coversAll(current, property, snapshots, sizes))
        synchronized (this) {
          current = state;
          if (!coversAll(current, property, snapshots, sizes))
            return extend(manager, property, snapshots, sizes, maxBucketId);
        }
      return view(current, property, snapshots, maxBucketId, null, null);
    }

    private static boolean coversAll(final State current, final String property, final ColumnarSnapshot[] snapshots, final int[] sizes) {
      for (int i = 0; i < snapshots.length; i++)
        if (!current.covers(snapshots[i].bucketId(), snapshots[i].dictionary(property), sizes[i]))
          return false;
      return true;
    }

    /**
     * Under the entry's lock: extends the arrays of the dictionaries that grew, rebuilds those of the buckets whose current dictionary
     * is a new instance, builds a private array for any older one, then publishes the new state.
     */
    private MergedOrdinalMap extend(final ColumnarIndexManager manager, final String property, final ColumnarSnapshot[] snapshots,
        final int[] sizes, final int maxBucketId) {
      final State previous = state;
      final int length = Math.max(previous.dictionaries.length, maxBucketId + 1);
      final GlobalDictionary[] dictionaries = Arrays.copyOf(previous.dictionaries, length);
      final int[][] toMerged = Arrays.copyOf(previous.toMerged, length);
      final Values values = new Values(previous.values, previous.size);
      try {
        return extend(manager, property, snapshots, sizes, maxBucketId, previous, dictionaries, toMerged, values);
      } catch (final RuntimeException | Error e) {
        // nothing was published: forget the values given a merged ordinal on the way, so a later extension reusing their slots never
        // resolves them to another value
        for (int merged = previous.size; merged < values.size; merged++)
          mergedByValue.remove(values.array[merged]);
        throw e;
      }
    }

    private MergedOrdinalMap extend(final ColumnarIndexManager manager, final String property, final ColumnarSnapshot[] snapshots,
        final int[] sizes, final int maxBucketId, final State previous, final GlobalDictionary[] dictionaries, final int[][] toMerged,
        final Values values) {
      boolean rebuilt = false;
      int[][] privates = null;
      long privateBytes = 0;

      for (int i = 0; i < snapshots.length; i++) {
        final int bucketId = snapshots[i].bucketId();
        final GlobalDictionary dictionary = snapshots[i].dictionary(property);
        final GlobalDictionary cached = dictionaries[bucketId];
        if (cached == dictionary) {
          if (toMerged[bucketId].length < sizes[i])
            toMerged[bucketId] = translate(dictionary, toMerged[bucketId], sizes[i], values);
        } else if (cached == null || isCurrent(manager, bucketId, property, dictionary)) {
          toMerged[bucketId] = translate(dictionary, new int[0], sizes[i], values);
          dictionaries[bucketId] = dictionary;
          rebuilt |= cached != null;
        } else {
          if (privates == null)
            privates = new int[snapshots.length][];
          privates[i] = new int[0];
          privateBytes += 16L + 4L * sizes[i];
        }
      }

      // inside the entry's lock: never wait, a refusal only makes this query's facet unavailable
      final FacetMemoryBudget.Reservation reservation = privates == null ? null : FacetMemoryBudget.instance().reserve(privateBytes, 0);
      if (reservation != null)
        try {
          for (int i = 0; i < snapshots.length; i++)
            if (privates[i] != null)
              privates[i] = translate(snapshots[i].dictionary(property), privates[i], sizes[i], values);
        } catch (final RuntimeException | Error e) {
          reservation.close();
          throw e;
        }

      // published even when refused: mergedByValue already names the values appended above
      final State next = new State(dictionaries, toMerged, values.array, values.size, rebuilt ? VERSIONS.incrementAndGet() : previous.version);
      state = next;
      if (privates != null && reservation == null)
        return null;
      return view(next, property, snapshots, maxBucketId, privates, reservation);
    }

    /** {@code from} extended to {@code size} ordinals of the dictionary, a value seen for the first time getting the next merged ordinal. */
    private int[] translate(final GlobalDictionary dictionary, final int[] from, final int size, final Values values) {
      final int[] array = Arrays.copyOf(from, size);
      for (int ordinal = from.length; ordinal < size; ordinal++) {
        final Object value = dictionary.valueOf(ordinal);
        Integer merged = mergedByValue.get(value);
        if (merged == null) {
          merged = values.add(value);
          mergedByValue.put(value, merged);
        }
        array[ordinal] = merged;
      }
      return array;
    }

    private static boolean isCurrent(final ColumnarIndexManager manager, final int bucketId, final String property,
        final GlobalDictionary dictionary) {
      final ColumnarProjection projection = manager.projection(bucketId);
      return projection != null && projection.dictionary(property) == dictionary;
    }

    private MergedOrdinalMap view(final State state, final String property, final ColumnarSnapshot[] snapshots, final int maxBucketId,
        final int[][] privates, final FacetMemoryBudget.Reservation reservation) {
      final int[] bucketIds = new int[snapshots.length];
      final int[][] toMerged = new int[maxBucketId + 1][];
      final GlobalDictionary[] dictionaries = new GlobalDictionary[maxBucketId + 1];
      for (int i = 0; i < snapshots.length; i++) {
        final int bucketId = snapshots[i].bucketId();
        bucketIds[i] = bucketId;
        toMerged[bucketId] = privates != null && privates[i] != null ? privates[i] : state.toMerged[bucketId];
        dictionaries[bucketId] = snapshots[i].dictionary(property);
      }
      return new MergedOrdinalMap(this, bucketIds, toMerged, dictionaries, state.values, state.size, state.version, reservation);
    }
  }

  /**
   * The merged values being appended under an entry's lock. The array is shared with the published states: a slot is written only at or
   * above every published size, which no reader of an older state reads, and the array is copied when it is full.
   */
  private static final class Values {
    private Object[] array;
    private int      size;

    Values(final Object[] array, final int size) {
      this.array = array;
      this.size = size;
    }

    int add(final Object value) {
      if (size == array.length)
        array = Arrays.copyOf(array, array.length * 2);
      array[size] = value;
      return size++;
    }
  }
}
