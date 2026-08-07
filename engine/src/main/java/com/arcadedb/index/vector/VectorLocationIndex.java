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
package com.arcadedb.index.vector;

import com.arcadedb.database.RID;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * Lightweight index that stores only vector location metadata (absolute file offset, RID) instead of the full
 * vector data. This dramatically reduces memory usage: one location carries ~20 bytes of payload against ~3KB for
 * a 768-dimension vector.
 * <p>
 * Uses absolute file offsets for direct random access without loading full pages.
 * <p>
 * Only live vectors are resident: once an id is tombstoned its location is released and the id survives as a single
 * bit (see {@link DeletedIds}), so the cost of a location follows the number of live vectors and not the number of
 * writes (issue #5516). What still follows the writes is one bit per id handed out since the last graph rebuild -
 * 1.2MB for the 9.3M ids that used to cost 970MB.
 * <p>
 * Used by LSMVectorIndex to implement lazy-loading of vectors from disk.
 *
 * <h2>Layout (issue #5588)</h2>
 * The mapping used to be a {@code ConcurrentHashMap<Integer, VectorLocation>} plus a
 * {@code ConcurrentHashMap<RID, int[]>} reverse index, which retained ~90 bytes per live vector - a map node and
 * its table slot, a boxed {@code Integer} key, the {@code VectorLocation}, the {@code RID} it pointed at, and the
 * reverse-index node with its {@code int[]} - to carry ~20 bytes of payload. Vector ids are dense and
 * monotonically assigned, so they address arrays directly and none of that overhead is necessary:
 * <ul>
 *   <li>{@code long[] offsetAndFlag} - the absolute file offset in bits 0..62 and {@code isCompacted} in bit 63</li>
 *   <li>{@code int[] bucketId} and {@code long[] position} - the RID, materialized on demand</li>
 *   <li>{@code long[] present} - one bit per id, replacing the null check</li>
 * </ul>
 * at {@value #CHUNK_RETAINED_BYTES} bytes per {@value #CHUNK_SIZE}-id chunk, i.e. ~21 bytes per id with zero
 * per-entry objects, plus 8 to 16 bytes per live vector for the reverse index. A 10M-vector index goes from ~900MB
 * to ~320MB, and the GC no longer traces one object graph per indexed vector.
 * <p>
 * <b>The arrays are chunked, not flat, and a chunk is released as soon as its last live id is tombstoned.</b> That
 * is not an optimisation, it is what preserves the property issue #5516 exists for: memory must follow the LIVE
 * vectors, not the ids handed out. A workload that re-embeds 4K vectors every few minutes hands out 9.3M ids for a
 * live set of 4K, and a flat id-indexed array would retain ~190MB where the map it replaces retained ~360KB. Ids
 * are assigned monotonically and an update tombstones the id it supersedes, so the chunks behind the live region
 * drain wholesale and are handed back. The residual cost is the partially drained band trailing the live region:
 * a chunk holding a single live id retains {@value #CHUNK_RETAINED_BYTES} bytes, which is why {@value #CHUNK_SIZE}
 * and not a larger chunk. Making the id space dense by construction - renumbering during the data file rewrite -
 * is the structural fix and is tracked separately in issue #5870.
 *
 * <h2>Concurrency</h2>
 * Writers serialize on a single monitor; every read is lock-free and weakly consistent, matching what the
 * {@link java.util.concurrent.ConcurrentHashMap} this replaces already provided.
 * <p>
 * Publication is by release/acquire on the array element itself, through
 * {@link MethodHandles#arrayElementVarHandle}, and it has to be at both levels. A plain element store into an
 * already-published array carries no happens-before edge: a reader that read the directory field before the store
 * may legally keep observing the old element, which for the directory means reading {@code null} for a chunk that
 * exists and dropping a just-inserted vector out of every search. Re-publishing the enclosing array reference does
 * not help, because the reader's volatile read of that field happened before the writer's volatile write of it and
 * therefore synchronizes-with nothing. Hence:
 * <ul>
 *   <li>the directory slot is written with {@code setRelease} and read with {@code getAcquire}; a grown directory
 *       is additionally published through the volatile field</li>
 *   <li>the {@code present} word is written with {@code setRelease} last and read with {@code getAcquire} first,
 *       so a reader that sees the bit is guaranteed to see the payload that was written before it</li>
 * </ul>
 * Three orderings are load-bearing and are called out where they are implemented:
 * <ol>
 *   <li><b>The offset and the compacted flag live in one word.</b> {@code LSMVectorIndex.applyReplicatedPageUpdate}
 *       re-points a live id at the compacted file, on a replica, without holding the index lock. Split across two
 *       arrays a reader could pair the new flag with the stale offset, and a misaligned read of an INT8/BINARY page
 *       yields a well-formed vector of the right dimension that passes every guard - a silently wrong search
 *       result. One 64-bit word makes that structurally impossible. The RID is never rewritten while an id is
 *       present: an {@code addOrUpdate} that would change it clears the presence bit first.</li>
 *   <li><b>A chunk is dropped only after its bits are cleared</b> (asserted). A reader holding the stale reference
 *       would otherwise still see the bit set and resurrect a tombstoned vector.</li>
 *   <li><b>{@link #clear()} zeroes the live chunks before publishing the empty directory.</b> It is the only path
 *       that reuses ids (it resets the sequence to 0), so without it a reader holding a pre-clear chunk would
 *       resolve the new id 0 to the record the old one belonged to.</li>
 * </ol>
 * The monitor is always taken inside {@code LSMVectorIndex}'s write lock and never the other way round, so there is
 * no lock-order inversion to reason about.
 * <p>
 * <b>This index does not evict, and it has no mode in which it can.</b> It used to carry a second, bounded backend
 * - a {@code Collections.synchronizedMap(LinkedHashMap)} that FIFO-evicted past a {@code maxSize} - selected by
 * {@code arcadedb.vectorIndex.locationCacheSize}. That was never a cache bound: there is no vector id to offset
 * index on disk, so an evicted location could not be recovered, and every reader reads "no location" as "deleted".
 * A cap therefore made {@code countEntries()} under-report and dropped the evicted vectors from every search
 * (issues #5568 and #5559). Reintroducing eviction requires giving the index file a vector id to offset lookup
 * first, so that a miss is recoverable. Until then, size an index at {@link #APPROX_RETAINED_BYTES_PER_LOCATION}
 * per live vector, or read {@link #estimatedRetainedBytes()} for what this instance actually holds.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorLocationIndex {
  /** Ids per chunk. Small on purpose: see the class javadoc on the partially drained band. */
  static final         int CHUNK_SIZE  = 128;
  private static final int CHUNK_SHIFT = 7;
  private static final int CHUNK_MASK  = CHUNK_SIZE - 1;
  private static final int CHUNK_WORDS = CHUNK_SIZE >>> 6;

  /**
   * Retained heap of one allocated chunk on a 64-bit JVM with compressed oops: the {@code Chunk} object (16 header
   * + 4 references + 1 int, padded to 40), {@code long[128]} twice at 1040, {@code int[128]} at 528 and
   * {@code long[2]} at 32. {@value} / {@value #CHUNK_SIZE} = ~21 bytes per id.
   * <p>
   * Compressed oops are off above a ~32GB heap, which is exactly the size of deployment that reads this figure, so
   * it is worth knowing what changes there: array headers go from 16 bytes to 24 and the {@code Chunk} object from
   * 40 to 56, i.e. 2728 instead of 2680, <b>+1.8%</b>. The layout is 97% payload - four primitive arrays whose size
   * does not depend on the oop width - so the estimate is off by less than the rounding in "~21 bytes per id" and
   * does not warrant reading the VM option back to correct it.
   */
  static final int CHUNK_RETAINED_BYTES = 40 + 1040 + 528 + 1040 + 32;

  /**
   * Approximate retained heap of one live location, and the single figure every caller and document should quote:
   * ~21 bytes for the chunk slot the id occupies plus 8 to 16 for its entry in the RID reverse index, whose
   * power-of-two capacity is sized at twice the live count and so lands anywhere between 2x and 4x it. An estimate
   * for sizing only - it also assumes a dense id space and ignores the partially drained chunks trailing the live
   * region. {@link #estimatedRetainedBytes()} reports what an instance actually holds, computed from the arrays it
   * has allocated rather than from a per-entry guess, and is what {@code getStats()} publishes.
   * <p>
   * It was 90 before issue #5588 replaced the two {@code ConcurrentHashMap}s this class used to be with primitive
   * arrays. The drop is a real reduction in residency, unlike the increase from 24 to 90 in 26.8.1, which was an
   * accounting correction from the payload size to the retained size.
   */
  public static final int APPROX_RETAINED_BYTES_PER_LOCATION = 32;

  /** What {@link #getOffsetAndFlag(int)} answers for an id that holds no location. */
  public static final long ABSENT = -1L;

  private static final long COMPACTED_FLAG = Long.MIN_VALUE;
  private static final long OFFSET_MASK    = Long.MAX_VALUE;
  /**
   * Offsets are stored biased by one so that {@code -1} - which the document-scan fallback uses for "this vector
   * has no page entry, read it from the document" - fits in the 63 bits it shares with the compacted flag.
   */
  private static final long OFFSET_BIAS    = 1L;
  /** The lowest offset a caller may store: {@code -1} is the no-page-entry sentinel, anything below it is a bug. */
  public static final  long NO_FILE_OFFSET = -1L;

  private static final Chunk[] EMPTY_DIRECTORY = new Chunk[0];
  private static final int[]   EMPTY_IDS       = new int[0];

  private static final VarHandle DIRECTORY = MethodHandles.arrayElementVarHandle(Chunk[].class);
  private static final VarHandle LONGS     = MethodHandles.arrayElementVarHandle(long[].class);
  private static final VarHandle INTS      = MethodHandles.arrayElementVarHandle(int[].class);

  /** Serializes every mutation. Reads never take it. */
  private final Object writeLock = new Object();

  private volatile Chunk[] chunks;
  /**
   * Number of resident locations. A single counter rather than a sum over the chunks: {@link #size()} is called on
   * the search path (the brute-force fallback budget, the auto-compaction ratio, the cache sizing), and summing
   * 73K chunk counters per query on a 9.3M-id index is not something a hot path can afford.
   */
  private volatile int     liveCount;

  // Times a present id had its RID rewritten in place, which retires it for the duration of the write. Zero on
  // every engine path; see put(). Written under writeLock, volatile so the accessor is honest for a caller that
  // reads it without having joined the writer - it is a diagnostic, and one that silently under-reports would be
  // worse than none.
  private volatile int     inPlaceRidRewrites;

  private final RidIndex   ridIndex;
  // Ids whose location was dropped because they were tombstoned: 1 bit per id instead of a resident location
  // (issue #5516). See DeletedIds.
  private final DeletedIds deletedIds;
  // Not an AtomicInteger any more: every mutation of it happens under `writeLock`, and the readers only need the
  // volatile read. Kept monotonic across a rebuild by LSMVectorIndex.publishLocationIndex.
  private volatile int     nextId;

  /**
   * One {@value #CHUNK_SIZE}-id slice of the location arrays. The arrays are final and full size, so a chunk is
   * never resized: growth allocates a new chunk, and a chunk whose last live id is tombstoned is dropped from the
   * directory rather than compacted.
   */
  private static final class Chunk {
    // Absolute file offset in bits 0..62, isCompacted in bit 63. One word so the pair cannot tear - see the class
    // javadoc.
    final long[] offsetAndFlag = new long[CHUNK_SIZE];
    final int[]  bucketId      = new int[CHUNK_SIZE];
    final long[] position      = new long[CHUNK_SIZE];
    final long[] present       = new long[CHUNK_WORDS];
    // Guarded by the enclosing instance's writeLock. Only decides when the chunk can be dropped; size() reads the
    // global counter instead.
    int liveCount;
  }

  /**
   * Compact set of the vector ids that have been tombstoned, one bit per id (issue #5516).
   * <p>
   * A tombstoned id is dead: its vector was superseded by a new id (update) or removed (delete), and the tombstone
   * is persisted on the index pages, so nothing needs its offset ever again - all the readers treat "no location"
   * exactly like "deleted location". Keeping the location resident only to carry a boolean made memory grow with
   * the number of writes instead of with the number of live vectors: re-embedding 4K vectors every few minutes
   * accumulated 9.3M locations, ~970MB of heap, before the process died. One bit per id costs ~1.2MB for the same
   * workload and is enough to know whether a persisted graph carries stale ordinals.
   * <p>
   * The array is sized by the highest id handed out since the last {@link #clear()}, not by the number of live
   * vectors: ids are preserved across a compaction rather than renumbered. Every graph rebuild (and therefore every
   * compaction) releases it, so it grows only across the writes between two rebuilds.
   * <p>
   * Writes are serialized on this object; reads are lock-free over a volatile array snapshot. Every mutation
   * republishes {@code bits} even when it did not resize the array: setting a bit is a plain array store, and a
   * reader's volatile read of {@code bits} only synchronizes-with a write to that field, so without the
   * re-publication the reader has no happens-before edge to the new bit and can keep missing it. A missed bit is
   * not benign - it is a stale "not deleted" answer on the reconstruction path, which then scans the pages and
   * finds the id's first (pre-tombstone) entry, resurrecting the vector this set exists to bury.
   */
  private static final class DeletedIds {
    private volatile long[] bits  = EMPTY_BITS;
    private volatile int    count = 0;

    private static final long[] EMPTY_BITS = new long[0];

    boolean contains(final int id) {
      if (id < 0)
        return false;
      final long[] snapshot = bits;
      // `1L << id` shifts by id & 63 (JLS 15.19), which is the bit within the word `id >>> 6` selects.
      final int word = id >>> 6;
      return word < snapshot.length && (snapshot[word] & (1L << id)) != 0;
    }

    synchronized void add(final int id) {
      if (id < 0)
        return;
      final int word = id >>> 6;
      long[] current = bits;
      if (word >= current.length)
        current = Arrays.copyOf(current, Math.max(word + 1, current.length * 2));

      final long mask = 1L << id;
      if ((current[word] & mask) == 0) {
        current[word] |= mask;
        count++;
        // A grown array always lands here (its words start at zero), so the resize is published too.
        bits = current; // publishes the bit above to every lock-free reader
      }
    }

    synchronized void remove(final int id) {
      if (id < 0)
        return;
      final int word = id >>> 6;
      final long[] current = bits;
      if (word >= current.length)
        return;
      final long mask = 1L << id;
      if ((current[word] & mask) != 0) {
        current[word] &= ~mask;
        count--;
        bits = current; // publishes the cleared bit to every lock-free reader
      }
    }

    synchronized void clear() {
      bits = EMPTY_BITS;
      count = 0;
    }

    int size() {
      return count;
    }

    long retainedBytes() {
      return 16L + 16L + (long) bits.length * Long.BYTES;
    }
  }

  /**
   * Reverse index RID -&gt; vector ids, so {@code remove(keys, rid)} resolves the ids for one RID in O(k) instead of
   * scanning every id in the index (issue #5318).
   * <p>
   * Open addressing with linear probing over a single {@code int[]} of vector ids. <b>No key is stored.</b> A
   * candidate slot is verified by reading the id's {@code bucketId}/{@code position} back out of the location
   * store, after checking that the id is still present. That is not a space trick, it is what makes the structure
   * correct by construction: "the live ids that point at RID R" is <i>defined</i> as "the live ids whose resident
   * RID equals R", so a slot that verifies is a correct answer whatever probe chain it was found in, and a slot
   * that does not verify is correctly rejected. The table can therefore never produce a wrong id, only miss one -
   * and a concurrently tombstoned id, or one whose chunk was released, is rejected for free.
   * <p>
   * That is also why <b>nothing is ever removed</b> and there is no tombstone sentinel: a dead slot is already
   * unreachable, so removing it would only reclaim space, and it is reclaimed at the next rehash instead. Removal
   * would have cost correctness here, not bought it: in the re-embedding workload of issue #5516 every id is
   * eventually dead, so a rehash trigger counting only live entries would let dead slots fill every empty slot, and
   * a probe that stops at the first empty slot would then wrap forever. The trigger counts <i>occupied</i> slots
   * and the replacement table is sized from the <i>live</i> count, never from the current capacity, so a table
   * under dead-entry pressure is cleaned in place rather than grown without bound. A probe is additionally bounded
   * by the table length so a full table degrades into a miss rather than a hang.
   * <p>
   * Writers hold the enclosing {@code writeLock}; readers take the volatile table reference once and read each slot
   * with {@code getAcquire}. A reader holding a pre-rehash table sees a consistent older generation and can miss an
   * id inserted after it started - the same weak consistency the {@code ConcurrentHashMap} it replaces had.
   */
  private final class RidIndex {
    private static final int   EMPTY        = -1;
    private static final float LOAD_FACTOR  = 0.7f;
    private static final int   MIN_CAPACITY = 16;

    private volatile int[] table;
    // Occupied slots, live and dead alike. Guarded by writeLock.
    private          int   occupied;

    RidIndex(final int expectedEntries) {
      final int[] initial = new int[capacityFor(expectedEntries)];
      Arrays.fill(initial, EMPTY);
      this.table = initial;
    }

    private static int capacityFor(final int entries) {
      long capacity = MIN_CAPACITY;
      final long wanted = Math.max(MIN_CAPACITY, (long) entries * 2L);
      while (capacity < wanted && capacity < (1L << 30))
        capacity <<= 1;
      return (int) capacity;
    }

    /**
     * Caller holds {@code writeLock}, and calls this BEFORE the id's presence bit goes up: {@link #rehash()}
     * rebuilds the table from the ids that are resident, so inserting an already-present id would give it two
     * slots.
     */
    void insert(final int vectorId) {
      if (occupied + 1 >= (int) (table.length * LOAD_FACTOR))
        rehash();

      if (!tryInsert(table, vectorId)) {
        // Only reachable with the capacity clamped at 2^30, i.e. past 750M live vectors in one index. Rehashing
        // cannot help there either, but failing to find a slot must not become an unbounded probe.
        rehash();
        if (!tryInsert(table, vectorId))
          throw new IllegalStateException(
              "The RID reverse index of the vector location index is full (" + table.length + " slots)");
      }
      occupied++;
    }

    private boolean tryInsert(final int[] target, final int vectorId) {
      final int mask = target.length - 1;
      int idx = hash(vectorId) & mask;
      for (int probes = 0; probes <= mask; probes++) {
        if (target[idx] == EMPTY) {
          INTS.setRelease(target, idx, vectorId);
          return true;
        }
        idx = (idx + 1) & mask;
      }
      return false;
    }

    /** Caller holds {@code writeLock}. Rebuilds the table from the ids that are still resident. */
    private void rehash() {
      final int[] replacement = new int[capacityFor(liveCount)];
      Arrays.fill(replacement, EMPTY);
      final int mask = replacement.length - 1;
      int inserted = 0;

      final Chunk[] directory = chunks;
      for (int c = 0; c < directory.length; c++) {
        final Chunk chunk = (Chunk) DIRECTORY.getAcquire(directory, c);
        if (chunk == null)
          continue;
        final int base = c << CHUNK_SHIFT;
        for (int w = 0; w < CHUNK_WORDS; w++) {
          long word = chunk.present[w];
          while (word != 0) {
            final int slot = (w << 6) + Long.numberOfTrailingZeros(word);
            word &= word - 1;
            final int vectorId = base + slot;
            // Hash from the RID in hand, not through hash(vectorId): that overload re-resolves the chunk this loop
            // is already standing in.
            int idx = hash(chunk.bucketId[slot], chunk.position[slot]) & mask;
            while (replacement[idx] != EMPTY)
              idx = (idx + 1) & mask;
            replacement[idx] = vectorId;
            inserted++;
          }
        }
      }

      occupied = inserted;
      table = replacement; // one volatile store publishes the fully populated replacement
    }

    /** Lock-free. Returns the live ids pointing at {@code rid}, in ascending order. */
    int[] get(final RID rid) {
      final int[] current = table;
      final int mask = current.length - 1;
      final int bucketId = rid.getBucketId();
      final long position = rid.getPosition();

      // A RID maps to exactly one live id in every workload the engine produces (an update takes a new id and
      // tombstones the old one), so the single match is answered without a growable buffer.
      int single = EMPTY;
      int[] found = null;
      int count = 0;
      int idx = hash(bucketId, position) & mask;
      for (int probes = 0; probes <= mask; probes++) {
        final int vectorId = (int) INTS.getAcquire(current, idx);
        if (vectorId == EMPTY)
          break;
        if (isLocationOf(vectorId, bucketId, position)) {
          if (count == 0)
            single = vectorId;
          else {
            if (found == null) {
              found = new int[4];
              found[0] = single;
            } else if (count == found.length)
              found = Arrays.copyOf(found, count * 2);
            found[count] = vectorId;
          }
          count++;
        }
        idx = (idx + 1) & mask;
      }

      if (count == 0)
        return EMPTY_IDS;
      if (count == 1)
        return new int[] { single };

      final int[] result = count == found.length ? found : Arrays.copyOf(found, count);
      // Ascending: probe order depends on the table generation, and callers (and their tests) compare the answer
      // element by element.
      Arrays.sort(result);

      // One vector id can own more than one slot, and every slot that verifies is the same correct answer. It gets
      // a second one whenever it is registered while an earlier slot of its own is still in the probe path being
      // walked: its RID changed and changed back, or it was tombstoned and then re-registered by an LSM replay of
      // the same id. The extra slots are pure waste, reclaimed at the next rehash, but the answer is a set and has
      // to look like one.
      int unique = 1;
      for (int i = 1; i < result.length; i++)
        if (result[i] != result[unique - 1])
          result[unique++] = result[i];
      return unique == result.length ? result : Arrays.copyOf(result, unique);
    }

    /** Caller holds {@code writeLock}. */
    void clear() {
      final int[] emptied = new int[MIN_CAPACITY];
      Arrays.fill(emptied, EMPTY);
      occupied = 0;
      table = emptied;
    }

    int capacity() {
      return table.length;
    }

    long retainedBytes() {
      return 16L + 16L + (long) table.length * Integer.BYTES;
    }

    /** The slot a vector id hashes to: its RID's, read back out of the location store. */
    private int hash(final int vectorId) {
      final Chunk chunk = chunkOf(vectorId);
      if (chunk == null)
        return 0;
      final int slot = vectorId & CHUNK_MASK;
      return hash(chunk.bucketId[slot], chunk.position[slot]);
    }

    private static int hash(final int bucketId, final long position) {
      long h = bucketId * 0x9E3779B97F4A7C15L + position;
      h ^= h >>> 32;
      h *= 0xFF51AFD7ED558CCDL;
      h ^= h >>> 33;
      return (int) h & 0x7FFFFFFF;
    }
  }

  /**
   * Represents the physical location of a vector on disk. Materialized on demand from the primitive arrays: no
   * instance of this class is resident since issue #5588, so a caller on a hot path should prefer the accessors
   * that answer one field ({@link #isLive(int)}, {@link #getOffsetAndFlag(int)}, {@link #isLocationOf(int, RID)})
   * over asking for the whole tuple.
   * <p>
   * {@code deleted} is always {@code false} on an instance this index produced: since issue #5516 a tombstoned id
   * keeps no location at all, so "resident" and "live" are the same thing. The field stays because the readers are
   * written as {@code loc != null && !loc.deleted} and that is the definition of active a location read straight
   * from a page has to pass.
   */
  public static class VectorLocation {
    public final boolean isCompacted;        // true if in the compacted file, false if in the mutable file
    public final long    absoluteFileOffset; // direct offset into the file for O(1) access
    public final RID     rid;                // document RID (bucketId + position)
    public final boolean deleted;            // LSM tombstone flag

    public VectorLocation(final boolean isCompacted, final long absoluteFileOffset, final RID rid,
        final boolean deleted) {
      this.isCompacted = isCompacted;
      this.absoluteFileOffset = absoluteFileOffset;
      this.rid = rid;
      this.deleted = deleted;
    }
  }

  /**
   * Create a VectorLocationIndex sized for a small index; it grows as entries are added.
   */
  public VectorLocationIndex() {
    this(16);
  }

  /**
   * Create a VectorLocationIndex with an initial capacity hint. Sizing the structures for the set they are about to
   * hold saves the growth a rebuild would otherwise pay on the way up.
   * <p>
   * The single-int constructor used to mean {@code maxSize} (issue #5559 removed bounding). A caller that still
   * passes an eviction limit here would now silently get a capacity hint instead, which is why the old
   * {@code -1 = unlimited} sentinel is rejected rather than quietly treated as a size.
   * <p>
   * That rejection is an {@link IllegalArgumentException}, deliberately NOT the {@code IndexException} that
   * {@code LSMVectorIndexMetadata.setLocationCacheSize} raises for the same setting. The two guard different
   * things: that one answers a user who wrote {@code locationCacheSize} in DDL and belongs in the error the
   * statement reports, while this one can only fire on a caller inside the engine that was not updated for the
   * changed parameter meaning - programming error, not user input.
   *
   * @param initialCapacity Initial capacity hint, at least 0
   */
  public VectorLocationIndex(final int initialCapacity) {
    if (initialCapacity < 0)
      throw new IllegalArgumentException(
          "Invalid initial capacity " + initialCapacity + " for a vector location index: this parameter used to be "
              + "an eviction limit, which issue #5559 removed because an evicted location cannot be recovered");

    // Only the directory is pre-sized: the chunks themselves are allocated as ids land in them, so a hint larger
    // than the set that materializes costs 4 bytes per chunk instead of a whole chunk.
    final int directorySize = initialCapacity == 0 ? 0 : (initialCapacity - 1) / CHUNK_SIZE + 1;
    this.chunks = directorySize == 0 ? EMPTY_DIRECTORY : new Chunk[directorySize];
    this.ridIndex = new RidIndex(initialCapacity);
    this.deletedIds = new DeletedIds();
    this.nextId = 0;
  }

  /**
   * Add a new vector location with an auto-generated ID.
   *
   * @param isCompacted        True if the vector is in the compacted file, false if in mutable file
   * @param absoluteFileOffset The absolute offset in the file where the vector entry is stored
   * @param rid                The document RID
   *
   * @return The assigned vector ID
   */
  public int addVector(final boolean isCompacted, final long absoluteFileOffset, final RID rid) {
    synchronized (writeLock) {
      final int id = nextId;
      nextId = id + 1;
      put(id, isCompacted, absoluteFileOffset, rid);
      return id;
    }
  }

  /**
   * Add or update a vector location with a specific ID.
   * Used during loading from pages (LSM style: later entries override earlier ones).
   * <p>
   * A tombstone ({@code deleted == true}) does NOT become a resident entry: the id is recorded in the compact
   * deleted-id set and any location it had is released (issue #5516). {@link #getLocation(int)} then answers
   * {@code null} for it, which every reader already treats as "deleted".
   *
   * @param id                 The vector ID
   * @param isCompacted        True if the vector is in the compacted file, false if in mutable file
   * @param absoluteFileOffset The absolute offset in the file where the vector entry is stored
   * @param rid                The document RID
   * @param deleted            Whether this vector is deleted (LSM tombstone)
   */
  public void addOrUpdate(final int id, final boolean isCompacted, final long absoluteFileOffset, final RID rid,
      final boolean deleted) {
    if (id < 0)
      throw new IllegalArgumentException("Invalid vector id " + id);

    synchronized (writeLock) {
      if (deleted) {
        // The tombstone bit goes up before the location comes down, so a lock-free reader never observes the
        // combination "no location and not deleted" - which the reconstruction path reads as "scan the pages",
        // finding the id's pre-tombstone entry and resurrecting it.
        deletedIds.add(id);
        removeLocation(id);
      } else {
        // LSM merge-on-read: a live entry read after a tombstone for the same id wins (it cannot happen through the
        // write path, which never reuses an id, but page order is the authority here).
        //
        // This is the mirror image of the delete path above and leaves the same transient combination - not
        // deleted, no location yet - between the bit coming down and the presence bit going up. It is benign here,
        // and the asymmetry is worth stating because the ordering above exists precisely to avoid it: what makes
        // that window dangerous on the delete path is that a reader falling through to the pages finds the id's
        // PRE-tombstone entry, an older generation than the one being installed. Here the pages hold the live
        // entry this call is installing - it is where the call came from - so a reader that falls through in the
        // window resolves to the same answer it would get after it. Ordering it the other way would be worse: a
        // location that is resident while the id still reads as deleted makes the delta scan skip a live vector.
        if (deletedIds.contains(id))
          deletedIds.remove(id);

        put(id, isCompacted, absoluteFileOffset, rid);
      }

      if (id >= nextId)
        nextId = id + 1;
    }
  }

  /**
   * Store a location for {@code vectorId}, allocating its chunk if needed. Caller holds {@code writeLock}.
   */
  private void put(final int vectorId, final boolean isCompacted, final long absoluteFileOffset, final RID rid) {
    if (absoluteFileOffset < NO_FILE_OFFSET || absoluteFileOffset >= OFFSET_MASK - OFFSET_BIAS)
      throw new IllegalArgumentException(
          "Invalid absolute file offset " + absoluteFileOffset + " for vector id " + vectorId
              + ": the offset shares its word with the compacted flag, so it must be at least " + NO_FILE_OFFSET
              + " (no page entry, read from the document) and below " + (OFFSET_MASK - OFFSET_BIAS));

    final int chunkIndex = vectorId >>> CHUNK_SHIFT;
    Chunk[] directory = chunks;
    if (chunkIndex >= directory.length) {
      directory = Arrays.copyOf(directory, Math.max(chunkIndex + 1, Math.max(4, directory.length * 2)));
      chunks = directory; // the grown array is published through the volatile field
    }

    Chunk chunk = (Chunk) DIRECTORY.getAcquire(directory, chunkIndex);
    if (chunk == null) {
      chunk = new Chunk();
      // setRelease on the element, not just the volatile field: a reader that read `chunks` before this store has
      // no edge to a plain element write and would keep seeing null.
      DIRECTORY.setRelease(directory, chunkIndex, chunk);
    }

    final int slot = vectorId & CHUNK_MASK;
    final int word = slot >>> 6;
    final long bit = 1L << slot;
    final boolean wasPresent = (chunk.present[word] & bit) != 0;
    final int bucketId = rid.getBucketId();
    final long position = rid.getPosition();
    final boolean ridChanged = !wasPresent || chunk.bucketId[slot] != bucketId || chunk.position[slot] != position;

    if (wasPresent && ridChanged) {
      // The RID of a present id is never rewritten in place: bucketId and position are two words, and a reader
      // pairing one from each generation could fabricate a RID that matches an unrelated record - which is how a
      // delete of record A ends up tombstoning the vector of record B.
      //
      // Retiring the id for the duration of the write is the safe answer, and it is the one taken here, but it is
      // not free: unlike the atomic reference swap the ConcurrentHashMap this replaces performed, it opens a
      // window in which a lock-free search reads the id as absent and skips a LIVE vector. No engine path does
      // this - an update takes a new id, and a page replay carries the id's own RID - and that is a precondition
      // worth more than a comment, so it is counted rather than asserted: `inPlaceRidRewrites` stays zero on
      // every engine workload and LSMVectorIndexTombstoneMemoryTest pins it there across the re-embedding cycles
      // that would trip it first. An assert would have been louder and wrong - it would also fire on the tests
      // that exist to prove this branch does the safe thing.
      inPlaceRidRewrites++;
      LONGS.setRelease(chunk.present, word, chunk.present[word] & ~bit);
    }

    chunk.bucketId[slot] = bucketId;
    chunk.position[slot] = position;
    // One word, so the offset and the flag can never be read from two different generations.
    final long biasedOffset = absoluteFileOffset + OFFSET_BIAS;
    LONGS.setRelease(chunk.offsetAndFlag, slot, isCompacted ? biasedOffset | COMPACTED_FLAG : biasedOffset);

    // Before the presence bit, never after: an insert can trigger a rehash, and a rehash rebuilds the table from
    // the ids that are resident, so a present id would end up with two slots.
    if (ridChanged)
      ridIndex.insert(vectorId);

    // Last, and with a release: a reader that sees this bit sees everything above it.
    LONGS.setRelease(chunk.present, word, chunk.present[word] | bit);

    if (!wasPresent) {
      chunk.liveCount++;
      liveCount++;
    }
  }

  /**
   * Drop the location of {@code vectorId}, releasing its chunk when it holds nothing else. Caller holds
   * {@code writeLock}.
   *
   * @return true if a location was resident and has been released
   */
  private boolean removeLocation(final int vectorId) {
    if (vectorId < 0)
      return false;
    final Chunk[] directory = chunks;
    final int chunkIndex = vectorId >>> CHUNK_SHIFT;
    if (chunkIndex >= directory.length)
      return false;
    final Chunk chunk = (Chunk) DIRECTORY.getAcquire(directory, chunkIndex);
    if (chunk == null)
      return false;

    final int slot = vectorId & CHUNK_MASK;
    final int word = slot >>> 6;
    final long bit = 1L << slot;
    if ((chunk.present[word] & bit) == 0)
      return false;

    // Clear the bit BEFORE the chunk can be dropped: a reader holding the reference to a dropped chunk still reads
    // its bits, and a set bit there is a live location pointing at an offset a compaction may already have reused.
    LONGS.setRelease(chunk.present, word, chunk.present[word] & ~bit);
    chunk.liveCount--;
    liveCount--;

    if (chunk.liveCount == 0) {
      assert noBitsSet(chunk) : "chunk " + chunkIndex + " reported empty with live bits still set";
      // Releasing the chunk is what keeps residency proportional to the live vectors instead of to the ids handed
      // out (issue #5516).
      DIRECTORY.setRelease(directory, chunkIndex, null);
    }
    return true;
  }

  private static boolean noBitsSet(final Chunk chunk) {
    for (int w = 0; w < CHUNK_WORDS; w++)
      if (chunk.present[w] != 0)
        return false;
    return true;
  }

  /** The chunk holding {@code vectorId}, or null if none is allocated. Lock-free. */
  private Chunk chunkOf(final int vectorId) {
    if (vectorId < 0)
      return null;
    final Chunk[] directory = chunks;
    final int chunkIndex = vectorId >>> CHUNK_SHIFT;
    if (chunkIndex >= directory.length)
      return null;
    return (Chunk) DIRECTORY.getAcquire(directory, chunkIndex);
  }

  private static boolean isPresent(final Chunk chunk, final int slot) {
    // Acquire: everything the writer stored before setting this bit is visible once it is observed set.
    final long word = (long) LONGS.getAcquire(chunk.present, slot >>> 6);
    return (word & (1L << slot)) != 0;
  }

  /**
   * Whether the id currently holds a location, i.e. whether it is a live vector. The cheapest question this index
   * answers - one bit - and the one the search filters ask per traversed ordinal.
   *
   * @param vectorId The vector ID
   *
   * @return true if the id is live
   */
  public boolean isLive(final int vectorId) {
    final Chunk chunk = chunkOf(vectorId);
    return chunk != null && isPresent(chunk, vectorId & CHUNK_MASK);
  }

  /**
   * The absolute file offset and the compacted flag of a vector, packed in one word so the two cannot be read from
   * different generations, or {@link #ABSENT} if the id holds no location. Decode with {@link #offsetOf(long)} and
   * {@link #isCompactedOf(long)}.
   * <p>
   * {@link #ABSENT} is unambiguous: it decodes to a compacted entry near {@link Long#MAX_VALUE}, and
   * {@link #addOrUpdate} rejects an offset anywhere near that, so no real entry can produce it.
   *
   * @param vectorId The vector ID
   *
   * @return the packed offset, or {@link #ABSENT}
   */
  public long getOffsetAndFlag(final int vectorId) {
    final Chunk chunk = chunkOf(vectorId);
    if (chunk == null)
      return ABSENT;
    final int slot = vectorId & CHUNK_MASK;
    if (!isPresent(chunk, slot))
      return ABSENT;
    return (long) LONGS.getAcquire(chunk.offsetAndFlag, slot);
  }

  /**
   * The absolute file offset carried by a {@link #getOffsetAndFlag(int)} answer, or {@link #NO_FILE_OFFSET} when the
   * vector has no entry on a page and has to be read from its document. Undefined for {@link #ABSENT}, which the
   * caller has to check for first.
   */
  public static long offsetOf(final long offsetAndFlag) {
    return (offsetAndFlag & OFFSET_MASK) - OFFSET_BIAS;
  }

  /** Whether a {@link #getOffsetAndFlag(int)} answer points into the compacted file. */
  public static boolean isCompactedOf(final long offsetAndFlag) {
    return offsetAndFlag < 0;
  }

  /**
   * The RID of a live vector, materialized on demand, or null if the id holds no location. Only the callers that
   * genuinely need the object - a {@code Set<RID>} membership test, a {@code lookupByRID}, a result row - should
   * ask; {@link #isLocationOf(int, RID)} answers the comparison without allocating.
   *
   * @param vectorId The vector ID
   *
   * @return the RID, or null
   */
  public RID getRid(final int vectorId) {
    final Chunk chunk = chunkOf(vectorId);
    if (chunk == null)
      return null;
    final int slot = vectorId & CHUNK_MASK;
    if (!isPresent(chunk, slot))
      return null;
    return new RID((int) INTS.getAcquire(chunk.bucketId, slot), (long) LONGS.getAcquire(chunk.position, slot));
  }

  /**
   * The bucket id of a live vector's RID, or -1 if the id holds no location.
   * <p>
   * Package-private: nothing in the engine needs half a RID - the readers either compare one
   * ({@link #isLocationOf(int, RID)}) or need the object ({@link #getRid(int)}). This and
   * {@link #getPosition(int)} exist so a test can read the two words straight out of the arrays and check
   * {@link #getRid(int)} against them, rather than checking it against itself.
   */
  int getBucketId(final int vectorId) {
    final Chunk chunk = chunkOf(vectorId);
    if (chunk == null)
      return -1;
    final int slot = vectorId & CHUNK_MASK;
    if (!isPresent(chunk, slot))
      return -1;
    return (int) INTS.getAcquire(chunk.bucketId, slot);
  }

  /** The position of a live vector's RID, or -1 if the id holds no location. See {@link #getBucketId(int)}. */
  long getPosition(final int vectorId) {
    final Chunk chunk = chunkOf(vectorId);
    if (chunk == null)
      return -1;
    final int slot = vectorId & CHUNK_MASK;
    if (!isPresent(chunk, slot))
      return -1;
    return (long) LONGS.getAcquire(chunk.position, slot);
  }

  /**
   * Whether {@code vectorId} is live and points at {@code rid}, without materializing anything.
   *
   * @param vectorId The vector ID
   * @param rid      The document RID to compare against
   *
   * @return true if the id is live and belongs to that RID
   */
  public boolean isLocationOf(final int vectorId, final RID rid) {
    return rid != null && isLocationOf(vectorId, rid.getBucketId(), rid.getPosition());
  }

  private boolean isLocationOf(final int vectorId, final int bucketId, final long position) {
    final Chunk chunk = chunkOf(vectorId);
    if (chunk == null)
      return false;
    final int slot = vectorId & CHUNK_MASK;
    if (!isPresent(chunk, slot))
      return false;
    return (int) INTS.getAcquire(chunk.bucketId, slot) == bucketId
        && (long) LONGS.getAcquire(chunk.position, slot) == position;
  }

  /**
   * Get the location metadata for a vector by ID.
   * <p>
   * Allocates: nothing of this shape is resident since issue #5588. Prefer {@link #isLive(int)},
   * {@link #getOffsetAndFlag(int)} and {@link #isLocationOf(int, RID)} on a path that runs per traversed ordinal.
   *
   * @param vectorId The vector ID
   *
   * @return The location metadata, or null if not found
   */
  public VectorLocation getLocation(final int vectorId) {
    final Chunk chunk = chunkOf(vectorId);
    if (chunk == null)
      return null;
    final int slot = vectorId & CHUNK_MASK;
    if (!isPresent(chunk, slot))
      return null;

    final long packed = (long) LONGS.getAcquire(chunk.offsetAndFlag, slot);
    final RID rid = new RID((int) INTS.getAcquire(chunk.bucketId, slot),
        (long) LONGS.getAcquire(chunk.position, slot));
    return new VectorLocation(isCompactedOf(packed), offsetOf(packed), rid, false);
  }

  /**
   * Return the live vector ids currently mapped to the given RID, in ascending order, resolved in O(k) via the
   * reverse index instead of scanning all vector ids.
   * <p>
   * A tombstoned id is structurally unreachable here: the reverse index verifies every candidate against the
   * resident location, and a tombstoned id has none. Callers do not have to re-check.
   *
   * @param rid The document RID
   *
   * Allocates the array it returns, so it is a resolution step and not something to call per traversed ordinal:
   * its callers are the delete path and the allow-list walk, both of which are already resolving records.
   *
   * @return the matching vector ids, or an empty array if none are live
   */
  public int[] getVectorIdsForRid(final RID rid) {
    if (rid == null)
      return EMPTY_IDS;
    return ridIndex.get(rid);
  }

  /**
   * Mark a vector as deleted (LSM tombstone). The location is released and the id is remembered in the compact
   * deleted-id set (issue #5516): a tombstoned id is never read back, and the tombstone itself is persisted on the
   * index pages by the caller, so keeping the location resident only costs heap.
   * <p>
   * An id that is not resident is left alone: it was already tombstoned, and the index never drops a location for
   * any other reason (issue #5559 removed eviction), so there is nothing to record.
   *
   * @param vectorId The vector ID to mark as deleted
   */
  public void markDeleted(final int vectorId) {
    synchronized (writeLock) {
      if (!isLive(vectorId))
        return;
      // Tombstone bit first, location second - see addOrUpdate.
      deletedIds.add(vectorId);
      removeLocation(vectorId);
    }
  }

  /**
   * Return whether the given id has been tombstoned, without touching the pages.
   *
   * @param vectorId The vector ID
   *
   * @return true if the id is tombstoned
   */
  public boolean isDeleted(final int vectorId) {
    return deletedIds.contains(vectorId);
  }

  /**
   * Number of tombstoned ids seen since the index was loaded or cleared. A value greater than zero means the
   * persisted graph's ordinals no longer match the live vector set (issue #3135).
   */
  public int getDeletedCount() {
    return deletedIds.size();
  }

  /**
   * Get a stream of all vector IDs in the index, in ascending order.
   * <p>
   * The order is not a convenience: {@code LSMVectorIndex} feeds this into {@code ordinalToVectorId} and then reads
   * that array with {@code Arrays.binarySearch}, so a traversal that was not ascending would silently make every
   * allow-list-filtered search return fewer results. The spliterator reports {@link Spliterator#SORTED}, which is
   * what turns the {@code .sorted()} those call sites still apply into a no-op.
   *
   * @return Stream of vector IDs
   */
  public IntStream getAllVectorIds() {
    return StreamSupport.intStream(new IdSpliterator(chunks, liveCount), false);
  }

  /**
   * Get a stream of active (non-deleted) vector IDs, in ascending order.
   * <p>
   * Since issue #5516 no id holds a location while tombstoned, so this returns the same set as
   * {@link #getAllVectorIds()}. It stays as the name of the concept the readers filter on.
   *
   * @return Stream of active vector IDs
   */
  public IntStream getActiveVectorIds() {
    return getAllVectorIds();
  }

  /**
   * Number of resident locations, which is the number of LIVE vectors: {@link #markDeleted} drops the location and
   * keeps only the id in the tombstone set, and nothing else ever removes one (issue #5559 took away the bounded
   * backend that could evict). Callers such as {@code LSMVectorIndex.estimatePagesForLiveSet} rely on that
   * equality. Constant time, and safe to call per query.
   *
   * @return Number of vectors whose location is currently held
   */
  public int size() {
    return liveCount;
  }

  /**
   * Get the count of active (non-deleted) vectors.
   * <p>
   * Counted independently of {@link #size()}, by popcount over the presence bits, so the two are a cross-check on
   * each other rather than two names for one counter.
   * <p>
   * O(allocated chunks), i.e. one word pair per 128 ids, against the O(live vectors) stream it replaces. Still not
   * something to call per query - {@code countEntries()} is its caller and {@code engine/CLAUDE.md} already says
   * so - but the reason is the walk, not the counter: {@link #size()} is the O(1) answer.
   *
   * @return Number of active vectors
   */
  public long getActiveCount() {
    final Chunk[] directory = chunks;
    long count = 0;
    for (int c = 0; c < directory.length; c++) {
      final Chunk chunk = (Chunk) DIRECTORY.getAcquire(directory, c);
      if (chunk == null)
        continue;
      for (int w = 0; w < CHUNK_WORDS; w++)
        count += Long.bitCount((long) LONGS.getAcquire(chunk.present, w));
    }
    return count;
  }

  /**
   * Get the next ID that will be assigned.
   *
   * @return The next vector ID
   */
  public int getNextId() {
    return nextId;
  }

  /**
   * The high-water mark of the id sequence, i.e. {@code getNextId() - 1}. NOT the highest live id: a tombstoned id
   * is still an id that was handed out, and {@code LSMVectorIndex} sizes the incremental builder from this.
   */
  public int getMaxVectorId() {
    return nextId - 1;
  }

  /**
   * Set the next ID (used during loading to restore ID sequence).
   *
   * @param id The next ID to use
   */
  public void setNextId(final int id) {
    synchronized (writeLock) {
      nextId = id;
    }
  }

  /**
   * Clear all vector locations.
   */
  public void clear() {
    synchronized (writeLock) {
      final Chunk[] directory = chunks;
      for (int c = 0; c < directory.length; c++) {
        final Chunk chunk = (Chunk) DIRECTORY.getAcquire(directory, c);
        if (chunk == null)
          continue;
        // Zero the bits before the chunk becomes unreachable from the directory. clear() resets the id sequence, so
        // it is the one path that hands out an id a stale chunk reference already holds a location for; without
        // this a reader mid-flight would resolve the new id 0 to the record the old one belonged to.
        for (int w = 0; w < CHUNK_WORDS; w++)
          LONGS.setRelease(chunk.present, w, 0L);
        chunk.liveCount = 0;
        DIRECTORY.setRelease(directory, c, null);
      }
      chunks = EMPTY_DIRECTORY;
      liveCount = 0;
      ridIndex.clear();
      deletedIds.clear();
      nextId = 0;
    }
  }

  /**
   * The heap this instance actually retains, summed over the arrays it has allocated rather than multiplied out
   * from a per-entry estimate. This is what {@code LSMVectorIndex.getStats()} publishes as
   * {@code estimatedLocationIndexBytes}, so an operator sizing a heap reads a measurement of this index and not an
   * assumption about its density.
   *
   * O(allocated chunks), like {@link #getActiveCount()}, so it belongs on the stats path it is called from and
   * nowhere hotter.
   * <p>
   * Assumes a 64-bit JVM with compressed oops, which is what everything below a ~32GB heap runs. Above that the
   * figure under-reports by under 2% - see {@link #CHUNK_RETAINED_BYTES} for where it goes - because the object
   * headers and the directory's references are all that widen and the layout is almost entirely primitive payload.
   *
   * @return retained bytes, on a 64-bit JVM with compressed oops
   */
  public long estimatedRetainedBytes() {
    final Chunk[] directory = chunks;
    long bytes = 16L + 16L + (long) directory.length * 4L; // this object, the directory array
    for (int c = 0; c < directory.length; c++)
      if (DIRECTORY.getAcquire(directory, c) != null)
        bytes += CHUNK_RETAINED_BYTES;
    return bytes + ridIndex.retainedBytes() + deletedIds.retainedBytes();
  }

  /**
   * Times a live vector id has had its RID rewritten in place since this instance was created. Exposed because it
   * must stay zero: that write retires the id for the duration of the write, so a lock-free search running through
   * it does not see a vector that is live. See {@code put()} for why the branch still exists.
   */
  int inPlaceRidRewriteCount() {
    return inPlaceRidRewrites;
  }

  /** Chunks currently allocated. Exposed so a test can assert that a drained id space is actually handed back. */
  int chunkCount() {
    final Chunk[] directory = chunks;
    int count = 0;
    for (int c = 0; c < directory.length; c++)
      if (DIRECTORY.getAcquire(directory, c) != null)
        count++;
    return count;
  }

  /** Slots in the RID reverse index. Exposed so a test can assert that dead entries do not grow it without bound. */
  int reverseTableCapacity() {
    return ridIndex.capacity();
  }

  /**
   * Walks the presence bits of a directory snapshot in ascending id order.
   * <p>
   * The directory is snapshotted once, at construction: re-reading the volatile field per element would let a
   * {@link VectorLocationIndex#clear()} landing mid-traversal hand the walk a shorter array. Individual chunks are
   * still read with an acquire and null-checked, so a chunk released during the walk simply stops yielding ids -
   * the weakly consistent iteration the {@code ConcurrentHashMap} keySet stream already gave.
   * <p>
   * {@code trySplit()} returns null on purpose: nothing in the engine parallelizes these streams, and a split that
   * handed back the suffix instead of the prefix would break the ascending order the callers depend on without
   * anything failing loudly.
   */
  private static final class IdSpliterator implements Spliterator.OfInt {
    private final Chunk[] directory;
    private final long    estimate;
    private       int     chunkIndex;
    private       int     wordIndex;
    private       long    word;
    private       Chunk   chunk;

    IdSpliterator(final Chunk[] directory, final long estimate) {
      this.directory = directory;
      this.estimate = estimate;
    }

    @Override
    public boolean tryAdvance(final IntConsumer action) {
      while (true) {
        if (word != 0) {
          final int slot = (wordIndex << 6) + Long.numberOfTrailingZeros(word);
          word &= word - 1;
          action.accept(((chunkIndex - 1) << CHUNK_SHIFT) + slot);
          return true;
        }
        if (chunk != null && ++wordIndex < CHUNK_WORDS) {
          word = (long) LONGS.getAcquire(chunk.present, wordIndex);
          continue;
        }
        if (chunkIndex >= directory.length)
          return false;
        chunk = (Chunk) DIRECTORY.getAcquire(directory, chunkIndex++);
        wordIndex = 0;
        word = chunk == null ? 0L : (long) LONGS.getAcquire(chunk.present, 0);
      }
    }

    @Override
    public OfInt trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      // A hint only: SIZED is deliberately not reported, because a concurrent mutation can make the walk yield a
      // different number of ids than the counter said when the stream was created.
      return estimate;
    }

    @Override
    public int characteristics() {
      return Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.SORTED | Spliterator.NONNULL;
    }

    @Override
    public Comparator<? super Integer> getComparator() {
      // Natural order, which is what SORTED over an ascending int traversal means.
      return null;
    }
  }
}
