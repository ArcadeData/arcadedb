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

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;

import java.util.NoSuchElementException;
import java.util.logging.Level;

/**
 * Map like optimized to avoid stressing the GC by using mechanical sympathy technique + compression of key and values.
 * This class is not synchronized. The key is a RID and value is a RIDs. This Map implementation doesn't support the overwrite of a
 * value. Values cannot be null.
 * <br>
 * This index is used for invert incoming edge creation.
 * <br>
 * A Binary object is used to store the hash table (the first part of it) and then keys and values. The RID key is serialized compressed in
 * the position pointed by the hash table, then a fixed-size integer containing the next entry (with the same hash) and after that the
 * compressed RID. Another slot is kept to point to the previous entry. The hash table always points to the last element
 * with a linked list in the only direction of the previous.
 * <p>
 * The hash table doubles its slots whenever the entries outnumber them (see {@link #growHashTable()}), so the number of slots
 * passed to the constructor is a starting size, not a limit. Growing replaces the chunk, so an {@link EntryIterator} must not be
 * kept open across a {@link #put}.
 * <p>
 * TODO support up to 4GB by using unsigned int
 */
public class CompressedRID2RIDIndex {
  /** The largest Binary the positions of this index (ints) and the JVM (arrays) can address. */
  private static final long MAX_CHUNK_SIZE = Integer.MAX_VALUE - 8;

  protected final Database         database;
  protected final BinarySerializer serializer;
  protected       int              keys;
  private         boolean          readOnly = false;

  private Thread lastThreadAccessed = null;

  protected Binary chunk;
  protected int    totalEntries   = 0;
  protected int    totalUsedSlots = 0;

  public class EntryIterator {
    private int posInHashTable = 0;
    private int nextKeyPos;

    private RID nextKeyRID;
    private RID valueRID;

    public boolean hasNext() {
      if (valueRID != null)
        return true;

      if (totalUsedSlots == 0)
        return false;

      if (nextKeyPos > 0) {
        // NEXT KEY ON SAME POSITION IN HASHTABLE
        chunk.position(nextKeyPos);

        nextKeyRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
        nextKeyPos = chunk.getInt();
        valueRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
        return true;
      }

      // NEXT POSITION IN HASHTABLE
      for (; posInHashTable < keys; ++posInHashTable) {
        int posInChunk = chunk.getInt(posInHashTable * Binary.INT_SERIALIZED_SIZE);
        if (posInChunk > 0) {
          chunk.position(posInChunk);

          // READ -> RID|INT|RID
          nextKeyRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
          nextKeyPos = chunk.getInt();
          valueRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
          ++posInHashTable;
          return true;
        }
      }

      return false;
    }

    public RID getKeyRID() {
      if (!hasNext())
        throw new NoSuchElementException();
      return nextKeyRID;
    }

    public RID getValueRID() {
      if (!hasNext())
        throw new NoSuchElementException();
      return valueRID;
    }

    public void moveNext() {
      valueRID = null;
    }
  }

  public CompressedRID2RIDIndex(final Database database, int expectedVertices, int expectedEdges) throws ClassNotFoundException {
    if (expectedVertices < 1)
      expectedVertices = 1_000_000;
    if (expectedEdges < 1)
      expectedEdges = expectedVertices;

    this.database = database;
    this.keys = expectedVertices;
    this.serializer = new BinarySerializer(database.getConfiguration());

    if (expectedEdges <= 0)
      expectedEdges = expectedVertices;

    this.chunk = new Binary(expectedVertices * 10 + expectedEdges * 10);
    this.chunk.setAllocationChunkSize(expectedEdges * 10 / 2);
    this.chunk.fill((byte) 0, keys * Binary.INT_SERIALIZED_SIZE);

    this.totalEntries = 0;
    this.totalUsedSlots = 0;
  }

  public CompressedRID2RIDIndex(final Database database, final Binary buffer) throws ClassNotFoundException {
    this.database = database;
    this.keys = buffer.size();
    this.serializer = new BinarySerializer(database.getConfiguration());
    this.chunk = buffer;
  }

  public int size() {
    return totalEntries;
  }

  public void setReadOnly() {
    readOnly = true;
  }

  public boolean isEmpty() {
    return totalEntries == 0;
  }

  public boolean containsKey(final RID key) {
    checkThreadAccess();

    if (key == null)
      throw new IllegalArgumentException("Key is null");

    return get(key) != null;
  }

  public RID get(final RID key) {
    checkThreadAccess();

    if (key == null)
      throw new IllegalArgumentException("Key is null");

    final int hash = (key.hashCode() & 0x7fffffff) % keys;

    final int pos = chunk.getInt(hash * Binary.INT_SERIALIZED_SIZE);
    if (pos == 0)
      return null;

    // SLOT OCCUPIED, CHECK FOR THE KEY
    // Termination invariant: each iteration either returns (key found), breaks
    // (chain terminator nextPos <= 0), or jumps to a strictly greater nextPos
    // because put() always appends new entries at chunk.size(). The loop is
    // therefore bounded by the number of collisions in this hash bucket.
    chunk.position(pos);
    int lastChainPos = pos;
    while (true) {
      final Object slotKey = serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
      if (BinaryComparator.equalsExact(slotKey, key)) {
        // FOUND KEY
        chunk.position(chunk.position() + Binary.INT_SERIALIZED_SIZE);
        return (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
      }

      final int nextPos = chunk.getInt();
      if (nextPos <= 0)
        break;

      assert nextPos > lastChainPos : "CompressedRID2RIDIndex.get chain must move forward to terminate";
      lastChainPos = nextPos;
      chunk.position(nextPos);
    }

    return null;
  }

  public void put(final RID key, final RID valueRID) {
    checkThreadAccess();

    if (key == null)
      throw new IllegalArgumentException("Key is null");

    if (valueRID == null)
      throw new IllegalArgumentException("Source vertex RID is null");

    if (totalEntries >= keys)
      growHashTable();

    final int hash = (key.hashCode() & 0x7fffffff) % keys;

    final int pos = chunk.getInt(hash * Binary.INT_SERIALIZED_SIZE);
    if (pos == 0) {
      // NEW KEY
      chunk.putInt(hash * Binary.INT_SERIALIZED_SIZE, chunk.size());
      chunk.position(chunk.size());

      // WRITE -> RID|INT|RID

      // WRITE THE KEY FIRST
      serializer.serializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, key);

      // LEAVE AN INT AS EMPTY SLOT FOR THE NEXT KEY
      chunk.putInt(0);

      // WRITE THE VALUE
      serializer.serializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, valueRID);

      ++totalUsedSlots;

    } else {
      // SLOT OCCUPIED, CHECK FOR THE KEY
      // Termination invariant: each iteration either throws (duplicate key), breaks
      // (chain terminator nextPos <= 0), or jumps to a strictly greater nextPos
      // because new entries are always appended at chunk.size(). The loop is bounded
      // by the number of collisions in this hash bucket.
      chunk.position(pos);
      int lastNextPos;
      int lastChainPos = pos;
      while (true) {
        final RID slotKey = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);

        if (BinaryComparator.equalsExact(slotKey, key))
          throw new IllegalArgumentException("Key " + key + " already inserted");

        lastNextPos = chunk.position();

        final int nextPos = chunk.getInt();
        if (nextPos <= 0)
          break;

        assert nextPos > lastChainPos : "CompressedRID2RIDIndex.put chain must move forward to terminate";
        lastChainPos = nextPos;
        chunk.position(nextPos);
      }

      // APPEND TO THE END
      chunk.position(chunk.size());
      final int entryPosition = chunk.position();

      // WRITE THE KEY FIRST
      serializer.serializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, key);

      // LEAVE AN INT AS EMPTY SLOT FOR THE NEXT KEY
      chunk.putInt(0);

      // WRITE THE VALUE
      serializer.serializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, valueRID);

      // WRITE THIS ENTRY POSITION TO THE PREVIOUS NEXT POSITION FIELD
      chunk.putInt(lastNextPos, entryPosition);
    }

    ++totalEntries;
  }

  /**
   * Doubles the slots of the hash table and rebuilds the chains into a new chunk. The table used to keep the slot count it was
   * built with for its whole life, so a caller that cannot know its entry count up front (the JSONL importer builds it with
   * 1,000 slots) walked chains of entries / 1,000 on every put() and get(), and a restore took time quadratic in the size of
   * the database (issue #12048). Called when the entries reach the slots: one pass over the entries per doubling keeps the total
   * linear, and the chains as short as the hash allows. {@link RID#hashCode()} mixes the bucket id and the position of the
   * whole key (#12481), so the chains stay short at any table size: only keys that share one hash value share a chain, and
   * those stay one chain however much the table grows.
   * <p>
   * Entries are appended to the tail of their new chain, so a chain still only moves forward in the chunk (the invariant the
   * chain walks of {@link #get} and {@link #put} assert). A table whose doubled form would not fit an int-addressed chunk keeps
   * its slots: longer chains, never a failed put().
   */
  private void growHashTable() {
    final long newKeys = (long) keys * 2;
    final long entriesSize = chunk.size() - (long) keys * Binary.INT_SERIALIZED_SIZE;
    if (newKeys * Binary.INT_SERIALIZED_SIZE + entriesSize > MAX_CHUNK_SIZE)
      return;

    final int newSlots = (int) newKeys;
    final Binary newChunk = new Binary((int) (newKeys * Binary.INT_SERIALIZED_SIZE + entriesSize));
    newChunk.setAllocationChunkSize(chunk.getAllocationChunkSize());
    newChunk.fill((byte) 0, newSlots * Binary.INT_SERIALIZED_SIZE);

    // POSITION OF THE NEXT-ENTRY FIELD OF THE LAST ENTRY OF EACH NEW CHAIN (0 = EMPTY SLOT): APPENDS IN CONSTANT TIME
    final int[] chainTails = new int[newSlots];
    int usedSlots = 0;

    for (int slot = 0; slot < keys; ++slot) {
      int pos = chunk.getInt(slot * Binary.INT_SERIALIZED_SIZE);
      while (pos > 0) {
        chunk.position(pos);
        final RID key = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
        final int nextPos = chunk.getInt();
        final RID value = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);

        final int newSlot = (key.hashCode() & 0x7fffffff) % newSlots;
        final int entryPosition = newChunk.size();
        newChunk.position(entryPosition);
        serializer.serializeValue(database, newChunk, BinaryTypes.TYPE_COMPRESSED_RID, key);
        final int nextFieldPosition = newChunk.position();
        newChunk.putInt(0);
        serializer.serializeValue(database, newChunk, BinaryTypes.TYPE_COMPRESSED_RID, value);

        if (chainTails[newSlot] == 0) {
          newChunk.putInt(newSlot * Binary.INT_SERIALIZED_SIZE, entryPosition);
          ++usedSlots;
        } else
          newChunk.putInt(chainTails[newSlot], entryPosition);
        chainTails[newSlot] = nextFieldPosition;

        pos = nextPos;
      }
    }

    chunk = newChunk;
    keys = newSlots;
    totalUsedSlots = usedSlots;
  }

  public int getKeys() {
    return keys;
  }

  public EntryIterator entryIterator() {
    checkThreadAccess();
    return new EntryIterator();
  }

  public int getChunkSize() {
    return chunk.size();
  }

  public int getChunkAllocated() {
    return chunk.capacity();
  }

  public int getTotalUsedSlots() {
    return totalUsedSlots;
  }

  private void checkThreadAccess() {
    if (!readOnly && lastThreadAccessed != null && lastThreadAccessed != Thread.currentThread())
      LogManager.instance().log(this, Level.WARNING, "Access by a different thread %d (%s). Previously it was %d (%s)", null, Thread.currentThread().threadId(),
          Thread.currentThread().getName(), lastThreadAccessed.getId(), lastThreadAccessed.getName());
    lastThreadAccessed = Thread.currentThread();
  }
}
