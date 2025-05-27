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

import java.util.*;
import java.util.logging.*;

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
 * TODO support up to 4GB by using unsigned int
 */
public class CompressedRID2RIDIndex {
  protected final Database         database;
  protected final BinarySerializer serializer;
  protected final int              keys;
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
    chunk.position(pos);
    while (true) {
      final Object slotKey = serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
      if (BinaryComparator.equals(slotKey, key)) {
        // FOUND KEY
        chunk.position(chunk.position() + Binary.INT_SERIALIZED_SIZE);
        return (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
      }

      final int nextPos = chunk.getInt();
      if (nextPos <= 0)
        break;

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
      chunk.position(pos);
      int lastNextPos = 0;
      while (true) {
        final RID slotKey = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);

        if (BinaryComparator.equals(slotKey, key))
          throw new IllegalArgumentException("Key " + key + " already inserted");

        lastNextPos = chunk.position();

        final int nextPos = chunk.getInt();
        if (nextPos <= 0)
          break;

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
      LogManager.instance().log(this, Level.WARNING, "Access by a different thread %d (%s). Previously it was %d (%s)", null, Thread.currentThread().getId(),
          Thread.currentThread().getName(), lastThreadAccessed.getId(), lastThreadAccessed.getName());
    lastThreadAccessed = Thread.currentThread();
  }
}
