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
import com.arcadedb.utility.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;

/**
 * Map like optimized to avoid stressing the GC by using mechanical sympathy technique + compression of key and values.
 * This class is not synchronized. The key is a RID and values are pairs of RIDs. This Map implementation doesn't support the overwrite of a
 * value. Values cannot be null.
 * <br>
 * This index is used for invert incoming edge creation.
 * <br>
 * A Binary object is used to store the hash table (the first part of it) and then keys and values. The RID key is serialized compressed in
 * the position pointed by the hash table, then a fixed-size integer containing the next entry (with the same hash) and after that the
 * compressed RIDs pair (edge+vertex). Another slot is kept to point to the previous entry. The hash table always points to the last element
 * with a linked list in the only direction of the previous.
 * <p>
 * TODO support up to 4GB by using unsigned int
 */
public class CompressedRID2RIDsIndex {
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
    private int posInChunk     = 0;
    private int nextEntryPos   = 0;
    private int nextKeyPos;

    private RID nextKeyRID;
    private RID nextEdgeRID;
    private RID nextVertexRID;

    public boolean hasNext() {
      if (nextVertexRID != null)
        return true;

      if (totalUsedSlots == 0)
        return false;

      if (nextEntryPos > 0) {
        // SAME KEY NEXT ENTRY
        chunk.position(nextEntryPos);

        nextEdgeRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
        nextVertexRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
        nextEntryPos = chunk.getInt();
        return true;
      }

      if (nextKeyPos > 0) {
        // NEXT KEY ON SAME POSITION IN HASHTABLE
        chunk.position(nextKeyPos);

        nextKeyRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
        nextKeyPos = chunk.getInt();
        nextEntryPos = chunk.getInt();
        nextEdgeRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
        nextVertexRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
        return true;
      }

      // NEXT POSITION IN HASHTABLE
      for (; posInHashTable < keys; ++posInHashTable) {
        posInChunk = chunk.getInt(posInHashTable * Binary.INT_SERIALIZED_SIZE);
        if (posInChunk > 0) {
          chunk.position(posInChunk);

          // READ -> RID|INT|INT|RID|RID

          nextKeyRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
          nextKeyPos = chunk.getInt();
          nextEntryPos = chunk.getInt();
          nextEdgeRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
          nextVertexRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
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

    public RID getEdgeRID() {
      if (!hasNext())
        throw new NoSuchElementException();
      return nextEdgeRID;
    }

    public RID getVertexRID() {
      if (!hasNext())
        throw new NoSuchElementException();
      return nextVertexRID;
    }

    public void moveNext() {
      nextVertexRID = null;
    }
  }

  public CompressedRID2RIDsIndex(final Database database, final int expectedVertices, int expectedEdges) {
    this.database = database;
    this.keys = expectedVertices;
    this.serializer = new BinarySerializer();

    if (expectedEdges <= 0)
      expectedEdges = expectedVertices;

    this.chunk = new Binary(expectedVertices * 10 + expectedEdges * 10);
    this.chunk.setAllocationChunkSize(expectedEdges * 10 / 2);
    this.chunk.fill((byte) 0, keys * Binary.INT_SERIALIZED_SIZE);

    this.totalEntries = 0;
    this.totalUsedSlots = 0;
  }

  public CompressedRID2RIDsIndex(final Database database, final Binary buffer) {
    this.database = database;
    this.keys = buffer.size();
    this.serializer = new BinarySerializer();
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

  public List<Pair<RID, RID>> get(final RID key) {
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
      Object slotKey = serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);

      if (BinaryComparator.equals(slotKey, key)) {
        // FOUND KEY, COLLECT ALL THE VALUE IN THE LINKED LIST
        final List<Pair<RID, RID>> list = new ArrayList<>();

        chunk.position(chunk.position() + Binary.INT_SERIALIZED_SIZE);

        int nextEntryPos = chunk.getInt();

        RID edgeRid = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
        RID vertexRid = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
        list.add(new Pair<>(edgeRid, vertexRid));

        while (nextEntryPos > 0) {
          chunk.position(nextEntryPos);

          edgeRid = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
          vertexRid = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);

          nextEntryPos = chunk.getInt();

          list.add(new Pair<>(edgeRid, vertexRid));
        }

        return list;
      }

      final int nextPos = chunk.getInt();
      if (nextPos <= 0)
        break;

      chunk.position(nextPos);
    }

    return null;
  }

  public void put(final RID key, final RID edgeRID, final RID vertexRID) {
    checkThreadAccess();

    if (key == null)
      throw new IllegalArgumentException("Key is null");

    if (vertexRID == null)
      throw new IllegalArgumentException("Source vertex RID is null");

    final int hash = (key.hashCode() & 0x7fffffff) % keys;

    final int pos = chunk.getInt(hash * Binary.INT_SERIALIZED_SIZE);
    if (pos == 0) {
      // NEW KEY
      chunk.putInt(hash * Binary.INT_SERIALIZED_SIZE, chunk.size());
      chunk.position(chunk.size());

      // WRITE -> RID|INT|INT|RID|RID

      // WRITE THE KEY FIRST
      serializer.serializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, key);

      // LEAVE AN INT AS EMPTY SLOT FOR THE NEXT KEY
      chunk.putInt(0);

      // LEAVE AN INT AS EMPTY SLOT FOR THE PREVIOUS ELEMENT
      chunk.putInt(0);

      // WRITE THE VALUE
      serializer.serializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, edgeRID);
      serializer.serializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, vertexRID);

      ++totalUsedSlots;

    } else {
      // SLOT OCCUPIED, CHECK FOR THE KEY
      chunk.position(pos);
      int lastNextPos = 0;
      while (true) {
        final RID slotKey = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);

        if (BinaryComparator.equals(slotKey, key)) {
          // FOUND THE KEY, GET PREVIOUS ITEM
          final int previousEntryOffset = chunk.position() + Binary.INT_SERIALIZED_SIZE; // SKIP NEXT KEY
          final int previousEntryPos = chunk.getInt(previousEntryOffset);

          // APPEND THE NEW ENTRY
          chunk.position(chunk.size());

          final int newEntryPosition = chunk.position();

          // WRITE -> RID|RID|INT

          serializer.serializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, edgeRID);
          serializer.serializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, vertexRID);

          if (previousEntryPos > 0)
            // THIS IS THE 3RD OR MAJOR ENTRY. APPEND THE POSITION OF THE PREVIOUS ENTRY
            chunk.putInt(previousEntryPos);
          else
            chunk.putInt(0);

          chunk.putInt(previousEntryOffset, newEntryPosition);
          ++totalEntries;
          return;
        }

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

      // LEAVE AN INT AS EMPTY SLOT FOR THE PREVIOUS ELEMENT
      chunk.putInt(0);

      // WRITE THE VALUE
      serializer.serializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, edgeRID);
      serializer.serializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, vertexRID);

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
