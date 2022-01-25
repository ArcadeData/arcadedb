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
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Map like optimized to avoid stressing the GC by using mechanical sympathy technique + compression of key and values.
 * This class is synchronized. Values are RIDs, key can be anything. This Map implementation doesn't support to overwrite a value.
 * Values cannot be null.
 * <br>
 * A Binary object is used to store the hash table (the first part of it) and then keys and values. The key is serialized in the position
 * pointed by the hash table, then a fixed-size integer containing the next entry (with the same hash) and after that the compressed RID.
 * <p>
 * TODO support up to 4GB by using unsigned int
 */
public class CompressedAny2RIDIndex<K> {
  private final Database         database;
  private final BinarySerializer serializer;
  private final byte             keyBinaryType;
  private final Type             keyType;
  private final Binary           chunk;
  private final int              keys;
  private       int              totalEntries   = 0;
  private       int              totalUsedSlots = 0;

  public class EntryIterator implements Iterator<RID> {
    private int posInHashTable = 0;
    private int posInChunk     = 0;
    private int nextKeyPos;

    private RID nextVertexRID;

    public boolean hasNext() {
      if (nextVertexRID != null)
        return true;

      if (totalUsedSlots == 0)
        return false;

      if (nextKeyPos > 0) {
        // IGNORE THE KEY AND TAKE THE VERTEX RID
        chunk.position(nextKeyPos);
        serializer.deserializeValue(database, chunk, keyBinaryType, null);

        // NEXT KEY ON SAME POSITION IN HASHTABLE
        nextKeyPos = chunk.getInt();
        nextVertexRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
        return true;
      }

      // NEXT POSITION IN HASHTABLE
      for (; posInHashTable < keys; ++posInHashTable) {
        posInChunk = chunk.getInt(posInHashTable * Binary.INT_SERIALIZED_SIZE);
        if (posInChunk > 0) {
          chunk.position(posInChunk);

          // IGNORE THE KEY AND TAKE THE VERTEX RID
          serializer.deserializeValue(database, chunk, keyBinaryType, null);

          nextKeyPos = chunk.getInt();
          nextVertexRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, null);
          ++posInHashTable;
          return true;
        }
      }

      return false;
    }

    public RID next() {
      if (!hasNext())
        throw new NoSuchElementException();
      try {
        return nextVertexRID;
      } finally {
        nextVertexRID = null;
      }
    }
  }

  public CompressedAny2RIDIndex(final Database database, final Type keyType, final int expectedSize) {
    this.database = database;

    this.keys = expectedSize;

    this.chunk = new Binary(expectedSize * 16); // 14 as an average size per entry
    this.chunk.setAllocationChunkSize(expectedSize);
    this.chunk.fill((byte) 0, keys * Binary.INT_SERIALIZED_SIZE);

    this.serializer = new BinarySerializer();

    this.keyType = keyType;
    this.keyBinaryType = keyType.getBinaryType();
  }

  public Type getKeyBinaryType() {
    return keyType;
  }

  public EntryIterator vertexIterator() {
    return new EntryIterator();
  }

  public int size() {
    return totalEntries;
  }

  public boolean isEmpty() {
    return totalEntries == 0;
  }

  public boolean containsKey(final Object key) {
    if (key == null)
      throw new IllegalArgumentException("Key is null");

    return get(key) != null;
  }

  public RID get(final Object key) {
    synchronized (this) {
      return get(chunk, key);
    }
  }

  public RID get(final Binary threadBuffer, final Object key) {
    if (key == null)
      throw new IllegalArgumentException("Key is null");

    final int hash = (key.hashCode() & 0x7fffffff) % keys;

    final int pos = threadBuffer.getInt(hash * Binary.INT_SERIALIZED_SIZE);
    if (pos == 0)
      return null;

    // SLOT OCCUPIED, CHECK FOR THE KEY
    threadBuffer.position(pos);
    while (true) {
      Object slotKey = serializer.deserializeValue(database, threadBuffer, keyBinaryType, null);

      if (BinaryComparator.equals(slotKey, key)) {
        threadBuffer.position(threadBuffer.position() + Binary.INT_SERIALIZED_SIZE);
        return (RID) serializer.deserializeValue(database, threadBuffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
      }

      final int nextPos = threadBuffer.getInt();
      if (nextPos <= 0)
        break;

      threadBuffer.position(nextPos);
    }

    return null;
  }

  public void put(final K key, final RID value) {
    if (key == null)
      throw new IllegalArgumentException("Key is null");

    if (value == null)
      throw new IllegalArgumentException("Value is null");

    final int hash = (key.hashCode() & 0x7fffffff) % keys;

    synchronized (this) {
      final int pos = chunk.getInt(hash * Binary.INT_SERIALIZED_SIZE);
      if (pos == 0) {
        // NEW KEY
        chunk.position(chunk.size());
        chunk.putInt(hash * Binary.INT_SERIALIZED_SIZE, chunk.position());

        // WRITE THE KEY FIRST
        serializer.serializeValue(database, chunk, keyBinaryType, key);

        // LEAVE AN INT AS EMPTY SLOT FOR THE NEXT KEY
        chunk.putInt(0);

        // WRITE THE VALUE
        serializer.serializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, value);

        ++totalUsedSlots;

      } else {
        // SLOT OCCUPIED, CHECK FOR THE KEY
        chunk.position(pos);
        int lastNextPos;
        while (true) {
          Object slotKey = serializer.deserializeValue(database, chunk, keyBinaryType, null);

          if (BinaryComparator.equals(slotKey, key))
            throw new IllegalArgumentException("Key '" + key + "' is already present in the map");

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
        serializer.serializeValue(database, chunk, keyBinaryType, key);

        // LEAVE AN INT AS EMPTY SLOT FOR THE NEXT KEY
        chunk.putInt(0);

        // WRITE THE VALUE
        serializer.serializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID, value);

        // WRITE THIS ENTRY POSITION TO THE PREVIOUS NEXT POSITION FIELD
        chunk.putInt(lastNextPos, entryPosition);
      }

      ++totalEntries;
    }
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

  public Binary getInternalBuffer() {
    return chunk;
  }
}
