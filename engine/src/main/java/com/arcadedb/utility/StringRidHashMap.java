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
package com.arcadedb.utility;

import com.arcadedb.database.RID;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Open-addressing hash map from an arbitrary {@link String} key to a RID, holding NO object per entry.
 * <p>
 * Written for the temporary-id to RID mapping of a streaming bulk load, which must survive for the whole request
 * (edges at the end of a payload may reference vertices created at the beginning) and is therefore the memory
 * ceiling of the load. A {@code HashMap<String, RID>} costs four live objects and ~170 bytes for a 50-character key
 * (String 24 + its byte[] 72 + RID 32 + Map.Node 32 + table slot ~10); this costs ~86 bytes and no object at all, so
 * a load of 16M vertices holds ~1.4GB instead of ~2.7GB and a handful of arrays instead of 64M objects - which is
 * what actually decides how long a full GC takes during a multi-hour import (issue #5470).
 * <p>
 * Layout: everything per-entry lives in a chunked byte arena as {@code [length:int][utf-8 key][bucketId:int]
 * [position:long]}, and the hash table is a single {@code long[]} of arena addresses. Keeping the table down to one
 * reference per slot is what makes it small: a power-of-two table holds 1.7 to 3.3 slots per entry, so every byte
 * added to a slot costs more than two bytes per entry.
 * <p>
 * Keys are compared byte by byte against the arena, so lookups are EXACT: unlike a hash-only map there is no chance
 * of two different ids resolving to the same vertex. The arena is append-only and is never rehashed or copied;
 * growth only doubles the address table and re-inserts, and updating an existing key rewrites its RID in place.
 * <p>
 * Entries are only inserted and looked up, never removed, which is why the open addressing needs no tombstones.
 * As with {@code HashMap.put}, storing a key twice overwrites the previous value.
 * <p>
 * NOTE: not thread safe. A reusable encoding buffer is kept per instance, so even {@link #get} must not be called
 * concurrently.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class StringRidHashMap {
  private static final long  EMPTY       = -1L;
  private static final float LOAD_FACTOR = 0.6f;
  private static final int   CHUNK_SIZE  = 1 << 20;

  // --- Arena: [length:int][utf-8 key][bucketId:int][position:long], appended, never moved ---
  private byte[][] chunks = new byte[8][];
  private int      chunkCount;
  private int      chunkPosition;
  private long     arenaBytes;

  // --- Table: one arena address per slot, nothing else ---
  private long[] addresses;   // (chunk index << 32) | offset of the length prefix, EMPTY when free
  private int    capacity;
  private int    mask;
  private int    size;
  private int    threshold;

  // --- Reusable encoding buffer, so a lookup allocates nothing ---
  private byte[] scratch = new byte[256];

  public StringRidHashMap() {
    this(1024);
  }

  public StringRidHashMap(final int initialCapacity) {
    capacity = nextPowerOfTwo(Math.max(16, initialCapacity));
    mask = capacity - 1;
    addresses = new long[capacity];
    threshold = (int) (capacity * LOAD_FACTOR);
    Arrays.fill(addresses, EMPTY);
  }

  /**
   * Associates {@code key} with the given RID, overwriting a previous association like {@code HashMap.put} does.
   */
  public void put(final String key, final RID rid) {
    put(key, rid.getBucketId(), rid.getPosition());
  }

  public void put(final String key, final int bucketId, final long position) {
    final int keyLength = encode(key);

    int idx = hash(scratch, 0, keyLength) & mask;
    while (addresses[idx] != EMPTY) {
      if (keyEquals(addresses[idx], keyLength)) {
        // Same key again: the RID lives in the arena, so it is rewritten in place and nothing is appended.
        final byte[] chunk = chunkOf(addresses[idx]);
        final int valueOffset = offsetOf(addresses[idx]) + Integer.BYTES + keyLength;
        writeInt(chunk, valueOffset, bucketId);
        writeLong(chunk, valueOffset + Integer.BYTES, position);
        return;
      }
      idx = (idx + 1) & mask;
    }

    addresses[idx] = store(keyLength, bucketId, position);

    if (++size >= threshold)
      resize();
  }

  /**
   * Returns the RID associated with {@code key}, or {@code null} when the key was never stored. The returned RID is
   * a fresh object: this map holds only the two primitives it is made of.
   */
  public RID get(final String key) {
    final int keyLength = encode(key);

    int idx = hash(scratch, 0, keyLength) & mask;
    while (addresses[idx] != EMPTY) {
      if (keyEquals(addresses[idx], keyLength))
        return ridAt(addresses[idx], keyLength);
      idx = (idx + 1) & mask;
    }
    return null;
  }

  public int size() {
    return size;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Iterates over every entry, rebuilding the key {@link String} from the arena on the fly. Only worth calling on a
   * map small enough to be materialized (the batch endpoint echoes the mapping back only below a threshold).
   */
  public void forEach(final EntryConsumer consumer) {
    for (int i = 0; i < capacity; i++) {
      final long address = addresses[i];
      if (address == EMPTY)
        continue;

      final byte[] chunk = chunkOf(address);
      final int offset = offsetOf(address);
      final int keyLength = readInt(chunk, offset);
      consumer.accept(new String(chunk, offset + Integer.BYTES, keyLength, StandardCharsets.UTF_8),
          ridAt(address, keyLength));
    }
  }

  /**
   * Bytes this map holds: the arena chunks allocated so far plus the address table. Used to report the memory a
   * streaming load is accumulating while it still has time to act on it.
   */
  public long retainedBytes() {
    return arenaBytes + (long) capacity * Long.BYTES;
  }

  @FunctionalInterface
  public interface EntryConsumer {
    void accept(String key, RID rid);
  }

  /**
   * UTF-8 encodes the key into the reusable buffer and returns its length in bytes. Hand-rolled because
   * {@code String.getBytes(UTF_8)} would allocate an array on every lookup - two per edge on a bulk load.
   */
  private int encode(final String key) {
    final int chars = key.length();
    // 3 bytes is the worst case per char: a surrogate PAIR takes 4 bytes for its 2 chars.
    if (scratch.length < chars * 3)
      scratch = new byte[Math.max(chars * 3, scratch.length * 2)];

    final byte[] buffer = scratch;
    int p = 0;
    for (int i = 0; i < chars; i++) {
      final char c = key.charAt(i);
      if (c < 0x80)
        buffer[p++] = (byte) c;
      else if (c < 0x800) {
        buffer[p++] = (byte) (0xC0 | (c >> 6));
        buffer[p++] = (byte) (0x80 | (c & 0x3F));
      } else if (Character.isHighSurrogate(c) && i + 1 < chars && Character.isLowSurrogate(key.charAt(i + 1))) {
        final int codePoint = Character.toCodePoint(c, key.charAt(++i));
        buffer[p++] = (byte) (0xF0 | (codePoint >> 18));
        buffer[p++] = (byte) (0x80 | ((codePoint >> 12) & 0x3F));
        buffer[p++] = (byte) (0x80 | ((codePoint >> 6) & 0x3F));
        buffer[p++] = (byte) (0x80 | (codePoint & 0x3F));
      } else {
        // Includes an unpaired surrogate, encoded as-is so that the same key always produces the same bytes.
        buffer[p++] = (byte) (0xE0 | (c >> 12));
        buffer[p++] = (byte) (0x80 | ((c >> 6) & 0x3F));
        buffer[p++] = (byte) (0x80 | (c & 0x3F));
      }
    }
    return p;
  }

  /**
   * Appends the encoded key sitting in {@link #scratch}, plus its RID, to the arena and returns its address.
   */
  private long store(final int keyLength, final int bucketId, final long position) {
    final int required = Integer.BYTES + keyLength + Integer.BYTES + Long.BYTES;

    if (chunkCount == 0 || chunkPosition + required > chunks[chunkCount - 1].length) {
      if (chunkCount == chunks.length)
        chunks = Arrays.copyOf(chunks, chunks.length * 2);
      // A key bigger than a chunk gets a chunk of its own, so an entry is never split across two arrays.
      final byte[] chunk = new byte[Math.max(CHUNK_SIZE, required)];
      chunks[chunkCount++] = chunk;
      chunkPosition = 0;
      arenaBytes += chunk.length;
    }

    final byte[] chunk = chunks[chunkCount - 1];
    final int offset = chunkPosition;
    writeInt(chunk, offset, keyLength);
    System.arraycopy(scratch, 0, chunk, offset + Integer.BYTES, keyLength);
    writeInt(chunk, offset + Integer.BYTES + keyLength, bucketId);
    writeLong(chunk, offset + Integer.BYTES + keyLength + Integer.BYTES, position);
    chunkPosition += required;

    return ((long) (chunkCount - 1) << 32) | (offset & 0xFFFFFFFFL);
  }

  /**
   * Compares the key stored at {@code address} with the encoded key sitting in {@link #scratch}.
   */
  private boolean keyEquals(final long address, final int keyLength) {
    final byte[] chunk = chunkOf(address);
    final int offset = offsetOf(address);
    if (readInt(chunk, offset) != keyLength)
      return false;

    final int from = offset + Integer.BYTES;
    return Arrays.equals(chunk, from, from + keyLength, scratch, 0, keyLength);
  }

  private RID ridAt(final long address, final int keyLength) {
    final byte[] chunk = chunkOf(address);
    final int valueOffset = offsetOf(address) + Integer.BYTES + keyLength;
    return new RID(readInt(chunk, valueOffset), readLong(chunk, valueOffset + Integer.BYTES));
  }

  private byte[] chunkOf(final long address) {
    return chunks[(int) (address >>> 32)];
  }

  private static int offsetOf(final long address) {
    return (int) address;
  }

  /**
   * Doubles the address table and re-inserts. The keys stay where they are and their hash is recomputed from the
   * arena: caching it in a parallel array would cost 4 bytes per SLOT, i.e. ~10 per entry, to save a rehash that
   * happens once per doubling.
   */
  private void resize() {
    final int newCapacity = capacity << 1;
    final int newMask = newCapacity - 1;
    final long[] newAddresses = new long[newCapacity];
    Arrays.fill(newAddresses, EMPTY);

    for (int i = 0; i < capacity; i++) {
      final long address = addresses[i];
      if (address == EMPTY)
        continue;

      final byte[] chunk = chunkOf(address);
      final int offset = offsetOf(address);
      int idx = hash(chunk, offset + Integer.BYTES, readInt(chunk, offset)) & newMask;
      while (newAddresses[idx] != EMPTY)
        idx = (idx + 1) & newMask;
      newAddresses[idx] = address;
    }

    addresses = newAddresses;
    capacity = newCapacity;
    mask = newMask;
    threshold = (int) (newCapacity * LOAD_FACTOR);
  }

  /**
   * FNV-1a over the key bytes, finished with a murmur-style avalanche: linear probing needs the low bits to be
   * well mixed, and FNV alone leaves them correlated for keys sharing a prefix - which bulk-load ids usually do.
   */
  private static int hash(final byte[] key, final int offset, final int length) {
    int h = 0x811C9DC5;
    for (int i = 0; i < length; i++)
      h = (h ^ key[offset + i]) * 0x01000193;

    h ^= h >>> 16;
    h *= 0x7FEB352D;
    h ^= h >>> 15;
    h *= 0x846CA68B;
    h ^= h >>> 16;
    return h;
  }

  private static void writeInt(final byte[] buffer, final int offset, final int value) {
    buffer[offset] = (byte) (value >>> 24);
    buffer[offset + 1] = (byte) (value >>> 16);
    buffer[offset + 2] = (byte) (value >>> 8);
    buffer[offset + 3] = (byte) value;
  }

  private static int readInt(final byte[] buffer, final int offset) {
    return ((buffer[offset] & 0xFF) << 24) | ((buffer[offset + 1] & 0xFF) << 16) | ((buffer[offset + 2] & 0xFF) << 8) | (
        buffer[offset + 3] & 0xFF);
  }

  private static void writeLong(final byte[] buffer, final int offset, final long value) {
    writeInt(buffer, offset, (int) (value >>> 32));
    writeInt(buffer, offset + Integer.BYTES, (int) value);
  }

  private static long readLong(final byte[] buffer, final int offset) {
    return ((long) readInt(buffer, offset) << 32) | (readInt(buffer, offset + Integer.BYTES) & 0xFFFFFFFFL);
  }

  private static int nextPowerOfTwo(final int v) {
    int n = v - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return n + 1;
  }
}
