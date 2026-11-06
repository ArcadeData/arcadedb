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
package com.arcadedb.index.columnar;

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.engine.block.BlockCorruptionException;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Append-only dictionary of the distinct normalized values of one column: dense ordinals in insert order, never reused. One writer
 * appends ({@link #getOrAdd}); any number of readers call {@link #ordinalOf}, {@link #valueOf} and {@link #size} lock-free and see a
 * consistent prefix of the ordinals.
 * <p>
 * Memory: values live in chunks of {@value #CHUNK_SIZE} entries, appended only; {@link #size} is a volatile written after the slot.
 * Lookup uses an open-addressing table of {@code ordinal + 1} keyed by {@code hashCode()}, replaced (never grown in place) when its
 * load passes 0.5 and published through a volatile reference. A reader reads {@link #size} first, so every slot it can reach below
 * that size was written before the volatile write it observed; entries at or above it are skipped.
 * <p>
 * Disk (optional): {@code dict-<indexFileId>.<generation>.dic} holds the header {@code magic "ACD1", version 1} then one record per
 * ordinal, {@code [varint payload length][type byte][payload]}. A record is valid only up to the {@code validBytes} the manifest
 * names; anything after it is a crash tail that {@link #open} truncates. An append is two positioned writes, first
 * {@code [varint length][type byte], then the payload}, so a crash can leave a record header without its payload; that tail is
 * rewritten by the next append or truncated by {@link #open}.
 * <p>
 * The rank view orders the ordinals present when it was built by value; newer ordinals are ranked between its neighbours.
 */
public final class GlobalDictionary {
  static final  int  MAGIC       = 0x41434431;
  static final  int  VERSION     = 1;
  static final  int  HEADER_SIZE = 8;
  private static final int  CHUNK_SIZE   = 4096;
  private static final int  CHUNK_SHIFT  = 12;
  private static final int  CHUNK_MASK   = CHUNK_SIZE - 1;
  private static final int  INITIAL_TABLE = 1024;
  private static final int  MAX_VARINT_BYTES = 10;

  /** Immutable rank view: {@code rankOfOrdinal[o]} is the position of ordinal o in {@code sortedOrdinals}. */
  private record RankView(int[] rankOfOrdinal, int[] sortedOrdinals) {
  }

  private static final RankView EMPTY_VIEW = new RankView(new int[0], new int[0]);

  private final FileChannel channel;
  private final Binary      scratch = new Binary(64);
  private final ByteBuffer  head    = ByteBuffer.allocate(MAX_VARINT_BYTES + 1);

  private volatile Object[][] chunks = new Object[16][];
  private volatile int[]      table  = new int[INITIAL_TABLE];
  private volatile int        size;
  private volatile RankView   rankView = EMPTY_VIEW;
  private volatile long       appendPosition;
  private volatile long       valueHeapBytes;
  private volatile boolean    closed;
  private volatile long       writeSequence;

  private final AtomicReference<Thread> activeWriter = new AtomicReference<>();
  private final boolean                 readOnly;

  private GlobalDictionary(final FileChannel channel, final long appendPosition, final boolean readOnly) {
    this.channel = channel;
    this.appendPosition = appendPosition;
    this.readOnly = readOnly;
  }

  /** A dictionary without a file, for tests and for the creation-time cardinality check. It is never published. */
  public static GlobalDictionary createInMemory() {
    return new GlobalDictionary(null, 0, false);
  }

  /**
   * Creates a new generation file, refusing an existing one. The header is written and fsynced before returning.
   *
   * @throws java.nio.file.FileAlreadyExistsException when the file exists
   */
  public static GlobalDictionary create(final Path file) throws IOException {
    final FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
    try {
      final ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
      header.putInt(MAGIC).putInt(VERSION).flip();
      writeFully(channel, header, 0);
      channel.force(false);
    } catch (final IOException | RuntimeException e) {
      channel.close();
      throw e;
    }
    return new GlobalDictionary(channel, HEADER_SIZE, false);
  }

  /**
   * Opens an existing generation file and loads the first {@code validEntries} records, which must end exactly at
   * {@code validBytes}. Whatever the file holds after {@code validBytes} is a crash tail and is truncated.
   *
   * @throws BlockCorruptionException when the valid region fails its format check: wrong magic or version, a file shorter than
   *                                  {@code validBytes}, a record that does not fit, or an entry count that differs from
   *                                  {@code validEntries}
   */
  public static GlobalDictionary open(final Path file, final long validBytes, final int validEntries) throws IOException {
    return open(file, validBytes, validEntries, false);
  }

  /**
   * Opens a generation file for reading only: the file is opened READ, never truncated (bytes past {@code validBytes} are ignored,
   * they may belong to a writer or a crash tail the owner has not cleaned) and {@link #getOrAdd} refuses to add a value with an
   * {@link IllegalStateException}. The same corruption checks as {@link #open} apply.
   */
  public static GlobalDictionary openReadOnly(final Path file, final long validBytes, final int entries) throws IOException {
    return open(file, validBytes, entries, true);
  }

  private static GlobalDictionary open(final Path file, final long validBytes, final int validEntries, final boolean readOnly)
      throws IOException {
    final FileChannel channel = readOnly ?
        FileChannel.open(file, StandardOpenOption.READ) :
        FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.READ);
    try {
      final long fileSize = channel.size();
      if (validBytes < HEADER_SIZE || fileSize < validBytes)
        throw new BlockCorruptionException(
            "Dictionary " + file + " is shorter than its manifest says: file " + fileSize + " bytes, valid " + validBytes);
      final GlobalDictionary dictionary = new GlobalDictionary(channel, validBytes, readOnly);
      dictionary.load(file, validBytes, validEntries);
      if (!readOnly && fileSize > validBytes) {
        channel.truncate(validBytes);
        channel.force(false);
      }
      return dictionary;
    } catch (final IOException | RuntimeException e) {
      channel.close();
      throw e;
    }
  }

  private void load(final Path file, final long validBytes, final int validEntries) throws IOException {
    channel.position(0);
    final DataInputStream in = new DataInputStream(new BufferedInputStream(Channels.newInputStream(channel), 1 << 16));
    try {
      if (in.readInt() != MAGIC)
        throw new BlockCorruptionException("Dictionary " + file + " has a wrong magic");
      if (in.readInt() != VERSION)
        throw new BlockCorruptionException("Dictionary " + file + " has an unsupported version");
      long position = HEADER_SIZE;
      int entries = 0;
      while (position < validBytes) {
        long length = 0;
        int shift = 0;
        int varintBytes = 0;
        int b;
        do {
          b = in.read();
          if (b < 0 || ++varintBytes > MAX_VARINT_BYTES)
            throw new BlockCorruptionException("Dictionary " + file + " has a bad record length at " + position);
          length |= (long) (b & 0x7F) << shift;
          shift += 7;
        } while ((b & 0x80) != 0);
        final long payloadStart = position + varintBytes + 1;
        // the length is checked against the valid region before it sizes an allocation
        if (length < 0 || length > validBytes - payloadStart)
          throw new BlockCorruptionException("Dictionary " + file + " has a record running past its valid region at " + position);
        final int type = in.read();
        if (type < 0)
          throw new BlockCorruptionException("Dictionary " + file + " is truncated at " + position);
        final byte[] record = new byte[(int) length + 1];
        record[0] = (byte) type;
        in.readFully(record, 1, (int) length);
        final Binary recordBytes = new Binary(record);
        final Object value = DictionaryValueCodec.decode(recordBytes);
        if (recordBytes.position() != record.length)
          throw new BlockCorruptionException("Dictionary " + file + " record at " + position + " has trailing bytes after its value");
        if (lookup(value, size) >= 0)
          throw new BlockCorruptionException("Dictionary " + file + " holds a duplicate value at " + position);
        if (entries == validEntries)
          throw new BlockCorruptionException("Dictionary " + file + " holds more than the " + validEntries + " entries the manifest names");
        append(value);
        entries++;
        position = payloadStart + length;
      }
      if (position != validBytes || entries != validEntries)
        throw new BlockCorruptionException(
            "Dictionary " + file + " holds " + entries + " entries ending at " + position + ", the manifest says " + validEntries + " ending at "
                + validBytes);
    } catch (final EOFException e) {
      throw new BlockCorruptionException("Dictionary " + file + " is truncated: " + e);
    }
  }

  /**
   * Returns the ordinal of the value, adding it when absent. Writer only: one thread at a time.
   *
   * @throws IllegalArgumentException when the value is not supported by {@link DictionaryValueCodec}
   * @throws UncheckedIOException     when the append to the file fails; the dictionary is unchanged
   */
  public int getOrAdd(final Object value) {
    if (!DictionaryValueCodec.isSupported(value))
      throw new IllegalArgumentException("Unsupported dictionary value: " + (value == null ? "null" : value.getClass().getName()));
    assert enterWriter();
    try {
      if (closed)
        throw new IllegalStateException("Dictionary is closed");
      final int existing = lookup(value, size);
      if (existing >= 0)
        return existing;
      if (readOnly)
        throw new IllegalStateException("Dictionary is open read-only");
      final long newPosition = channel != null ? writeRecord(value) : appendPosition;
      // odd sequence while the (position, size) pair is being advanced, so fsync() never reads a torn pair; if the append fails
      // the position is not advanced and the pair stays at its previous consistent state (the written record is overwritten later)
      writeSequence++;
      try {
        final int ordinal = append(value);
        appendPosition = newPosition;
        return ordinal;
      } finally {
        writeSequence++;
      }
    } finally {
      assert exitWriter();
    }
  }

  /** The ordinal of the value, or -1 when absent. Lock-free. */
  public int ordinalOf(final Object value) {
    return value == null ? -1 : lookup(value, size);
  }

  /** @throws IndexOutOfBoundsException when the ordinal is negative or not below {@link #size()} */
  public Object valueOf(final int ordinal) {
    if (ordinal < 0 || ordinal >= size)
      throw new IndexOutOfBoundsException("Ordinal " + ordinal + " out of bounds for size " + size);
    return chunks[ordinal >>> CHUNK_SHIFT][ordinal & CHUNK_MASK];
  }

  /** The published number of ordinals. */
  public int size() {
    return size;
  }

  /**
   * Position of the ordinal in value order: {@code 2 * r + 1} for an ordinal inside the rank view, {@code 2 * insertionPoint} for a
   * newer one (insertionPoint = view values smaller than it), so it ranks strictly between its view neighbours and equal to a
   * newer ordinal in the same gap. Odd ranks are view ordinals, even ranks are gaps.
   */
  public int rank(final int ordinal) {
    return rank(ordinal, rankView);
  }

  /** Orders two ordinals by value: by rank, and by value when the ranks tie (two ordinals newer than the view in the same gap). */
  public int compare(final int ordinalA, final int ordinalB) {
    if (ordinalA == ordinalB)
      return 0;
    final RankView view = rankView;
    final int rankA = rank(ordinalA, view);
    final int rankB = rank(ordinalB, view);
    if (rankA != rankB)
      return Integer.compare(rankA, rankB);
    return DictionaryValueCodec.compareValues(valueOf(ordinalA), valueOf(ordinalB));
  }

  /** The durable prefix of a dictionary file: its byte length (a record boundary) and the number of entries it holds. */
  public record DurableState(long bytes, int entries) {
  }

  /**
   * Forces the appended tail to disk.
   *
   * @return the durable byte length and the entry count that belong to it, read together; record both in the manifest
   */
  public DurableState fsync() throws IOException {
    // a consistent (position, size) pair read before the force: a record appended meanwhile may or may not be durable and is not claimed
    long bytes;
    int entries;
    long sequence;
    do {
      sequence = writeSequence;
      bytes = appendPosition;
      entries = size;
      if ((sequence & 1) != 0)
        Thread.onSpinWait();
    } while ((sequence & 1) != 0 || sequence != writeSequence);
    if (channel != null)
      channel.force(false);
    return new DurableState(bytes, entries);
  }

  /** Sorts the ordinals present now by value and publishes the result. Safe to call while the writer appends. */
  public void rebuildRankView() {
    final int count = size;
    if (count <= rankView.rankOfOrdinal.length)
      return;
    final int[] sorted = new int[count];
    for (int i = 0; i < count; i++)
      sorted[i] = i;
    final Object[][] snapshot = chunks;
    sortByValue(sorted, new int[count], 0, count, snapshot);
    final int[] rankOfOrdinal = new int[count];
    for (int r = 0; r < count; r++)
      rankOfOrdinal[sorted[r]] = r;
    final RankView built = new RankView(rankOfOrdinal, sorted);
    if (rankView.rankOfOrdinal.length < count)
      rankView = built;
  }

  /** The ordinals the rank view covers: the first {@code rankViewSize()} ordinals rank by position, the newer ones by search. */
  public int rankViewSize() {
    return rankView.rankOfOrdinal.length;
  }

  /**
   * Whether the ordinals appended since the rank view was built are enough to rebuild it: more than {@code max(1024, rankViewSize / 8)}
   * (plan 3), so a sort ranks them by position again without re-sorting a large dictionary for a few new values.
   */
  public boolean isRankViewStale() {
    final int covered = rankView.rankOfOrdinal.length;
    return size - covered > Math.max(1024, covered / 8);
  }

  /** The rank view's ordinals in value order. Not copied: callers must not mutate it. */
  public int[] sortedOrdinals() {
    return rankView.sortedOrdinals;
  }

  /** Estimated heap held by the values, the chunks, the lookup table and the rank view. */
  public long heapBytes() {
    final RankView view = rankView;
    final long allocatedChunks = ((long) size + CHUNK_SIZE - 1) / CHUNK_SIZE;
    return valueHeapBytes + ((long) chunks.length + allocatedChunks * CHUNK_SIZE) * 8 + (long) table.length * 4
        + ((long) view.rankOfOrdinal.length + view.sortedOrdinals.length) * 4;
  }

  /** Closes the file. Reads stay valid; further appends are refused. */
  public void close() {
    closed = true;
    if (channel != null)
      try {
        channel.close();
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
  }

  private int rank(final int ordinal, final RankView view) {
    if (ordinal < view.rankOfOrdinal.length)
      return 2 * view.rankOfOrdinal[ordinal] + 1;
    final Object value = valueOf(ordinal);
    // insertion point: number of view values strictly smaller than the value
    final int[] sorted = view.sortedOrdinals;
    final Object[][] snapshot = chunks;
    int low = 0;
    int high = sorted.length;
    while (low < high) {
      final int mid = (low + high) >>> 1;
      final int o = sorted[mid];
      if (DictionaryValueCodec.compareValues(snapshot[o >>> CHUNK_SHIFT][o & CHUNK_MASK], value) < 0)
        low = mid + 1;
      else
        high = mid;
    }
    return 2 * low;
  }

  private int lookup(final Object value, final int visibleSize) {
    final int[] t = table;
    final int mask = t.length - 1;
    final Object[][] snapshot = chunks;
    int slot = slotOf(value.hashCode()) & mask;
    int entry;
    while ((entry = t[slot]) != 0) {
      final int ordinal = entry - 1;
      // an ordinal at or above the visible size is not published yet
      if (ordinal < visibleSize && value.equals(snapshot[ordinal >>> CHUNK_SHIFT][ordinal & CHUNK_MASK]))
        return ordinal;
      slot = (slot + 1) & mask;
    }
    return -1;
  }

  private static int slotOf(final int hash) {
    final int x = hash * 0x9E3779B1;
    return x ^ (x >>> 15);
  }

  /** Adds a value known to be absent: slot, then table, then the volatile size. */
  private int append(final Object value) {
    final int ordinal = size;
    final int chunk = ordinal >>> CHUNK_SHIFT;
    Object[][] current = chunks;
    if (chunk >= current.length) {
      current = Arrays.copyOf(current, current.length * 2);
      chunks = current;
    }
    if (current[chunk] == null)
      current[chunk] = new Object[CHUNK_SIZE];
    current[chunk][ordinal & CHUNK_MASK] = value;

    final int[] t = table;
    if ((ordinal + 1) * 2L > t.length) {
      final int[] grown = new int[t.length * 2];
      for (int o = 0; o <= ordinal; o++)
        insert(grown, current[o >>> CHUNK_SHIFT][o & CHUNK_MASK], o);
      table = grown;
    } else
      insert(t, value, ordinal);
    valueHeapBytes += estimateHeap(value);
    size = ordinal + 1;
    return ordinal;
  }

  private static void insert(final int[] t, final Object value, final int ordinal) {
    final int mask = t.length - 1;
    int slot = slotOf(value.hashCode()) & mask;
    while (t[slot] != 0)
      slot = (slot + 1) & mask;
    t[slot] = ordinal + 1;
  }

  private static long estimateHeap(final Object value) {
    return switch (value) {
      case String string -> 40L + string.length();
      case BigDecimal decimal -> 80L + decimal.unscaledValue().bitLength() / 8;
      case RID ignored -> 24L;
      case Boolean ignored -> 0L;
      default -> 16L;
    };
  }

  /** Writes the record at the current append position and returns the position after it; the position is not advanced here. */
  private long writeRecord(final Object value) {
    scratch.clear();
    final int total = DictionaryValueCodec.encode(value, scratch);
    final byte[] bytes = scratch.getContent();
    // varint payload length (everything after the type byte), then the type byte
    long length = total - 1;
    head.clear();
    while ((length & ~0x7FL) != 0) {
      head.put((byte) ((length & 0x7F) | 0x80));
      length >>>= 7;
    }
    head.put((byte) length);
    head.put(bytes[0]);
    head.flip();
    final long start = appendPosition;
    try {
      final int headBytes = head.remaining();
      writeFully(channel, head, start);
      // the second write: a crash between the two leaves a header without payload
      ColumnarTestHooks.reach("dictionary.appendPartial");
      writeFully(channel, ByteBuffer.wrap(bytes, 1, total - 1), start + headBytes);
      return start + headBytes + total - 1;
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void writeFully(final FileChannel channel, final ByteBuffer buffer, final long position) throws IOException {
    long at = position;
    while (buffer.hasRemaining())
      at += channel.write(buffer, at);
  }

  private static void sortByValue(final int[] ordinals, final int[] temp, final int from, final int to, final Object[][] values) {
    if (to - from < 2)
      return;
    final int mid = (from + to) >>> 1;
    sortByValue(ordinals, temp, from, mid, values);
    sortByValue(ordinals, temp, mid, to, values);
    int i = from;
    int j = mid;
    int k = from;
    while (i < mid && j < to) {
      final int a = ordinals[i];
      final int b = ordinals[j];
      if (DictionaryValueCodec.compareValues(values[a >>> CHUNK_SHIFT][a & CHUNK_MASK], values[b >>> CHUNK_SHIFT][b & CHUNK_MASK]) <= 0) {
        temp[k++] = a;
        i++;
      } else {
        temp[k++] = b;
        j++;
      }
    }
    while (i < mid)
      temp[k++] = ordinals[i++];
    while (j < to)
      temp[k++] = ordinals[j++];
    System.arraycopy(temp, from, ordinals, from, to - from);
  }

  /** Assertion helper (only evaluated with -ea): the single writer may change between calls, but two must never overlap. */
  private boolean enterWriter() {
    return activeWriter.compareAndSet(null, Thread.currentThread());
  }

  private boolean exitWriter() {
    activeWriter.set(null);
    return true;
  }
}
