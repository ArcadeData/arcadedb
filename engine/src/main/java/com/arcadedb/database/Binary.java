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

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.UnsignedBytesComparator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.logging.Level;

/**
 * Binary data type. It is backed by Java Byte Buffers.
 *
 * @author Luca Garulli
 */
public class Binary implements BinaryStructure, Comparable<Binary> {
  public static final int BYTE_SERIALIZED_SIZE   = 1;
  public static final int SHORT_SERIALIZED_SIZE  = 2;
  public static final int INT_SERIALIZED_SIZE    = 4;
  public static final int LONG_SERIALIZED_SIZE   = 8;
  public static final int FLOAT_SERIALIZED_SIZE  = 4;
  public static final int DOUBLE_SERIALIZED_SIZE = 8;

  private final static int DEFAULT_ALLOCATION_CHUNK = 512;

  public interface FetchCallback {
    void fetch(Binary newBuffer) throws IOException;
  }

  protected boolean       autoResizable       = true;
  protected byte[]        content;
  protected ByteBuffer    buffer;
  protected int           size;
  protected int           allocationChunkSize = DEFAULT_ALLOCATION_CHUNK;
  protected FetchCallback fetchCallback;

  public Binary() {
    this.content = new byte[allocationChunkSize];
    this.buffer = ByteBuffer.wrap(content);
    size = 0;
  }

  public Binary(final int initialSize) {
    this.content = new byte[initialSize];
    this.buffer = ByteBuffer.wrap(content);
    size = 0;
  }

  public Binary(final byte[] buffer) {
    this(buffer, buffer.length);
  }

  public Binary(final byte[] buffer, final int contentSize) {
    this.content = buffer;
    this.buffer = ByteBuffer.wrap(content);
    this.size = contentSize;
    this.autoResizable = false;
  }

  public Binary(final ByteBuffer buffer) {
    this.content = buffer.array();
    this.buffer = buffer;
    this.size = buffer.limit();
    this.autoResizable = false;
  }

  public Binary copy() {
    final Binary copy = new Binary(Arrays.copyOfRange(content, buffer.arrayOffset(), buffer.arrayOffset() + size), size);
    copy.setAutoResizable(autoResizable);
    return copy;
  }

  public void clear() {
    size = 0;
    buffer.clear();
    buffer.position(0);
  }

  public void rewind() {
    buffer.position(0);
  }

  public void setAutoResizable(final boolean autoResizable) {
    this.autoResizable = autoResizable;
  }

  public int getAllocationChunkSize() {
    return allocationChunkSize;
  }

  public void setAllocationChunkSize(int allocationChunkSize) {
    this.allocationChunkSize = allocationChunkSize;
  }

  @Override
  public void append(final Binary toCopy) {
    final int contentSize = toCopy.size();
    if (contentSize > 0) {
      checkForAllocation(buffer.position(), contentSize);
      buffer.put(toCopy.content, 0, contentSize);
    }
  }

  @Override
  public int position() {
    return buffer.position();
  }

  @Override
  public void position(final int index) {
    try {
      buffer.position(index);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid position " + index + " (size=" + buffer.limit() + ")");
    }
  }

  @Override
  public void putByte(final int index, final byte value) {
    checkForAllocation(index, BYTE_SERIALIZED_SIZE);
    buffer.put(index, value);
  }

  @Override
  public void putByte(final byte value) {
    checkForAllocation(buffer.position(), BYTE_SERIALIZED_SIZE);
    buffer.put(value);
  }

  @Override
  public int putNumber(final int index, long value) {
    value = (value << 1) ^ (value >> 63);
    return putUnsignedNumber(index, value);
  }

  @Override
  public int putNumber(long value) {
    value = (value << 1) ^ (value >> 63);
    return putUnsignedNumber(value);
  }

  @Override
  public int putUnsignedNumber(final int index, final long value) {
    position(index);
    return putUnsignedNumber(value);
  }

  public int getVarSize(final long value) {
    int bytesUsed = 0;
    long v = value;
    while ((v & 0xFFFFFFFFFFFFFF80L) != 0L) {
      bytesUsed++;
      v >>>= 7;
    }
    bytesUsed++;

    return bytesUsed;
  }

  @Override
  public int putUnsignedNumber(final long value) {
    int bytesUsed = 0;
    long v = value;
    while ((v & 0xFFFFFFFFFFFFFF80L) != 0L) {
      checkForAllocation(buffer.position(), BYTE_SERIALIZED_SIZE);
      buffer.put((byte) (v & 0x7F | 0x80));
      bytesUsed++;
      v >>>= 7;
    }
    checkForAllocation(buffer.position(), BYTE_SERIALIZED_SIZE);
    buffer.put((byte) (v & 0x7F));
    bytesUsed++;

    return bytesUsed;
  }

  @Override
  public void putShort(final int index, final short value) {
    checkForAllocation(index, SHORT_SERIALIZED_SIZE);
    buffer.putShort(index, value);
  }

  @Override
  public void putShort(final short value) {
    checkForAllocation(buffer.position(), SHORT_SERIALIZED_SIZE);
    buffer.putShort(value);
  }

  @Override
  public void putInt(final int index, final int value) {
    checkForAllocation(index, INT_SERIALIZED_SIZE);
    buffer.putInt(index, value);
  }

  @Override
  public void putInt(final int value) {
    checkForAllocation(buffer.position(), INT_SERIALIZED_SIZE);
    buffer.putInt(value);
  }

  @Override
  public void putLong(final int index, final long value) {
    checkForAllocation(index, LONG_SERIALIZED_SIZE);
    buffer.putLong(index, value);
  }

  @Override
  public void putLong(final long value) {
    checkForAllocation(buffer.position(), LONG_SERIALIZED_SIZE);
    buffer.putLong(value);
  }

  @Override
  public int putString(final int index, final String value) {
    return putBytes(index, value.getBytes(DatabaseFactory.getDefaultCharset()));
  }

  @Override
  public int putString(final String value) {
    return putBytes(value.getBytes(DatabaseFactory.getDefaultCharset()));
  }

  @Override
  public int putBytes(final int index, final byte[] value) {
    position(index);
    return putBytes(value);
  }

  @Override
  public int putBytes(final byte[] value) {
    final int bytesUsed = putUnsignedNumber(value.length);
    checkForAllocation(buffer.position(), value.length);
    buffer.put(value);
    return bytesUsed + value.length;
  }

  @Override
  public int putBytes(final byte[] value, final int size) {
    final int bytesUsed = putUnsignedNumber(size);
    checkForAllocation(buffer.position(), size);
    buffer.put(value, 0, size);
    return bytesUsed + size;
  }

  @Override
  public void putByteArray(final int index, final byte[] value) {
    position(index);
    putByteArray(value);
  }

  @Override
  public void putByteArray(final int index, final byte[] value, final int length) {
    position(index);
    putByteArray(value, length);
  }

  @Override
  public void putByteArray(final byte[] value) {
    checkForAllocation(buffer.position(), value.length);
    buffer.put(value);
  }

  @Override
  public void putBuffer(final ByteBuffer value) {
    checkForAllocation(buffer.position(), value.limit());
    buffer.put(value);
  }

  @Override
  public void putByteArray(final byte[] value, final int length) {
    checkForAllocation(buffer.position(), length);
    buffer.put(value, 0, length);
  }

  @Override
  public byte getByte(final int index) {
    return buffer.get(index);
  }

  @Override
  public byte getByte() {
    checkForFetching(1);
    return buffer.get();
  }

  @Override
  public long[] getNumberAndSize(final int index) {
    position(index);
    final long[] raw = getUnsignedNumberAndSize();
    final long temp = (((raw[0] << 63) >> 63) ^ raw[0]) >> 1;
    // This extra step lets us deal with the largest signed values by
    // treating negative results from read unsigned methods as like unsigned values
    // Must re-flip the top bit if the original read value had it set.
    raw[0] = temp ^ (raw[0] & (1L << 63));
    return raw;
  }

  @Override
  public long getNumber() {
    final long raw = getUnsignedNumber();
    final long temp = (((raw << 63) >> 63) ^ raw) >> 1;
    // This extra step lets us deal with the largest signed values by
    // treating negative results from read unsigned methods as like unsigned values
    // Must re-flip the top bit if the original read value had it set.
    return temp ^ (raw & (1L << 63));
  }

  @Override
  public long getUnsignedNumber() {
    long value = 0L;
    int i = 0;
    long b;
    while (((b = getByte()) & 0x80L) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i > 63)
        throw new IllegalArgumentException("Variable length quantity is too long (must be <= 63)");
    }
    return value | (b << i);
  }

  @Override
  public long[] getUnsignedNumberAndSize() {
    long value = 0L;
    int i = 0;
    long b;
    int byteRead = 1;
    while (((b = getByte()) & 0x80L) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i > 63)
        throw new IllegalArgumentException("Variable length quantity is too long (must be <= 63)");
      ++byteRead;
    }
    return new long[] { value | (b << i), byteRead };
  }

  @Override
  public short getShort(final int index) {
    return buffer.getShort(index);
  }

  @Override
  public short getShort() {
    checkForFetching(2);
    return buffer.getShort();
  }

  @Override
  public short getUnsignedShort() {
    checkForFetching(2);
    int firstByte = (0x000000FF & ((int) buffer.get()));
    int secondByte = (0x000000FF & ((int) buffer.get()));
    return (short) (firstByte << 8 | secondByte);
  }

  @Override
  public int getInt() {
    checkForFetching(4);
    return buffer.getInt();
  }

  @Override
  public int getInt(final int index) {
    return buffer.getInt(index);
  }

  @Override
  public long getLong() {
    checkForFetching(8);
    return buffer.getLong();
  }

  @Override
  public long getLong(final int index) {
    return buffer.getLong(index);
  }

  @Override
  public String getString() {
    return new String(getBytes(),DatabaseFactory.getDefaultCharset());
  }

  @Override
  public String getString(final int index) {
    return new String(getBytes(index), DatabaseFactory.getDefaultCharset());
  }

  @Override
  public void getByteArray(final byte[] buffer) {
    this.buffer.get(buffer);
  }

  @Override
  public void getByteArray(final int index, final byte[] buffer) {
    this.buffer.position(index);
    this.buffer.get(buffer);
  }

  @Override
  public void getByteArray(final int index, final byte[] buffer, final int offset, final int length) {
    this.buffer.position(index);
    this.buffer.get(buffer, offset, length);
  }

  @Override
  public byte[] getBytes() {
    final byte[] result = new byte[(int) getUnsignedNumber()];
    if (result.length > 0) {
      checkForFetching(result.length);
      buffer.get(result);
    }
    return result;
  }

  @Override
  public byte[] getBytes(final int index) {
    buffer.position(index);
    return getBytes();
  }

  @Override
  public byte[] toByteArray() {
    final byte[] result = new byte[size];
    System.arraycopy(content, buffer.arrayOffset(), result, 0, result.length);
    return result;
  }

  @Override
  public byte[] remainingToByteArray() {
    final int tot = size - buffer.position();
    if (tot < 1)
      return new byte[0];

    final byte[] result = new byte[tot];
    System.arraycopy(content, buffer.position(), result, 0, result.length);
    return result;
  }

  @Override
  public ByteBuffer getByteBuffer() {
    return buffer;
  }

  public void flip() {
    size = buffer.position();
    buffer.flip();
  }

  /**
   * Creates a copy of this object referring to the same underlying buffer.
   *
   * @return the binary copy
   */
  public Binary slice() {
    buffer.rewind();
    return new Binary(buffer.slice());
  }

  /**
   * Creates a copy of this object referring to the same underlying buffer, starting from a position.
   *
   * @param position the starting position
   *
   * @return the binary copy
   */
  public Binary slice(final int position) {
    buffer.position(position);
    return new Binary(buffer.slice());
  }

  /**
   * Creates a copy of this object referring to the same underlying buffer, starting from a position and with a custom length.
   *
   * @param position the starting position
   * @param length   the length
   *
   * @return the binary copy
   */
  public Binary slice(final int position, final int length) {
    buffer.position(position);
    final ByteBuffer result = buffer.slice();
    result.position(length);
    result.flip();
    return new Binary(result);
  }

  @Override
  public int size() {
    return size;
  }

  public void size(int newSize) {
    if (newSize > content.length)
      checkForAllocation(0, newSize);
    else
      size = newSize;
  }

  public void move(final int startPosition, final int destPosition, final int length) {
    if (length == 0)
      return;
    checkForAllocation(0, destPosition + length);
    System.arraycopy(content, buffer.arrayOffset() + startPosition, content, buffer.arrayOffset() + destPosition, length);
  }

  public byte[] getContent() {
    return content;
  }

  public int readFromStream(final InputStream is) throws IOException {
    final int read = is.read(content, buffer.position(), buffer.capacity() - buffer.position());
    size += read;
    return read;
  }

  public int getContentSize() {
    return content.length;
  }

  public static int getNumberSpace(long value) {
    value = (value << 1) ^ (value >> 63);

    int bytesUsed = 0;
    long v = value;
    while ((v & 0xFFFFFFFFFFFFFF80L) != 0L) {
      bytesUsed++;
      v >>>= 7;
    }
    bytesUsed++;

    return bytesUsed;
  }

  @Override
  public String toString() {
    return "Binary size=" + size + " pos=" + buffer.position();
  }

  /**
   * Allocates enough space (max 1 page) and update the size according to the bytes to write.
   *
   * @param offset       the offset
   * @param bytesToWrite number of bytes to write
   */
  protected void checkForAllocation(final int offset, final int bytesToWrite) {
    if (offset + bytesToWrite > content.length) {

      if (!autoResizable)
        throw new IllegalArgumentException("Cannot resize the buffer (autoResizable=false)");

      final int newSize;
      if (offset + bytesToWrite > allocationChunkSize) {
        newSize = (((offset + bytesToWrite) / allocationChunkSize) + 1) * allocationChunkSize;
      } else
        newSize = allocationChunkSize;

      final byte[] newContent = new byte[newSize];
      System.arraycopy(content, 0, newContent, 0, content.length);
      this.content = newContent;

      final int oldPosition = this.buffer.position();
      final int oldOffset = this.buffer.arrayOffset();
      this.buffer = ByteBuffer.wrap(this.content, oldOffset, this.content.length);
      this.buffer.position(oldPosition);
    }

    if (offset + bytesToWrite > size)
      size = offset + bytesToWrite;
  }

  public Object executeInLock(final Callable<Object> callable) throws Exception {
    return callable.call();
  }

  public int capacity() {
    return content.length;
  }

  public void fill(final byte filler, final int size) {
    checkForAllocation(buffer.position(), size);
    for (int i = 0; i < size; ++i)
      buffer.put(filler);
  }

  public void fetch(final FetchCallback callback) {
    fetchCallback = callback;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof Binary))
      return false;
    final Binary binary = (Binary) o;
    return UnsignedBytesComparator.BEST_COMPARATOR.compare(content, binary.content) == 0;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(content);
  }

  @Override
  public int compareTo(final Binary o) {
    return UnsignedBytesComparator.BEST_COMPARATOR.compare(content, o.content);
  }

  private void checkForFetching(final int bytes) {
    if (fetchCallback == null)
      return;

    if (size - buffer.position() - 1 < bytes) {
      try {

        // ADD REMAINING CONTENT USING A NEW BUFFER OF THE SAME SIZE OF THE CURRENT ONE
        final Binary newBuffer = new Binary(buffer.capacity());
        newBuffer.putByteArray(this.remainingToByteArray());

        // FETCH NEW CONTENT
        fetchCallback.fetch(newBuffer);

        // REPLACE NEW CONTENT WITH CURRENT ONE
        newBuffer.rewind();
        buffer = newBuffer.buffer;
        content = newBuffer.content;
        size = newBuffer.size;

      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on fetching", e);
      }
    }
  }
}
