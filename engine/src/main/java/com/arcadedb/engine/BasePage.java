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
package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseFactory;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Low level base page implementation of (default) 65536 bytes (2 exp 16 = 65Kb). The first 4 bytes (the header) are reserved to
 * store the page version (MVCC), then 4 bytes more for the actual page content size. Content size is stored in Binary object. The
 * maximum content for a page is pageSize - 16.
 * <br>
 * NOTE: This class is not thread safe and must be not used by multiple threads at the same time.
 */
public abstract class BasePage {
  protected static final int PAGE_VERSION_OFFSET     = 0;
  protected static final int PAGE_CONTENTSIZE_OFFSET = Binary.INT_SERIALIZED_SIZE;
  public static final    int PAGE_HEADER_SIZE        = Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE;

  protected final PageId pageId;
  protected       Binary content;
  protected final int    size;
  protected       int    version;

  protected BasePage(final PageId pageId, final int size, final byte[] buffer, final int version,
      final int contentSize) {
    this.pageId = pageId;
    this.size = size;
    this.content = new Binary(buffer, contentSize);
    this.version = version;
  }

  /**
   * Returns an immutable view of the underlying binary object.
   */
  public abstract Binary getImmutableView(final int index, final int length);

  public abstract MutablePage modify();

  public void loadMetadata() {
    version = content.getInt(PAGE_VERSION_OFFSET);
    content.size(content.getInt(PAGE_CONTENTSIZE_OFFSET));
  }

  public int getPhysicalSize() {
    return size;
  }

  public int getMaxContentSize() {
    return size - PAGE_HEADER_SIZE;
  }

  public int getAvailableContentSize() {
    return size - getContentSize();
  }

  public int getContentSize() {
    return content.size() - PAGE_HEADER_SIZE;
  }

  public long getVersion() {
    return version;
  }

  /**
   * Reads a signed variable-length number without allocating (#10656). The number of bytes it occupies on the page is
   * {@link #readNumberSize(int)}. Neither moves the buffer position nor keeps any state on the page, so the pair is safe on
   * a view shared by many threads ({@link SharedImmutablePage}, #10930).
   */
  public long readNumber(final int index) {
    return this.content.getNumberAt(PAGE_HEADER_SIZE + index);
  }

  /**
   * The number of bytes the variable-length number at {@code index} occupies on the page, without decoding it.
   */
  public int readNumberSize(final int index) {
    return this.content.getNumberSizeAt(PAGE_HEADER_SIZE + index);
  }

  /**
   * Reads a signed variable-length number. Allocates the returned pair on every call: a hot path uses
   * {@link #readNumber(int)} and {@link #readNumberSize(int)} instead.
   *
   * @return An array of longs with the number in the 1st position and the occupied bytes on the 2nd position.
   */
  public long[] readNumberAndSize(final int index) {
    return new long[] { readNumber(index), readNumberSize(index) };
  }

  public long readLong(final int index) {
    return this.content.getLong(PAGE_HEADER_SIZE + index);
  }

  public int readInt(final int index) {
    return this.content.getInt(PAGE_HEADER_SIZE + index);
  }

  public float readFloat(final int index) {
    return this.content.getFloat(PAGE_HEADER_SIZE + index);
  }

  public long readUnsignedInt(final int index) {
    return (long) this.content.getInt(PAGE_HEADER_SIZE + index) & 0xffffffffL;
  }

  public short readShort(final int index) {
    return this.content.getShort(PAGE_HEADER_SIZE + index);
  }

  public int readUnsignedShort(final int index) {
    return (int) this.content.getShort(PAGE_HEADER_SIZE + index) & 0xffff;
  }

  public byte readByte(final int index) {
    return this.content.getByte(PAGE_HEADER_SIZE + index);
  }

  public int readUnsignedByte(final int index) {
    return (int) this.content.getByte(PAGE_HEADER_SIZE + index) & 0xFF;
  }

  /**
   * Whether {@code length} bytes of this page, from {@code index}, are the same as {@code length} bytes of
   * {@code other}, from {@code otherIndex}: {@link Binary#isSameRegionAs} against the page's own content, so a
   * caller comparing a record image against the page it came from neither copies the region out first nor walks it
   * a byte at a time (#6217).
   */
  public boolean isSameContentAs(final int index, final Binary other, final int otherIndex, final int length) {
    return this.content.isSameRegionAs(PAGE_HEADER_SIZE + index, other, otherIndex, length);
  }

  public void readByteArray(final int index, final byte[] buffer) {
    this.content.getByteArray(PAGE_HEADER_SIZE + index, buffer);
  }

  public void readByteArray(final int index, final byte[] buffer, final int offset, final int length) {
    this.content.getByteArray(PAGE_HEADER_SIZE + index, buffer, offset, length);
  }

  public byte[] readBytes(final int index) {
    return this.content.getBytes(PAGE_HEADER_SIZE + index);
  }

  public byte[] readBytes() {
    return this.content.getBytes();
  }

  public String readString() {
    return new String(readBytes(), DatabaseFactory.getDefaultCharset());
  }

  public String readString(final int index) {
    // readBytes(int) ALREADY SKIPS THE PAGE HEADER
    return new String(readBytes(index), DatabaseFactory.getDefaultCharset());
  }

  public PageId getPageId() {
    return pageId;
  }

  /**
   * Returns the underlying ByteBuffer. If any changes occur bypassing the page object, must be tracked by calling #updateModifiedRange() method.
   */
  public ByteBuffer getContent() {
    return content.getByteBuffer();
  }

  /**
   * Returns the underlying ByteBuffer. If any changes occur bypassing the page object, must be tracked by calling #updateModifiedRange() method.
   * Creates a new ByteBuffer with independent position/limit but sharing the same backing array to ensure thread-safety.
   * This fixes issue #1509: Index iterators must be isolated from concurrent modifications.
   */
  public ByteBuffer slice() {
    final ByteBuffer buffer = content.getByteBuffer();
    // slice(index, length) creates a new ByteBuffer with independent position/limit
    // This prevents concurrent threads from interfering with each other's buffer position
    // #10654: one buffer, where duplicate().position(PAGE_HEADER_SIZE).slice() built two (the same bytes, the same limit)
    return buffer.slice(PAGE_HEADER_SIZE, buffer.limit() - PAGE_HEADER_SIZE);
  }

  public int getBufferPosition() {
    return this.content.position() - PAGE_HEADER_SIZE;
  }

  public void setBufferPosition(final int newPos) {
    this.content.position(PAGE_HEADER_SIZE + newPos);
  }

  /**
   * Value equality on {@code pageId} (and the concrete page class), consistent with {@link #hashCode()}.
   * {@code version} is intentionally NOT part of identity: it is mutable ({@link MutablePage#incrementVersion()}),
   * and keying equality on a mutable field is fragile (see issue #4544, where a stale flush could evict a newer
   * indexed page once their versions happened to match). Equality therefore stays stable across version changes.
   * Callers that need to distinguish two instances for the same {@link PageId} (e.g. the flush index removal in
   * {@code PageManagerFlushThread.removeFromFlushIndex}) must rely on reference identity, not on this method.
   */
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final BasePage other = (BasePage) o;

    return Objects.equals(pageId, other.pageId);
  }

  @Override
  public int hashCode() {
    return pageId.hashCode();
  }

  @Override
  public String toString() {
//    final byte[] c = content.getByteBuffer().array();
//    final Checksum crc32 = new CRC32();
//    crc32.update(c, 0, c.length);
//    return pageId.toString() + " v=" + version + " chk=" + crc32.getValue() + " records=" + readShort(0);
    return pageId.toString() + " v=" + version;
  }
}
