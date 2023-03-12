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

import java.nio.*;
import java.util.*;
import java.util.zip.*;

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

  protected final PageManager manager;

  protected final PageId pageId;
  protected       Binary content;
  protected final int    size;
  protected       int    version;

  protected BasePage(final PageManager manager, final PageId pageId, final int size, final byte[] buffer, final int version, final int contentSize) {
    this.manager = manager;
    this.pageId = pageId;
    this.size = size;
    this.content = new Binary(buffer, contentSize);
    this.version = version;
  }

  /**
   * Returns an immutable view of the underlying binary object.
   */
  public abstract Binary getImmutableView(final int index, final int length);

  public MutablePage modify() {
    final byte[] array = this.content.getByteBuffer().array();
    // COPY THE CONTENT, SO CHANGES DOES NOT AFFECT IMMUTABLE COPY
    return new MutablePage(manager, pageId, size, Arrays.copyOf(array, array.length), version, content.size());
  }

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
   * Reads an unsigned number.
   *
   * @return An array of longs with the unsigned number in the 1st position and the occupied bytes on the 2nd position.
   */
  public long[] readNumberAndSize(final int index) {
    return this.content.getNumberAndSize(PAGE_HEADER_SIZE + index);
  }

  public long readLong(final int index) {
    return this.content.getLong(PAGE_HEADER_SIZE + index);
  }

  public int readInt(final int index) {
    return this.content.getInt(PAGE_HEADER_SIZE + index);
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
    return new String(readBytes(PAGE_HEADER_SIZE + index), DatabaseFactory.getDefaultCharset());
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
   */
  public ByteBuffer slice() {
    final ByteBuffer buffer = content.getByteBuffer();
    buffer.position(PAGE_HEADER_SIZE);
    return buffer.slice();
  }

  public int getBufferPosition() {
    return this.content.position() - PAGE_HEADER_SIZE;
  }

  public void setBufferPosition(final int newPos) {
    this.content.position(PAGE_HEADER_SIZE + newPos);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final BasePage other = (BasePage) o;

    if (!Objects.equals(pageId, other.pageId))
      return false;

    return version == other.version;
  }

  @Override
  public int hashCode() {
    return pageId.hashCode();
  }

  @Override
  public String toString() {
    final byte[] c = content.getByteBuffer().array();
    final Checksum crc32 = new CRC32();
    crc32.update(c, 0, c.length);

    return pageId.toString() + " v=" + version + " chk=" + crc32.getValue();
  }
}
