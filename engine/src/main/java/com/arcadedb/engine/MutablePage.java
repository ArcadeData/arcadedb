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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.TrackableBinary;
import com.arcadedb.log.LogManager;

import java.nio.*;
import java.util.*;
import java.util.logging.*;

/**
 * Mutable page that accepts updates. It keeps track of the modified bytes.
 * <br>
 * NOTE: This class is not thread safe and must be not used by multiple threads at the same time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MutablePage extends BasePage implements TrackableContent {
  private static final byte[]  ZERO_BYTES_ARRAY;
  private              int     modifiedRangeFrom = Integer.MAX_VALUE;
  private              int     modifiedRangeTo   = -1;
  private              WALFile walFile;

  static {
    ZERO_BYTES_ARRAY = new byte[GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getValueAsInteger()];
  }

  public MutablePage(final PageId pageId, final int size) {
    this(pageId, size, new byte[size], 0, 0);
    updateModifiedRange(0, size - 1);
  }

  public MutablePage(final PageId pageId, final int size, final byte[] array, final int version, final int contentSize) {
    super(pageId, size, array, version, contentSize);
  }

  /**
   * Returns a copy of the underlying buffer because it could change in the current thread.
   */
  @Override
  public Binary getImmutableView(final int index, final int length) {
    final int offset = content.getByteBuffer().arrayOffset() + index + PAGE_HEADER_SIZE;
    final Binary copy = new Binary(Arrays.copyOfRange(content.getContent(), offset, offset + length));
    copy.setAutoResizable(true);
    return copy;
  }

  @Override
  public MutablePage modify() {
    return this;
  }

  public TrackableBinary getTrackable() {
    final ByteBuffer buffer = content.getByteBuffer();
    buffer.position(PAGE_HEADER_SIZE);
    return new TrackableBinary(this, buffer.slice());
  }

  public void setContentSize(final int value) {
    content.size(value + PAGE_HEADER_SIZE);
  }

  public void clearContent() {
    content.clear();
  }

  public void updateMetadata() {
    content.putInt(PAGE_VERSION_OFFSET, version);
    content.putInt(PAGE_CONTENTSIZE_OFFSET, content.size());
  }

  public void incrementVersion() {
    version++;
  }

  public int writeNumber(int index, final long content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.LONG_SERIALIZED_SIZE + 1); // WITH VARSIZE NUMBER THE WORST CASE SCENARIO IS 1 BYTE MORE
    return this.content.putNumber(index, content);
  }

  public void writeLong(int index, final long content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.LONG_SERIALIZED_SIZE);
    this.content.putLong(index, content);
  }

  public void writeInt(int index, final int content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.INT_SERIALIZED_SIZE);
    this.content.putInt(index, content);
  }

  public void writeUnsignedInt(int index, final long content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.INT_SERIALIZED_SIZE);
    this.content.putInt(index, (int) content);
  }

  public void writeShort(int index, final short content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.SHORT_SERIALIZED_SIZE);
    this.content.putShort(index, content);
  }

  public void writeUnsignedShort(int index, final int content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.SHORT_SERIALIZED_SIZE);
    this.content.putShort(index, (short) content);
  }

  public void writeByte(int index, final byte content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.BYTE_SERIALIZED_SIZE);
    this.content.putByte(index, content);
  }

  public int writeBytes(int index, final byte[] content) {
    index += PAGE_HEADER_SIZE;
    final int varSizeBytesUsed = Binary.getUnsignedNumberSpace(content.length);
    checkBoundariesOnWrite(index, varSizeBytesUsed + content.length);
    return this.content.putBytes(index, content);
  }

  public void writeByteArray(int index, final byte[] content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, content.length);
    this.content.putByteArray(index, content);
  }

  public void writeByteArray(int index, final byte[] content, final int contentOffset, final int contentSize) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, contentSize);
    this.content.putByteArray(index, content, contentOffset, contentSize);
  }

  public void writeZeros(int index, final int contentLength) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, contentLength);
    if (contentLength <= ZERO_BYTES_ARRAY.length)
      // FAST COPY
      this.content.putByteArray(index, ZERO_BYTES_ARRAY, 0, contentLength);
    else
      for (int i = 0; i < contentLength; i++)
        this.content.putByte(index + i, (byte) 0);
  }

  public int writeString(final int index, final String content) {
    return writeBytes(index, content.getBytes(DatabaseFactory.getDefaultCharset()));
  }

  public int getAvailableContentSize() {
    return getPhysicalSize() - getContentSize();
  }

  @Override
  public int[] getModifiedRange() {
    return new int[] { modifiedRangeFrom, modifiedRangeTo };
  }

  @Override
  public void updateModifiedRange(final int start, final int end) {
    if (start < 0 || end >= getPhysicalSize())
      throw new IllegalArgumentException(
          "Update range (" + start + "-" + end + ") out of bound (0-" + (getPhysicalSize() - 1) + ")");

    if (start < modifiedRangeFrom)
      modifiedRangeFrom = start;
    if (end > modifiedRangeTo)
      modifiedRangeTo = end;
  }

  public WALFile getWALFile() {
    return walFile;
  }

  public void setWALFile(final WALFile WALFile) {
    this.walFile = WALFile;
  }

  public void move(int startPosition, int destPosition, final int length) {
    startPosition += PAGE_HEADER_SIZE;
    destPosition += PAGE_HEADER_SIZE;
    updateModifiedRange(startPosition, destPosition + length);
    content.move(startPosition, destPosition, length);
  }

  private void checkBoundariesOnWrite(final int start, final int length) {
    if (start < 0)
      throw new IllegalArgumentException("Invalid position " + start);

    if (start + length > getPhysicalSize())
      throw new IllegalArgumentException(
          "Cannot write outside the page space (" + (start + length) + ">" + getPhysicalSize() + ")");

    updateModifiedRange(start, start + length - 1);
  }
}
