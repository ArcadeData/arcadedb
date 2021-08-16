/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import com.arcadedb.database.TrackableBinary;

/**
 * Mutable page that accepts updates. It keeps track of the modified bytes.
 */
public class MutablePage extends BasePage implements TrackableContent {
  private int     modifiedRangeFrom = Integer.MAX_VALUE;
  private int     modifiedRangeTo   = -1;
  private WALFile walFile;

  public MutablePage(final PageManager manager, final PageId pageId, final int size) {
    this(manager, pageId, size, new byte[size], 0, 0);
    updateModifiedRange(0, size - 1);
  }

  public MutablePage(final PageManager manager, final PageId pageId, final int size, final byte[] array, final int version, final int contentSize) {
    super(manager, pageId, size, array, version, contentSize);
  }

  public TrackableBinary getTrackable() {
    content.getByteBuffer().position(PAGE_HEADER_SIZE);
    return new TrackableBinary(this, content.getByteBuffer().slice());
  }

  public void incrementVersion() {
    updateModifiedRange(0, Binary.LONG_SERIALIZED_SIZE);
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
    final int varSizeBytesUsed = this.content.getVarSize(content.length);
    checkBoundariesOnWrite(index, varSizeBytesUsed + content.length);
    return this.content.putBytes(index, content);
  }

  public void writeByteArray(int index, final byte[] content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, content.length);
    this.content.putByteArray(index, content);
  }

  public int writeString(final int index, final String content) {
    return writeBytes(index, content.getBytes());
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
      throw new IllegalArgumentException("Update range (" + start + "-" + end + ") out of bound (0-" + getPhysicalSize() + ")");

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
      throw new IllegalArgumentException("Cannot write outside the page space (" + (start + length) + ">" + getPhysicalSize() + ")");

    updateModifiedRange(start, start + length - 1);
  }
}
