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
package com.arcadedb.index.lsm;

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;

import java.util.*;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

public class LSMTreeIndexUnderlyingPageCursor extends LSMTreeIndexUnderlyingAbstractCursor {
  protected final PageId   pageId;
  protected final Binary   buffer;
  protected final int      keyStartPosition;
  protected       int      currentEntryIndex;
  protected       int      valuePosition = -1;
  protected       Object[] nextKeys;
  protected       RID[]    nextValue;

  public LSMTreeIndexUnderlyingPageCursor(final LSMTreeIndexAbstract index, final BasePage page, final int currentEntryInPage,
      final int keyStartPosition, final byte[] keyTypes, final int totalKeys, final boolean ascendingOrder) {
    super(index, keyTypes, totalKeys, ascendingOrder);
    this.keyStartPosition = keyStartPosition;
    this.pageId = page.getPageId();
    this.buffer = new Binary(page.slice());
    this.currentEntryIndex = currentEntryInPage;
  }

  public boolean hasNext() {
    if (ascendingOrder)
      return currentEntryIndex < totalKeys - 1;
    return currentEntryIndex > 0;
  }

  public void next() {
    currentEntryIndex += ascendingOrder ? 1 : -1;
    nextKeys = null;
    nextValue = null;
  }

  public Object[] getKeys() {
    if (nextKeys != null)
      return nextKeys;

    if (currentEntryIndex < 0)
      throw new IllegalStateException("Invalid page cursor index " + currentEntryIndex);

    int readPos = currentEntryIndex;

    // For DESC iteration, scan backward to find the leftmost occurrence of the key at
    // currentEntryIndex. We then read from the leftmost and merge forward, so that:
    //  1. value insertion order is preserved (oldest first), keeping the deletion
    //     semantics in LSMTreeIndexCursor.next() correct;
    //  2. after the merge, currentEntryIndex is reset to the leftmost position so that
    //     the next call to next() skips past the entire duplicate group. Without this
    //     reset, next() would decrement by 1 and re-enter the same merge group,
    //     causing an infinite loop and returning duplicate entries.
    if (!ascendingOrder) {
      int currentContentPos = buffer.getInt(keyStartPosition + (currentEntryIndex * INT_SERIALIZED_SIZE));
      buffer.position(currentContentPos);
      final Object[] currentKey = new Object[keyTypes.length];
      for (int k = 0; k < keyTypes.length; ++k) {
        final boolean notNull = index.getVersion() < 1 || buffer.getByte() == 1;
        if (notNull)
          currentKey[k] = index.getDatabase().getSerializer().deserializeValue(index.getDatabase(), buffer, keyTypes[k], null);
        else
          currentKey[k] = null;
      }

      for (int pos = currentEntryIndex - 1; pos >= 0; --pos) {
        final int prevContentPos = buffer.getInt(keyStartPosition + (pos * INT_SERIALIZED_SIZE));
        buffer.position(prevContentPos);

        final Object[] adjacentKeys = new Object[keyTypes.length];
        for (int k = 0; k < keyTypes.length; ++k) {
          final boolean notNull = index.getVersion() < 1 || buffer.getByte() == 1;
          if (notNull)
            adjacentKeys[k] = index.getDatabase().getSerializer().deserializeValue(index.getDatabase(), buffer, keyTypes[k], null);
          else
            adjacentKeys[k] = null;
        }

        if (LSMTreeIndexMutable.compareKeys(index.comparator, keyTypes, currentKey, adjacentKeys) != 0)
          break;

        readPos = pos;
      }
    }

    int contentPos = buffer.getInt(keyStartPosition + (readPos * INT_SERIALIZED_SIZE));
    buffer.position(contentPos);

    nextKeys = new Object[keyTypes.length];
    for (int k = 0; k < keyTypes.length; ++k) {
      final boolean notNull = index.getVersion() < 1 || buffer.getByte() == 1;
      if (notNull)
        nextKeys[k] = index.getDatabase().getSerializer().deserializeValue(index.getDatabase(), buffer, keyTypes[k], null);
      else
        nextKeys[k] = null;
    }

    valuePosition = buffer.position();
    nextValue = index.readEntryValues(buffer);

    final int leftmost = readPos;

    for (int pos = readPos + 1; pos < totalKeys; ++pos) {
      contentPos = buffer.getInt(keyStartPosition + (pos * INT_SERIALIZED_SIZE));
      buffer.position(contentPos);

      final Object[] adjacentKeys = new Object[keyTypes.length];
      for (int k = 0; k < keyTypes.length; ++k) {
        final boolean notNull = index.getVersion() < 1 || buffer.getByte() == 1;
        if (notNull)
          adjacentKeys[k] = index.getDatabase().getSerializer().deserializeValue(index.getDatabase(), buffer, keyTypes[k], null);
        else
          adjacentKeys[k] = null;
      }

      final int compare = LSMTreeIndexMutable.compareKeys(index.comparator, keyTypes, nextKeys, adjacentKeys);
      if (compare != 0)
        break;

      currentEntryIndex = pos;

      // SAME KEY, MERGE VALUES
      valuePosition = buffer.position();
      final RID[] adjacentValue = index.readEntryValues(buffer);

      if (adjacentValue.length > 0) {
        final RID[] newArray = Arrays.copyOf(nextValue, nextValue.length + adjacentValue.length);
        for (int i = nextValue.length; i < newArray.length; ++i)
          newArray[i] = adjacentValue[i - nextValue.length];
        nextValue = newArray;
      }
    }

    if (!ascendingOrder)
      currentEntryIndex = leftmost;

    return nextKeys;
  }

  public RID[] getValue() {
    if (nextValue == null) {
      if (valuePosition < 0)
        getKeys();
      buffer.position(valuePosition);
      nextValue = index.readEntryValues(buffer);
      valuePosition = -1;
    }
    return nextValue;
  }

  @Override
  public PageId getCurrentPageId() {
    return pageId;
  }

  @Override
  public int getCurrentPositionInPage() {
    return currentEntryIndex;
  }
}
