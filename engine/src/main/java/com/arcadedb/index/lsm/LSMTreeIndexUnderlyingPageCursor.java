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

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;

public class LSMTreeIndexUnderlyingPageCursor extends LSMTreeIndexUnderlyingAbstractCursor {
  protected final PageId pageId;
  protected final Binary buffer;
  protected final int    keyStartPosition;

  protected int      currentEntryIndex;
  protected int      valuePosition = -1;
  protected Object[] nextKeys;
  protected RID[]    nextValue;

  public LSMTreeIndexUnderlyingPageCursor(final LSMTreeIndexAbstract index, final BasePage page, final int currentEntryInPage, final int keyStartPosition,
      final byte[] keyTypes, final int totalKeys, final boolean ascendingOrder) {
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

    int contentPos = buffer.getInt(keyStartPosition + (currentEntryIndex * INT_SERIALIZED_SIZE));
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
//
//    for (int pos = currentEntryIndex + 1; pos < totalKeys; ++pos) {
//      contentPos = buffer.getInt(keyStartPosition + (pos * INT_SERIALIZED_SIZE));
//      buffer.position(contentPos);
//
//      final Object[] adjacentKeys = new Object[keyTypes.length];
//      for (int k = 0; k < keyTypes.length; ++k)
//        adjacentKeys[k] = index.getDatabase().getSerializer().deserializeValue(index.getDatabase(), buffer, keyTypes[k]);
//
//      final int compare = LSMTreeIndexMutable.compareKeys(index.comparator, keyTypes, nextKeys, adjacentKeys);
//      if (compare != 0)
//        break;
//
//      // SAME KEY, MERGE VALUES
//      valuePosition = buffer.position();
//      final RID[] adjacentValue = index.readEntryValues(buffer);
//
//      if (adjacentValue.length > 0) {
//        final RID[] newArray = Arrays.copyOf(nextValue, nextValue.length + adjacentValue.length);
//        for (int i = nextValue.length; i < newArray.length; ++i)
//          newArray[i] = adjacentValue[i - nextValue.length];
//        nextValue = newArray;
//      }
//    }

    return nextKeys;
  }

  public RID[] getValue() {
    if (nextValue == null) {
      if (valuePosition < 0)
        getKeys();
      buffer.position(valuePosition);
      nextValue = index.readEntryValues(buffer);
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
