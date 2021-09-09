/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.bucket.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
public final class CellBTreeSingleValueBucketV3<K> extends ODurablePage {
  private static final int RID_SIZE = OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;

  private static final int FREE_POINTER_OFFSET = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int IS_LEAF_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int LEFT_SIBLING_OFFSET = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int RIGHT_SIBLING_OFFSET = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int NEXT_FREE_LIST_PAGE_OFFSET = NEXT_FREE_POSITION;

  private static final int POSITIONS_ARRAY_OFFSET =
      RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  public CellBTreeSingleValueBucketV3(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void switchBucketType() {
    if (!isEmpty()) {
      throw new IllegalStateException(
          "Type of bucket can be changed only bucket if bucket is empty");
    }

    final boolean isLeaf = isLeaf();
    if (isLeaf) {
      setByteValue(IS_LEAF_OFFSET, (byte) 0);
    } else {
      setByteValue(IS_LEAF_OFFSET, (byte) 1);
    }

    addPageOperation(new CellBTreeBucketSingleValueV3SwitchBucketTypePO());
  }

  public void init(boolean isLeaf) {
    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);

    setByteValue(IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));
    setLongValue(LEFT_SIBLING_OFFSET, -1);
    setLongValue(RIGHT_SIBLING_OFFSET, -1);

    addPageOperation(new CellBTreeBucketSingleValueV3InitPO(isLeaf));
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public int find(final K key, final OBinarySerializer<K> keySerializer) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final K midVal = getKey(mid, keySerializer);
      final int cmp = comparator.compare(midVal, key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }

    return -(low + 1); // key not found.
  }

  public int removeLeafEntry(final int entryIndex, byte[] key, byte[] value) {
    final int entryPosition =
        getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);

    final int entrySize;
    if (isLeaf()) {
      entrySize = key.length + RID_SIZE;
    } else {
      throw new IllegalStateException("Remove is applies to leaf buckets only");
    }

    int size = getIntValue(SIZE_OFFSET);
    if (entryIndex < size - 1) {
      moveData(
          POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE,
          (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(SIZE_OFFSET, size);

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      final int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition) {
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      }
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    addPageOperation(new CellBTreeBucketSingleValueV3RemoveLeafEntryPO(entryIndex, key, value));

    return size;
  }

  public int removeNonLeafEntry(
      final int entryIndex,
      boolean removeLeftChildPointer,
      final OBinarySerializer<K> keySerializer) {
    if (isLeaf()) {
      throw new IllegalStateException("Remove is applied to non-leaf buckets only");
    }

    final int entryPosition =
        getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
    final int keySize =
        getObjectSizeInDirectMemory(keySerializer, entryPosition + 2 * OIntegerSerializer.INT_SIZE);
    final byte[] key = getBinaryValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE, keySize);

    return removeNonLeafEntry(entryIndex, key, removeLeftChildPointer);
  }

  public int removeNonLeafEntry(
      final int entryIndex, final byte[] key, boolean removeLeftChildPointer) {
    if (isLeaf()) {
      throw new IllegalStateException("Remove is applied to non-leaf buckets only");
    }

    final int entryPosition =
        getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
    final int entrySize = key.length + 2 * OIntegerSerializer.INT_SIZE;
    int size = getIntValue(SIZE_OFFSET);

    final int leftChild = getIntValue(entryPosition);
    final int rightChild = getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);

    if (entryIndex < size - 1) {
      moveData(
          POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE,
          (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(SIZE_OFFSET, size);

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      final int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition) {
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      }
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    if (size > 0) {
      final int childPointer = removeLeftChildPointer ? rightChild : leftChild;

      if (entryIndex > 0) {
        final int prevEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + (entryIndex - 1) * OIntegerSerializer.INT_SIZE);
        setIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE, childPointer);
      }
      if (entryIndex < size) {
        final int nextEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
        setIntValue(nextEntryPosition, childPointer);
      }
    }

    addPageOperation(
        new CellBTreeBucketSingleValueV3RemoveNonLeafEntryPO(
            entryIndex, key, leftChild, rightChild));

    return size;
  }

  public int size() {
    return getIntValue(SIZE_OFFSET);
  }

  public CellBTreeEntry<K> getEntry(
      final int entryIndex, final OBinarySerializer<K> keySerializer) {
    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (isLeaf()) {
      final K key;

      key = deserializeFromDirectMemory(keySerializer, entryPosition);

      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);

      final int clusterId = getShortValue(entryPosition);
      final long clusterPosition = getLongValue(entryPosition + OShortSerializer.SHORT_SIZE);

      return new CellBTreeEntry<>(-1, -1, key, new ORecordId(clusterId, clusterPosition));
    } else {
      final int leftChild = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final int rightChild = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final K key = deserializeFromDirectMemory(keySerializer, entryPosition);

      return new CellBTreeEntry<>(leftChild, rightChild, key, null);
    }
  }

  public int getLeft(final int entryIndex) {
    assert !isLeaf();

    final int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    return getIntValue(entryPosition);
  }

  public int getRight(final int entryIndex) {
    assert !isLeaf();

    final int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    return getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);
  }

  public byte[] getRawEntry(final int entryIndex, final OBinarySerializer<K> keySerializer) {
    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int startEntryPosition = entryPosition;

    if (isLeaf()) {
      final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);

      return getBinaryValue(startEntryPosition, keySize + RID_SIZE);
    } else {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;

      final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);

      return getBinaryValue(startEntryPosition, keySize + 2 * OIntegerSerializer.INT_SIZE);
    }
  }

  /**
   * Obtains the value stored under the given entry index in this bucket.
   *
   * @param entryIndex the value entry index.
   * @return the obtained value.
   */
  public ORID getValue(final int entryIndex, final OBinarySerializer<K> keySerializer) {
    assert isLeaf();

    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    // skip key
    entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);

    final int clusterId = getShortValue(entryPosition);
    final long clusterPosition = getLongValue(entryPosition + OShortSerializer.SHORT_SIZE);

    return new ORecordId(clusterId, clusterPosition);
  }

  byte[] getRawValue(final int entryIndex, final OBinarySerializer<K> keySerializer) {
    assert isLeaf();

    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    // skip key
    entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);

    return getBinaryValue(entryPosition, RID_SIZE);
  }

  public K getKey(final int index, final OBinarySerializer<K> keySerializer) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf()) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    }

    return deserializeFromDirectMemory(keySerializer, entryPosition);
  }

  public boolean isLeaf() {
    return getByteValue(IS_LEAF_OFFSET) > 0;
  }

  public void addAll(final List<byte[]> rawEntries, final OBinarySerializer<K> keySerializer) {
    final int currentSize = size();
    for (int i = 0; i < rawEntries.size(); i++) {
      appendRawEntry(i + currentSize, rawEntries.get(i));
    }

    setIntValue(SIZE_OFFSET, rawEntries.size() + currentSize);

    addPageOperation(
        new CellBTreeBucketSingleValueV3AddAllPO(currentSize, rawEntries, keySerializer));
  }

  public void shrink(final int newSize, final OBinarySerializer<K> keySerializer) {
    final int currentSize = size();
    final List<byte[]> rawEntries = new ArrayList<>(newSize);
    final List<byte[]> removedEntries = new ArrayList<>(currentSize - newSize);

    for (int i = 0; i < newSize; i++) {
      rawEntries.add(getRawEntry(i, keySerializer));
    }

    for (int i = newSize; i < currentSize; i++) {
      removedEntries.add(getRawEntry(i, keySerializer));
    }

    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);

    for (int i = 0; i < newSize; i++) {
      appendRawEntry(i, rawEntries.get(i));
    }

    setIntValue(SIZE_OFFSET, newSize);

    addPageOperation(
        new CellBTreeBucketSingleValueV3ShrinkPO(newSize, removedEntries, keySerializer));
  }

  public boolean addLeafEntry(
      final int index, final byte[] serializedKey, final byte[] serializedValue) {
    final int entrySize = serializedKey.length + serializedValue.length;

    assert isLeaf();
    final int size = getIntValue(SIZE_OFFSET);

    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize
        < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return false;
    }

    if (index <= size - 1) {
      moveData(
          POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE,
          (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    setBinaryValue(freePointer, serializedKey);
    setBinaryValue(freePointer + serializedKey.length, serializedValue);

    addPageOperation(
        new CellBTreeBucketSingleValueV3AddLeafEntryPO(index, serializedKey, serializedValue));

    return true;
  }

  private void appendRawEntry(final int index, final byte[] rawEntry) {
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    freePointer -= rawEntry.length;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);

    setBinaryValue(freePointer, rawEntry);
  }

  public boolean addNonLeafEntry(
      final int index, final int leftChildIndex, final int newRightChildIndex, final byte[] key) {
    assert !isLeaf();

    final int keySize = key.length;

    final int entrySize = keySize + 2 * OIntegerSerializer.INT_SIZE;

    int size = size();
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize
        < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return false;
    }

    if (index <= size - 1) {
      moveData(
          POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE,
          (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    freePointer += setIntValue(freePointer, leftChildIndex);
    freePointer += setIntValue(freePointer, newRightChildIndex);

    setBinaryValue(freePointer, key);

    size++;

    if (size > 1) {
      if (index < size - 1) {
        final int nextEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE);
        setIntValue(nextEntryPosition, newRightChildIndex);
      }
    }

    addPageOperation(
        new CellBTreeBucketSingleValueV3AddNonLeafEntryPO(
            index, key, leftChildIndex, newRightChildIndex));

    return true;
  }

  public void updateValue(final int index, final byte[] value, final int keySize) {
    final int entryPosition =
        getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) + keySize;

    final byte[] prevValue = getBinaryValue(entryPosition, RID_SIZE);

    setBinaryValue(entryPosition, value);
    addPageOperation(
        new CellBTreeBucketSingleValueV3UpdateValuePO(index, keySize, prevValue, value));
  }

  public void setLeftSibling(final long pageIndex) {
    final int prevLeft = (int) getLongValue(LEFT_SIBLING_OFFSET);
    setLongValue(LEFT_SIBLING_OFFSET, pageIndex);

    addPageOperation(new CellBTreeBucketSingleValueV3SetLeftSiblingPO(prevLeft, (int) pageIndex));
  }

  public long getLeftSibling() {
    return getLongValue(LEFT_SIBLING_OFFSET);
  }

  public void setRightSibling(final long pageIndex) {
    final int prevRight = (int) getLongValue(RIGHT_SIBLING_OFFSET);

    setLongValue(RIGHT_SIBLING_OFFSET, pageIndex);

    addPageOperation(new CellBTreeBucketSingleValueV3SetRightSiblingPO(prevRight, (int) pageIndex));
  }

  public int getNextFreeListPage() {
    return getIntValue(NEXT_FREE_LIST_PAGE_OFFSET);
  }

  public void setNextFreeListPage(int nextFreeListPage) {
    final int prevNextFreeListPage = getIntValue(NEXT_FREE_LIST_PAGE_OFFSET);
    setIntValue(NEXT_FREE_LIST_PAGE_OFFSET, nextFreeListPage);

    addPageOperation(
        new CellBTreeBucketSingleValueV3SetNextFreeListPagePO(
            nextFreeListPage, prevNextFreeListPage));
  }

  public long getRightSibling() {
    return getLongValue(RIGHT_SIBLING_OFFSET);
  }

  public static final class CellBTreeEntry<K> implements Comparable<CellBTreeEntry<K>> {
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    protected final int leftChild;
    protected final int rightChild;
    public final K key;
    public final ORID value;

    public CellBTreeEntry(
        final int leftChild, final int rightChild, final K key, final ORID value) {
      this.leftChild = leftChild;
      this.rightChild = rightChild;
      this.key = key;
      this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final CellBTreeEntry<?> that = (CellBTreeEntry<?>) o;
      return leftChild == that.leftChild
          && rightChild == that.rightChild
          && Objects.equals(key, that.key)
          && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(leftChild, rightChild, key, value);
    }

    @Override
    public String toString() {
      return "CellBTreeEntry{"
          + "leftChild="
          + leftChild
          + ", rightChild="
          + rightChild
          + ", key="
          + key
          + ", value="
          + value
          + '}';
    }

    @Override
    public int compareTo(final CellBTreeEntry<K> other) {
      return comparator.compare(key, other.key);
    }
  }
}
