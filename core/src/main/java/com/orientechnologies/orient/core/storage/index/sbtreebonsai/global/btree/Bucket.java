package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.IntSerializer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree.BTree.TreeEntry;
import java.util.ArrayList;
import java.util.List;

final class Bucket extends ODurablePage {

  private static final int FREE_POINTER_OFFSET = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int IS_LEAF_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int LEFT_SIBLING_OFFSET = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int RIGHT_SIBLING_OFFSET = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int POSITIONS_ARRAY_OFFSET =
      RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  public Bucket(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init(boolean isLeaf) {
    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);

    setByteValue(IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));
    setLongValue(LEFT_SIBLING_OFFSET, -1);
    setLongValue(RIGHT_SIBLING_OFFSET, -1);
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
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public int size() {
    return getIntValue(SIZE_OFFSET);
  }

  public boolean isLeaf() {
    return getByteValue(IS_LEAF_OFFSET) > 0;
  }

  public int find(final EdgeKey key) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final EdgeKey midKey = getKey(mid);
      final int cmp = midKey.compareTo(key);

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

  public EdgeKey getKey(final int index) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf()) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    }

    return deserializeFromDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);
  }

  private byte[] getRawKey(final int index) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf()) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    }

    final int keySize = getObjectSizeInDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);
    return getBinaryValue(entryPosition, keySize);
  }

  public void removeLeafEntry(final int entryIndex, final int keySize, final int valueSize) {
    final int entryPosition =
        getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);

    final int entrySize;
    if (isLeaf()) {
      entrySize = keySize + valueSize;
    } else {
      throw new IllegalStateException("Remove is applies to leaf buckets only");
    }

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;
    assert freePointer + entrySize <= ODurablePage.MAX_PAGE_SIZE_BYTES;

    int size = getIntValue(SIZE_OFFSET);
    if (entryIndex < size - 1) {
      moveData(
          POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE,
          (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(SIZE_OFFSET, size);

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
  }

  public void removeNonLeafEntry(final int entryIndex, final byte[] key, final int prevChild) {
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
    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;

    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
    }

    assert freePointer + entrySize <= ODurablePage.MAX_PAGE_SIZE_BYTES;
    setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      final int currentEntryPosition = getIntValue(currentPositionOffset);
      if (currentEntryPosition < entryPosition) {
        setIntValue(currentPositionOffset, currentEntryPosition + entrySize);
      }
      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    if (prevChild >= 0) {
      if (entryIndex > 0) {
        final int prevEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + (entryIndex - 1) * OIntegerSerializer.INT_SIZE);
        setIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE, prevChild);
      }

      if (entryIndex < size) {
        final int nextEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);
        setIntValue(nextEntryPosition, prevChild);
      }
    }
  }

  public TreeEntry getEntry(final int entryIndex) {
    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (isLeaf()) {
      final EdgeKey key;

      key = deserializeFromDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);

      entryPosition += getObjectSizeInDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);

      final int value = deserializeFromDirectMemory(IntSerializer.INSTANCE, entryPosition);

      return new TreeEntry(-1, -1, key, value);
    } else {
      final int leftChild = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final int rightChild = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final EdgeKey key = deserializeFromDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);

      return new TreeEntry(leftChild, rightChild, key, -1);
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

  public int getValue(final int entryIndex) {
    assert isLeaf();

    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    // skip key
    entryPosition += getObjectSizeInDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);
    return deserializeFromDirectMemory(IntSerializer.INSTANCE, entryPosition);
  }

  byte[] getRawValue(final int entryIndex) {
    assert isLeaf();

    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    // skip key
    entryPosition += getObjectSizeInDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);

    final int intSize = getObjectSizeInDirectMemory(IntSerializer.INSTANCE, entryPosition);
    return getBinaryValue(entryPosition, intSize);
  }

  public void addAll(final List<byte[]> rawEntries) {
    final int currentSize = size();
    for (int i = 0; i < rawEntries.size(); i++) {
      appendRawEntry(i + currentSize, rawEntries.get(i));
    }

    setIntValue(SIZE_OFFSET, rawEntries.size() + currentSize);
  }

  private void appendRawEntry(final int index, final byte[] rawEntry) {
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;

    freePointer -= rawEntry.length;

    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);

    setBinaryValue(freePointer, rawEntry);
  }

  public void shrink(final int newSize) {
    final List<byte[]> rawEntries = new ArrayList<>(newSize);

    for (int i = 0; i < newSize; i++) {
      rawEntries.add(getRawEntry(i));
    }

    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);

    for (int i = 0; i < newSize; i++) {
      appendRawEntry(i, rawEntries.get(i));
    }

    setIntValue(SIZE_OFFSET, newSize);
  }

  public byte[] getRawEntry(final int entryIndex) {
    int entryPosition =
        getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int startEntryPosition = entryPosition;

    if (isLeaf()) {
      final int keySize = getObjectSizeInDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);
      final int valueSize =
          getObjectSizeInDirectMemory(IntSerializer.INSTANCE, startEntryPosition + keySize);
      return getBinaryValue(startEntryPosition, keySize + valueSize);
    } else {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;

      final int keySize = getObjectSizeInDirectMemory(EdgeKeySerializer.INSTANCE, entryPosition);

      return getBinaryValue(startEntryPosition, keySize + 2 * OIntegerSerializer.INT_SIZE);
    }
  }

  public boolean addLeafEntry(
      final int index, final byte[] serializedKey, final byte[] serializedValue) {
    final int entrySize = serializedKey.length + serializedValue.length;

    assert isLeaf();
    final int size = getIntValue(SIZE_OFFSET);

    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;

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

    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    setBinaryValue(freePointer, serializedKey);
    setBinaryValue(freePointer + serializedKey.length, serializedValue);

    return true;
  }

  public boolean addNonLeafEntry(
      final int index,
      final int leftChild,
      final int rightChild,
      final byte[] key,
      final boolean updateNeighbors) {
    assert !isLeaf();

    final int keySize = key.length;

    final int entrySize = keySize + 2 * OIntegerSerializer.INT_SIZE;

    int size = size();
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;

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

    assert freePointer <= ODurablePage.MAX_PAGE_SIZE_BYTES;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    freePointer += setIntValue(freePointer, leftChild);
    freePointer += setIntValue(freePointer, rightChild);

    setBinaryValue(freePointer, key);

    size++;

    if (updateNeighbors && size > 1) {
      if (index < size - 1) {
        final int nextEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE);
        setIntValue(nextEntryPosition, rightChild);
      }

      if (index > 0) {
        final int prevEntryPosition =
            getIntValue(POSITIONS_ARRAY_OFFSET + (index - 1) * OIntegerSerializer.INT_SIZE);
        setIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE, leftChild);
      }
    }

    return true;
  }

  public void setLeftSibling(final long pageIndex) {
    setLongValue(LEFT_SIBLING_OFFSET, pageIndex);
  }

  public long getLeftSibling() {
    return getLongValue(LEFT_SIBLING_OFFSET);
  }

  public void setRightSibling(final long pageIndex) {
    setLongValue(RIGHT_SIBLING_OFFSET, pageIndex);
  }

  public long getRightSibling() {
    return getLongValue(RIGHT_SIBLING_OFFSET);
  }

  public void updateValue(final int index, final byte[] value, final int keySize) {
    final int entryPosition =
        getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) + keySize;

    final int valueSize = getObjectSizeInDirectMemory(IntSerializer.INSTANCE, entryPosition);
    if (valueSize == value.length) {
      setBinaryValue(entryPosition, value);
    } else {
      final byte[] rawKey = getRawKey(index);

      removeLeafEntry(index, keySize, valueSize);
      addLeafEntry(index, rawKey, value);
    }
  }
}
