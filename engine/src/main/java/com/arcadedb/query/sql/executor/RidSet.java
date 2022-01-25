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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.RID;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Special implementation of Java Set&lt;ORID&gt; to efficiently handle memory and performance. It does not store actual RIDs, but
 * it only keeps track that a RID was stored, so the iterator will return new instances.
 *
 * @author Luigi Dell'Aquila
 */
public class RidSet implements Set<RID> {

  CommandContext ctx; //TODO, set this!!!

  protected static final int INITIAL_BLOCK_SIZE = 4096;

  /*
   * bucket / offset / bitmask
   * eg. inserting #12:0 you will have content[12][0][0] = 1
   * eg. inserting #12:(63*maxArraySize + 1) you will have content[12][1][0] = 1
   *
   */
  protected long[][][] content = new long[8][][];

  long size = 0;

  protected final int maxArraySize;

  /**
   * instantiates an ORidSet with a bucket size of Integer.MAX_VALUE / 10
   */
  public RidSet() {
    this(Integer.MAX_VALUE / 10);
  }

  /**
   * @param bucketSize
   */
  public RidSet(int bucketSize) {
    maxArraySize = bucketSize;
  }

  @Override
  public int size() {
    return size <= Integer.MAX_VALUE ? (int) size : Integer.MAX_VALUE;
  }

  @Override
  public boolean isEmpty() {
    return size == 0L;
  }

  @Override
  public boolean contains(Object o) {
    if (size == 0L)
      return false;

    if (!(o instanceof RID))
      throw new IllegalArgumentException();

    final RID identifiable = ((RID) o);

    final int bucket = identifiable.getBucketId();
    final long position = identifiable.getPosition();
    if (bucket < 0 || position < 0)
      return false;

    final long positionByte = (position / 63);
    int positionBit = (int) (position % 63);
    int block = (int) (positionByte / maxArraySize);
    int blockPositionByteInt = (int) (positionByte % maxArraySize);

    if (content.length <= bucket)
      return false;

    if (content[bucket] == null)
      return false;

    if (content[bucket].length <= block)
      return false;

    if (content[bucket][block] == null)
      return false;

    if (content[bucket][block].length <= blockPositionByteInt)
      return false;

    final long currentMask = 1L << positionBit;
    final long existed = content[bucket][block][blockPositionByteInt] & currentMask;

    return existed > 0L;
  }

  @Override
  public Iterator<RID> iterator() {
    return new RidSetIterator(ctx, this);
  }

  @Override
  public Object[] toArray() {
    return new Object[0];
  }

  @Override
  public <T> T[] toArray(final T[] a) {
    return null;
  }

  @Override
  public boolean add(final RID identifiable) {
    if (identifiable == null)
      throw new IllegalArgumentException();

    int bucket = identifiable.getBucketId();
    long position = identifiable.getPosition();
    if (bucket < 0 || position < 0)
      throw new IllegalArgumentException("negative RID");//TODO

    final long positionByte = (position / 63);
    final int positionBit = (int) (position % 63);
    final int block = (int) (positionByte / maxArraySize);
    final int blockPositionByteInt = (int) (positionByte % maxArraySize);

    if (content.length <= bucket) {
      final long[][][] oldContent = content;
      content = new long[bucket + 1][][];
      System.arraycopy(oldContent, 0, content, 0, oldContent.length);
    }

    if (content[bucket] == null)
      content[bucket] = createClusterArray(block, blockPositionByteInt);

    if (content[bucket].length <= block)
      content[bucket] = expandClusterBlocks(content[bucket], block, blockPositionByteInt);

    if (content[bucket][block] == null)
      content[bucket][block] = expandClusterArray(new long[INITIAL_BLOCK_SIZE], blockPositionByteInt);

    if (content[bucket][block].length <= blockPositionByteInt)
      content[bucket][block] = expandClusterArray(content[bucket][block], blockPositionByteInt);

    final long original = content[bucket][block][blockPositionByteInt];
    final long currentMask = 1L << positionBit;
    final long existed = content[bucket][block][blockPositionByteInt] & currentMask;
    content[bucket][block][blockPositionByteInt] = original | currentMask;
    if (existed == 0L)
      size++;

    return existed == 0L;
  }

  private static long[][] expandClusterBlocks(final long[][] longs, final int block, final int blockPositionByteInt) {
    final long[][] result = new long[block + 1][];
    System.arraycopy(longs, 0, result, 0, longs.length);
    result[block] = expandClusterArray(new long[INITIAL_BLOCK_SIZE], blockPositionByteInt);
    return result;
  }

  private static long[][] createClusterArray(final int block, final int positionByteInt) {
    int currentSize = INITIAL_BLOCK_SIZE;
    while (currentSize <= positionByteInt) {
      currentSize *= 2;
      if (currentSize < 0) {
        currentSize = positionByteInt + 1;
        break;
      }
    }

    final long[][] result = new long[block + 1][];
    result[block] = new long[currentSize];
    return result;
  }

  private static long[] expandClusterArray(final long[] original, final int positionByteInt) {
    int currentSize = original.length;
    while (currentSize <= positionByteInt) {
      currentSize *= 2;
      if (currentSize < 0) {
        currentSize = positionByteInt + 1;
        break;
      }
    }

    final long[] result = new long[currentSize];
    System.arraycopy(original, 0, result, 0, original.length);
    return result;
  }

  @Override
  public boolean remove(final Object o) {
    if (!(o instanceof RID))
      throw new IllegalArgumentException();

    final RID identifiable = ((RID) o);

    final int bucket = identifiable.getBucketId();
    final long position = identifiable.getPosition();
    if (bucket < 0 || position < 0) {
      throw new IllegalArgumentException("negative RID");//TODO
    }
    final long positionByte = (position / 63);
    final int positionBit = (int) (position % 63);
    final int block = (int) (positionByte / maxArraySize);
    final int blockPositionByteInt = (int) (positionByte % maxArraySize);

    if (content.length <= bucket)
      return false;

    if (content[bucket] == null)
      return false;

    if (content[bucket].length <= block)
      return false;

    if (content[bucket][block].length <= blockPositionByteInt)
      return false;

    final long original = content[bucket][block][blockPositionByteInt];
    long currentMask = 1L << positionBit;
    final long existed = content[bucket][block][blockPositionByteInt] & currentMask;
    currentMask = ~currentMask;
    content[bucket][block][blockPositionByteInt] = original & currentMask;
    if (existed > 0)
      size--;

    return existed == 0L;
  }

  @Override
  public boolean containsAll(final Collection<?> c) {
    for (Object o : c) {
      if (!contains(o)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(final Collection<? extends RID> c) {
    boolean added = false;
    for (RID o : c)
      added = added && add(o);

    return added;
  }

  @Override
  public boolean retainAll(final Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(final Collection<?> c) {
    for (Object o : c)
      remove(o);

    return true;
  }

  @Override
  public void clear() {
    content = new long[8][][];
    size = 0;
  }
}
