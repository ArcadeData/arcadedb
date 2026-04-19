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

import java.util.*;

/**
 * Bitmap-based {@link Set} of RIDs. Stores presence as a bit per position per bucket ({@code content[bucket][block][word]}) so memory cost is proportional to
 * the MAX offset seen per bucket, not the element count. Best choice when RIDs are dense and contiguous (e.g. type-scan results); falls off fast when RIDs
 * are sparse and scattered across a wide offset range, in which case use {@link com.arcadedb.utility.RidHashSet} instead.
 * <p>
 * Iteration produces freshly allocated {@link RID} instances because the structure itself does not keep them.
 *
 * @author Luigi Dell'Aquila
 */
public class RidSet implements Set<RID> {

  CommandContext context;

  protected static final int INITIAL_BLOCK_SIZE = 4096;

  /*
   * Layout: content[bucketId][block][wordIndex], each long packs 64 consecutive bit positions.
   * position -> wordIndex = position >>> 6, bitInWord = position & 63, block = wordIndex / maxArraySize.
   */
  protected long[][][] content = new long[8][][];

  long size = 0;

  protected final int maxArraySize;

  /**
   * instantiates a RidSet with a bucket size of Integer.MAX_VALUE / 10
   */
  public RidSet() {
    this(Integer.MAX_VALUE / 10);
  }

  /**
   * instantiates a RidSet with a bucket size of Integer.MAX_VALUE / 10 and a CommandContext
   *
   * @param context the command context for iterator support
   */
  public RidSet(final CommandContext context) {
    this(context, Integer.MAX_VALUE / 10);
  }

  /**
   * @param bucketSize
   */
  public RidSet(final int bucketSize) {
    this.maxArraySize = bucketSize;
  }

  /**
   * @param context    the command context for iterator support
   * @param bucketSize the maximum bucket size
   */
  public RidSet(final CommandContext context, final int bucketSize) {
    this.context = context;
    this.maxArraySize = bucketSize;
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
  public boolean contains(final Object o) {
    if (!(o instanceof RID rid))
      throw new IllegalArgumentException();
    return contains(rid.getBucketId(), rid.getPosition());
  }

  /**
   * Primitive-pair contains check. Avoids the RID wrapper allocation on hot paths.
   */
  public boolean contains(final int bucket, final long position) {
    if (size == 0L)
      return false;
    if (bucket < 0 || position < 0)
      return false;

    final long wordIndex = position >>> 6;
    final int bitInWord = (int) (position & 63L);
    final int block = (int) (wordIndex / maxArraySize);
    final int wordInBlock = (int) (wordIndex % maxArraySize);

    if (content.length <= bucket)
      return false;

    final long[][] bucketBlocks = content[bucket];
    if (bucketBlocks == null || bucketBlocks.length <= block)
      return false;

    final long[] words = bucketBlocks[block];
    if (words == null || words.length <= wordInBlock)
      return false;

    return (words[wordInBlock] & (1L << bitInWord)) != 0L;
  }

  @Override
  public Iterator<RID> iterator() {
    return new RidSetIterator(context, this);
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
    return add(identifiable.getBucketId(), identifiable.getPosition());
  }

  /**
   * Primitive-pair add. Avoids the RID wrapper allocation on hot paths.
   */
  public boolean add(final int bucket, final long position) {
    if (bucket < 0 || position < 0)
      throw new IllegalArgumentException("negative RID");

    final long wordIndex = position >>> 6;
    final int bitInWord = (int) (position & 63L);
    final int block = (int) (wordIndex / maxArraySize);
    final int wordInBlock = (int) (wordIndex % maxArraySize);

    if (content.length <= bucket) {
      final long[][][] oldContent = content;
      content = new long[bucket + 1][][];
      System.arraycopy(oldContent, 0, content, 0, oldContent.length);
    }

    if (content[bucket] == null)
      content[bucket] = createClusterArray(block, wordInBlock);

    if (content[bucket].length <= block)
      content[bucket] = expandClusterBlocks(content[bucket], block, wordInBlock);

    if (content[bucket][block] == null)
      content[bucket][block] = expandClusterArray(new long[INITIAL_BLOCK_SIZE], wordInBlock);

    if (content[bucket][block].length <= wordInBlock)
      content[bucket][block] = expandClusterArray(content[bucket][block], wordInBlock);

    final long mask = 1L << bitInWord;
    final long original = content[bucket][block][wordInBlock];
    final boolean isNew = (original & mask) == 0L;
    if (isNew) {
      content[bucket][block][wordInBlock] = original | mask;
      size++;
    }
    return isNew;
  }

  private static long[][] expandClusterBlocks(final long[][] longs, final int block, final int wordInBlock) {
    final long[][] result = new long[block + 1][];
    System.arraycopy(longs, 0, result, 0, longs.length);
    result[block] = expandClusterArray(new long[INITIAL_BLOCK_SIZE], wordInBlock);
    return result;
  }

  private static long[][] createClusterArray(final int block, final int wordInBlock) {
    int currentSize = INITIAL_BLOCK_SIZE;
    while (currentSize <= wordInBlock) {
      currentSize *= 2;
      if (currentSize < 0) {
        currentSize = wordInBlock + 1;
        break;
      }
    }

    final long[][] result = new long[block + 1][];
    result[block] = new long[currentSize];
    return result;
  }

  private static long[] expandClusterArray(final long[] original, final int wordInBlock) {
    int currentSize = original.length;
    while (currentSize <= wordInBlock) {
      currentSize *= 2;
      if (currentSize < 0) {
        currentSize = wordInBlock + 1;
        break;
      }
    }

    final long[] result = new long[currentSize];
    System.arraycopy(original, 0, result, 0, original.length);
    return result;
  }

  @Override
  public boolean remove(final Object o) {
    if (!(o instanceof RID rid))
      throw new IllegalArgumentException();
    return remove(rid.getBucketId(), rid.getPosition());
  }

  /**
   * Primitive-pair remove. Avoids the RID wrapper allocation on hot paths.
   */
  public boolean remove(final int bucket, final long position) {
    if (bucket < 0 || position < 0)
      throw new IllegalArgumentException("negative RID");

    final long wordIndex = position >>> 6;
    final int bitInWord = (int) (position & 63L);
    final int block = (int) (wordIndex / maxArraySize);
    final int wordInBlock = (int) (wordIndex % maxArraySize);

    if (content.length <= bucket)
      return false;

    final long[][] bucketBlocks = content[bucket];
    if (bucketBlocks == null || bucketBlocks.length <= block)
      return false;

    final long[] words = bucketBlocks[block];
    if (words == null || words.length <= wordInBlock)
      return false;

    final long mask = 1L << bitInWord;
    final long original = words[wordInBlock];
    final boolean existed = (original & mask) != 0L;
    if (existed) {
      words[wordInBlock] = original & ~mask;
      size--;
    }
    return existed;
  }

  @Override
  public boolean containsAll(final Collection<?> c) {
    for (final Object o : c) {
      if (!contains(o)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(final Collection<? extends RID> c) {
    boolean added = false;
    for (final RID o : c)
      added = add(o) || added;

    return added;
  }

  @Override
  public boolean retainAll(final Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(final Collection<?> c) {
    for (final Object o : c)
      remove(o);

    return true;
  }

  @Override
  public void clear() {
    content = new long[8][][];
    size = 0;
  }
}
