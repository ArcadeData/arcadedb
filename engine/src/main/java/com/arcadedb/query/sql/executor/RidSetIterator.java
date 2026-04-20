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
 * Iterator that walks a {@link RidSet}'s bitmap and materialises a fresh {@link RID} per set bit. Sparse regions are skipped in O(1) per zero-word using
 * {@link Long#numberOfTrailingZeros(long)}, so iteration cost scales with the number of present bits rather than the max offset range.
 *
 * @author Luigi Dell'Aquila
 */
public class RidSetIterator implements Iterator<RID> {

  final CommandContext context;
  private final RidSet set;

  // -1 when exhausted; otherwise currentBucket is the bucket id and currentWordAbs is the ABSOLUTE word index (block * maxArraySize + wordInBlock) whose
  // unconsumed bits are tracked in remainingWord.
  private int  currentBucket  = -1;
  private int  currentBlock   = 0;
  private int  currentWordPos = 0;
  private long remainingWord  = 0L;
  private int  nextBit        = -1;

  RidSetIterator(final CommandContext context, final RidSet set) {
    this.context = context;
    this.set = set;
    advance();
  }

  @Override
  public boolean hasNext() {
    return nextBit >= 0;
  }

  @Override
  public RID next() {
    if (nextBit < 0)
      throw new NoSuchElementException();
    final long absoluteWord = (long) currentBlock * set.maxArraySize + currentWordPos;
    final long position = (absoluteWord << 6) | nextBit;
    final RID result = RID.create(context != null ? context.getDatabase() : null, currentBucket, position);
    // Clear the bit we just returned, then advance to the next set bit.
    remainingWord &= remainingWord - 1;
    advance();
    return result;
  }

  private void advance() {
    // First try to consume more bits in the word we already have.
    if (remainingWord != 0L) {
      nextBit = Long.numberOfTrailingZeros(remainingWord);
      return;
    }

    // Otherwise walk forward through words / blocks / buckets until we find a non-zero word.
    if (currentBucket < 0) {
      currentBucket = 0;
      currentBlock = 0;
      currentWordPos = 0;
    } else {
      // Move past the word we just finished.
      currentWordPos++;
    }

    while (currentBucket < set.content.length) {
      final long[][] bucketBlocks = set.content[currentBucket];
      if (bucketBlocks != null) {
        while (currentBlock < bucketBlocks.length) {
          final long[] words = bucketBlocks[currentBlock];
          if (words != null) {
            while (currentWordPos < words.length) {
              final long w = words[currentWordPos];
              if (w != 0L) {
                remainingWord = w;
                nextBit = Long.numberOfTrailingZeros(w);
                return;
              }
              currentWordPos++;
            }
          }
          currentBlock++;
          currentWordPos = 0;
        }
      }
      currentBucket++;
      currentBlock = 0;
      currentWordPos = 0;
    }

    currentBucket = -1;
    nextBit = -1;
  }
}
