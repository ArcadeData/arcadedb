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

import java.util.Iterator;

/**
 * Created by luigidellaquila on 25/10/16.
 */
public class RidSetIterator implements Iterator<RID> {

  final CommandContext ctx;

  private final RidSet set;
  int  currentCluster = -1;
  long currentId      = -1;

  RidSetIterator(CommandContext ctx, RidSet set) {
    this.set = set;
    this.ctx = ctx;
    fetchNext();
  }

  @Override
  public boolean hasNext() {
    return currentCluster >= 0;
  }

  @Override
  public RID next() {
    if (!hasNext()) {
      throw new IllegalStateException();
    }
    RID result = new RID(ctx.getDatabase(), currentCluster, currentId);
    currentId++;
    fetchNext();
    return result;
  }

  private void fetchNext() {
    if (currentCluster < 0) {
      currentCluster = 0;
      currentId = 0;
    }

    long currentArrayPos = currentId / 63;
    long currentBit = currentId % 63;
    int block = (int) (currentArrayPos / set.maxArraySize);
    int blockPositionByteInt = (int) (currentArrayPos % set.maxArraySize);

    while (currentCluster < set.content.length) {
      while (set.content[currentCluster] != null && block < set.content[currentCluster].length) {
        while (set.content[currentCluster][block] != null && blockPositionByteInt < set.content[currentCluster][block].length) {
          if (currentBit == 0 && set.content[currentCluster][block][blockPositionByteInt] == 0L) {
            blockPositionByteInt++;
            currentArrayPos++;
            continue;
          }
          if (set.contains(new RID(ctx.getDatabase(), currentCluster, currentArrayPos * 63 + currentBit))) {
            currentId = currentArrayPos * 63 + currentBit;
            return;
          } else {
            currentBit++;
            if (currentBit > 63) {
              currentBit = 0;
              blockPositionByteInt++;
              currentArrayPos++;
            }
          }
        }
        if (set.content[currentCluster][block] == null && set.content[currentCluster].length >= block) {
          currentArrayPos += set.maxArraySize;
        }
        block++;
        blockPositionByteInt = 0;
        currentBit = 0;
      }
      block = 0;
      currentBit = 0;
      currentArrayPos = 0;
      blockPositionByteInt = 0;
      currentCluster++;
    }

    currentCluster = -1;
  }

}
