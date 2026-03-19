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
package com.arcadedb.graph;

import com.arcadedb.database.RID;

import java.util.NoSuchElementException;

/**
 * Iterator that returns connected vertex RIDs from edge segments without loading vertex records.
 * This is the fastest way to enumerate neighbor RIDs when no edge type filtering is needed.
 */
public class RIDIterator extends ResettableIteratorBase<RID> {

  public RIDIterator(final EdgeSegment current) {
    super(null, current);
  }

  @Override
  public boolean hasNext() {
    if (currentContainer == null)
      return false;

    while (true) {
      if (currentPosition.get() < currentContainer.getUsed())
        return true;

      currentContainer = currentContainer.getPrevious();
      if (currentContainer == null)
        break;

      currentPosition.set(MutableEdgeSegment.CONTENT_START_POSITION);
    }

    return false;
  }

  @Override
  public RID next() {
    if (!hasNext())
      throw new NoSuchElementException();

    currentContainer.getRID(currentPosition); // skip edge RID
    final RID rid = currentContainer.getRID(currentPosition);

    ++browsed;
    return rid;
  }
}
