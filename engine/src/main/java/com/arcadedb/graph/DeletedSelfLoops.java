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

import java.util.HashMap;
import java.util.Map;

/**
 * The self-loops the OUTGOING walk of a vertex being deleted actually deleted, which the INCOMING walk then passes over, so each
 * self-loop is deleted, and reported to the delete listeners, once (issues #10369, #10520).
 * <p>
 * Kept as a multiset of the edge pointers as the edge lists store them: a record-backed edge by its RID, unique, and a lightweight
 * edge by its type's bucket with the record-less offset, which every lightweight edge of the type shares, so one is counted per
 * deletion. The incoming walk consumes one occurrence per entry it skips: an entry the outgoing walk never deleted - because its
 * list could not be opened, was cut short before it, or simply did not hold it - finds nothing to consume and is deleted.
 * <p>
 * Allocated per vertex deletion and empty unless the vertex has self-loops, so the common case costs one small object.
 */
final class DeletedSelfLoops {
  private Map<Entry, int[]> entries;

  private record Entry(int bucketId, long position) {
  }

  /** Records the deletion of a self-loop whose edge pointer is {@code edge}. */
  void add(final RID edge) {
    if (entries == null)
      entries = new HashMap<>();
    entries.computeIfAbsent(keyOf(edge), k -> new int[1])[0]++;
  }

  /**
   * @return true, consuming one occurrence, when the self-loop entry whose edge pointer is {@code edge} was deleted by the
   * outgoing walk, false when the incoming walk must delete it
   */
  boolean consume(final RID edge) {
    if (entries == null)
      return false;
    final Entry key = keyOf(edge);
    final int[] count = entries.get(key);
    if (count == null)
      return false;
    if (--count[0] == 0)
      entries.remove(key);
    return true;
  }

  boolean isEmpty() {
    return entries == null || entries.isEmpty();
  }

  private static Entry keyOf(final RID edge) {
    // EVERY RECORD-LESS OFFSET IS THE SAME "NOTHING TO LOAD": A LIGHTWEIGHT EDGE IS NAMED BY ITS TYPE'S BUCKET ONLY
    final long position = edge.getPosition();
    return new Entry(edge.getBucketId(), position < 0 ? LightEdgeRID.NO_RECORD_OFFSET : position);
  }
}
