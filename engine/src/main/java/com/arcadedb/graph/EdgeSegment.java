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

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.*;
import java.util.concurrent.atomic.*;

@ExcludeFromJacocoGeneratedReport
public interface EdgeSegment extends Record {
  byte RECORD_TYPE = 3;

  boolean add(RID edgeRID, RID vertexRID);

  boolean containsEdge(RID edgeRID);

  RID getFirstEdgeConnectedToVertex(RID vertexRID, final int[] edgeBucketFilter);

  int removeEdge(RID edgeRID);

  int removeVertex(RID vertexRID);

  EdgeSegment getPrevious();

  void setPrevious(EdgeSegment next);

  Binary getContent();

  int getUsed();

  RID getRID(AtomicInteger currentPosition);

  int getRecordSize();

  long count();

  boolean removeEntry(int currentItemPosition, int nextItemPosition);

  EdgeSegment copy();

  boolean isEmpty();

  /**
   * Returns the cached total count of edges in this linked list.
   * Only valid for first segments; returns -1 for continuation segments.
   *
   * @return total edge count (0 to 4,294,967,295) or -1 if not first segment
   */
  long getTotalCount();

  /**
   * Returns true if this is the first segment in the linked list (has count cache).
   *
   * @return true if first segment, false if continuation segment
   */
  boolean isFirstSegment();

  /**
   * Returns the position where edge entries start in this segment's buffer.
   * Different for v0 (offset 13) vs v1 (offset 18) formats.
   *
   * @return byte offset where entries begin
   */
  int getContentStartOffset();
}
