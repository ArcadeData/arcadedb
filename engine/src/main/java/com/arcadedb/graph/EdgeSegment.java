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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public interface EdgeSegment extends Record {
  byte RECORD_TYPE = 3;

  boolean add(RID edgeRID, RID vertexRID);

  boolean containsEdge(RID edgeRID);

  boolean containsVertex(RID vertexRID, final int[] edgeBucketFilter);

  int removeEdge(RID edgeRID);

  int removeVertex(RID vertexRID);

  EdgeSegment getNext();

  void setNext(EdgeSegment next);

  Binary getContent();

  int getUsed();

  RID getRID(AtomicInteger currentPosition);

  int getRecordSize();

  long count(Set<Integer> fileIds);

  boolean removeEntry(int currentItemPosition, int nextItemPosition);
}
