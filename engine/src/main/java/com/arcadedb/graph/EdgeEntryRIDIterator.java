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

import java.util.Iterator;

/**
 * The vertex RIDs of an edge list, as {@link EdgeLinkedList#ridIterator} walks them, with the RID of the edge each entry names
 * available on request.
 * <p>
 * An adjacency entry is the pair (edge, vertex at the other end), both stored inline in the segment, so a walk that only follows
 * the entries needs no record at all - and so does not notice an entry whose edge record is gone. A caller that must tell such an
 * entry from a live one (the path procedures decide reachability on live edges only, issue #10148) asks for the edge of the entry
 * it is about to follow and probes the bucket for it, once per node it reaches rather than once per entry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface EdgeEntryRIDIterator extends Iterator<RID> {
  /**
   * The RID of the edge of the entry the last {@link #next()} returned. Valid until the following {@code hasNext()} or
   * {@code next()}, which may move on to the next entry. A lightweight edge has no record of its own: its RID has a negative
   * position.
   *
   * @throws IllegalStateException when {@code next()} has not returned an entry yet
   */
  RID getEdgeRID();
}
