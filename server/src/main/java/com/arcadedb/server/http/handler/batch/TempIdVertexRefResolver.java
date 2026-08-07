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
package com.arcadedb.server.http.handler.batch;

import com.arcadedb.database.RID;
import com.arcadedb.utility.StringRidHashMap;

/**
 * Resolves edges against the arbitrary {@code @id} the payload gave each vertex - the general case, and the only
 * one available when the client cannot number its vertices in stream order.
 * <p>
 * Backed by {@link StringRidHashMap}, so an entry costs the id itself plus a hash slot (~87 bytes for a 50-character
 * id) and no object at all. It is still proportional to the payload: for the largest loads
 * {@link OrdinalVertexRefResolver} removes the key entirely (issue #5470).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TempIdVertexRefResolver implements VertexRefResolver {

  private final StringRidHashMap map;
  private       int              verticesWithoutId;

  public TempIdVertexRefResolver(final int expectedVertices) {
    // The map doubles from its initial capacity, so a hint only saves the copies, never bounds anything.
    this.map = expectedVertices > 0 ? new StringRidHashMap(expectedVertices * 2) : new StringRidHashMap();
  }

  @Override
  public void put(final String tempId, final int ordinal, final RID rid) {
    if (tempId != null)
      map.put(tempId, rid);
  }

  /**
   * The message reports what the load actually knows instead of asserting one cause. "Vertices must appear before
   * edges that reference them" used to be the whole explanation, and on the 17M-vertex load of issue #5618 it sent
   * the user looking for a vertex that was in the file, thousands of lines above the edge - the ordering was never
   * the problem. The two numbers below separate the cases that message conflated: how many ids this payload
   * actually declared, and how many of its vertices declared none and therefore cannot be referenced at all.
   */
  @Override
  public RID get(final String ref, final int lineNumber) {
    final RID rid = map.get(ref);
    if (rid == null) {
      final StringBuilder message = new StringBuilder("Unknown temporary ID '").append(ref).append("' at line ")
          .append(lineNumber).append(": no vertex earlier in this payload declared it as its @id (")
          .append(map.size()).append(" ids mapped so far");
      if (verticesWithoutId > 0)
        message.append(", and ").append(verticesWithoutId)
            .append(" vertices carried no @id at all, so nothing can reference them");
      message.append("). Vertices must appear before the edges that reference them, and each request resolves only "
          + "the ids of its OWN payload: a vertex loaded by an earlier request has to be referenced by RID "
          + "(#bucket:position)");
      throw new IllegalArgumentException(message.toString());
    }
    return rid;
  }

  @Override
  public void checkVertexId(final String tempId, final int ordinal, final int lineNumber) {
    // Any id is acceptable here, including none: a vertex nothing points at does not need one. It is counted
    // though, because a payload that meant to reference it has no other way to find out (issue #5618).
    if (tempId == null)
      verticesWithoutId++;
  }

  @Override
  public int unreferenceableVertices() {
    return verticesWithoutId;
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public long retainedBytes() {
    return map.retainedBytes();
  }

  @Override
  public void forEach(final EntryConsumer consumer) {
    map.forEach(consumer::accept);
  }
}
