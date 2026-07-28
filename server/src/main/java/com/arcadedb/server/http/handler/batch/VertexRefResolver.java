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

/**
 * Resolves the {@code @from} / {@code @to} reference of an edge to the RID of the vertex it points at, for the
 * whole duration of a batch request: an edge at the end of a payload may reference a vertex created by its first
 * line, so nothing can be released early and this structure is the memory ceiling of a streaming load
 * (issue #5470).
 * <p>
 * Two shapes, chosen by the {@code refMode} parameter:
 * <ul>
 *   <li>{@link TempIdVertexRefResolver} keys the vertices by an arbitrary temporary id, which costs the id itself
 *       plus a hash table - the general case;</li>
 *   <li>{@link OrdinalVertexRefResolver} keys them by their position in the payload, which is a plain array
 *       lookup and stores no key at all: 12 bytes per vertex against ~87, and no hashing or comparison per
 *       edge.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface VertexRefResolver {

  /**
   * Records the RID assigned to a created vertex.
   *
   * @param tempId  the {@code @id} the payload declared, {@code null} when it carried none
   * @param ordinal 0-based position of the vertex in the payload
   */
  void put(String tempId, int ordinal, RID rid);

  /**
   * Resolves a reference that is not a RID.
   *
   * @throws IllegalArgumentException when the reference does not name a vertex of this payload; the message is
   *                                  echoed to the client, so it must say what the reference should have looked like
   */
  RID get(String ref, int lineNumber);

  /**
   * Checks a vertex declaration before it is created, so a payload that does not match the chosen {@code refMode}
   * fails on its first line instead of on the first edge that cannot be resolved.
   *
   * @throws IllegalArgumentException when the declared {@code @id} cannot be used by this resolver
   */
  void checkVertexId(String tempId, int ordinal, int lineNumber);

  int size();

  boolean isEmpty();

  /**
   * Bytes this resolver holds, for the heap warning of a long load.
   */
  long retainedBytes();

  /**
   * Iterates the mapping to echo it back, keyed the way the payload referenced it.
   */
  void forEach(EntryConsumer consumer);

  @FunctionalInterface
  interface EntryConsumer {
    void accept(String ref, RID rid);
  }
}
