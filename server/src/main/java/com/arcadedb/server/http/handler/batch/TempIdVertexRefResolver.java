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

  public TempIdVertexRefResolver(final int expectedVertices) {
    // The map doubles from its initial capacity, so a hint only saves the copies, never bounds anything.
    this.map = expectedVertices > 0 ? new StringRidHashMap(expectedVertices * 2) : new StringRidHashMap();
  }

  @Override
  public void put(final String tempId, final int ordinal, final RID rid) {
    if (tempId != null)
      map.put(tempId, rid);
  }

  @Override
  public RID get(final String ref, final int lineNumber) {
    final RID rid = map.get(ref);
    if (rid == null)
      throw new IllegalArgumentException("Unknown temporary ID '" + ref + "' at line " + lineNumber
          + ". Vertices must appear before edges that reference them");
    return rid;
  }

  @Override
  public void checkVertexId(final String tempId, final int ordinal, final int lineNumber) {
    // Any id is acceptable here, including none: a vertex nothing points at does not need one.
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
