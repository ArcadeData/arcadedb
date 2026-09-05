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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Vertex;

/**
 * Re-resolves a bound vertex to its latest state before its edges are enumerated.
 * <p>
 * Every MERGE-shaped write has to ask "does this edge already exist?" and answer it by walking the start
 * vertex's outgoing edges. The vertex instance a row carries was loaded when the row was produced, which is
 * before any of the writes the rows behind it have applied - and appending the first edge in a direction is
 * the one write that rewrites the vertex record's edge-list head pointer. Walking the row's own instance
 * therefore misses an edge a previous row already created, and the "merge" creates a second one.
 * <p>
 * {@code database.lookupByRID} re-reads the RID - the current transaction's cache first, the page store
 * otherwise - so it observes the latest state regardless of which transaction wrote it. Extracted from
 * {@code MergeStep}, where it was written for issue #6461, once {@code merge.relationship} turned out to
 * have the identical hole (issue #7174).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CypherVertexReload {
  private CypherVertexReload() {
  }

  /**
   * @return the vertex re-read from {@code database}, or {@code vertex} unchanged when it has no identity yet
   * (not persisted) or was concurrently deleted - both left for the ordinary handling downstream to react to
   */
  public static Vertex latest(final Database database, final Vertex vertex) {
    if (vertex == null)
      return null;
    final RID rid = vertex.getIdentity();
    if (rid == null)
      return vertex;
    try {
      final Record fresh = database.lookupByRID(rid, true);
      return fresh instanceof Vertex reloaded ? reloaded : vertex;
    } catch (final RecordNotFoundException e) {
      return vertex;
    }
  }
}
