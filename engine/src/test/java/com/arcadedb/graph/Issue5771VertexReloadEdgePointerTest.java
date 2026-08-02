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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5771: {@code reload()} on an {@link ImmutableVertex} does not refresh the edge pointers,
 * so the vertex keeps reporting its pre-reload edges after a reload that should pick up another transaction's changes.
 * <p>
 * The fix introduced a {@code positionAtProperties()} hook in {@link com.arcadedb.database.ImmutableDocument}
 * that both {@code reload()} and {@code checkForLazyLoading()} call after a fresh buffer is available.
 * {@link ImmutableVertex} overrides it to call {@code parseEdgePointers()}, {@link ImmutableEdge} overrides it to
 * re-parse the out/in RIDs.
 */
public class Issue5771VertexReloadEdgePointerTest extends BaseGraphTest {

  @Test
  void vertexReloadRefreshesEdgePointers() {
    // 1. Materialise the vertex BEFORE any edge exists (so edge pointers are null)
    database.begin();
    final ImmutableVertex v1 = (ImmutableVertex) database.lookupByRID(root, false);
    // Force materialisation by calling any accessor that triggers checkForLazyLoading + parseEdgePointers
    assertThat(v1.getOutEdgesHeadChunk()).isNull();
    assertThat(v1.getInEdgesHeadChunk()).isNull();
    database.commit();

    // 2. In a new transaction, add an edge to the vertex
    database.begin();
    final Vertex v2 = (Vertex) database.lookupByRID(root, true);
    final Vertex other = database.newVertex(VERTEX2_TYPE_NAME);
    other.set("name", "other");
    other.save();
    v2.newEdge(EDGE1_TYPE_NAME, other, "name", "E1");
    database.commit();

    // 3. Now reload the ORIGINAL vertex reference (v1, not v2) in a fresh transaction
    database.begin();
    v1.reload();

    // 4. After reload, the edge pointers should reflect the new edge
    final RID outEdges = v1.getOutEdgesHeadChunk();
    assertThat(outEdges).as("reload() should refresh outEdges to pick up the new edge").isNotNull();

    // 5. Verify countEdges also reflects the new state
    assertThat(v1.countEdges(Vertex.DIRECTION.OUT, EDGE1_TYPE_NAME)).isEqualTo(1);
    database.commit();
  }

  @Test
  void vertexReloadRefreshesInEdgesOnTarget() {
    // 1. Create two vertices, materialise them both before any edges
    database.begin();
    final ImmutableVertex v1 = (ImmutableVertex) database.lookupByRID(root, false);
    final Vertex target = database.newVertex(VERTEX2_TYPE_NAME);
    target.set("name", "target");
    target.save();
    final ImmutableVertex immutableTarget = (ImmutableVertex) database.lookupByRID(target.getIdentity(), false);
    assertThat(immutableTarget.getInEdgesHeadChunk()).isNull();
    database.commit();

    // 2. Add an edge from v1 to target
    database.begin();
    final Vertex v1Mut = (Vertex) database.lookupByRID(root, true);
    v1Mut.newEdge(EDGE1_TYPE_NAME, target, "name", "E1");
    database.commit();

    // 3. Reload the target vertex
    database.begin();
    immutableTarget.reload();

    // 4. Verify inEdges is now populated
    assertThat(immutableTarget.getInEdgesHeadChunk())
        .as("reload() on the target vertex should refresh inEdges").isNotNull();
    assertThat(immutableTarget.countEdges(Vertex.DIRECTION.IN, EDGE1_TYPE_NAME)).isEqualTo(1);
    database.commit();
  }

  @Test
  void vertexReloadWithNoEdgesStillWorks() {
    // 1. Materialise a vertex that will never get edges
    database.begin();
    final Vertex isolated = database.newVertex(VERTEX2_TYPE_NAME);
    isolated.set("name", "isolated");
    isolated.save();
    final ImmutableVertex iso = (ImmutableVertex) database.lookupByRID(isolated.getIdentity(), false);
    assertThat(iso.getOutEdgesHeadChunk()).isNull();
    assertThat(iso.getInEdgesHeadChunk()).isNull();
    database.commit();

    // 2. Reload in a fresh transaction — should not throw
    database.begin();
    iso.reload();
    // Edge pointers should still be null (no edges were added)
    assertThat(iso.getOutEdgesHeadChunk()).isNull();
    assertThat(iso.getInEdgesHeadChunk()).isNull();
    database.commit();
  }
}