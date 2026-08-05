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
 * Regression test for issue #5771: {@code reload()} on an {@link ImmutableVertex} kept the edge pointers it had parsed
 * out of the previous buffer, so a vertex reloaded to pick up another transaction's changes went on reporting its
 * pre-reload edges.
 * <p>
 * The pointers are re-derived from the buffer by {@code parseRecordPrefix()}, which
 * {@link com.arcadedb.database.BaseDocument#reload()} calls on the buffer it has just installed. The test holds the
 * immutable instance across the transaction that adds the edge, which is the only shape that meets the defect: an edge
 * added through a freshly looked-up vertex never sees a stale parse.
 */
public class Issue5771VertexReloadEdgePointerTest extends BaseGraphTest {

  @Test
  void reloadRefreshesOutEdgesHeadChunk() {
    final RID[] rids = createTwoUnconnectedVertices();
    final RID sourceRID = rids[0];
    final RID targetRID = rids[1];

    // MATERIALISE THE IMMUTABLE VERTEX WHILE IT STILL HAS NO EDGES: THIS IS WHAT PARSES THE PREFIX THE FIX HAS TO REDO
    database.begin();
    final ImmutableVertex source = (ImmutableVertex) database.lookupByRID(sourceRID, true);
    assertThat(source.getOutEdgesHeadChunk()).isNull();
    database.commit();

    database.transaction(() -> database.lookupByRID(sourceRID, true).asVertex()
        .newEdge(EDGE1_TYPE_NAME, database.lookupByRID(targetRID, true), "name", "reloaded"));

    database.begin();
    try {
      source.reload();

      assertThat(source.getOutEdgesHeadChunk()).as("reload() must re-read the out-edge head chunk from the fresh buffer")
          .isEqualTo(((VertexInternal) database.lookupByRID(sourceRID, true)).getOutEdgesHeadChunk());
      assertThat(source.countEdges(Vertex.DIRECTION.OUT, EDGE1_TYPE_NAME)).isEqualTo(1L);
    } finally {
      database.commit();
    }
  }

  @Test
  void reloadRefreshesInEdgesHeadChunk() {
    final RID[] rids = createTwoUnconnectedVertices();
    final RID sourceRID = rids[0];
    final RID targetRID = rids[1];

    database.begin();
    final ImmutableVertex target = (ImmutableVertex) database.lookupByRID(targetRID, true);
    assertThat(target.getInEdgesHeadChunk()).isNull();
    database.commit();

    database.transaction(() -> database.lookupByRID(sourceRID, true).asVertex()
        .newEdge(EDGE1_TYPE_NAME, database.lookupByRID(targetRID, true), "name", "reloaded"));

    database.begin();
    try {
      target.reload();

      assertThat(target.getInEdgesHeadChunk()).as("reload() must re-read the in-edge head chunk from the fresh buffer")
          .isEqualTo(((VertexInternal) database.lookupByRID(targetRID, true)).getInEdgesHeadChunk());
      assertThat(target.countEdges(Vertex.DIRECTION.IN, EDGE1_TYPE_NAME)).isEqualTo(1L);
    } finally {
      database.commit();
    }
  }

  /**
   * The properties have to survive the re-parse: the prefix is read again to find where they start, and getting that
   * wrong reads the properties from the middle of the edge pointers instead.
   */
  @Test
  void reloadKeepsReadingTheVertexProperties() {
    final RID[] rids = createTwoUnconnectedVertices();
    final RID sourceRID = rids[0];
    final RID targetRID = rids[1];

    database.begin();
    final ImmutableVertex source = (ImmutableVertex) database.lookupByRID(sourceRID, true);
    assertThat(source.getString("name")).isEqualTo("source");
    database.commit();

    database.transaction(() -> {
      database.lookupByRID(sourceRID, true).asVertex().newEdge(EDGE1_TYPE_NAME, database.lookupByRID(targetRID, true));
      database.lookupByRID(sourceRID, true).asVertex().modify().set("name", "renamed").save();
    });

    database.begin();
    try {
      source.reload();

      assertThat(source.getString("name")).isEqualTo("renamed");
      assertThat(source.getPropertyNames()).contains("name");
      assertThat(source.getOutEdgesHeadChunk()).isNotNull();
    } finally {
      database.commit();
    }
  }

  /**
   * A vertex that never gains an edge must come back from a reload with both pointers still null, and must not blow up
   * on the re-parse of a prefix whose RIDs are the {@code -1} markers.
   */
  @Test
  void reloadOfAVertexWithoutEdgesKeepsBothPointersNull() {
    final RID[] rids = createTwoUnconnectedVertices();
    final RID isolatedRID = rids[0];

    database.begin();
    final ImmutableVertex isolated = (ImmutableVertex) database.lookupByRID(isolatedRID, true);
    assertThat(isolated.getOutEdgesHeadChunk()).isNull();
    assertThat(isolated.getInEdgesHeadChunk()).isNull();
    database.commit();

    database.begin();
    try {
      isolated.reload();

      assertThat(isolated.getOutEdgesHeadChunk()).isNull();
      assertThat(isolated.getInEdgesHeadChunk()).isNull();
      assertThat(isolated.getString("name")).isEqualTo("source");
    } finally {
      database.commit();
    }
  }

  /**
   * The edge shape of the same hook: {@link ImmutableEdge} re-derives its out/in RIDs and its properties offset from
   * the reloaded buffer rather than from the fields the previous buffer left behind.
   */
  @Test
  void reloadOfAnEdgeKeepsItsEndpointsAndSeesTheNewProperty() {
    final RID[] rids = createTwoUnconnectedVertices();
    final RID sourceRID = rids[0];
    final RID targetRID = rids[1];

    final RID[] edgeRID = new RID[1];
    database.transaction(() -> edgeRID[0] = database.lookupByRID(sourceRID, true).asVertex()
        .newEdge(EDGE1_TYPE_NAME, database.lookupByRID(targetRID, true), "name", "before").getIdentity());

    database.begin();
    final ImmutableEdge edge = (ImmutableEdge) database.lookupByRID(edgeRID[0], true);
    assertThat(edge.getString("name")).isEqualTo("before");
    database.commit();

    database.transaction(() -> database.lookupByRID(edgeRID[0], true).asEdge().modify().set("name", "after").save());

    database.begin();
    try {
      edge.reload();

      assertThat(edge.getOut()).isEqualTo(sourceRID);
      assertThat(edge.getIn()).isEqualTo(targetRID);
      assertThat(edge.getString("name")).isEqualTo("after");
    } finally {
      database.commit();
    }
  }

  /**
   * Creates a source and a target vertex with no edge between them. {@code BaseGraphTest} wires its own {@code root}
   * vertex up with edges in the fixture, so a test about "the pointers were null when the vertex was parsed" needs
   * vertices of its own.
   *
   * @return the source RID at index 0 and the target RID at index 1
   */
  private RID[] createTwoUnconnectedVertices() {
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      final MutableVertex source = database.newVertex(VERTEX1_TYPE_NAME);
      source.set("name", "source");
      source.save();

      final MutableVertex target = database.newVertex(VERTEX2_TYPE_NAME);
      target.set("name", "target");
      target.save();

      rids[0] = source.getIdentity();
      rids[1] = target.getIdentity();
    });
    return rids;
  }
}
