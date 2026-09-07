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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.LightEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression coverage for issue #7148: moving a vertex must not reverse incoming edges.
 */
class Issue7148MoveVertexEdgeDirectionTest extends TestHelper {

  @Test
  void movingTheIncomingEndpointPreservesDirectionAndProperties() {
    final RID[] vertices = new RID[3];
    database.transaction(() -> {
      database.getSchema().createVertexType("Issue7148Source");
      database.getSchema().createVertexType("Issue7148OldTarget");
      database.getSchema().createVertexType("Issue7148NewTarget");
      database.getSchema().createEdgeType("Issue7148Regular");
      database.getSchema().buildEdgeType().withName("Issue7148Light").withLightweight(true).create();

      final MutableVertex regularSource = database.newVertex("Issue7148Source");
      regularSource.set("uid", "regular-source");
      regularSource.save();
      vertices[0] = regularSource.getIdentity();

      final MutableVertex lightSource = database.newVertex("Issue7148Source");
      lightSource.set("uid", "light-source");
      lightSource.save();
      vertices[1] = lightSource.getIdentity();

      final MutableVertex target = database.newVertex("Issue7148OldTarget");
      target.set("uid", "target");
      target.save();
      vertices[2] = target.getIdentity();

      regularSource.newEdge("Issue7148Regular", target, "tag", "preserved").save();
      lightSource.newEdge("Issue7148Light", target);
    });

    final RID moved = moveVertex(vertices[2], "Issue7148NewTarget");

    database.transaction(() -> {
      final Vertex regularSource = vertices[0].asVertex(true);
      final Edge regular = regularSource.getEdges(Vertex.DIRECTION.OUT, "Issue7148Regular").getFirstOrNull();
      assertThat(regular).isNotNull();
      assertThat(regular.getOut()).isEqualTo(vertices[0]);
      assertThat(regular.getIn()).isEqualTo(moved);
      assertThat(regular.getString("tag")).isEqualTo("preserved");
      assertThat(regularSource.getConnectedVertexRIDs(Vertex.DIRECTION.OUT, "Issue7148Regular"))
          .containsExactly(moved);

      final Vertex lightSource = vertices[1].asVertex(true);
      final Edge light = lightSource.getEdges(Vertex.DIRECTION.OUT, "Issue7148Light").getFirstOrNull();
      assertThat(light).isInstanceOf(LightEdge.class);
      assertThat(light.getOut()).isEqualTo(vertices[1]);
      assertThat(light.getIn()).isEqualTo(moved);
      assertThat(lightSource.getConnectedVertexRIDs(Vertex.DIRECTION.OUT, "Issue7148Light"))
          .containsExactly(moved);

      final Vertex movedVertex = moved.asVertex(true);
      assertThat(movedVertex.getConnectedVertexRIDs(Vertex.DIRECTION.IN, "Issue7148Regular"))
          .containsExactly(vertices[0]);
      assertThat(movedVertex.getConnectedVertexRIDs(Vertex.DIRECTION.IN, "Issue7148Light"))
          .containsExactly(vertices[1]);
      assertThat(movedVertex.countEdges(Vertex.DIRECTION.OUT, "Issue7148Regular", "Issue7148Light")).isZero();
    });
  }

  @Test
  void movingASelfLoopRecreatesEachEdgeOnceAgainstTheNewRid() {
    final RID[] original = new RID[1];
    database.transaction(() -> {
      database.getSchema().createVertexType("Issue7148LoopOld");
      database.getSchema().createVertexType("Issue7148LoopNew");
      database.getSchema().createEdgeType("Issue7148RegularLoop");
      database.getSchema().buildEdgeType().withName("Issue7148LightLoop").withLightweight(true).create();

      final MutableVertex vertex = database.newVertex("Issue7148LoopOld");
      vertex.save();
      original[0] = vertex.getIdentity();
      vertex.newEdge("Issue7148RegularLoop", vertex, "tag", "loop").save();
      vertex.newEdge("Issue7148LightLoop", vertex);
    });

    final RID moved = moveVertex(original[0], "Issue7148LoopNew");

    database.transaction(() -> {
      final Vertex movedVertex = moved.asVertex(true);

      assertThat(movedVertex.countEdges(Vertex.DIRECTION.OUT, "Issue7148RegularLoop")).isEqualTo(1);
      assertThat(movedVertex.countEdges(Vertex.DIRECTION.IN, "Issue7148RegularLoop")).isEqualTo(1);
      final Edge regular = movedVertex.getEdges(Vertex.DIRECTION.OUT, "Issue7148RegularLoop").getFirstOrNull();
      assertThat(regular).isNotNull();
      assertThat(regular.getOut()).isEqualTo(moved);
      assertThat(regular.getIn()).isEqualTo(moved);
      assertThat(regular.getString("tag")).isEqualTo("loop");

      assertThat(movedVertex.countEdges(Vertex.DIRECTION.OUT, "Issue7148LightLoop")).isEqualTo(1);
      assertThat(movedVertex.countEdges(Vertex.DIRECTION.IN, "Issue7148LightLoop")).isEqualTo(1);
      final Edge light = movedVertex.getEdges(Vertex.DIRECTION.OUT, "Issue7148LightLoop").getFirstOrNull();
      assertThat(light).isInstanceOf(LightEdge.class);
      assertThat(light.getOut()).isEqualTo(moved);
      assertThat(light.getIn()).isEqualTo(moved);
    });
  }

  /**
   * Every incoming edge of a super-node is recreated from the same source vertex, whose outgoing edge list is
   * rewritten once per edge. Reusing one cached source instance across the whole loop must not lose any of those
   * appends, and the moved vertex must end up with all of them on its IN side and none on its OUT side.
   */
  @Test
  void manyIncomingEdgesFromASingleSourceAreAllPreserved() {
    final int edges = 25;
    final RID[] vertices = new RID[2];
    database.transaction(() -> {
      database.getSchema().createVertexType("Issue7148FanSource");
      database.getSchema().createVertexType("Issue7148FanOld");
      database.getSchema().createVertexType("Issue7148FanNew");
      database.getSchema().createEdgeType("Issue7148Fan");

      final MutableVertex source = database.newVertex("Issue7148FanSource");
      source.save();
      vertices[0] = source.getIdentity();

      final MutableVertex target = database.newVertex("Issue7148FanOld");
      target.save();
      vertices[1] = target.getIdentity();

      for (int i = 0; i < edges; i++)
        source.newEdge("Issue7148Fan", target, "seq", i).save();
    });

    final RID moved = moveVertex(vertices[1], "Issue7148FanNew");

    database.transaction(() -> {
      final Vertex source = vertices[0].asVertex(true);
      assertThat(source.countEdges(Vertex.DIRECTION.OUT, "Issue7148Fan")).isEqualTo(edges);

      final Vertex movedVertex = moved.asVertex(true);
      assertThat(movedVertex.countEdges(Vertex.DIRECTION.IN, "Issue7148Fan")).isEqualTo(edges);
      assertThat(movedVertex.countEdges(Vertex.DIRECTION.OUT, "Issue7148Fan")).isZero();

      final Set<Integer> sequences = new HashSet<>();
      for (final Edge edge : source.getEdges(Vertex.DIRECTION.OUT, "Issue7148Fan")) {
        assertThat(edge.getOut()).isEqualTo(vertices[0]);
        assertThat(edge.getIn()).isEqualTo(moved);
        sequences.add(edge.getInteger("seq"));
      }
      assertThat(sequences).hasSize(edges);
    });
  }

  /**
   * The same neighbour on both sides: the moved vertex holds one edge towards it and one edge from it. The
   * neighbour appears in the outgoing and the incoming collection with a different edge each time, so neither may
   * be mistaken for a self-loop and skipped.
   */
  @Test
  void anEdgePairWithTheSameNeighbourKeepsBothDirections() {
    final RID[] vertices = new RID[2];
    database.transaction(() -> {
      database.getSchema().createVertexType("Issue7148PairPeer");
      database.getSchema().createVertexType("Issue7148PairOld");
      database.getSchema().createVertexType("Issue7148PairNew");
      database.getSchema().createEdgeType("Issue7148Pair");

      final MutableVertex peer = database.newVertex("Issue7148PairPeer");
      peer.save();
      vertices[0] = peer.getIdentity();

      final MutableVertex target = database.newVertex("Issue7148PairOld");
      target.save();
      vertices[1] = target.getIdentity();

      peer.newEdge("Issue7148Pair", target, "dir", "incoming").save();
      target.newEdge("Issue7148Pair", peer, "dir", "outgoing").save();
    });

    final RID moved = moveVertex(vertices[1], "Issue7148PairNew");

    database.transaction(() -> {
      final Vertex movedVertex = moved.asVertex(true);
      assertThat(movedVertex.countEdges(Vertex.DIRECTION.IN, "Issue7148Pair")).isEqualTo(1);
      assertThat(movedVertex.countEdges(Vertex.DIRECTION.OUT, "Issue7148Pair")).isEqualTo(1);

      final Edge incoming = movedVertex.getEdges(Vertex.DIRECTION.IN, "Issue7148Pair").getFirstOrNull();
      assertThat(incoming).isNotNull();
      assertThat(incoming.getOut()).isEqualTo(vertices[0]);
      assertThat(incoming.getIn()).isEqualTo(moved);
      assertThat(incoming.getString("dir")).isEqualTo("incoming");

      final Edge outgoing = movedVertex.getEdges(Vertex.DIRECTION.OUT, "Issue7148Pair").getFirstOrNull();
      assertThat(outgoing).isNotNull();
      assertThat(outgoing.getOut()).isEqualTo(moved);
      assertThat(outgoing.getIn()).isEqualTo(vertices[0]);
      assertThat(outgoing.getString("dir")).isEqualTo("outgoing");

      final Vertex peer = vertices[0].asVertex(true);
      assertThat(peer.countEdges(Vertex.DIRECTION.OUT, "Issue7148Pair")).isEqualTo(1);
      assertThat(peer.countEdges(Vertex.DIRECTION.IN, "Issue7148Pair")).isEqualTo(1);
    });
  }

  /**
   * {@code MOVE VERTEX ... TO BUCKET:} goes through the very same {@code moveTo()} as the {@code TO TYPE:} form, so
   * it reversed incoming edges in exactly the same way and needs its own coverage.
   */
  @Test
  void movingToAnotherBucketPreservesIncomingEdgeDirection() {
    final RID[] vertices = new RID[2];
    database.transaction(() -> {
      database.getSchema().createVertexType("Issue7148BucketSource");
      database.getSchema().createVertexType("Issue7148BucketTarget")
          .addBucket(database.getSchema().createBucket("Issue7148Bucket_extra"));
      database.getSchema().createEdgeType("Issue7148BucketEdge");

      final MutableVertex source = database.newVertex("Issue7148BucketSource");
      source.save();
      vertices[0] = source.getIdentity();

      final MutableVertex target = database.newVertex("Issue7148BucketTarget");
      target.save();
      vertices[1] = target.getIdentity();

      source.newEdge("Issue7148BucketEdge", target, "tag", "preserved").save();
    });

    database.setAutoTransaction(true);
    final RID moved;
    try (final ResultSet result = database.command("sql",
        "MOVE VERTEX " + vertices[1] + " TO BUCKET:Issue7148Bucket_extra")) {
      assertThat(result.hasNext()).isTrue();
      moved = result.next().getIdentity().orElseThrow();
      assertThat(result.hasNext()).isFalse();
    }
    assertThat(moved.getBucketId())
        .isEqualTo(database.getSchema().getBucketByName("Issue7148Bucket_extra").getFileId());

    database.transaction(() -> {
      final Vertex source = vertices[0].asVertex(true);
      final Edge edge = source.getEdges(Vertex.DIRECTION.OUT, "Issue7148BucketEdge").getFirstOrNull();
      assertThat(edge).isNotNull();
      assertThat(edge.getOut()).isEqualTo(vertices[0]);
      assertThat(edge.getIn()).isEqualTo(moved);
      assertThat(edge.getString("tag")).isEqualTo("preserved");

      final Vertex movedVertex = moved.asVertex(true);
      assertThat(movedVertex.countEdges(Vertex.DIRECTION.IN, "Issue7148BucketEdge")).isEqualTo(1);
      assertThat(movedVertex.countEdges(Vertex.DIRECTION.OUT, "Issue7148BucketEdge")).isZero();
    });
  }

  private RID moveVertex(final RID source, final String targetType) {
    database.setAutoTransaction(true);
    try (final ResultSet result = database.command("sql", "MOVE VERTEX " + source + " TO TYPE:" + targetType)) {
      assertThat(result.hasNext()).isTrue();
      final RID moved = result.next().getIdentity().orElseThrow();
      assertThat(result.hasNext()).isFalse();
      return moved;
    }
  }
}
