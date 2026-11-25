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

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12191: the remote client ({@code RemoteVertex}) pages the edges of a vertex in RID order, each page resuming after the
 * last RID read, and has no other way to name a lightweight edge than the RID {@code #<bucket>:-1} every lightweight edge of a
 * type shares. It therefore leans on three facts of the engine, pinned here so that a change of any of them is a red test in
 * the engine and not a silently wrong page in a client:
 * <ul>
 *   <li>a parsed {@code #<bucket>:-1}, which carries no endpoints, sorts before every lightweight edge of the bucket, after
 *   every edge of a lower bucket and before the first record ({@code #<bucket>:0}) of the bucket ({@code RID#compareTo});</li>
 *   <li>the lightweight edges of a bucket in the edge list of one vertex come in the order of the vertex at their other end
 *   (their endpoints are the order, and one of them is always the owner of the list), {@code @in} for the outgoing list and
 *   {@code @out} for the incoming one, ties being twins (same type and endpoints);</li>
 *   <li>{@code @rid} and {@code @in}/{@code @out} compare in {@code WHERE} the way {@code ORDER BY @rid} orders them.</li>
 * </ul>
 * The last test runs the exact query shape of the client from every position of a list that holds lightweight and record-backed
 * edges of two types, in several buckets, with twins.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12191LightEdgeRidOrderTest extends TestHelper {
  private static final int LEAVES = 8;

  private RID outHub;
  private RID inHub;

  @Override
  protected void beginTest() {
    database.getSchema().buildVertexType().withName("Hub").create();
    database.getSchema().buildVertexType().withName("Leaf").withTotalBuckets(3).create();
    database.getSchema().buildEdgeType().withName("TypeA").withTotalBuckets(2).create();
    database.getSchema().buildEdgeType().withName("TypeB").withTotalBuckets(2).create();
    database.transaction(() -> {
      outHub = populate(Vertex.DIRECTION.OUT);
      inHub = populate(Vertex.DIRECTION.IN);
    });
  }

  /**
   * A hub with, for each of the two edge types, seven lightweight edges (the one to the fourth leaf twice) and four
   * record-backed ones, outgoing or incoming.
   */
  private RID populate(final Vertex.DIRECTION direction) {
    final MutableVertex hub = database.newVertex("Hub").save();
    final List<MutableVertex> leaves = new ArrayList<>();
    for (int i = 0; i < LEAVES; i++)
      leaves.add(database.newVertex("Leaf").save());

    for (final String type : new String[] { "TypeA", "TypeB" }) {
      for (int i = 0; i < 6; i++) {
        connect(hub, leaves.get(i), type, true, direction);
        if (i == 3)
          connect(hub, leaves.get(i), type, true, direction);
      }
      for (int i = 4; i < LEAVES; i++)
        connect(hub, leaves.get(i), type, false, direction);
    }
    return hub.getIdentity();
  }

  private static void connect(final MutableVertex hub, final MutableVertex leaf, final String type, final boolean lightweight,
      final Vertex.DIRECTION direction) {
    final MutableVertex from = direction == Vertex.DIRECTION.OUT ? hub : leaf;
    final MutableVertex to = direction == Vertex.DIRECTION.OUT ? leaf : hub;
    if (lightweight)
      from.newLightEdge(type, to);
    else
      from.newEdge(type, to);
  }

  private RID hub(final Vertex.DIRECTION direction) {
    return direction == Vertex.DIRECTION.OUT ? outHub : inHub;
  }

  private static String list(final RID hub, final Vertex.DIRECTION direction) {
    return "select from ( select expand( " + (direction == Vertex.DIRECTION.OUT ? "outE" : "inE") + "() ) from " + hub + " )";
  }

  private List<Edge> edges(final String sql) {
    final List<Edge> edges = new ArrayList<>();
    try (final ResultSet resultSet = database.query("sql", sql)) {
      while (resultSet.hasNext())
        edges.add(resultSet.next().getEdge().get());
    }
    return edges;
  }

  private List<Edge> ordered(final Vertex.DIRECTION direction) {
    return edges(list(hub(direction), direction) + " ORDER BY @rid");
  }

  private static RID far(final Edge edge, final Vertex.DIRECTION direction) {
    return direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
  }

  private static boolean lightweight(final Edge edge) {
    return edge.getIdentity().getPosition() < 0;
  }

  /** The edge as the client tells it from another: its RID, which does not tell lightweight edges apart, and the far vertex. */
  private static List<String> described(final List<Edge> edges, final Vertex.DIRECTION direction) {
    final List<String> described = new ArrayList<>(edges.size());
    for (final Edge edge : edges)
      described.add(edge.getIdentity() + " -> " + far(edge, direction));
    return described;
  }

  private static List<Integer> lightweightBuckets(final List<Edge> edges) {
    final List<Integer> buckets = new ArrayList<>();
    for (final Edge edge : edges)
      if (lightweight(edge) && !buckets.contains(edge.getIdentity().getBucketId()))
        buckets.add(edge.getIdentity().getBucketId());
    return buckets;
  }

  @Test
  void theFixtureHoldsLightweightAndRecordEdgesOfTwoTypesInSeveralBuckets() {
    for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN }) {
      final List<Edge> all = ordered(direction);
      assertThat(all).as("%s edges", direction).hasSize(2 * (7 + 4));
      assertThat(lightweightBuckets(all)).as("buckets of the lightweight %s edges", direction).hasSize(2);
      assertThat(all.stream().filter(edge -> !lightweight(edge)).map(edge -> edge.getIdentity().getBucketId()).distinct().count())
          .as("buckets of the record-backed %s edges", direction).isGreaterThanOrEqualTo(2);
    }
  }

  @Test
  void aBoundOfTheBucketSortsBeforeItsLightweightEdgesAndBeforeItsFirstRecord() {
    for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN }) {
      final List<Edge> all = ordered(direction);
      for (final int bucket : lightweightBuckets(all)) {
        final String list = list(hub(direction), direction);

        // every edge of the bucket and of the buckets after it, the lightweight ones included, and none of the buckets before
        final List<Edge> afterTheBound = edges(list + " WHERE @rid > " + new RID(bucket, -1L));
        assertThat(described(afterTheBound, direction)).as("%s edges after #%d:-1", direction, bucket)
            .containsExactlyInAnyOrderElementsOf(
                described(all.stream().filter(edge -> edge.getIdentity().getBucketId() >= bucket).toList(), direction));

        // from the first record of the bucket on: no lightweight edge of the bucket, the lightweight ones of later buckets
        final List<Edge> fromTheFirstRecord = edges(list + " WHERE @rid >= " + new RID(bucket, 0L));
        assertThat(described(fromTheFirstRecord, direction)).as("%s edges from #%d:0", direction, bucket)
            .containsExactlyInAnyOrderElementsOf(described(all.stream().filter(
                edge -> edge.getIdentity().getBucketId() > bucket || (edge.getIdentity().getBucketId() == bucket
                    && !lightweight(edge))).toList(), direction));
      }
    }
  }

  @Test
  void theLightweightEdgesOfABucketAreOrderedByTheVertexAtTheirOtherEnd() {
    for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN }) {
      final List<Edge> all = ordered(direction);
      Edge previous = null;
      for (final Edge edge : all) {
        if (previous != null) {
          final RID before = previous.getIdentity();
          final RID rid = edge.getIdentity();
          // by bucket, then the lightweight edges (position -1) before the records of the bucket
          assertThat(rid.getBucketId() > before.getBucketId()
              || (rid.getBucketId() == before.getBucketId() && rid.getPosition() >= before.getPosition())).as("%s: %s after %s",
              direction, rid, before).isTrue();
          // and the lightweight ones of one bucket by the vertex at their other end
          if (lightweight(edge) && lightweight(previous) && rid.getBucketId() == before.getBucketId())
            assertThat(far(edge, direction).compareTo(far(previous, direction))).as("%s: %s after %s", direction,
                far(edge, direction), far(previous, direction)).isGreaterThanOrEqualTo(0);
        }
        previous = edge;
      }
    }
  }

  @Test
  void theContinuationQueryOfTheRemoteClientReturnsExactlyTheEdgesAfterTheCursor() {
    for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN }) {
      final String farAttribute = direction == Vertex.DIRECTION.OUT ? "@in" : "@out";
      final List<Edge> all = ordered(direction);

      for (int cursor = 0; cursor < all.size(); cursor++) {
        final Edge last = all.get(cursor);
        final RID rid = last.getIdentity();
        final StringBuilder query = new StringBuilder(list(hub(direction), direction));
        int skip = 0;
        if (!lightweight(last))
          query.append(" WHERE @rid > ").append(rid);
        else {
          final RID far = far(last, direction);
          // the twins of the last edge read so far: same bucket, lightweight, same vertex at the other end
          for (int i = 0; i <= cursor; i++) {
            final Edge read = all.get(i);
            if (lightweight(read) && read.getIdentity().getBucketId() == rid.getBucketId() && far(read, direction).equals(far))
              ++skip;
          }
          query.append(" WHERE @rid > ").append(new RID(rid.getBucketId(), -1L)).append(" AND (@rid >= ")
              .append(new RID(rid.getBucketId(), 0L)).append(" OR ").append(farAttribute).append(" >= ").append(far).append(')');
        }
        query.append(" ORDER BY @rid");
        if (skip > 0)
          query.append(" SKIP ").append(skip);

        assertThat(described(edges(query.toString()), direction)).as("%s after position %d of %d: %s", direction, cursor,
            all.size(), query).containsExactlyElementsOf(described(all.subList(cursor + 1, all.size()), direction));
      }
    }
  }
}
