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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.schema.EdgeType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #10314: on a super-node whose edge list is striped, deleting ONE lightweight edge removed one of its
 * twins (same type, same neighbour) from every generation of the stripe directory that held one, instead of exactly one,
 * as the classic layout does.
 */
class Issue10314StripedLightEdgeDeleteTest extends TestHelper {
  private static final int TWINS = 40;

  private RID v0;
  private RID v1;

  @BeforeEach
  void createStripedTwins() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT").close();
    database.command("sql", "CREATE EDGE TYPE R").close();
    database.transaction(() -> {
      v0 = database.newVertex("V").set("id", 0).save().getIdentity();
      v1 = database.newVertex("V").set("id", 1).save().getIdentity();
    });
    database.transaction(() -> {
      for (int i = 0; i < TWINS; i++)
        v0.asVertex().newEdge("L", v1.asVertex());
    });

    // BOTH ENDPOINTS ARE STRIPED SUPER-NODES, WITH TWINS IN MORE THAN ONE GENERATION
    final StripeDirectory out = (StripeDirectory) database.lookupByRID(((VertexInternal) v0.asVertex()).getOutEdgesHeadChunk(), true);
    final StripeDirectory in = (StripeDirectory) database.lookupByRID(((VertexInternal) v1.asVertex()).getInEdgesHeadChunk(), true);
    assertThat(out.getGenerationCount()).isGreaterThan(1);
    assertThat(in.getGenerationCount()).isGreaterThan(1);
    assertCount(TWINS);
  }

  @Test
  void eachDeleteInItsOwnTransactionRemovesExactlyOneTwin() {
    for (int i = 1; i <= TWINS; i++) {
      database.transaction(() -> lightEdge().delete());
      assertCount(TWINS - i);
    }
  }

  @Test
  void deletesInOneTransactionRemoveExactlyOneTwinEach() {
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        lightEdge().delete();
      assertCount(TWINS - 5);
    });
    assertCount(TWINS - 5);
  }

  @Test
  void deletingAnEdgeReturnedByTheGraphApiRemovesExactlyOneTwin() {
    database.transaction(() -> {
      final Edge edge = v0.asVertex().getEdges(Vertex.DIRECTION.OUT, "L").iterator().next();
      edge.delete();
    });
    assertCount(TWINS - 1);
  }

  @Test
  void deletingAnEdgeReturnedBySqlRemovesExactlyOneTwin() {
    // A LIGHTWEIGHT EDGE HANDLE MATERIALISED BY A SQL QUERY, THEN DELETED
    database.transaction(() -> {
      final Edge edge = database.query("sql", "SELECT expand(outE('L')) FROM " + v0 + " LIMIT 1").next().getEdge().get();
      edge.delete();
    });
    assertCount(TWINS - 1);
  }

  @Test
  void deletingARegularEdgeOnTheStripedListRemovesExactlyThatEdge() {
    final RID[] regular = new RID[2];
    database.transaction(() -> {
      regular[0] = v0.asVertex().newEdge("R", v1.asVertex()).getIdentity();
      regular[1] = v0.asVertex().newEdge("R", v1.asVertex()).getIdentity();
    });

    database.transaction(() -> regular[0].asEdge().delete());

    assertThat(v0.asVertex().countEdges(Vertex.DIRECTION.OUT, "R")).isEqualTo(1);
    assertThat(v1.asVertex().countEdges(Vertex.DIRECTION.IN, "R")).isEqualTo(1);
    assertThat(v0.asVertex().getEdges(Vertex.DIRECTION.OUT, "R").iterator().next().getIdentity()).isEqualTo(regular[1]);
    assertCount(TWINS);
  }

  private void assertCount(final int expected) {
    assertThat(v0.asVertex().countEdges(Vertex.DIRECTION.OUT, "L")).isEqualTo(expected);
    assertThat(v1.asVertex().countEdges(Vertex.DIRECTION.IN, "L")).isEqualTo(expected);
  }

  private Edge lightEdge() {
    final EdgeType type = (EdgeType) database.getSchema().getType("L");
    return new ImmutableLightEdge(database, type, type.getFirstBucketId(), v0, v1);
  }
}
