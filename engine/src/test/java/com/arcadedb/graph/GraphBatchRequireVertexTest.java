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
import com.arcadedb.exception.RecordNotFoundException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link GraphBatch#newEdge} takes its endpoints on trust, and a batch handed one that is not an existing vertex leaves the whole
 * batch unreachable (issue #12895). {@link GraphBatch#requireVertex} is what a caller that takes RIDs from outside asks first.
 */
class GraphBatchRequireVertexTest extends TestHelper {

  @Test
  void requireVertexAcceptsAnExistingVertexAndRefusesTheRest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("RV");
      database.getSchema().createEdgeType("RE");
      database.getSchema().createDocumentType("RD");
    });
    final RID[] rids = new RID[3];
    final RID[] edge = new RID[1];
    database.transaction(() -> {
      final MutableVertex v0 = database.newVertex("RV").set("n", 0).save();
      final MutableVertex v2 = database.newVertex("RV").set("n", 2).save();
      rids[0] = v0.getIdentity();
      rids[1] = database.newDocument("RD").set("n", 1).save().getIdentity();
      rids[2] = v2.getIdentity();
      edge[0] = v0.newEdge("RE", v2).getIdentity();
    });

    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      assertThatCode(() -> batch.requireVertex(rids[0])).doesNotThrowAnyException();

      // a record that is not a vertex: a document, and an edge
      assertThatThrownBy(() -> batch.requireVertex(rids[1])).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(rids[1].toString()).hasMessageContaining("is not a vertex").hasMessageContaining("RD");
      assertThatThrownBy(() -> batch.requireVertex(edge[0])).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(edge[0].toString()).hasMessageContaining("is not a vertex").hasMessageContaining("RE");
      assertThatThrownBy(() -> batch.requireVertex(null)).isInstanceOf(IllegalArgumentException.class);

      // a record that does not exist is a RecordNotFoundException, as everywhere else in the engine: a bucket that is not there,
      // a position past the end of one that is, and a position that is below it and holds no record
      assertThatThrownBy(() -> batch.requireVertex(new RID(99, 0))).isInstanceOf(RecordNotFoundException.class)
          .hasMessageContaining("#99:0").hasMessageContaining("does not exist");
      assertThatThrownBy(() -> batch.requireVertex(new RID(rids[0].getBucketId(), 99_999))).isInstanceOf(
          RecordNotFoundException.class).hasMessageContaining("does not exist");
      assertThatThrownBy(() -> batch.requireVertex(new RID(rids[0].getBucketId(), -1))).isInstanceOf(
          RecordNotFoundException.class);
    }

    // a vertex that was there and is gone is as missing as one that never was
    database.transaction(() -> database.lookupByRID(rids[2], true).asVertex().delete());
    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      assertThatThrownBy(() -> batch.requireVertex(rids[2])).isInstanceOf(RecordNotFoundException.class)
          .hasMessageContaining(rids[2].toString());
    }
  }

  /** A vertex of a subtype is a vertex: the check is on the family of the bucket, not on the exact type. */
  @Test
  void requireVertexAcceptsAVertexOfASubtype() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Base");
      database.getSchema().createVertexType("Derived").addSuperType("Base");
    });
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newVertex("Derived").save().getIdentity());

    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      assertThatCode(() -> batch.requireVertex(rid[0])).doesNotThrowAnyException();
    }
  }

  /**
   * What a front end does with it: every endpoint of an edge that came from outside is asked first, and the edge that fails the
   * check is not buffered, so the batch has nothing but whole edges to write and the edges before the refused one are reachable
   * after the close. Handed to {@code newEdge} instead, an endpoint that does not exist fails the batch at close() with the edge
   * records written and no vertex leading to them, the valid edge included (issue #12895).
   */
  @Test
  void aRefusedEndpointIsNotBufferedAndTheEdgesBeforeItAreReachable() {
    database.transaction(() -> {
      database.getSchema().createVertexType("PV");
      database.getSchema().createEdgeType("PE");
    });
    final RID[] v = new RID[2];
    database.transaction(() -> {
      v[0] = database.newVertex("PV").set("n", 0).save().getIdentity();
      v[1] = database.newVertex("PV").set("n", 1).save().getIdentity();
    });

    final List<RID[]> edges = List.of(new RID[] { v[0], v[1] }, new RID[] { v[0], new RID(99, 0) });
    int refused = 0;
    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      for (final RID[] edge : edges) {
        try {
          batch.requireVertex(edge[0]);
          batch.requireVertex(edge[1]);
        } catch (final RecordNotFoundException e) {
          ++refused;
          continue;
        }
        batch.newEdge(edge[0], "PE", edge[1]);
      }
    }

    assertThat(refused).isEqualTo(1);
    assertThat(database.countType("PE", false)).isEqualTo(1);
    assertThat(database.query("sql", "select out('PE').size() as s from PV where n = 0").next().<Integer>getProperty("s")).isEqualTo(1);
  }
}
