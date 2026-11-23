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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12253: a second delete of an edge record the same transaction already deleted was counted
 * again in the bucket's cached record count. {@code GraphEngine.deleteEdge} ignores the not-found of the physical delete,
 * and the caller then folded a second -1 into the counter, so {@code count(*)} under-reported for good (persisted in the
 * statistics). openCypher reached it through {@code MATCH (a)-[r]->(b) DETACH DELETE a DELETE r}: the DETACH deletes
 * {@code r} with {@code a}, and the second DELETE clause deleted it again. A lazily loaded edge was refused with the
 * not-found, a loaded one was not.
 */
class EdgeDeletedTwiceRecordCountIssue12253Test extends TestHelper {
  private static final int OTHER_EDGES = 5;

  /** @return the last of the {@code edgesFromA} edges a->b, then a, then b */
  private RID[] createGraph(final int edgesFromA) {
    database.getSchema().createVertexType("V");
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("F");
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("V").save();
      final MutableVertex c = database.newVertex("V").save();
      for (int i = 0; i < edgesFromA; i++)
        rids[0] = a.newEdge("F", b).getIdentity();
      rids[1] = a.getIdentity();
      rids[2] = b.getIdentity();
      for (int i = 0; i < OTHER_EDGES; i++)
        b.newEdge("F", c);
    });
    return rids;
  }

  private long scanned(final String type) {
    long scanned = 0;
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + type)) {
      while (rs.hasNext()) {
        rs.next();
        scanned++;
      }
    }
    return scanned;
  }

  private void assertCountsMatch(final long expectedEdges) {
    assertThat(scanned("F")).isEqualTo(expectedEdges);
    assertThat(database.countType("F", false)).isEqualTo(expectedEdges);
    assertThat(database.countType("V", false)).isEqualTo(scanned("V"));
    assertThat(database.countType("P", false)).isEqualTo(scanned("P"));
  }

  /** Runs the statement in its own transaction and answers what it reports about the relationships it deleted. */
  private int relationshipsDeletedBy(final String cypher) {
    final int[] deleted = new int[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext())
          rs.next();
        deleted[0] = rs.getStatistics().orElseThrow().getRelationshipsDeleted();
      }
    });
    return deleted[0];
  }

  @Test
  void cypherDeleteOfRelationshipAlreadyDetached() {
    createGraph(1);
    assertThat(relationshipsDeletedBy("MATCH (a:P)-[r]->(b) DETACH DELETE a DELETE r")).isEqualTo(1);
    assertCountsMatch(OTHER_EDGES);
  }

  // Control: this shape was already fixed by #12254, it passes without the guard of #12253
  @Test
  void cypherDeleteOfRelationshipsDetachedByAnEarlierRow() {
    createGraph(3);
    assertThat(relationshipsDeletedBy("MATCH (a:P)-[r]->(b) DELETE r DETACH DELETE a")).isEqualTo(3);
    assertCountsMatch(OTHER_EDGES);
  }

  @Test
  void cypherDetachDeleteThenDeleteSeveralRelationships() {
    createGraph(3);
    assertThat(relationshipsDeletedBy("MATCH (a:P)-[r]->(b) DETACH DELETE a DELETE r")).isEqualTo(3);
    assertCountsMatch(OTHER_EDGES);
  }

  // Control: a self-loop is the one edge both endpoints of which are deleted by the DETACH, and it passes without the guard too
  @Test
  void cypherDetachDeleteThenDeleteASelfLoop() {
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("F");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      a.newEdge("F", a);
    });
    assertThat(relationshipsDeletedBy("MATCH (a:P)-[r]->(a) DETACH DELETE a DELETE r")).isEqualTo(1);
    assertThat(scanned("F")).isZero();
    assertThat(database.countType("F", false)).isZero();
    assertThat(database.countType("P", false)).isZero();
  }

  @Test
  void loadedEdgeDeletedTwiceInOneTransaction() {
    final RID[] rids = createGraph(1);
    database.transaction(() -> {
      final Edge first = rids[0].asEdge(true);
      final Edge second = rids[0].asEdge(true);
      first.delete();
      assertThatThrownBy(second::delete).isInstanceOf(RecordNotFoundException.class);
    });
    assertCountsMatch(OTHER_EDGES);
  }

  @Test
  void lazyEdgeDeletedTwiceInOneTransaction() {
    final RID[] rids = createGraph(1);
    database.transaction(() -> {
      final Edge first = rids[0].asEdge(false);
      final Edge second = rids[0].asEdge(false);
      first.delete();
      assertThatThrownBy(second::delete).isInstanceOf(RecordNotFoundException.class);
    });
    assertCountsMatch(OTHER_EDGES);
  }

  @Test
  void loadedEdgeDeletedAfterItsVertex() {
    final RID[] rids = createGraph(1);
    database.transaction(() -> {
      final Edge edge = rids[0].asEdge(true);
      rids[1].asVertex().delete();
      assertThatThrownBy(edge::delete).isInstanceOf(RecordNotFoundException.class);
    });
    assertCountsMatch(OTHER_EDGES);
  }

  // The vertex and document cases below are controls: they pin the not-found a second delete already answered before #12253,
  // which the fix leaves unchanged (it only adds the check for an edge record).
  @Test
  void loadedVertexDeletedTwiceInOneTransaction() {
    final RID[] rids = createGraph(1);
    database.transaction(() -> {
      final Vertex first = rids[2].asVertex(true);
      final Vertex second = rids[2].asVertex(true);
      first.delete();
      assertThatThrownBy(second::delete).isInstanceOf(RecordNotFoundException.class);
    });
    // b took the edge a->b and its 5 edges to c with it
    assertCountsMatch(0);
  }

  @Test
  void documentDeletedTwiceInOneTransaction() {
    database.getSchema().createDocumentType("D");
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        rid[0] = database.newDocument("D").save().getIdentity();
    });
    database.transaction(() -> {
      final Document first = (Document) rid[0].getRecord(true);
      final Document second = (Document) rid[0].getRecord(true);
      first.delete();
      assertThatThrownBy(second::delete).isInstanceOf(RecordNotFoundException.class);
    });
    assertThat(database.countType("D", false)).isEqualTo(2);
    assertThat(scanned("D")).isEqualTo(2);
  }
}
