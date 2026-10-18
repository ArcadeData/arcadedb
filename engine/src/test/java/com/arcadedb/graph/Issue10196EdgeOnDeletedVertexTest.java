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
import com.arcadedb.exception.VertexNotFoundException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10196, engine side: an edge whose endpoint this same transaction had already deleted was created and committed. The delete
 * wins over the later write to the vertex record (#7149), so the adjacency update was dropped while the edge record was stored,
 * leaving an edge whose vertex no longer exists; when the deleted vertex had edges, the append met its freed head chunk and was
 * refused as a retryable "concurrent commit in flight".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10196EdgeOnDeletedVertexTest extends TestHelper {
  private RID v1;
  private RID v2;

  @BeforeEach
  void seed() {
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("E");
    database.getSchema().buildEdgeType().withName("L").withLightweight(true).create();
    database.transaction(() -> {
      v1 = database.newVertex("N").set("uid", 1).save().getIdentity();
      v2 = database.newVertex("N").set("uid", 2).save().getIdentity();
    });
  }

  private long edges() {
    // count(), NOT count(*): THE CACHED COUNTER IS NOT GROUND TRUTH
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM E")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  void edgeFromADeletedVertexIsRefused() {
    database.begin();
    final Vertex source = v1.asVertex();
    source.delete();
    assertThatThrownBy(() -> source.newEdge("E", v2)).isInstanceOf(VertexNotFoundException.class)
        .hasMessageContaining("source vertex " + v1).hasMessageContaining("deleted in this transaction");
    database.commit();

    assertThat(edges()).isZero();
    assertThat(database.existsRecord(v1)).isFalse();
  }

  @Test
  void edgeToADeletedVertexIsRefused() {
    database.begin();
    final Vertex target = v2.asVertex();
    target.delete();
    assertThatThrownBy(() -> v1.asVertex().newEdge("E", target)).isInstanceOf(VertexNotFoundException.class)
        .hasMessageContaining("destination vertex " + v2);
    database.commit();

    assertThat(edges()).isZero();
  }

  @Test
  void edgeFromADeletedVertexWithEdgesIsRefusedAsNotFoundNotAsAConflict() {
    database.transaction(() -> v1.asVertex().newEdge("E", v2));

    database.begin();
    final Vertex source = v1.asVertex();
    source.delete();
    assertThatThrownBy(() -> source.newEdge("E", v2)).isInstanceOf(VertexNotFoundException.class);
    database.commit();

    assertThat(edges()).isZero();
  }

  @Test
  void lightweightEdgeOnADeletedVertexIsRefused() {
    database.begin();
    final Vertex source = v1.asVertex();
    source.delete();
    assertThatThrownBy(() -> source.newEdge("L", v2)).isInstanceOf(VertexNotFoundException.class);
    database.commit();

    assertThat(v2.asVertex().countEdges(Vertex.DIRECTION.IN, "L")).isZero();
  }

  @Test
  void sqlCreateEdgeFromAVertexDeletedEarlierInTheTransactionIsRefused() {
    // THE LOOKUP OF THE ENDPOINT BY RID ALREADY FINDS NOTHING
    database.begin();
    database.command("sql", "DELETE FROM " + v1).close();
    assertThatThrownBy(() -> database.command("sql", "CREATE EDGE E FROM " + v1 + " TO " + v2).close())
        .isInstanceOf(RecordNotFoundException.class);
    database.rollback();

    assertThat(edges()).isZero();
    assertThat(database.existsRecord(v1)).isTrue();
  }

  @Test
  void cypherCreateFromANodeDeletedEarlierInTheStatementIsRefused() {
    // NEO4J REFUSES IT TOO: THE NODE WAS DELETED IN THIS TRANSACTION
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (a:N {uid: 1}), (b:N {uid: 2}) DETACH DELETE a CREATE (a)-[:E]->(b)").close()))
        .hasMessageContaining("node `a` was deleted in this transaction");

    assertThat(edges()).isZero();
    assertThat(database.existsRecord(v1)).isTrue();
  }

  @Test
  void anEdgeCreatedBeforeTheDeleteIsStillTakenWithIt() {
    database.transaction(() -> {
      final Vertex source = v1.asVertex();
      source.newEdge("E", v2);
      source.asVertex().delete();
    });

    assertThat(edges()).isZero();
    assertThat(v2.asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isZero();
  }
}
