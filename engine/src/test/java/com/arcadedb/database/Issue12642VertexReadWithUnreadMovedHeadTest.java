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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12642: a vertex a transaction read off its own image of a page, whose edge-list head a concurrent transaction moved, was
 * a stale read whether or not anything read that head: the page merge refused the commit. A {@code MATCH (a {uuid: ..})} that binds
 * the vertex a relationship is then created from reads what the vertex is, not its lists, so a concurrent relationship load on
 * shared vertices lost most of its commits to it. The head is a stale read only when a traversal read it, which the transaction
 * records (#12320). It is stale only when it was read after the concurrent commit: a head read before it was the committed one, a
 * valid READ_COMMITTED read, and the move is then an ordinary phantom (#13572, {@link #aTraversalReadBeforeTheHeadMovedStillMerges()}).
 */
class Issue12642VertexReadWithUnreadMovedHeadTest extends TestHelper {

  @Test
  void aVertexReadForItsPropertiesIsCurrentWhenOnlyAnUnreadHeadMoved() throws Exception {
    createGraph();

    database.begin();
    // the transaction writes the page, then reads another vertex of it from that image
    database.command("opencypher", "MATCH (a:V {id: 1}) SET a.f = 1").close();
    assertThat(valueOf("MATCH (a:V {id: 2}) RETURN a.id AS v")).isEqualTo(2);
    // a concurrent transaction gives vertex 2 its first incoming relationship: its IN head moves
    commitFromAnotherThread("MATCH (a:V {id: 3}), (b:V {id: 2}) CREATE (a)-[:E]->(b)");
    database.commit();

    assertThat(valueOf("MATCH (a:V {id: 1}) RETURN a.f AS v")).isEqualTo(1L);
    assertThat(valueOf("MATCH (b:V {id: 2})<-[e:E]-() RETURN count(e) AS v")).isEqualTo(1L);
  }

  @Test
  void aVertexWhoseMovedHeadATraversalReadIsStillRefused() throws Exception {
    createGraph();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 1}) SET a.f = 1").close();
    commitFromAnotherThread("MATCH (a:V {id: 3}), (b:V {id: 2}) CREATE (a)-[:E]->(b)");
    // the traversal reads the IN head of vertex 2 off the transaction's image, which misses the move committed before: a stale decision
    // on its incoming relationships (a read made before that commit would be current, #13572)
    assertThat(valueOf("MATCH (b:V {id: 2})<-[e:E]-() RETURN count(e) AS v")).isEqualTo(0L);

    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
    assertThat(valueOf("MATCH (a:V {id: 1}) RETURN a.f AS v")).isNull();
  }

  @Test
  void aVertexThisTransactionWritesIsStillRefusedWhenAConcurrentTransactionMovedItsHead() throws Exception {
    createGraph();

    database.begin();
    // the vertex itself is written: its slot is replayed by the merges, not checked as a read, and two writers of one record conflict
    database.command("opencypher", "MATCH (a:V {id: 2}) SET a.f = 1").close();
    commitFromAnotherThread("MATCH (a:V {id: 3}), (b:V {id: 2}) CREATE (a)-[:E]->(b)");

    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
    assertThat(valueOf("MATCH (a:V {id: 2}) RETURN a.f AS v")).isNull();
    assertThat(valueOf("MATCH (b:V {id: 2})<-[e:E]-() RETURN count(e) AS v")).isEqualTo(1L);
  }

  @Test
  void aVertexReadForItsPropertiesIsCurrentWhenOnlyAnUnreadOutgoingHeadMoved() throws Exception {
    createGraph();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 1}) SET a.f = 1").close();
    assertThat(valueOf("MATCH (a:V {id: 2}) RETURN a.id AS v")).isEqualTo(2);
    // a concurrent transaction gives vertex 2 its first outgoing relationship: its OUT head moves
    commitFromAnotherThread("MATCH (a:V {id: 2}), (b:V {id: 3}) CREATE (a)-[:E]->(b)");
    database.commit();

    assertThat(valueOf("MATCH (a:V {id: 1}) RETURN a.f AS v")).isEqualTo(1L);
    assertThat(valueOf("MATCH (a:V {id: 2})-[e:E]->() RETURN count(e) AS v")).isEqualTo(1L);
  }

  @Test
  void aVertexWhoseMovedOutgoingHeadATraversalReadIsStillRefused() throws Exception {
    createGraph();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 1}) SET a.f = 1").close();
    commitFromAnotherThread("MATCH (a:V {id: 2}), (b:V {id: 3}) CREATE (a)-[:E]->(b)");
    assertThat(valueOf("MATCH (a:V {id: 2})-[e:E]->() RETURN count(e) AS v")).isEqualTo(0L);

    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
    assertThat(valueOf("MATCH (a:V {id: 1}) RETURN a.f AS v")).isNull();
  }

  @Test
  void anAppendToTheOtherListDoesNotMakeAnUnreadMovedHeadStale() throws Exception {
    createGraph();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 1}) SET a.f = 1").close();
    // vertex 0 has an outgoing chunk with room: the new relationship is appended to it, the vertex itself is not written
    database.command("opencypher", "MATCH (a:V {id: 0}), (b:V {id: 4}) CREATE (a)-[:E]->(b)").close();
    // a concurrent transaction gives vertex 0 its first incoming relationship: its IN head moves, its OUT list nobody else touched
    commitFromAnotherThread("MATCH (a:V {id: 3}), (b:V {id: 0}) CREATE (a)-[:E]->(b)");
    database.commit();

    assertThat(valueOf("MATCH (a:V {id: 1}) RETURN a.f AS v")).isEqualTo(1L);
    assertThat(valueOf("MATCH (a:V {id: 0})-[e:E]->() RETURN count(e) AS v")).isEqualTo(2L);
    assertThat(valueOf("MATCH (a:V {id: 0})<-[e:E]-() RETURN count(e) AS v")).isEqualTo(1L);
  }

  @Test
  void anAppendToTheOtherListDoesNotMakeATraversedMovedHeadCurrent() throws Exception {
    createGraph();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 1}) SET a.f = 1").close();
    database.command("opencypher", "MATCH (a:V {id: 0}), (b:V {id: 4}) CREATE (a)-[:E]->(b)").close();
    commitFromAnotherThread("MATCH (a:V {id: 3}), (b:V {id: 0}) CREATE (a)-[:E]->(b)");
    // the incoming list of vertex 0 is read: its outgoing appends do not make that read current
    assertThat(valueOf("MATCH (b:V {id: 0})<-[e:E]-() RETURN count(e) AS v")).isEqualTo(0L);

    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
    assertThat(valueOf("MATCH (a:V {id: 0})-[e:E]->() RETURN count(e) AS v")).isEqualTo(1L);
  }

  @Test
  void aCountOfTheEdgesReadsTheHeadAndIsRefused() throws Exception {
    createGraph();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 1}) SET a.f = 1").close();
    commitFromAnotherThread("MATCH (a:V {id: 3}), (b:V {id: 2}) CREATE (a)-[:E]->(b)");
    final Vertex vertex = vertexOf(2);
    assertThat(vertex.countEdges(Vertex.DIRECTION.IN)).isZero();

    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  void aTraversalOfAnUnsavedModifiedCopyReadsTheHeadAndIsRefused() throws Exception {
    createGraph();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 1}) SET a.f = 1").close();
    // the copy is built from the transaction's own image of the page, traversed, and never saved
    commitFromAnotherThread("MATCH (a:V {id: 3}), (b:V {id: 2}) CREATE (a)-[:E]->(b)");
    final MutableVertex copy = vertexOf(2).modify();
    assertThat(copy.countEdges(Vertex.DIRECTION.IN)).isZero();

    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  void aTraversalOfAVertexReloadedFromTheOwnImageReadsTheHeadAndIsRefused() throws Exception {
    createGraph();

    database.begin();
    // read before the transaction holds the page: the content is not the transaction's image of it
    final Vertex vertex = vertexOf(2);
    assertThat(vertex.getInteger("id")).isEqualTo(2);
    database.command("opencypher", "MATCH (a:V {id: 1}) SET a.f = 1").close();
    commitFromAnotherThread("MATCH (a:V {id: 3}), (b:V {id: 2}) CREATE (a)-[:E]->(b)");
    // reloaded now, from the transaction's own image of the page
    vertex.reload();
    assertThat(vertex.countEdges(Vertex.DIRECTION.IN)).isZero();

    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
  }

  /**
   * #13572: the order of {@link #aVertexWhoseMovedHeadATraversalReadIsStillRefused()} with the concurrent commit AFTER the traversal.
   * Nobody had committed to the page when the head was read, so the count was the committed one: the move that comes after is a phantom
   * READ_COMMITTED allows, as it would be had the transaction not written the page at all, and the page still merges.
   */
  @Test
  void aTraversalReadBeforeTheHeadMovedStillMerges() throws Exception {
    createGraph();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 1}) SET a.f = 1").close();
    assertThat(valueOf("MATCH (b:V {id: 2})<-[e:E]-() RETURN count(e) AS v")).isEqualTo(0L);
    commitFromAnotherThread("MATCH (a:V {id: 3}), (b:V {id: 2}) CREATE (a)-[:E]->(b)");
    database.commit();

    assertThat(valueOf("MATCH (a:V {id: 1}) RETURN a.f AS v")).isEqualTo(1L);
    assertThat(valueOf("MATCH (b:V {id: 2})<-[e:E]-() RETURN count(e) AS v")).isEqualTo(1L);
  }

  private void createGraph() {
    database.transaction(() -> {
      // one bucket: every vertex is on the same page
      database.getSchema().createVertexType("V", 1).createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      database.getSchema().createEdgeType("E", 1);
      for (int i = 0; i < 6; i++)
        database.newVertex("V").set("id", i).save();
      database.command("opencypher", "MATCH (a:V {id: 0}), (x:V {id: 5}) CREATE (a)-[:E]->(x)").close();
    });
  }

  private Vertex vertexOf(final int id) {
    return database.lookupByKey("V", "id", id).next().getRecord().asVertex();
  }

  private Object valueOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().getProperty("v");
    }
  }

  private void commitFromAnotherThread(final String command) throws InterruptedException {
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try {
        database.transaction(() -> database.command("opencypher", command).close());
      } catch (final Throwable e) {
        error.set(e);
      }
    });
    thread.start();
    thread.join();
    assertThat(error.get()).isNull();
  }
}
