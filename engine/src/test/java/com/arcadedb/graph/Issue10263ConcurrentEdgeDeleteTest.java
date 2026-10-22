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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Regression for issue #10263: a transaction deleting an edge after a concurrent commit changed what it had read - the
 * edge list, or the edge record itself - left an entry in a vertex's edge list for an edge that no longer existed, which
 * became an entry naming another edge, or a duplicate, once the RID was reused.
 * <p>
 * The deterministic tests drive each window with a second thread committing between two steps of the first
 * transaction; the stress tests are the reporter's program.
 */
class Issue10263ConcurrentEdgeDeleteTest extends TestHelper {
  private static final int THREADS = 2;
  private static final int COMMITS = 200;
  private static final int SEEDS   = 40;

  /**
   * The transaction appended to the vertex's OUT list (its own copy of the head chunk) before a concurrent commit
   * appended the edge to the same chunk, so the removal found nothing in the list it reads. It must not go on and
   * delete the edge record: at commit the append merge replays this transaction's append on the newer chunk, which
   * still holds the entry.
   */
  @Test
  void deletingAnEdgeAddedAfterTheListWasWrittenIsAConflict() throws Exception {
    final RID[] v = createVertices(2);
    // ANOTHER EDGE TYPE, SO THE EDGE THIS TRANSACTION CREATES IS NOT ON THE PAGE OF THE ONE IT DELETES
    database.command("sql", "CREATE EDGE TYPE F").close();
    database.transaction(() -> v[0].asVertex().newEdge("E", v[1].asVertex()));

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      v[0].asVertex().newEdge("F", v[1].asVertex());

      final RID added = addEdgeInOwnTransaction(other, v[0], v[1]);

      assertThatThrownBy(() -> added.asEdge().delete()).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();
    } finally {
      other.shutdownNow();
    }

    assertEdgeListsMatchRecords(v);
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(2);
  }

  /**
   * The same window on a super-node, whose list is striped: the committed list is walked along the stripe chains the
   * neighbour's entries live in.
   */
  @Test
  void deletingAnEdgeAddedAfterAStripedListWasWrittenIsAConflict() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
    try {
      final RID[] v = createVertices(2);
      database.command("sql", "CREATE EDGE TYPE F").close();
      database.transaction(() -> {
        for (int i = 0; i < 40; i++)
          v[0].asVertex().newEdge("E", v[1].asVertex());
      });
      assertThat(database.lookupByRID(((VertexInternal) v[0].asVertex()).getOutEdgesHeadChunk(), true))
          .isInstanceOf(StripeDirectory.class);

      final ExecutorService other = Executors.newSingleThreadExecutor();
      try {
        database.begin();
        // SAME NEIGHBOUR, SO THE SAME STRIPE CHAIN AS THE EDGE DELETED BELOW
        v[0].asVertex().newEdge("F", v[1].asVertex());

        final RID added = addEdgeInOwnTransaction(other, v[0], v[1]);

        assertThatThrownBy(() -> added.asEdge().delete()).isInstanceOf(ConcurrentModificationException.class);
        database.rollback();
      } finally {
        other.shutdownNow();
      }

      assertEdgeListsMatchRecords(v);
      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(41);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD,
          GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getDefValue());
    }
  }

  /**
   * The conflict is retryable: the retry reads the list again and the delete goes through, leaving nothing behind.
   */
  @Test
  void theRetryOfTheRefusedDeleteSucceeds() throws Exception {
    final RID[] v = createVertices(2);
    database.command("sql", "CREATE EDGE TYPE F").close();
    database.transaction(() -> v[0].asVertex().newEdge("E", v[1].asVertex()));

    final ExecutorService other = Executors.newSingleThreadExecutor();
    final RID[] added = new RID[1];
    final int[] attempts = new int[1];
    try {
      database.transaction(() -> {
        ++attempts[0];
        if (attempts[0] == 1) {
          v[0].asVertex().newEdge("F", v[1].asVertex());
          try {
            added[0] = addEdgeInOwnTransaction(other, v[0], v[1]);
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
        }
        added[0].asEdge().delete();
      }, false, 3);
    } finally {
      other.shutdownNow();
    }

    assertThat(attempts[0]).isEqualTo(2);
    assertEdgeListsMatchRecords(v);
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
  }

  /**
   * The same window when the transaction holds its own copy of the vertex from before the vertex had any list in that
   * direction: the removal finds no list at all.
   */
  @Test
  void deletingAnEdgeOfAVertexHeldWithoutAnyListIsAConflict() throws Exception {
    final RID[] v = createVertices(2);

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      v[0].asVertex().modify().set("value", 1).save();
      v[1].asVertex().modify().set("value", 1).save();

      final RID added = addEdgeInOwnTransaction(other, v[0], v[1]);

      assertThatThrownBy(() -> added.asEdge().delete()).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();
    } finally {
      other.shutdownNow();
    }

    assertEdgeListsMatchRecords(v);
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
  }

  /**
   * The transaction read an edge that a concurrent transaction then deleted and whose RID a new edge between other
   * vertices took: deleting by RID would remove the new edge's record and leave its entries behind.
   */
  @Test
  void deletingAnEdgeWhoseRidWasReusedByAnotherEdgeIsAConflict() throws Exception {
    final RID[] v = createVertices(4);
    final RID[] edge = new RID[1];
    database.transaction(() -> edge[0] = v[0].asVertex().newEdge("E", v[1].asVertex()).getIdentity());

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      final Edge read = edge[0].asEdge();
      assertThat(read.getOut()).isEqualTo(v[0]);

      other.submit(() -> database.transaction(() -> edge[0].asEdge().delete())).get();
      final RID reused = addEdgeInOwnTransaction(other, v[2], v[3]);
      assumeThat(reused).as("the bucket handed the freed RID to the new edge").isEqualTo(edge[0]);

      assertThatThrownBy(read::delete).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();
    } finally {
      other.shutdownNow();
    }

    assertEdgeListsMatchRecords(v);
    assertThat(v[2].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
  }

  /**
   * The transaction read an edge that a concurrent transaction then deleted: the delete found nothing to remove, and
   * counting it anyway put the bucket's record counter one behind the bucket for good.
   */
  @Test
  void deletingAnEdgeDeletedConcurrentlyIsAConflict() throws Exception {
    final RID[] v = createVertices(2);
    final RID[] edge = new RID[1];
    database.transaction(() -> edge[0] = v[0].asVertex().newEdge("E", v[1].asVertex()).getIdentity());

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      final Edge read = edge[0].asEdge();
      assertThat(read.getIn()).isEqualTo(v[1]);

      other.submit(() -> database.transaction(() -> edge[0].asEdge().delete())).get();

      assertThatThrownBy(read::delete).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();
    } finally {
      other.shutdownNow();
    }

    assertEdgeListsMatchRecords(v);
    assertThat(database.countType("E", true)).isZero();
    // THE CACHED RECORD COUNTER IS HELD AGAINST THE BUCKET BY THE END-OF-TEST INTEGRITY CHECK OF TestHelper
  }

  /**
   * An edge whose entries are genuinely missing (an orphan record) still has to be deletable: nothing concurrent
   * happened, so the removal finding nothing is not a conflict.
   */
  @Test
  void anOrphanEdgeRecordIsStillDeleted() {
    final RID[] v = createVertices(2);
    final RID[] edge = new RID[1];
    database.transaction(() -> edge[0] = v[0].asVertex().newEdge("E", v[1].asVertex()).getIdentity());
    database.transaction(() -> {
      ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk((VertexInternal) v[0].asVertex(), Vertex.DIRECTION.OUT).removeEdgeRID(edge[0]);
      ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk((VertexInternal) v[1].asVertex(), Vertex.DIRECTION.IN).removeEdgeRID(edge[0]);
    });
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isZero();

    database.transaction(() -> edge[0].asEdge().delete());

    assertThat(database.countType("E", true)).isZero();
  }

  @Tag("slow")
  @Test
  void concurrentCreateAndCrossDeleteKeepsEdgeListsExact() throws Exception {
    final List<String> wrong = new ArrayList<>();
    for (long seed = 1; seed <= SEEDS; seed++)
      wrong.addAll(run("A", seed, 4, false));
    assertThat(wrong).as("edge lists that disagree with the E records").isEmpty();
  }

  /**
   * The 40-vertex variant of the report, with vertex property updates in the mix: the update rewrites the vertex record
   * that holds the edge-list heads.
   */
  @Tag("slow")
  @Test
  void concurrentCreateCrossDeleteAndVertexUpdatesKeepEdgeListsExact() throws Exception {
    final List<String> wrong = new ArrayList<>();
    for (long seed = 1; seed <= SEEDS; seed++)
      wrong.addAll(run("B", seed, 40, true));
    assertThat(wrong).as("edge lists that disagree with the E records").isEmpty();
  }

  private RID[] createVertices(final int count) {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE EDGE TYPE E").close();
    final RID[] vertices = new RID[count];
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        vertices[i] = database.newVertex("V").set("id", i).save().getIdentity();
    });
    return vertices;
  }

  private RID addEdgeInOwnTransaction(final ExecutorService other, final RID from, final RID to) throws Exception {
    return other.submit(() -> {
      final RID[] added = new RID[1];
      database.transaction(() -> added[0] = from.asVertex().newEdge("E", to.asVertex()).getIdentity());
      return added[0];
    }).get();
  }

  private void assertEdgeListsMatchRecords(final RID[] vertices) {
    final List<String> wrong = new ArrayList<>();
    database.transaction(() -> wrong.addAll(check(0, List.of(vertices), "E")));
    assertThat(wrong).as("edge lists that disagree with the E records").isEmpty();
  }

  private List<String> run(final String prefix, final long seed, final int vertexCount, final boolean updateVertices)
      throws Exception {
    final String vType = prefix + "V" + seed;
    final String eType = prefix + "E" + seed;
    database.command("sql", "CREATE VERTEX TYPE " + vType).close();
    database.command("sql", "CREATE EDGE TYPE " + eType).close();

    final List<RID> vertices = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < vertexCount; i++)
        vertices.add(database.newVertex(vType).set("id", i).save().getIdentity());
    });

    // Edges a committed transaction created and none deleted
    final List<RID> committed = new ArrayList<>();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(THREADS);
    for (int t = 0; t < THREADS; t++) {
      final Random random = new Random(seed * 7 + t);
      new Thread(() -> {
        try {
          for (int c = 0; c < COMMITS; c++) {
            for (int attempt = 0; attempt < 30; attempt++) {
              final List<RID> made = new ArrayList<>();
              final List<RID> gone = new ArrayList<>();
              try {
                database.begin();
                for (int op = 1 + random.nextInt(5); op > 0; op--) {
                  final int kind = random.nextInt(updateVertices ? 8 : 7);
                  if (kind == 7)
                    vertices.get(random.nextInt(vertexCount)).asVertex().modify().set("value", random.nextInt()).save();
                  else if (kind < 4)
                    made.add(vertices.get(random.nextInt(vertexCount)).asVertex()
                        .newEdge(eType, vertices.get(random.nextInt(vertexCount)).asVertex()).save().getIdentity());
                  else {
                    final RID e;
                    synchronized (committed) {
                      e = committed.isEmpty() ? null : committed.get(random.nextInt(committed.size()));
                    }
                    if (e != null && !gone.contains(e)) {
                      gone.add(e);
                      e.asEdge().delete();
                    }
                  }
                }
                database.commit();
                synchronized (committed) {
                  committed.addAll(made);
                  committed.removeAll(gone);
                }
                break;
              } catch (final Exception e) {
                if (database.isTransactionActive())
                  database.rollback();
              }
            }
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        } finally {
          done.countDown();
        }
      }).start();
    }
    assertThat(done.await(5, TimeUnit.MINUTES)).as("writer threads did not finish in time").isTrue();
    assertThat(failure.get()).isNull();

    return check(seed, vertices, eType);
  }

  /**
   * Holds every vertex's OUT and IN lists against the edge records: each entry must be an existing record whose endpoint
   * is that vertex, listed once, and every record must be in both its endpoints' lists.
   */
  private List<String> check(final long seed, final List<RID> vertices, final String eType) {
    final Map<RID, RID[]> records = new HashMap<>();
    for (var it = database.iterateType(eType, true); it.hasNext(); ) {
      final Edge e = it.next().asEdge();
      records.put(e.getIdentity(), new RID[] { e.getOut(), e.getIn() });
    }

    final List<String> wrong = new ArrayList<>();
    for (final RID v : vertices) {
      for (final Vertex.DIRECTION d : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN }) {
        final Set<RID> seen = new HashSet<>();
        try {
          for (final Edge e : v.asVertex().getEdges(d, eType)) {
            final RID er = e.getIdentity();
            final RID[] rec = records.get(er);
            if (!seen.add(er))
              wrong.add("seed " + seed + ": " + d + " list of " + v + " holds " + er + " twice");
            else if (rec == null)
              wrong.add("seed " + seed + ": " + d + " list of " + v + " holds " + er + ", which is not an E record");
            else if (!(d == Vertex.DIRECTION.OUT ? rec[0] : rec[1]).equals(v))
              wrong.add("seed " + seed + ": " + d + " list of " + v + " holds " + er + ", an edge " + rec[0] + "->" + rec[1]);
          }
        } catch (final Exception e) {
          wrong.add("seed " + seed + ": " + d + " list of " + v + " throws " + e);
        }
      }
    }
    for (final Map.Entry<RID, RID[]> en : records.entrySet()) {
      if (!contains(en.getValue()[0], Vertex.DIRECTION.OUT, eType, en.getKey()))
        wrong.add("seed " + seed + ": E record " + en.getKey() + " is not in the OUT list of " + en.getValue()[0]);
      if (!contains(en.getValue()[1], Vertex.DIRECTION.IN, eType, en.getKey()))
        wrong.add("seed " + seed + ": E record " + en.getKey() + " is not in the IN list of " + en.getValue()[1]);
    }
    return wrong;
  }

  private static boolean contains(final RID v, final Vertex.DIRECTION d, final String eType, final RID edge) {
    for (final Edge e : v.asVertex().getEdges(d, eType))
      if (e.getIdentity().equals(edge))
        return true;
    return false;
  }
}
