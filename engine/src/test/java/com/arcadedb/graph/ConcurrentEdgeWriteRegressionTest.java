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
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Regression tests, one nested class per fixed issue.
 */
class ConcurrentEdgeWriteRegressionTest {

  /**
   * Regression for #10263: a transaction deleting an edge after a concurrent commit changed what it had read - the
   * edge list, or the edge record itself - left an entry in a vertex's edge list for an edge that no longer existed, which
   * became an entry naming another edge, or a duplicate, once the RID was reused.
   * <p>
   * The deterministic tests drive each window with a second thread committing between two steps of the first
   * transaction; the stress tests are the reporter's program.
   */
  @Nested
  class ConcurrentEdgeDeleteTest extends TestHelper {
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

  /**
   * Regression for #11498: concurrent transactions creating relationships between shared vertices conflicted on the edge-list bookkeeping
   * of those vertices, which commutes: two transactions that both moved the head of a vertex's edge list (its first chunk, or a
   * newer chunk in front of a full one), both appended to the same chunk until it filled up, or inserted into a page the other
   * one filled meanwhile. With eight writers most attempts were thrown away and the load ran three times slower than with one.
   * The commit now merges those changes, and still refuses the ones that do not commute: a property written with the edge.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ConcurrentEdgeListMergeTest extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("V", 1).createProperty("name", Type.STRING);
        database.getSchema().createEdgeType("E", 1);
        database.getSchema().createDocumentType("Doc", 1);
      });
    }

    @Test
    void bothFirstEdgesOfTheSameVertexCommit() throws Exception {
      final RID a = newVertex("a");
      final RID b1 = newVertex("b1");
      final RID b2 = newVertex("b2");
      final long mergesBefore = stats().edgeListHeadMerges;

      final Throwable failure = interleave(() -> vertex(a).newEdge("E", vertex(b1)), NOTHING, () -> vertex(a).newEdge("E", vertex(b2)));

      assertThat(failure).isNull();
      assertThat(stats().edgeListHeadMerges).isGreaterThan(mergesBefore);
      database.transaction(() -> {
        assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(2);
        assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrder(b1, b2);
        assertThat(neighbours(b1, Vertex.DIRECTION.IN)).containsExactly(a);
        assertThat(neighbours(b2, Vertex.DIRECTION.IN)).containsExactly(a);
      });
    }

    @Test
    void bothFillingAndMovingTheHeadOfTheSameListCommit() throws Exception {
      final RID a = newVertex("a");
      final List<RID> targets = new ArrayList<>();
      for (int i = 0; i < 63; i++)
        targets.add(newVertex("t" + i));
      // the list already holds a few edges, so both transactions first append to the same chunk, fill it and move the head
      database.transaction(() -> {
        for (int i = 0; i < 3; i++)
          vertex(a).newEdge("E", vertex(targets.get(i)));
      });

      final Throwable failure = interleave(() -> {
        for (int i = 3; i < 33; i++)
          vertex(a).newEdge("E", vertex(targets.get(i)));
      }, NOTHING, () -> {
        for (int i = 33; i < 63; i++)
          vertex(a).newEdge("E", vertex(targets.get(i)));
      });

      assertThat(failure).isNull();
      database.transaction(() -> {
        assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(63);
        assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrderElementsOf(targets);
        for (final RID target : targets)
          assertThat(neighbours(target, Vertex.DIRECTION.IN)).containsExactly(a);
      });
    }

    @Test
    void appendsToAChunkFilledConcurrentlyOnAFullPageGoInANewHeadChunk() throws Exception {
      final RID a = newVertex("a");
      final List<RID> targets = new ArrayList<>();
      for (int i = 0; i < 40; i++)
        targets.add(newVertex("t" + i));

      // edges one at a time until the list moves its head to a second, bigger chunk holding one edge
      final RID firstChunk = compute(() -> {
        vertex(a).newEdge("E", vertex(targets.getFirst()));
        return ((VertexInternal) vertex(a)).getOutEdgesHeadChunk();
      });
      int used = 1;
      while (firstChunk.equals(headOf(a))) {
        final RID target = targets.get(used++);
        database.transaction(() -> vertex(a).newEdge("E", vertex(target)));
      }
      final RID chunk = headOf(a);

      // then the chunk's page is filled with the first chunks of other vertices, so the chunk cannot grow in it
      final RID sink = newVertex("sink");
      database.transaction(() -> {
        for (int i = 0; i < 1200; i++)
          database.newVertex("V").set("name", "f" + i).save().newEdge("E", sink.asVertex());
      });
      final LocalBucket edgeBucket = (LocalBucket) database.getSchema().getBucketById(chunk.getBucketId());
      final int freeTail = edgeBucket.getFreeTailInPage(((DatabaseInternal) database).getPageManager().getImmutablePage(
          new PageId(database, chunk.getBucketId(), (int) (chunk.getPosition() / edgeBucket.getMaxRecordsInPage())),
          edgeBucket.getPageSize(), false, false));
      // a first chunk takes 64 bytes: the tail of a full page is shorter than that, and shorter than what is appended below
      assertThat(freeTail).as("the chunk's page must be full").isLessThan(66);

      // 11 edges fill most of the chunk in the first transaction, which does not fill it; the concurrent one fills it and moves on
      final List<RID> mine = targets.subList(used, used + 11);
      final List<RID> theirs = targets.subList(used + 11, targets.size());
      final List<RID> myEdges = new CopyOnWriteArrayList<>();
      final Throwable failure = interleave(() -> {
        for (final RID t : mine)
          myEdges.add(vertex(a).newEdge("E", vertex(t)).getIdentity());
        assertThat(headOf(a)).as("the first transaction must not fill the chunk itself").isEqualTo(chunk);
      }, NOTHING, () -> {
        for (final RID t : theirs)
          vertex(a).newEdge("E", vertex(t));
      });

      assertThat(failure).isNull();
      database.transaction(() -> {
        assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(targets.size());
        assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrderElementsOf(targets);
        for (final RID target : targets)
          assertThat(neighbours(target, Vertex.DIRECTION.IN)).containsExactly(a);
        // the edges of the transaction that lost the race are in a new chunk at the head of the list
        final EdgeSegment head = (EdgeSegment) database.lookupByRID(headOf(a), true);
        for (final RID edge : myEdges)
          assertThat(head.containsEdge(edge)).isTrue();
      });
    }

    @Test
    void aConcurrentPropertyChangeIsKeptWhenTheEdgeListHeadIsMerged() throws Exception {
      final RID a = newVertex("a");
      final RID b = newVertex("b");

      final Throwable failure = interleave(() -> vertex(a).newEdge("E", vertex(b)), NOTHING,
          () -> vertex(a).modify().set("name", "renamed").save());

      assertThat(failure).isNull();
      database.transaction(() -> {
        assertThat(vertex(a).getString("name")).isEqualTo("renamed");
        assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactly(b);
      });
    }

    @Test
    void aPropertyWrittenWithTheEdgeStillConflicts() throws Exception {
      final RID a = newVertex("a");
      final RID b1 = newVertex("b1");
      final RID b2 = newVertex("b2");

      final Throwable failure = interleave(() -> {
        vertex(a).modify().set("name", "first").save();
        vertex(a).newEdge("E", vertex(b1));
      }, NOTHING, () -> vertex(a).newEdge("E", vertex(b2)));

      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
      database.transaction(() -> {
        assertThat(vertex(a).getString("name")).isEqualTo("a");
        assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactly(b2);
        assertThat(neighbours(b1, Vertex.DIRECTION.IN)).isEmpty();
      });
    }

    @Test
    void aCommitRefusedAfterTheMergeLeavesNothingBehind() throws Exception {
      final RID a = newVertex("a");
      final RID b1 = newVertex("b1");
      final RID b2 = newVertex("b2");
      final RID doc = compute(() -> database.newDocument("Doc").set("n", 0).save().getIdentity());

      // the head of a's list merges, then the same document written by both refuses the commit: what the merge prepared is dropped
      final Throwable failure = interleave(() -> {
        vertex(a).newEdge("E", vertex(b1));
        doc.asDocument(true).modify().set("n", 1).save();
      }, NOTHING, () -> {
        vertex(a).newEdge("E", vertex(b2));
        doc.asDocument(true).modify().set("n", 2).save();
      });

      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
      database.transaction(() -> {
        assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactly(b2);
        assertThat(neighbours(b1, Vertex.DIRECTION.IN)).isEmpty();
        assertThat(database.countType("E", false)).isEqualTo(1);
        assertThat(doc.asDocument(true).getInteger("n")).isEqualTo(2);
      });

      // and the same work committed again finds a consistent list
      database.transaction(() -> {
        vertex(a).newEdge("E", vertex(b1));
        doc.asDocument(true).modify().set("n", 1).save();
      });
      database.transaction(() -> assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrder(b1, b2));
    }

    @Test
    void appendsOnAPageTheTransactionCreatedAChunkOnAreReplayed() throws Exception {
      final RID a = newVertex("a");
      final RID fresh = newVertex("fresh");
      final RID t0 = newVertex("t0");
      final RID t1 = newVertex("t1");
      final RID t2 = newVertex("t2");
      final RID t3 = newVertex("t3");
      database.transaction(() -> vertex(a).newEdge("E", vertex(t0)));
      final long mergesBefore = stats().edgeAppendMerges;

      // the first transaction appends to a's chunk and creates the first chunk of another vertex, on the same page of the edge
      // bucket: the page is out of the edge-append merge, so the append is a slot rewrite, and the concurrent append to the same
      // chunk used to be a conflict on the same record
      final Throwable failure = interleave(() -> {
        vertex(a).newEdge("E", vertex(t1));
        vertex(fresh).newEdge("E", vertex(t2));
      }, NOTHING, () -> vertex(a).newEdge("E", vertex(t3)));

      assertThat(failure).isNull();
      assertThat(stats().edgeAppendMerges).isGreaterThan(mergesBefore);
      database.transaction(() -> {
        assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrder(t0, t1, t3);
        assertThat(neighbours(fresh, Vertex.DIRECTION.OUT)).containsExactly(t2);
      });
    }

    @Test
    void anEdgeToAVertexDeletedConcurrentlyStillConflicts() throws Exception {
      final RID a = newVertex("a");
      final RID b = newVertex("b");

      final Throwable failure = interleave(() -> vertex(a).newEdge("E", vertex(b)), NOTHING, () -> vertex(a).delete());

      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
      database.transaction(() -> {
        assertThat(database.countType("V", false)).isEqualTo(1);
        assertThat(database.countType("E", false)).isZero();
        assertThat(neighbours(b, Vertex.DIRECTION.IN)).isEmpty();
      });
    }

    @Test
    void aVertexReadBeforeAConcurrentEdgeIsMergedWhenOnlyItsEdgesChange() throws Exception {
      final RID a = newVertex("a");
      final RID b1 = newVertex("b1");
      final RID b2 = newVertex("b2");
      final AtomicReference<Vertex> readBefore = new AtomicReference<>();

      final Throwable failure = interleave(() -> readBefore.set(vertex(a)), () -> readBefore.get().newEdge("E", vertex(b1)),
          () -> vertex(a).newEdge("E", vertex(b2)));

      assertThat(failure).isNull();
      database.transaction(() -> assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrder(b1, b2));
    }

    @Test
    void anInsertIntoAPageACommitWroteMeanwhileDoesNotConflict() throws Exception {
      final RID first = compute(() -> database.newDocument("Doc").set("n", 0).save().getIdentity());
      final AtomicReference<RID> inserted = new AtomicReference<>();

      // the transaction pins the page with an update, a concurrent one inserts into it, then the first one inserts too: alone on
      // the page, it used to take the slot from its own older image of it, one the concurrent commit had used
      final Throwable failure = interleave(() -> first.asDocument(true).modify().set("n", 1).save(),
          () -> inserted.set(database.newDocument("Doc").set("n", -1).save().getIdentity()), () -> {
            for (int i = 0; i < 20; i++)
              database.newDocument("Doc").set("n", 100 + i).save();
          });

      assertThat(failure).isNull();
      database.transaction(() -> {
        assertThat(database.countType("Doc", false)).isEqualTo(22);
        assertThat(inserted.get().asDocument(true).getInteger("n")).isEqualTo(-1);
        assertThat(first.asDocument(true).getInteger("n")).isEqualTo(1);
      });
    }

    @Test
    void anInsertIntoAPageACommitFilledMeanwhileDoesNotConflict() throws Exception {
      final String payload = "x".repeat(4_000);
      final RID first = compute(() -> database.newDocument("Doc").set("n", 0).save().getIdentity());
      final AtomicReference<RID> inserted = new AtomicReference<>();

      final Throwable failure = interleave(() -> first.asDocument(true).modify().set("n", 1).save(),
          () -> inserted.set(database.newDocument("Doc").set("n", -1, "payload", payload).save().getIdentity()), () -> {
            for (int i = 0; i < 15; i++)
              database.newDocument("Doc").set("n", 100 + i, "payload", payload).save();
          });

      assertThat(failure).isNull();
      database.transaction(() -> {
        assertThat(database.countType("Doc", false)).isEqualTo(17);
        assertThat(inserted.get().asDocument(true).getString("payload")).isEqualTo(payload);
      });
    }

    // under a second on a laptop: a hang detector, not a latency bound
    @Test
    @Timeout(300)
    void concurrentWritersOnSharedVerticesLoseNoEdge() throws Exception {
      final int vertices = 2_000;
      final int writers = 8;
      final int transactionsPerWriter = 25;
      final int edgesPerTransaction = 200;

      final RID[] rids = new RID[vertices];
      database.transaction(() -> {
        for (int i = 0; i < vertices; i++)
          rids[i] = database.newVertex("V").set("name", "v" + i).save().getIdentity();
      });

      final Set<RID> committed = ConcurrentHashMap.newKeySet();
      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final AtomicInteger conflictsReachingTheCaller = new AtomicInteger();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        final int seed = w;
        final Thread thread = new Thread(() -> {
          final Random random = new Random(seed);
          try {
            start.await();
            for (int t = 0; t < transactionsPerWriter; t++) {
              final int[][] pairs = new int[edgesPerTransaction][];
              for (int i = 0; i < edgesPerTransaction; i++)
                pairs[i] = new int[] { random.nextInt(vertices), random.nextInt(vertices) };
              for (int attempt = 0; ; attempt++) {
                final List<RID> created = new ArrayList<>(edgesPerTransaction);
                try {
                  database.transaction(() -> {
                    created.clear();
                    for (final int[] pair : pairs)
                      created.add(vertex(rids[pair[0]]).newEdge("E", vertex(rids[pair[1]])).getIdentity());
                  });
                  committed.addAll(created);
                  break;
                } catch (final NeedRetryException e) {
                  conflictsReachingTheCaller.incrementAndGet();
                  // a transaction the merges keep refusing is a regression, not contention to wait out
                  if (attempt == MAX_CALLER_RETRIES)
                    throw new AssertionError("a transaction still conflicts after " + MAX_CALLER_RETRIES + " retries", e);
                }
              }
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "writer-" + w);
        threads.add(thread);
        thread.start();
      }
      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " writer(s) failed, first: " + errors.getFirst(), errors.getFirst());

      assertThat(committed).hasSize(writers * transactionsPerWriter * edgesPerTransaction);
      assertEdgeListsHold(rids, committed);
    }

    /**
     * Writers creating edges between shared vertices and deleting some of the ones they committed, then a crash: what the merges
     * committed is what the WAL brings back, and every edge list still holds exactly the live edges.
     */
    @Test
    @Timeout(300)
    void concurrentInsertsAndDeletesSurviveACrash() throws Exception {
      final int vertices = 500;
      final int writers = 6;
      final int transactionsPerWriter = 20;
      final int edgesPerTransaction = 50;

      final RID[] rids = new RID[vertices];
      database.transaction(() -> {
        for (int i = 0; i < vertices; i++)
          rids[i] = database.newVertex("V").set("name", "v" + i).save().getIdentity();
      });

      // the creates minus the deletes committed on each RID, the RIDs at zero removed. Not a set of live edges: a deleted edge's slot is
      // reused by an edge another writer creates, and that writer can record its create before the deleting writer records its delete,
      // which a set would turn into a missing live edge. The commits of one RID alternate create and delete, so the sum ends at 1 for a
      // live edge and 0 for a deleted one whatever order the writers record them in
      final ConcurrentHashMap<RID, Integer> net = new ConcurrentHashMap<>();
      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        final int seed = 100 + w;
        final Thread thread = new Thread(() -> {
          final Random random = new Random(seed);
          final List<RID> mine = new ArrayList<>();
          try {
            start.await();
            for (int t = 0; t < transactionsPerWriter; t++) {
              final int[][] pairs = new int[edgesPerTransaction][];
              for (int i = 0; i < edgesPerTransaction; i++)
                pairs[i] = new int[] { random.nextInt(vertices), random.nextInt(vertices) };
              // every other transaction also deletes some of the edges this writer committed before
              final List<RID> toDelete = new ArrayList<>();
              if (t % 2 == 1)
                for (int i = 0; i < 10 && !mine.isEmpty(); i++)
                  toDelete.add(mine.remove(random.nextInt(mine.size())));

              for (int attempt = 0; ; attempt++) {
                final List<RID> created = new ArrayList<>(edgesPerTransaction);
                try {
                  database.transaction(() -> {
                    created.clear();
                    for (final int[] pair : pairs)
                      created.add(vertex(rids[pair[0]]).newEdge("E", vertex(rids[pair[1]])).getIdentity());
                    for (final RID edge : toDelete)
                      edge.asEdge(true).delete();
                  });
                  mine.addAll(created);
                  for (final RID edge : created)
                    net.merge(edge, 1, ConcurrentEdgeListMergeTest::sumOrRemove);
                  for (final RID edge : toDelete)
                    net.merge(edge, -1, ConcurrentEdgeListMergeTest::sumOrRemove);
                  break;
                } catch (final NeedRetryException e) {
                  if (attempt == MAX_CALLER_RETRIES)
                    throw new AssertionError("a transaction still conflicts after " + MAX_CALLER_RETRIES + " retries", e);
                }
              }
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "writer-" + w);
        threads.add(thread);
        thread.start();
      }
      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " writer(s) failed, first: " + errors.getFirst(), errors.getFirst());

      assertThat(net.values()).as("net creates of each RID still recorded").containsOnly(1);
      final Set<RID> live = new HashSet<>(net.keySet());
      assertThat(live).hasSize(writers * transactionsPerWriter * edgesPerTransaction - writers * (transactionsPerWriter / 2) * 10);
      assertEdgeListsHold(rids, live);

      // a crash: the database is opened again from what the WAL holds
      ((DatabaseInternal) database).kill();
      database.close();
      database = factory.open();
      assertEdgeListsHold(rids, live);
    }

    private static Integer sumOrRemove(final Integer current, final Integer delta) {
      final int sum = current + delta;
      return sum == 0 ? null : sum;
    }

    /** Every edge of {@code edges} is in the OUT list of its source and the IN list of its destination, once, and no other is. */
    private void assertEdgeListsHold(final RID[] rids, final Set<RID> edges) {
      database.transaction(() -> {
        assertThat(database.countType("E", false)).isEqualTo(edges.size());
        final Set<RID> fromOutLists = new HashSet<>();
        final Set<RID> fromInLists = new HashSet<>();
        long outEntries = 0;
        long inEntries = 0;
        for (final RID rid : rids) {
          for (final Edge e : vertex(rid).getEdges(Vertex.DIRECTION.OUT, "E")) {
            fromOutLists.add(e.getIdentity());
            assertThat(e.getOut()).isEqualTo(rid);
            ++outEntries;
          }
          for (final Edge e : vertex(rid).getEdges(Vertex.DIRECTION.IN, "E")) {
            fromInLists.add(e.getIdentity());
            assertThat(e.getIn()).isEqualTo(rid);
            ++inEntries;
          }
        }
        assertThat(outEntries).isEqualTo(edges.size());
        assertThat(inEntries).isEqualTo(edges.size());
        assertThat(fromOutLists).isEqualTo(edges);
        assertThat(fromInLists).isEqualTo(edges);
      });
    }

    /**
     * Runs {@code firstBefore} and {@code firstAfter} in one transaction on a thread of its own, and {@code second} in a transaction
     * committed on this thread between the two; then commits the first one.
     *
     * @return what the first transaction failed with, or null when it committed
     */
    private Throwable interleave(final Runnable firstBefore, final Runnable firstAfter, final Runnable second) throws Exception {
      final CountDownLatch firstStarted = new CountDownLatch(1);
      final CountDownLatch secondCommitted = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      final Thread first = new Thread(() -> {
        try {
          database.begin();
          try {
            if (firstBefore != null)
              firstBefore.run();
          } finally {
            firstStarted.countDown();
          }
          secondCommitted.await();
          firstAfter.run();
          database.commit();
        } catch (final Throwable e) {
          failure.set(e);
          if (database.isTransactionActive())
            database.rollback();
        }
      }, "first");
      first.start();
      firstStarted.await();
      try {
        database.transaction(second::run);
      } finally {
        secondCommitted.countDown();
        first.join();
      }
      return failure.get();
    }

    private static final Runnable NOTHING            = () -> {
    };
    private static final int      MAX_CALLER_RETRIES = 100;

    private RID headOf(final RID vertex) {
      return compute(() -> ((VertexInternal) vertex(vertex)).getOutEdgesHeadChunk());
    }

    private <T> T compute(final Supplier<T> supplier) {
      final AtomicReference<T> result = new AtomicReference<>();
      database.transaction(() -> result.set(supplier.get()));
      return result.get();
    }

    private RID newVertex(final String name) {
      return compute(() -> database.newVertex("V").set("name", name).save().getIdentity());
    }

    /** Through the database the test holds now, so a RID taken before a crash reads the reopened database. */
    private Vertex vertex(final RID rid) {
      return database.lookupByRID(rid, true).asVertex();
    }

    private List<RID> neighbours(final RID rid, final Vertex.DIRECTION direction) {
      final List<RID> result = new ArrayList<>();
      for (final Vertex v : vertex(rid).getVertices(direction, "E"))
        result.add(v.getIdentity());
      return result;
    }

    private PageManager.PPageManagerStats stats() {
      return ((DatabaseInternal) database).getPageManager().getStats();
    }
  }

  /**
   * Regression for #5147: concurrent edge insertion into the SAME super-node (hot) vertex must not drop
   * edges. The head chunk was read via an immutable lookup that (under READ_COMMITTED) did not retain the page,
   * and the page was only captured at the deferred {@code updateRecord} - at a newer version if a concurrent
   * append committed in between. The commit-time MVCC check then compared matching versions, found no conflict,
   * and the stale chunk buffer silently overwrote the concurrent append, so an edge record survived with no
   * back-reference from the hub (a lost update / dropped edge). {@code GraphEngine} now anchors the head chunk's
   * page in the transaction at read time, so the conflict is detected and the transaction retries.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("slow")
  class SuperNodeChunkRaceTest extends TestHelper {

    private static final int THREADS          = 8;
    private static final int EDGES_PER_THREAD = 4000;
    private static final int TOTAL            = THREADS * EDGES_PER_THREAD;

    @Test
    void concurrentAppendsToSuperNodeDoNotLoseEdges() throws Exception {
      final int savedRetryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
      GlobalConfiguration.TX_RETRY_DELAY.setValue(1);
      try {
        database.transaction(() -> {
          database.getSchema().createVertexType("Hub", 1);
          // Spread the source vertices / edge records across many buckets so their insertion is not the
          // bottleneck: the contention we exercise is the hub's single edge-list head chunk.
          database.getSchema().createVertexType("Src", 16);
          database.getSchema().createEdgeType("LINK", 16);
        });
        final MutableVertex[] hubHolder = new MutableVertex[1];
        database.transaction(() -> {
          final MutableVertex hub = database.newVertex("Hub");
          hub.save();
          hubHolder[0] = hub;
        });
        final RID hubRID = hubHolder[0].getIdentity();

        final AtomicLong committed = new AtomicLong();
        final AtomicLong failed = new AtomicLong();
        final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(THREADS);
        for (int t = 0; t < THREADS; t++) {
          new Thread(() -> {
            try {
              start.await();
              for (int i = 0; i < EDGES_PER_THREAD; i++) {
                database.transaction(() -> {
                  final MutableVertex src = database.newVertex("Src");
                  src.save();
                  src.newEdge("LINK", hubRID);
                }, false, 10_000);
                committed.incrementAndGet();
              }
            } catch (final Throwable e) {
              // A failure must FAIL the test, not silently kill this thread and hang done.await() forever.
              failed.incrementAndGet();
              firstFailure.compareAndSet(null, e);
            } finally {
              done.countDown();
            }
          }).start();
        }
        start.countDown();
        assertThat(done.await(5, TimeUnit.MINUTES)).as("writer threads did not finish in time").isTrue();

        assertThat(failed.get()).as("writer failures, first: " + firstFailure.get()).isZero();
        assertThat(committed.get()).isEqualTo(TOTAL);

        // Every committed edge must have its back-reference in the hub IN list (no lost update).
        final long[] inDegree = new long[1];
        database.transaction(() -> inDegree[0] = hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"));
        assertThat(inDegree[0]).isEqualTo(TOTAL);

        // And the integrity check must be clean (no missingReferenceBack). This is additionally enforced by the
        // TestHelper end-of-test "check database" assertion.
      } finally {
        GlobalConfiguration.TX_RETRY_DELAY.setValue(savedRetryDelay);
      }
    }
  }

  /**
   * Regression for #5155: the edge-removal chain-walk must anchor (fetch a mutable, tx-retained page for) ONLY the chunk it
   * actually modifies, not every read-only chunk it traverses on the way there. Removing an edge that lives in
   * the deepest chunk of a multi-page super-node must therefore touch the same tiny number of pages as removing
   * an edge that lives in the head chunk. Before this optimization the walk anchored every visited chunk via
   * page.modify(), so a deep removal copied the whole chain prefix's pages into the transaction for nothing
   * (those unwritten pages are pruned again before the commit-time version check - pure churn/GC). The
   * #5148/#5153 correctness guarantee (anchor the modified chunk before mutating it) is kept.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("slow")
  class EdgeRemovalAnchorTest extends TestHelper {

    private static final int EDGES      = 30_000; // enough IN edges on the hub to span several pages of 8KB chunks
    private static final int BATCH_SIZE = 1_000;

    @Override
    protected void beginTest() {
      // This white-box suite counts anchored pages while walking the CLASSIC single-chain layout (its stats
      // helper casts the head to EdgeSegment): pin super-node promotion off (#5156) so the 30K-edge hub does not
      // switch to the striped layout, whose per-chain removal inherits the same anchoring contract and is
      // covered by SuperNodeStripingTest. TestHelper's teardown resets the configuration.
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0);
    }

    /**
     * White-box: removing a deep edge must anchor no more pages than removing a head edge, and strictly fewer
     * pages than the whole chain spans - proving read-only hops are not anchored.
     */
    @Test
    void removalAnchorsOnlyModifiedChunk() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub", 1);
        database.getSchema().createVertexType("Src", 8);
        database.getSchema().createEdgeType("LINK", 8);
      });

      final RID[] hubHolder = new RID[1];
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("Hub");
        hub.save();
        hubHolder[0] = hub.getIdentity();
      });
      final RID hubRID = hubHolder[0];

      // Insert the edges in insertion order (batched for speed); keep the very first (oldest -> deepest/tail
      // chunk, walked last) and the very last (newest -> head chunk, walked first) edge RIDs.
      final RID[] firstEdge = new RID[1];
      final RID[] lastEdge = new RID[1];
      for (int b = 0; b < EDGES / BATCH_SIZE; b++) {
        final int base = b * BATCH_SIZE;
        database.transaction(() -> {
          for (int i = 0; i < BATCH_SIZE; i++) {
            final MutableVertex src = database.newVertex("Src");
            src.save();
            final RID edgeRID = src.newEdge("LINK", hubRID).getIdentity();
            if (base == 0 && i == 0)
              firstEdge[0] = edgeRID;
            lastEdge[0] = edgeRID;
          }
        });
      }

      // Precondition: the IN chain must really span more than one page, else the assertion below is vacuous.
      final int[] chainStats = chainPageStats(hubRID);
      final int chunkCount = chainStats[0];
      final int distinctPages = chainStats[1];
      assertThat(chunkCount).as("hub IN chain chunk count").isGreaterThanOrEqualTo(3);
      assertThat(distinctPages).as("hub IN chain distinct pages").isGreaterThanOrEqualTo(2);

      final int pagesHead = anchoredPagesForRemoval(hubRID, lastEdge[0]);
      final int pagesDeep = anchoredPagesForRemoval(hubRID, firstEdge[0]);

      // Head removal anchors exactly the head chunk's page.
      assertThat(pagesHead).as("pages anchored removing head edge").isEqualTo(1);
      // Deep removal walked the whole (multi-page) chain but anchors just the one chunk it modifies - no more
      // pages than the head removal, and strictly fewer than the chain spans.
      assertThat(pagesDeep).as("pages anchored removing deepest edge").isEqualTo(pagesHead);
      assertThat(pagesDeep).as("pages anchored removing deepest edge vs whole chain").isLessThan(distinctPages);
    }

    /**
     * Functional: removals at the head, deep in the chain, and of every edge must all stay exact and leave a
     * clean integrity check - the read-only-walk refactor must not drop or mis-link anything.
     */
    @Test
    void multiChunkRemovalStaysExactAndClean() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub", 1);
        database.getSchema().createVertexType("Src", 8);
        database.getSchema().createEdgeType("LINK", 8);
      });

      final RID[] hubHolder = new RID[1];
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("Hub");
        hub.save();
        hubHolder[0] = hub.getIdentity();
      });
      final RID hubRID = hubHolder[0];

      final int total = 2_000;
      final List<RID> edges = new ArrayList<>(total);
      for (int b = 0; b < total / BATCH_SIZE; b++) {
        database.transaction(() -> {
          for (int i = 0; i < BATCH_SIZE; i++) {
            final MutableVertex src = database.newVertex("Src");
            src.save();
            edges.add(src.newEdge("LINK", hubRID).getIdentity());
          }
        });
      }

      assertThat(inDegree(hubRID)).isEqualTo(total);

      // Remove the newest edge (head chunk).
      database.transaction(() -> edges.get(total - 1).asEdge().delete());
      // Remove the oldest edge (deepest chunk).
      database.transaction(() -> edges.get(0).asEdge().delete());
      // Remove a mid-chain edge.
      database.transaction(() -> edges.get(total / 2).asEdge().delete());

      assertThat(inDegree(hubRID)).isEqualTo(total - 3);

      // Drain everything that is left, exercising empty-chunk relink across the whole chain.
      for (int i = 1; i < total - 1; i++) {
        if (i == total / 2)
          continue;
        final RID e = edges.get(i);
        database.transaction(() -> e.asEdge().delete());
      }

      assertThat(inDegree(hubRID)).isEqualTo(0);
      assertIntegrityClean();
    }

    private long inDegree(final RID hubRID) {
      final long[] holder = new long[1];
      database.transaction(() -> holder[0] = hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"));
      return holder[0];
    }

    /** Walks the hub IN chunk chain (read-only) and returns {chunkCount, distinctPageCount}. */
    private int[] chainPageStats(final RID hubRID) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int[] stats = new int[2];
      db.transaction(() -> {
        final VertexInternal hub = (VertexInternal) hubRID.asVertex();
        RID chunkRID = hub.getInEdgesHeadChunk();
        final Set<Integer> pages = new HashSet<>();
        int chunks = 0;
        while (chunkRID != null) {
          final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(chunkRID.getBucketId());
          pages.add((int) (chunkRID.getPosition() / bucket.getMaxRecordsInPage()));
          chunks++;
          chunkRID = ((EdgeSegment) db.lookupByRID(chunkRID, true)).getPreviousRID();
        }
        stats[0] = chunks;
        stats[1] = pages.size();
      });
      return stats;
    }

    /**
     * Runs a single {@code removeEdgeRID} on the hub IN list inside an isolated transaction and returns how many
     * pages it anchored (modified/new pages delta). The transaction is rolled back so measurements are
     * independent and leave the data unchanged.
     */
    private int anchoredPagesForRemoval(final RID hubRID, final RID edgeRID) {
      final DatabaseInternal db = (DatabaseInternal) database;
      db.begin();
      try {
        final VertexInternal hub = (VertexInternal) hubRID.asVertex();
        final EdgeLinkedList inList = db.getGraphEngine().getEdgeHeadChunk(hub, Vertex.DIRECTION.IN);
        final int before = db.getTransaction().getModifiedPages();
        inList.removeEdgeRID(edgeRID);
        return db.getTransaction().getModifiedPages() - before;
      } finally {
        db.rollback();
      }
    }

    private void assertIntegrityClean() {
      try (final ResultSet rs = database.command("sql", "check database")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat(longProperty(row, "autoFix")).as("check database autoFix: " + row.toJSON()).isEqualTo(0L);
          assertThat(longProperty(row, "totalErrors")).as("check database totalErrors: " + row.toJSON()).isEqualTo(0L);
        }
      }
    }

    private static long longProperty(final Result row, final String name) {
      final Object value = row.getProperty(name);
      return value == null ? 0L : ((Number) value).longValue();
    }
  }

  /**
   * Regression for #5381: the "false conflict" (reinforcing #5279): concurrent
   * transactions that each write DIFFERENT records which happen to be co-located on the SAME bucket page collide with
   * a page-level {@link ConcurrentModificationException} even though the records are logically unrelated. The
   * disjoint-slot merge (TX_PAGE_SLOT_MERGE) replays each transaction's own slot writes on top of the newer committed page
   * instead of failing the whole transaction, and only when the concurrent commit did not touch the same record.
   */
  @Nested
  @Tag("slow")
  class FalseConflictTest extends TestHelper {
    private boolean savedSlotMerge;
    private int     savedThreshold;
    private int     savedStripes;
    private long    savedMaxBytes;

    @BeforeEach
    void saveConfig() {
      savedSlotMerge = GlobalConfiguration.TX_PAGE_SLOT_MERGE.getValueAsBoolean();
      savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
      savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
      savedMaxBytes = GlobalConfiguration.TX_PAGE_SLOT_MERGE_MAX_BYTES.getValueAsLong();
    }

    @AfterEach
    void restoreConfig() {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(savedSlotMerge);
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
      GlobalConfiguration.TX_PAGE_SLOT_MERGE_MAX_BYTES.setValue(savedMaxBytes);
    }

    /**
     * Pure disjoint-slot IN-PLACE UPDATE contention: N vertices packed onto a single bucket page, each updated by
     * exactly ONE thread (so no two threads ever touch the same record - every page conflict is provably false).
     * With attempts=1 the merge alone (no retry loop) must absorb the false conflicts, the merge counter must fire,
     * and every per-record value must be exactly its final write.
     */
    @Test
    void concurrentInPlaceUpdatesOfDistinctCoLocatedRecords() throws Exception {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);

      final int records = 12;
      final int updatesPerRecord = 500;

      // Fixed-width value so every update is a same-size in-place overwrite (like #5381's RID->RID head-pointer
      // flip) - the shape this test is about. Growth is covered by the merge too since #5279 and has its own tests.
      final RID[] rids = new RID[records];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Doc", 1).createProperty("tag", Type.STRING);
        for (int i = 0; i < records; i++) {
          final var doc = database.newDocument("Doc");
          doc.set("tag", String.format("%08d", 0));
          doc.save();
          rids[i] = doc.getIdentity();
        }
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      // Per-record last value this thread actually committed (single owner per record => no cross-thread races).
      final int[] lastCommitted = new int[records];
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      for (int t = 0; t < records; t++) {
        final RID rid = rids[t];
        final int idx = t;
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int i = 1; i <= updatesPerRecord; i++) {
              final String value = String.format("%08d", i);
              try {
                database.transaction(() -> rid.asDocument(true).modify().set("tag", value).save(), true, 1);
                lastCommitted[idx] = i;
              } catch (final ConcurrentModificationException ignore) {
                // absorbed-or-not is asserted below via the merge counter and exact final state, not a ratio.
              }
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "updater-" + t);
        threads.add(thread);
        thread.start();
      }

      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      // Feature actually worked: the merge fired at least once (0 would mean every false conflict was thrown).
      final long merges = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges;
      assertThat(merges).as("disjoint-slot update merge must fire").isGreaterThan(0);

      // Correctness, flake-free regardless of contention: each record holds EXACTLY its own last committed value
      // (no lost, torn, or cross-record write from a rebase). A single owner per record makes this exact.
      database.transaction(() -> {
        for (int i = 0; i < records; i++)
          assertThat(rids[i].asDocument(true).getString("tag")).isEqualTo(String.format("%08d", lastCommitted[i]));
      });
    }

    /**
     * Exercises the INSERT rebase path (trackRebasableInsert -> writeRecordAtSlot): one inserter thread appends
     * brand-new records into a single shared bucket page while several updater threads churn OTHER slots of that
     * same page in place. With attempts=1 the inserter can only keep committing if the merge replays its insert into
     * its still-free slot on the newer committed page; without it, every insert into the churning page would fail.
     */
    @Test
    void concurrentInsertIntoPageChurnedByOtherSlots() throws Exception {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);

      final int hubs = 8;          // pre-existing records that share page 0 and get churned in place
      final int inserts = 300;     // small records, all land on the reused shared page(s)
      final int updatesPerHub = 400;

      final RID[] hub = new RID[hubs];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Shared", 1).createProperty("tag", Type.STRING);
        for (int i = 0; i < hubs; i++) {
          final var d = database.newDocument("Shared");
          d.set("tag", String.format("%08d", 0));
          d.set("kind", "hub");
          d.save();
          hub[i] = d.getIdentity();
        }
      });

      final long mergesBefore = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges;
      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final AtomicInteger insertCme = new AtomicInteger();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      // Churn threads: repeated same-size in-place updates of distinct hubs -> constant version bumps on the page.
      for (int t = 0; t < hubs; t++) {
        final RID h = hub[t];
        final Thread churn = new Thread(() -> {
          try {
            start.await();
            for (int i = 1; i <= updatesPerHub; i++) {
              final String v = String.format("%08d", i);
              try {
                database.transaction(() -> h.asDocument(true).modify().set("tag", v).save(), true, 1);
              } catch (final ConcurrentModificationException ignore) {
                // not under test here
              }
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "churn-" + t);
        threads.add(churn);
      }

      // Single inserter: no insert-vs-insert self-collision, so its only conflicts are the churn threads' OTHER slots.
      final Thread inserter = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < inserts; i++) {
            final int n = i;
            try {
              database.transaction(() -> {
                final var d = database.newDocument("Shared");
                d.set("tag", String.format("%08d", n));
                d.set("kind", "leaf");
                d.save();
              }, true, 1);
            } catch (final ConcurrentModificationException e) {
              insertCme.incrementAndGet();
            }
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "inserter");
      threads.add(inserter);

      for (final Thread thread : threads)
        thread.start();
      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      final long merges = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges - mergesBefore;
      assertThat(merges).as("insert/update slot merges must fire on the churned shared page").isGreaterThan(0);

      // Insert-rebase proof, robust to CI load: with attempts=1 into a page churned by other slots, WITHOUT the
      // insert rebase nearly every insert would fail. Requiring a majority to commit is a wide, non-flaky bound that
      // still can only hold if writeRecordAtSlot is replaying the inserts.
      assertThat(insertCme.get()).as("most inserts into the churned page must be absorbed by the merge")
          .isLessThan(inserts / 2);

      // Every committed leaf must be present exactly once (no insert lost or duplicated by a rebase), hubs intact.
      database.transaction(() -> {
        assertThat(database.countType("Shared", false)).isEqualTo(hubs + inserts - insertCme.get());
        long leaves = 0;
        try (final var rs = database.query("SQL", "SELECT count(*) AS c FROM Shared WHERE kind = 'leaf'")) {
          leaves = rs.next().<Number>getProperty("c").longValue();
        }
        assertThat(leaves).isEqualTo(inserts - insertCme.get());
      });
    }

    /**
     * A record-GROWTH update (serialized size increases) on a page shared with other records. Since #5279 the merge
     * replays it as long as it stays inside the page, and falls back to a normal retry when the record has to spill
     * out of it. Several threads each grow their OWN co-located record, up to the point the page can no longer host
     * them: whichever of the two paths each step takes, every record must hold exactly its last committed value -
     * neither a merge onto a stale page nor a fallback may tear it.
     */
    @Test
    void growingUpdatesOnCoLocatedRecordsStayCorrect() throws Exception {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);

      final int records = 6;
      final int steps = 250;

      final RID[] rids = new RID[records];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Grow", 1).createProperty("tag", Type.STRING);
        for (int i = 0; i < records; i++) {
          final var doc = database.newDocument("Grow");
          doc.set("tag", "");
          doc.save();
          rids[i] = doc.getIdentity();
        }
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();
      for (int t = 0; t < records; t++) {
        final RID rid = rids[t];
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int i = 1; i <= steps; i++) {
              final String value = "x".repeat(i); // strictly growing -> record-growth path (merged, or spilled+retried)
              database.transaction(() -> rid.asDocument(true).modify().set("tag", value).save(), true, 50);
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "grow-" + t);
        threads.add(thread);
        thread.start();
      }
      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      // Correct final length proves every growth landed intact, whether it was merged or retried.
      database.transaction(() -> {
        for (int i = 0; i < records; i++)
          assertThat(rids[i].asDocument(true).getString("tag")).isEqualTo("x".repeat(steps));
      });
    }

    /**
     * With the per-transaction retention cap set tiny, the merge disables itself mid-transaction and every touched
     * page falls back to plain MVCC. This must stay correct: no data loss or corruption, values still consistent.
     */
    @Test
    void tinyRetentionCapFallsBackWithoutCorruption() throws Exception {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
      GlobalConfiguration.TX_PAGE_SLOT_MERGE_MAX_BYTES.setValue(1L); // disable after the very first tracked image

      final int records = 8;
      final int updatesPerRecord = 200;

      final RID[] rids = new RID[records];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Capped", 1).createProperty("tag", Type.STRING);
        for (int i = 0; i < records; i++) {
          final var doc = database.newDocument("Capped");
          doc.set("tag", String.format("%08d", 0));
          doc.save();
          rids[i] = doc.getIdentity();
        }
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();
      for (int t = 0; t < records; t++) {
        final RID rid = rids[t];
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int i = 1; i <= updatesPerRecord; i++) {
              final String value = String.format("%08d", i);
              // Retry here: with the merge capped off, contention resolves the old way.
              database.transaction(() -> rid.asDocument(true).modify().set("tag", value).save(), true, 50);
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "capped-" + t);
        threads.add(thread);
        thread.start();
      }
      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      database.transaction(() -> {
        for (int i = 0; i < records; i++)
          assertThat(rids[i].asDocument(true).getString("tag")).isEqualTo(String.format("%08d", updatesPerRecord));
      });
    }

    /**
     * A genuine SAME-record conflict must still raise ConcurrentModificationException: the merge must never silently
     * overwrite (lose) a concurrent update to the very record this transaction is updating.
     */
    @Test
    void concurrentUpdatesOfTheSameRecordStillConflict() throws Exception {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);

      final RID[] rid = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("One", 1).createProperty("n", Type.INTEGER);
        final var d = database.newDocument("One");
        d.set("n", 0);
        d.save();
        rid[0] = d.getIdentity();
      });

      // Our transaction reads the base and stages a write to record 'rid'.
      database.begin();
      final var a = rid[0].asDocument(true).modify();
      a.set("n", 100);
      a.save();

      // A competing transaction (another thread) commits a write to the SAME record while ours is still open.
      final Thread other = new Thread(() -> database.transaction(() -> rid[0].asDocument(true).modify().set("n", 200).save()));
      other.start();
      other.join();

      // Committing ours must raise a conflict (same record changed under us), NOT be merged away.
      assertThat(database.isTransactionActive()).isTrue();
      try {
        database.commit();
        throw new AssertionError("Expected a ConcurrentModificationException on a genuine same-record conflict");
      } catch (final ConcurrentModificationException expected) {
        // correct: the same-record conflict surfaced instead of being merged
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      // The competitor's write must be the one that survived.
      database.transaction(() -> assertThat(rid[0].asDocument(true).<Integer>getInteger("n")).isEqualTo(200));
    }

    /**
     * The #5381 shape end-to-end: several hot HUB vertices packed onto a single vertex bucket, each fed edges by its
     * own thread on the classic (non-striped) layout. The head-pointer flip that rewrites a hub vertex whenever its
     * head chunk fills makes DIFFERENT hubs on the same page collide falsely. Leaf/edge inserts use many buckets so
     * the only interesting contention is the head-flip false conflict. With the merge and a small retry budget the
     * workload must complete with EVERY edge preserved and the merge must have fired.
     */
    @Test
    void hotVertexEdgeAppendPreservesAllEdges() throws Exception {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0); // classic layout, as in a 26.7.x deployment

      final int hubs = 6;
      final int edgesPerHub = 500;

      database.transaction(() -> {
        database.getSchema().createVertexType("Hub", 1).createProperty("id", Type.INTEGER); // single bucket: hubs share pages
        database.getSchema().createVertexType("Leaf", 16);
        database.getSchema().createEdgeType("LINK", 16);
      });

      final RID[] hub = new RID[hubs];
      database.transaction(() -> {
        for (int i = 0; i < hubs; i++) {
          final MutableVertex v = database.newVertex("Hub");
          v.set("id", i);
          v.save();
          hub[i] = v.getIdentity();
        }
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      for (int t = 0; t < hubs; t++) {
        final RID h = hub[t];
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int i = 0; i < edgesPerHub; i++)
              database.transaction(() -> {
                final MutableVertex leaf = database.newVertex("Leaf");
                leaf.save();
                h.asVertex(true).newEdge("LINK", leaf);
              }, true, 10);
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "hub-" + t);
        threads.add(thread);
        thread.start();
      }

      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      final long merges = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges;
      assertThat(merges).as("head-flip false conflicts must be merged").isGreaterThan(0);

      database.transaction(() -> {
        long total = 0;
        for (int i = 0; i < hubs; i++) {
          final long c = hub[i].asVertex(true).countEdges(Vertex.DIRECTION.OUT, "LINK");
          assertThat(c).as("hub " + i + " must keep all its edges").isEqualTo(edgesPerHub);
          total += c;
        }
        assertThat(total).isEqualTo((long) hubs * edgesPerHub);
      });
    }

    /**
     * Regression for the multi-page corruption gap: a multi-page record's continuation chunks land on reused pages
     * via inline record-table writes that bypass the tracking hooks. If such a page also carried a tracked small
     * insert, an erroneous rebase would replay only the insert and drop the chunk, silently corrupting the large
     * record. The fix poisons every page a multi-page write touches. Here each transaction inserts BOTH a small
     * record and a large (multi-page) record into a single shared bucket under concurrency, so small inserts and
     * chunk writes co-locate and pages conflict; every large record must still read back intact.
     */
    @Test
    void multiPageRecordCoLocatedWithTrackedInsertStaysIntact() throws Exception {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);

      // Deterministic layout: a 16 KB page holds ~8 KB of usable content. Seed page 0 with a ~3 KB filler so it has
      // ~5 KB free. One transaction then, on page 0: inserts a small record (a TRACKED rebasable insert) and a large
      // record whose FIRST chunk fills a fresh page while its trailing chunk (~4 KB) reuses page 0's free space. A
      // competitor bumps page 0's version, forcing this transaction to conflict on page 0. Without the fix, page 0 is
      // still "rebasable" (only the small insert is tracked, the chunk write never poisoned it), so the rebase replays
      // the insert and silently drops the chunk - corrupting the large record. The fix poisons page 0 on the chunk
      // write, so the transaction cleanly retries and the large record stays intact.
      final int pageSize = 16 * 1024;
      final String bigPayload = "B".repeat(12_000);   // spans 2 pages; trailing chunk fits page 0's ~5 KB free
      final String filler = "F".repeat(3_000);

      final RID[] fillerRid = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("MP", 1, pageSize);
        final var f = database.newDocument("MP").set("kind", "filler").set("s", filler);
        f.save();
        fillerRid[0] = f.getIdentity();
      });

      final CountDownLatch inserted = new CountDownLatch(1);
      final CountDownLatch competitorDone = new CountDownLatch(1);
      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final RID[] bigRid = new RID[1];

      final Thread main = new Thread(() -> {
        try {
          for (int attempt = 0; attempt < 20; attempt++) {
            database.begin();
            database.newDocument("MP").set("kind", "small").set("s", "a").save(); // tracked insert on page 0
            final var big = database.newDocument("MP").set("kind", "big").set("blob", bigPayload);
            big.save(); // multi-page; the trailing chunk lands on page 0 alongside the tracked insert
            if (attempt == 0) {
              inserted.countDown();      // let the competitor bump page 0
              competitorDone.await();    // ...and wait until it has committed, so our commit conflicts on page 0
            }
            try {
              database.commit();
              bigRid[0] = big.getIdentity();
              break;
            } catch (final ConcurrentModificationException e) {
              database.rollback(); // expected WITH the fix on attempt 0: page 0 poisoned -> clean retry
            }
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "main");

      final Thread competitor = new Thread(() -> {
        try {
          inserted.await();
          // Same-size in-place update of the filler on page 0 -> bumps page 0's committed version.
          database.transaction(() -> fillerRid[0].asDocument(true).modify().set("s", filler).save(), true, 50);
        } catch (final Throwable e) {
          errors.add(e);
        } finally {
          competitorDone.countDown();
        }
      }, "competitor");

      main.start();
      competitor.start();
      main.join();
      competitor.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      // The large record must read back byte-for-byte: a dropped chunk would truncate it (or fail to deserialize).
      database.transaction(() -> {
        assertThat(bigRid[0]).as("the big record must have committed").isNotNull();
        assertThat(bigRid[0].asDocument(true).getString("blob")).isEqualTo(bigPayload);
      });
    }

    /**
     * A merged in-place update must keep secondary indexes consistent. Each record's INDEXED property is updated by
     * its own thread (distinct co-located records sharing a page => false conflicts absorbed by the merge). Because
     * the rebase preserves the RID and re-applies the same bytes the transaction computed, the index delete/insert
     * entries staged for that RID stay valid: every record must be findable by its final value via the index, with
     * no stale entries pointing at old values.
     */
    @Test
    void mergedInPlaceUpdateKeepsIndexConsistent() throws Exception {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);

      final int records = 10;
      final int updatesPerRecord = 300;

      final RID[] rids = new RID[records];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Idx", 1).createProperty("k", Type.STRING);
        database.command("SQL", "CREATE INDEX ON Idx (k) NOTUNIQUE");
        for (int i = 0; i < records; i++) {
          final var doc = database.newDocument("Idx");
          doc.set("k", String.format("%02d-%08d", i, 0)); // fixed width => same-size in-place updates
          doc.save();
          rids[i] = doc.getIdentity();
        }
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final int[] lastCommitted = new int[records];
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();
      for (int t = 0; t < records; t++) {
        final RID rid = rids[t];
        final int idx = t;
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int i = 1; i <= updatesPerRecord; i++) {
              final String v = String.format("%02d-%08d", idx, i); // globally unique per (record, iteration)
              try {
                database.transaction(() -> rid.asDocument(true).modify().set("k", v).save(), true, 1);
                lastCommitted[idx] = i;
              } catch (final ConcurrentModificationException ignore) {
                // fine; correctness is checked against the actually-committed value below
              }
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "idx-" + t);
        threads.add(thread);
        thread.start();
      }
      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      final long merges = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges;
      assertThat(merges).as("in-place update merge must fire").isGreaterThan(0);

      database.transaction(() -> {
        for (int i = 0; i < records; i++) {
          final String finalValue = String.format("%02d-%08d", i, lastCommitted[i]);

          // The record's stored value matches its last committed write...
          assertThat(rids[i].asDocument(true).getString("k")).isEqualTo(finalValue);

          // ...and the INDEX finds exactly that record by that value (no lost/duplicated index entry).
          long hits = 0;
          RID found = null;
          try (final var rs = database.query("SQL", "SELECT FROM Idx WHERE k = ?", finalValue)) {
            while (rs.hasNext()) {
              found = rs.next().getIdentity().orElse(null);
              hits++;
            }
          }
          assertThat(hits).as("record " + i + " must be found exactly once by index").isEqualTo(1);
          assertThat(found).isEqualTo(rids[i]);

          // And no stale index entry survives for a superseded value.
          if (lastCommitted[i] > 1)
            try (final var rs = database.query("SQL", "SELECT FROM Idx WHERE k = ?",
                String.format("%02d-%08d", i, lastCommitted[i] - 1))) {
              assertThat(rs.hasNext()).as("stale index value for record " + i + " must be gone").isFalse();
            }
        }
      });
    }

    /**
     * A DELETE on a page shared with other records, mixed with in-place updates of the co-located survivors. Since
     * #5569 the delete of a plain in-place record is itself a tracked single-slot write and merges like the others;
     * before that it poisoned the page and fell back to a retry. Either way the outcome asserted here is the same and
     * is the point of the test: the survivors keep their exact last committed value and every victim is gone, so
     * neither the merge nor the fallback ever mixes the two.
     */
    @Test
    void deleteOnSharedPageFallsBackAndStaysCorrect() throws Exception {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);

      final int pairs = 6;
      final int updates = 250;

      final RID[] survivor = new RID[pairs];
      final RID[] victim = new RID[pairs];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Mix", 1).createProperty("tag", Type.STRING);
        for (int i = 0; i < pairs; i++) {
          final var s = database.newDocument("Mix");
          s.set("tag", String.format("%08d", 0));
          s.set("role", "survivor");
          s.save();
          survivor[i] = s.getIdentity();
          final var v = database.newDocument("Mix");
          v.set("role", "victim");
          v.save();
          victim[i] = v.getIdentity();
        }
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final int[] lastCommitted = new int[pairs];
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      for (int t = 0; t < pairs; t++) {
        final RID s = survivor[t];
        final int idx = t;
        threads.add(new Thread(() -> {
          try {
            start.await();
            for (int i = 1; i <= updates; i++) {
              final String v = String.format("%08d", i);
              try {
                database.transaction(() -> s.asDocument(true).modify().set("tag", v).save(), true, 1);
                lastCommitted[idx] = i;
              } catch (final ConcurrentModificationException ignore) {
              }
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "survivor-" + t));

        final RID vic = victim[t];
        threads.add(new Thread(() -> {
          try {
            start.await();
            // Delete the co-located victim (poisons the shared page); retry until it commits.
            database.transaction(() -> vic.asDocument(true).delete(), true, 50);
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "deleter-" + t));
      }

      for (final Thread thread : threads)
        thread.start();
      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      database.transaction(() -> {
        // Survivors keep their exact last committed value; victims are gone; nothing corrupted.
        for (int i = 0; i < pairs; i++)
          assertThat(survivor[i].asDocument(true).getString("tag")).isEqualTo(String.format("%08d", lastCommitted[i]));
        assertThat(database.countType("Mix", false)).isEqualTo(pairs);
        try (final var rs = database.query("SQL", "SELECT count(*) AS c FROM Mix WHERE role = 'victim'")) {
          assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
        }
      });
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Slot merge vs edge-append merge on the SAME page (#5156 striping): since super-node striping, a segments
    // bucket file hosts TWO record kinds - edge-list segments (type 3, owned by the edge-append merge) and
    // StripeDirectory records (type 7, a slot-merge candidate). The two mechanisms are keyed by PAGE, so each must
    // decline any page carrying writes it cannot replay. These tests pin the two cross-mechanism collisions.
    // ---------------------------------------------------------------------------------------------------------------

    private void createStripedSchema() {
      database.transaction(() -> {
        // One Hub bucket => all Hub IN edge lists (segments AND directories) share ONE in_edges segments file, so
        // early records pack onto its first page. Distinct source/edge types per writer keep every OTHER file
        // conflict-free: the only contended page in these tests must be the shared segments page.
        database.getSchema().createVertexType("SHub", 1);
        database.getSchema().createVertexType("SSrcA", 1);
        database.getSchema().createVertexType("SSrcB", 1);
        database.getSchema().createEdgeType("SLinkA", 1);
        database.getSchema().createEdgeType("SLinkB", 1);
      });
    }

    private RID newHub() {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newVertex("SHub").save().getIdentity());
      return rid[0];
    }

    /** Appends one edge srcType -> hub (giving the hub an IN edge) in its own transaction. */
    private void appendInEdge(final RID hub, final String srcType, final String edgeType) {
      database.transaction(() -> {
        final MutableVertex src = database.newVertex(srcType).save();
        src.newEdge(edgeType, hub);
      });
    }

    private Object loadInHead(final RID hub) {
      final Object[] head = new Object[1];
      database.transaction(() -> {
        final RID headRID = ((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk();
        head[0] = headRID == null ? null : database.lookupByRID(headRID, true);
      });
      return head[0];
    }

    private long countIn(final RID hub, final String edgeType) {
      final long[] count = new long[1];
      database.transaction(() -> count[0] = hub.asVertex(true).countEdges(Vertex.DIRECTION.IN, edgeType));
      return count[0];
    }

    /** Asserts the two RIDs live on the same page of the same segments file (the co-location the tests rely on). */
    private void assertSamePage(final RID a, final RID b) {
      assertThat(a.getBucketId()).as("same segments bucket").isEqualTo(b.getBucketId());
      final int maxRecords = ((LocalBucket) database.getSchema().getBucketById(a.getBucketId())).getMaxRecordsInPage();
      assertThat(a.getPosition() / maxRecords).as("same segments page").isEqualTo(b.getPosition() / maxRecords);
    }

    /**
     * A {@link StripeDirectory} head flip (stripe chunk-full) is a tracked in-place slot update; a classic in-chunk
     * append on the SAME page is a type-3 segment update owned by the edge-append merge and thus INVISIBLE to the
     * slot map. When both happen in one transaction and the page conflicts, the slot merge must NOT re-derive the
     * page from the directory write alone - that would silently drop this transaction's appends. The segment update
     * must poison the slot map so the conflict falls back to a plain retry.
     */
    @Test
    void stripedDirectoryUpdateCoLocatedWithSegmentAppendFallsBack() throws Exception {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(1); // first chunk-full promotes
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(2);
      createStripedSchema();

      // C and C2: classic low-degree hubs whose head chunks land on the first page of the shared segments file.
      final RID c = newHub();
      final RID c2 = newHub();
      appendInEdge(c, "SSrcA", "SLinkA");
      appendInEdge(c2, "SSrcB", "SLinkB");

      // H: promoted in setup (threshold=1 -> the first chunk-full promotes). Its directory lands on the same page.
      final RID h = newHub();
      for (int i = 0; i < 40 && !(loadInHead(h) instanceof StripeDirectory); i++)
        appendInEdge(h, "SSrcA", "SLinkA");
      assertThat(loadInHead(h)).isInstanceOf(StripeDirectory.class);
      final long hSetupEdges = countIn(h, "SLinkA");

      final RID cHead = ((VertexInternal) c.asVertex(true)).getInEdgesHeadChunk();
      final RID dirRID = ((VertexInternal) h.asVertex(true)).getInEdgesHeadChunk();
      assertSamePage(cHead, dirRID);

      // Concurrent committer: bumps the shared page's version (C2's in-chunk append) while the main tx is open.
      final CountDownLatch mainTxWritesDone = new CountDownLatch(1);
      final CountDownLatch bumpCommitted = new CountDownLatch(1);
      final List<Throwable> bumpErrors = new CopyOnWriteArrayList<>();
      final Thread bumper = new Thread(() -> {
        try {
          mainTxWritesDone.await();
          database.transaction(() -> {
            final MutableVertex src = database.newVertex("SSrcB").save();
            src.newEdge("SLinkB", c2);
          });
        } catch (final Throwable e) {
          bumpErrors.add(e);
        } finally {
          bumpCommitted.countDown();
        }
      }, "bumper");
      bumper.start();

      final int hMainEdges = 30; // enough appends to fill at least one 64-byte stripe first-chunk -> directory flip
      database.transaction(() -> {
        for (int i = 0; i < hMainEdges; i++) {
          final MutableVertex src = database.newVertex("SSrcA").save();
          src.newEdge("SLinkA", h);
        }
        // The co-located classic in-chunk append the slot merge must not drop.
        final MutableVertex src = database.newVertex("SSrcA").save();
        src.newEdge("SLinkA", c);
        // Let the bumper commit on the shared page, then commit on top of it (idempotent across retries).
        mainTxWritesDone.countDown();
        try {
          bumpCommitted.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }, true, 3);
      bumper.join();

      if (!bumpErrors.isEmpty())
        throw new AssertionError("bumper failed: " + bumpErrors.getFirst(), bumpErrors.getFirst());

      // Content only (not the attempt count): however the conflict was resolved, no write may be lost.
      assertThat(countIn(c, "SLinkA")).as("classic in-chunk append co-located with the directory").isEqualTo(2);
      assertThat(countIn(h, "SLinkA")).as("striped hub keeps every appended edge").isEqualTo(hSetupEdges + hMainEdges);
      assertThat(countIn(c2, "SLinkB")).as("concurrent committer's append").isEqualTo(2);
    }

    /**
     * Same collision as above but at the PROMOTION moment: {@code EdgeLinkedList#tryPromoteToSuperNode} creates the
     * {@link StripeDirectory} in the SAME segments bucket as the classic chains, and the create is slot-TRACKED (a
     * rebasable insert). The directory create already poisons the EDGE-append merge for its page
     * (LocalDatabase.createRecord), so on a conflict the edge branch declines and the SLOT branch decides: without
     * the segment-update poison it would replay the directory insert alone, silently dropping this transaction's
     * co-located in-chunk append.
     * <p>
     * The promotion must be the transaction's FIRST chunk-full (a classic head-flip would poison the slot map via
     * its new-chunk create and mask the gap): the hub's first chunk is pre-filled EXACTLY to capacity in setup -
     * capacity probed empirically on a throwaway hub - so the single main-transaction append promotes directly.
     */
    @Test
    void promotionDirectoryCoLocatedWithTrackedAppendSurvivesConflict() throws Exception {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(2);
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0); // no promotion during setup
      createStripedSchema();

      final RID c = newHub();
      final RID c2 = newHub();
      appendInEdge(c, "SSrcA", "SLinkA");
      appendInEdge(c2, "SSrcB", "SLinkB");

      // Probe the first-chunk capacity on a throwaway hub: the append on which the head RID changes is the first
      // one that no longer fit, so capacity = flip position - 1. Runs with promotion off (threshold=0).
      final RID probe = newHub();
      final RID[] probeFirstHead = new RID[1];
      int capacity = -1;
      for (int i = 1; i <= 50; i++) {
        appendInEdge(probe, "SSrcA", "SLinkA");
        final RID head = ((VertexInternal) probe.asVertex(true)).getInEdgesHeadChunk();
        if (i == 1)
          probeFirstHead[0] = head;
        else if (!head.equals(probeFirstHead[0])) {
          capacity = i - 1;
          break;
        }
      }
      assertThat(capacity).as("probed first-chunk capacity").isGreaterThan(1);

      // H: fill its first chunk EXACTLY to capacity - the head must never flip during the fill.
      final RID h = newHub();
      RID hFirstHead = null;
      for (int i = 0; i < capacity; i++) {
        appendInEdge(h, "SSrcA", "SLinkA");
        final RID head = ((VertexInternal) h.asVertex(true)).getInEdgesHeadChunk();
        if (hFirstHead == null)
          hFirstHead = head;
        else
          assertThat(head).as("no head flip while filling exactly to capacity (layout drift otherwise)").isEqualTo(hFirstHead);
      }
      assertThat(loadInHead(h)).isNotInstanceOf(StripeDirectory.class);
      final long hSetupEdges = countIn(h, "SLinkA");
      assertThat(hSetupEdges).isEqualTo(capacity);

      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(1); // now the next chunk-full promotes immediately

      final CountDownLatch mainTxWritesDone = new CountDownLatch(1);
      final CountDownLatch bumpCommitted = new CountDownLatch(1);
      final List<Throwable> bumpErrors = new CopyOnWriteArrayList<>();
      final Thread bumper = new Thread(() -> {
        try {
          mainTxWritesDone.await();
          database.transaction(() -> {
            final MutableVertex src = database.newVertex("SSrcB").save();
            src.newEdge("SLinkB", c2);
          });
        } catch (final Throwable e) {
          bumpErrors.add(e);
        } finally {
          bumpCommitted.countDown();
        }
      }, "bumper");
      bumper.start();

      database.transaction(() -> {
        // The co-located classic in-chunk append: the ONLY tracked edge-append on the shared page, and it replays
        // cleanly - so a pre-fix edge rebase of that page succeeds and silently drops the directory record.
        final MutableVertex srcC = database.newVertex("SSrcA").save();
        srcC.newEdge("SLinkA", c);
        // Single append to the exactly-full hub: chunk-full -> immediate promotion (threshold=1), no head-flip, no
        // tracked append on H's own chunk. The directory is created on the shared segments page.
        final MutableVertex srcH = database.newVertex("SSrcA").save();
        srcH.newEdge("SLinkA", h);
        final RID headRID = ((VertexInternal) h.asVertex(true)).getInEdgesHeadChunk();
        assertThat(database.lookupByRID(headRID, true))
            .as("the single append must have promoted the exactly-full hub (layout drift otherwise)")
            .isInstanceOf(StripeDirectory.class);
        // Let the bumper commit on the shared page, then commit on top of it (idempotent across retries).
        mainTxWritesDone.countDown();
        try {
          bumpCommitted.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }, true, 3);
      bumper.join();

      if (!bumpErrors.isEmpty())
        throw new AssertionError("bumper failed: " + bumpErrors.getFirst(), bumpErrors.getFirst());

      // The vertex head must be a READABLE directory (a dropped directory record leaves a dangling head)...
      assertThat(loadInHead(h)).isInstanceOf(StripeDirectory.class);
      // ...and no write may be lost, on any of the three lists.
      assertThat(countIn(h, "SLinkA")).as("promoted hub keeps every appended edge").isEqualTo(hSetupEdges + 1);
      assertThat(countIn(c, "SLinkA")).as("classic in-chunk append co-located with the directory").isEqualTo(2);
      assertThat(countIn(c2, "SLinkB")).as("concurrent committer's append").isEqualTo(2);
    }
  }

  /**
   * Regression for #5596: both commit-time page merges - the commutative edge-append merge and the disjoint-slot merge - drop
   * this transaction's whole image of a conflicting page and replay only its tracked writes on top of the newer
   * committed version. That is sound ONLY if every byte the transaction wrote to that page belongs to a tracked write.
   * Before this fix the invariant was maintained by hand, through {@code poisonEdgeAppendPage}/
   * {@code poisonSlotRebasePage} calls scattered across the writers: a writer that forgot one committed a page from
   * which its own change had silently vanished - a lost write on the leader, replicated faithfully to every follower.
   * <p>
   * The merges now have to PROVE their coverage: a page is re-derivable only when every modification was made inside a
   * declaration naming that merge ({@code MutablePage.beginCoveredWrite}). These tests use the synthetic writer the
   * issue asks for - a transaction that dirties bytes belonging to no tracked write at all - once against each merge,
   * so they hold no matter which poison calls exist, and pin that the merges still fire on a fully covered page.
   * <p>
   * DELIBERATELY NOT {@code @Tag("slow")}, unlike the contention suites it sits next to
   * ({@code ConcurrentEdgeWriteRegressionTest.FalseConflictTest}, {@code EdgeAppendMergeRaceTest}, ...). Those run thousands of transactions;
   * these are sized to the smallest contention that reproduces, and the whole class measures ~0.37s (heaviest method
   * 0.19s; the 16-thread growth/delete one 0.04s) - well inside a regular CI run. It belongs there: a merge that stops
   * firing because a writer forgot its declaration is a silent throughput regression no correctness assertion catches,
   * so the merge-counter and coverage-decline assertions below have to run on every build to be worth having.
   * Re-measure before tagging this class, rather than tagging it by family resemblance.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MergeCoverageTest extends TestHelper {
    private static final String MARKER = "ZZZZZZZZ";
    private static final String POKED  = "QQQQQQQQ";

    private boolean savedEdgeMerge;
    private boolean savedSlotMerge;
    private int     savedThreshold;

    @BeforeEach
    void saveConfig() {
      savedEdgeMerge = GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.getValueAsBoolean();
      savedSlotMerge = GlobalConfiguration.TX_PAGE_SLOT_MERGE.getValueAsBoolean();
      savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
      GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.setValue(true);
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0); // classic (non-striped) edge lists
    }

    @AfterEach
    void restoreConfig() {
      GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.setValue(savedEdgeMerge);
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(savedSlotMerge);
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    }

    /**
     * Three hubs of one bucket, so their OUT edge-list head chunks share the first page of the single shared
     * {@code Hub_out_edges} segments file. Distinct leaf/edge types per writer keep every OTHER file uncontended.
     */
    private void createSchema() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub", 1);
        database.getSchema().createVertexType("LeafA", 1);
        database.getSchema().createVertexType("LeafC", 1);
        database.getSchema().createEdgeType("LinkA", 1);
        database.getSchema().createEdgeType("LinkC", 1);
      });
    }

    private RID newHubWithOneEdge(final String leafType, final String edgeType) {
      final RID[] rid = new RID[1];
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("Hub").save();
        final MutableVertex leaf = database.newVertex(leafType).save();
        hub.newEdge(edgeType, leaf);
        rid[0] = hub.getIdentity();
      });
      return rid[0];
    }

    private RID outHeadChunk(final RID hub) {
      final RID[] chunk = new RID[1];
      database.transaction(() -> chunk[0] = ((VertexInternal) hub.asVertex(true)).getOutEdgesHeadChunk());
      return chunk[0];
    }

    /** Page merges declined because the page could not prove its coverage (#5596). */
    private long declinedByCoverage() {
      return ((DatabaseInternal) database).getPageManager().getStats().mergesDeclinedByCoverage;
    }

    private void assertSamePage(final RID a, final RID b) {
      assertThat(a.getBucketId()).as("same segments bucket").isEqualTo(b.getBucketId());
      final int maxRecords = ((LocalBucket) database.getSchema().getBucketById(a.getBucketId())).getMaxRecordsInPage();
      assertThat(a.getPosition() / maxRecords).as("same segments page").isEqualTo(b.getPosition() / maxRecords);
    }

    /**
     * Creates, in its own committed transaction, an edge-list chunk no vertex points to, carrying {@code marker} in
     * its otherwise unused tail. It lands on the same segments page as the hubs' head chunks, so the test has bytes it
     * can poke there without breaking any edge list.
     */
    private RID createOrphanChunk(final int segmentsBucketId, final String marker) {
      final RID[] rid = new RID[1];
      final String bucketName = database.getSchema().getBucketById(segmentsBucketId).getName();
      database.transaction(() -> {
        final MutableEdgeSegment orphan = new MutableEdgeSegment((DatabaseInternal) database, 128);
        // Straight into the chunk's unused tail: no edge ever occupies it, so the poke below cannot corrupt a list.
        orphan.getBuffer().putByteArray(64, marker.getBytes());
        ((DatabaseInternal) database).createRecord(orphan, bucketName);
        rid[0] = orphan.getIdentity();
      });
      return rid[0];
    }

    /** Reads the marker back from the COMMITTED page, so a merged-away write cannot hide behind a cached record. */
    private String readMarker(final PageId pageId, final int pageSize) {
      final String[] value = new String[1];
      database.transaction(() -> {
        try {
          final BasePage page = ((DatabaseInternal) database).getTransaction().getPage(pageId, pageSize);
          final byte[] found = new byte[MARKER.length()];
          final int offset = findMarkerOffset(page, POKED.getBytes());
          page.readByteArray(offset >= 0 ? offset : findMarkerOffset(page, MARKER.getBytes()), found);
          value[0] = new String(found);
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      });
      return value[0];
    }

    /**
     * The synthetic writer the issue asks for, on the edge-append side: a transaction registers a tracked in-chunk
     * append AND dirties bytes of a co-located record on the same segments page that no tracked append accounts for -
     * standing in for the inline record-table writes of the multi-page writers, a bulk load, or any writer that does
     * not know the merge exists. A competitor then bumps the shared page's version.
     * <p>
     * Without the coverage proof the page still looks rebasable from the tracked append alone: the merge reloads the
     * committed page, replays the append and commits - those bytes are gone with no error at all. With the proof the
     * merge declines, the transaction retries, and the write lands. Deliberately independent of the poison calls: an
     * undeclared writer is refused whether or not anybody remembered to exclude it.
     */
    @Test
    void anUndeclaredPageWriteIsNeverEdgeMergedAway() throws Exception {
      createSchema();

      final RID hubA = newHubWithOneEdge("LeafA", "LinkA");   // the tracked in-chunk append
      final RID hubC = newHubWithOneEdge("LeafC", "LinkC");   // the competitor's append

      final RID chunkA = outHeadChunk(hubA);
      final RID chunkC = outHeadChunk(hubC);
      // A chunk no vertex points to, so poking its bytes cannot break the graph: the marker is written into its
      // otherwise unused tail, and read back raw.
      final RID chunkB = createOrphanChunk(chunkA.getBucketId(), MARKER);
      assertSamePage(chunkA, chunkB);
      assertSamePage(chunkA, chunkC);

      final LocalBucket segments = (LocalBucket) database.getSchema().getBucketById(chunkA.getBucketId());
      final PageId pageId = new PageId(database, segments.getFileId(),
          (int) (chunkA.getPosition() / segments.getMaxRecordsInPage()));

      final long declinedBefore = declinedByCoverage();
      final CountDownLatch mainTxWritesDone = new CountDownLatch(1);
      final CountDownLatch bumpCommitted = new CountDownLatch(1);
      final List<Throwable> errors = new CopyOnWriteArrayList<>();

      // Competitor: an in-chunk append to hubC, i.e. a plain version bump of the shared segments page.
      final Thread bumper = new Thread(() -> {
        try {
          mainTxWritesDone.await();
          database.transaction(() -> {
            final MutableVertex leaf = database.newVertex("LeafC").save();
            hubC.asVertex(true).newEdge("LinkC", leaf);
          }, true, 50);
        } catch (final Throwable e) {
          errors.add(e);
        } finally {
          bumpCommitted.countDown();
        }
      }, "bumper");
      bumper.start();

      boolean committed = false;
      for (int attempt = 0; attempt < 20 && !committed; attempt++) {
        database.begin();
        try {
          final MutableVertex leaf = database.newVertex("LeafA").save();
          hubA.asVertex(true).newEdge("LinkA", leaf);   // TRACKED append on the shared page

          // The UNDECLARED writer: bytes on the very same page that no tracked append accounts for.
          final MutablePage page = ((DatabaseInternal) database).getTransaction()
              .getPageToModify(pageId, segments.getPageSize(), false);
          final int markerOffset = findMarker(page);
          assertThat(markerOffset).as("the marker must be locatable on the segments page").isGreaterThan(0);
          page.writeByteArray(markerOffset, POKED.getBytes());

          if (attempt == 0) {
            mainTxWritesDone.countDown();
            bumpCommitted.await();
          }
          database.commit();
          committed = true;
        } catch (final ConcurrentModificationException expected) {
          // WITH the fix this is the outcome of attempt 0: the merge refuses the page and the transaction retries.
          if (database.isTransactionActive())
            database.rollback();
        }
      }
      bumper.join();

      if (!errors.isEmpty())
        throw new AssertionError("competitor failed: " + errors.getFirst(), errors.getFirst());

      assertThat(committed).as("the transaction must eventually commit").isTrue();

      // The heart of the issue: an edge-merged page would have discarded these bytes without a word.
      assertThat(readMarker(pageId, segments.getPageSize())).as("the undeclared write must not have been merged away")
          .isEqualTo(POKED);

      // ...and the coverage proof is what refused the page, not some unrelated conflict.
      assertThat(declinedByCoverage() - declinedBefore).as("the merge must have been declined for lack of coverage")
          .isGreaterThan(0);

      // ...and neither writer's append may be lost either.
      database.transaction(() -> {
        assertThat(hubA.asVertex(true).countEdges(Vertex.DIRECTION.OUT, "LinkA")).isEqualTo(2);
        assertThat(hubC.asVertex(true).countEdges(Vertex.DIRECTION.OUT, "LinkC")).isEqualTo(2);
      });
    }

    /**
     * The other half of the guard: the coverage proof must not be so tight that it refuses everything. The same
     * shared-page contention WITHOUT the undeclared writer must still be absorbed by the merge - the counter has to
     * move, and every edge has to survive.
     * <p>
     * The conflict is forced, not hoped for: two free-running writers of a few dozen tiny transactions each need not
     * overlap at all (a writer can be done before the other is even scheduled, which a loaded CI runner did), and then
     * nothing conflicts and nothing merges. Each round, the competitor commits its append to the shared page while this
     * transaction holds its own append to the same page uncommitted, exactly the shape the tests above use.
     */
    @Test
    void aCleanAppendConflictIsStillMerged() throws Exception {
      createSchema();

      final RID hubA = newHubWithOneEdge("LeafA", "LinkA");
      final RID hubC = newHubWithOneEdge("LeafC", "LinkC");
      assertSamePage(outHeadChunk(hubA), outHeadChunk(hubC));

      final long mergesBefore = ((DatabaseInternal) database).getPageManager().getStats().edgeAppendMerges;
      final long declinedBefore = declinedByCoverage();

      final int rounds = 40;
      try (final ExecutorService competitor = Executors.newSingleThreadExecutor()) {
        for (int i = 0; i < rounds; i++) {
          boolean committed = false;
          for (int attempt = 0; attempt < 50 && !committed; attempt++) {
            database.begin();
            try {
              final MutableVertex leaf = database.newVertex("LeafA").save();
              hubA.asVertex(true).newEdge("LinkA", leaf);

              if (attempt == 0)
                // The competitor's append commits in full, on its own thread, while this one is still pending
                competitor.submit(() -> database.transaction(() -> {
                  final MutableVertex other = database.newVertex("LeafC").save();
                  hubC.asVertex(true).newEdge("LinkC", other);
                }, true, 50)).get();

              database.commit();
              committed = true;
            } catch (final ConcurrentModificationException e) {
              // A round whose append could not be replayed (a new chunk, not an in-chunk append) retries as a plain one
              if (database.isTransactionActive())
                database.rollback();
            }
          }
          assertThat(committed).as("round " + i + " must eventually commit").isTrue();
        }
      }

      final long merges = ((DatabaseInternal) database).getPageManager().getStats().edgeAppendMerges - mergesBefore;
      assertThat(merges).as("the edge-append merge must still fire on a fully covered page").isGreaterThan(0);
      // The load-bearing half: a writer that forgot its declaration would show up HERE, as declines instead of merges.
      assertThat(declinedByCoverage() - declinedBefore)
          .as("an append-only page must never be declined for lack of coverage").isZero();

      database.transaction(() -> {
        assertThat(hubA.asVertex(true).countEdges(Vertex.DIRECTION.OUT, "LinkA")).isEqualTo(1 + rounds);
        assertThat(hubC.asVertex(true).countEdges(Vertex.DIRECTION.OUT, "LinkC")).isEqualTo(1 + rounds);
      });
    }

    /**
     * The same over-tightening guard for the disjoint-slot merge, on the two shapes whose declarations are the easiest
     * to get wrong because they write far more than one record's bytes: a record GROWTH (which shifts the records that
     * follow and rewrites their slot-table offsets) and a plain record DELETE (#5569). Both are fully declared, so on a
     * page several writers contend for the merge must keep firing and NOTHING may be declined for lack of coverage - a
     * missing declaration on either would silently turn absorbed contention back into retries, which no correctness
     * assertion would notice.
     */
    @Test
    void cleanGrowthAndDeleteConflictsAreStillMerged() throws Exception {
      final int records = 8;
      final int steps = 60;

      final RID[] grow = new RID[records];
      final RID[] victim = new RID[records];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Mixed", 1);
        for (int i = 0; i < records; i++) {
          grow[i] = database.newDocument("Mixed").set("role", "grow").set("tag", "").save().getIdentity();
          victim[i] = database.newDocument("Mixed").set("role", "victim").save().getIdentity();
        }
      });

      final long mergesBefore = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges;
      final long declinedBefore = declinedByCoverage();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final List<Thread> threads = new java.util.ArrayList<>();

      for (int t = 0; t < records; t++) {
        final RID g = grow[t];
        threads.add(new Thread(() -> {
          try {
            start.await();
            for (int i = 1; i <= steps; i++) {
              final String value = "x".repeat(i); // strictly growing -> the in-page growth declaration
              database.transaction(() -> g.asDocument(true).modify().set("tag", value).save(), true, 50);
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "grow-" + t));

        final RID v = victim[t];
        threads.add(new Thread(() -> {
          try {
            start.await();
            database.transaction(() -> v.asDocument(true).delete(), true, 50);
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "delete-" + t));
      }

      for (final Thread thread : threads)
        thread.start();
      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      final long merges = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges - mergesBefore;
      assertThat(merges).as("growth and delete slot merges must still fire on fully covered pages").isGreaterThan(0);
      assertThat(declinedByCoverage() - declinedBefore)
          .as("a page of declared growths and deletes must never be declined").isZero();

      // And the writes themselves are intact: every grower reached its final length, every victim is gone.
      database.transaction(() -> {
        for (int i = 0; i < records; i++)
          assertThat(grow[i].asDocument(true).getString("tag")).isEqualTo("x".repeat(steps));
        assertThat(database.countType("Mixed", false)).isEqualTo(records);
      });
    }

    /**
     * The disjoint-slot merge half of the same guard, with the synthetic writer the issue asks for: a transaction
     * makes ONE tracked in-place record update on a page and then dirties bytes that belong to no tracked slot write
     * at all (here, inside the content of a co-located record - a stand-in for the inline record-table writes of the
     * multi-page writers, or for any future writer that does not know the merge exists). A competitor bumps the page's
     * version. Replaying the tracked update alone on the newer committed page would silently discard those bytes; the
     * coverage proof refuses the page instead, so the transaction retries and the write survives.
     */
    @Test
    void anUndeclaredPageWriteIsNeverSlotMergedAway() throws Exception {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Poke", 1);
        database.newDocument("Poke").set("role", "tracked").set("tag", "00000000").save();
        database.newDocument("Poke").set("role", "victim").set("marker", MARKER).save();
        database.newDocument("Poke").set("role", "bumped").set("tag", "00000000").save();
      });

      final RID tracked = ridOf("tracked");
      final RID bumped = ridOf("bumped");
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(tracked.getBucketId());
      final PageId pageId = new PageId(database, bucket.getFileId(),
          (int) (tracked.getPosition() / bucket.getMaxRecordsInPage()));
      assertThat(bumped.getPosition() / bucket.getMaxRecordsInPage()).as("all three records must share one page")
          .isEqualTo(tracked.getPosition() / bucket.getMaxRecordsInPage());

      final long declinedBefore = declinedByCoverage();
      final CountDownLatch mainTxWritesDone = new CountDownLatch(1);
      final CountDownLatch bumpCommitted = new CountDownLatch(1);
      final List<Throwable> errors = new CopyOnWriteArrayList<>();

      // Competitor: a same-size in-place update of ANOTHER slot on the page -> a false page conflict.
      final Thread bumper = new Thread(() -> {
        try {
          mainTxWritesDone.await();
          database.transaction(() -> bumped.asDocument(true).modify().set("tag", "99999999").save(), true, 50);
        } catch (final Throwable e) {
          errors.add(e);
        } finally {
          bumpCommitted.countDown();
        }
      }, "bumper");
      bumper.start();

      boolean committed = false;
      for (int attempt = 0; attempt < 20 && !committed; attempt++) {
        database.begin();
        try {
          // The TRACKED write: a same-size in-place overwrite of one record.
          tracked.asDocument(true).modify().set("tag", "11111111").save();

          // The UNDECLARED write: bytes on the same page that no tracked slot write accounts for.
          final MutablePage page = ((DatabaseInternal) database).getTransaction().getPageToModify(pageId, bucket.getPageSize(), false);
          final int markerOffset = findMarker(page);
          assertThat(markerOffset).as("the marker must be locatable on the page").isGreaterThan(0);
          page.writeByteArray(markerOffset, POKED.getBytes());

          if (attempt == 0) {
            mainTxWritesDone.countDown();
            bumpCommitted.await();
          }
          database.commit();
          committed = true;
        } catch (final ConcurrentModificationException expected) {
          // WITH the fix: the merge declines the page because of the undeclared bytes, and we retry.
          if (database.isTransactionActive())
            database.rollback();
        }
      }
      bumper.join();

      if (!errors.isEmpty())
        throw new AssertionError("competitor failed: " + errors.getFirst(), errors.getFirst());

      assertThat(committed).as("the transaction must eventually commit").isTrue();

      database.transaction(() -> {
        assertThat(ridOf("victim").asDocument(true).getString("marker")).as("the undeclared write must not be merged away")
            .isEqualTo(POKED);
        assertThat(declinedByCoverage() - declinedBefore).as("the merge must have been declined for lack of coverage")
            .isGreaterThan(0);
        assertThat(tracked.asDocument(true).getString("tag")).isEqualTo("11111111");
        assertThat(bumped.asDocument(true).getString("tag")).isEqualTo("99999999");
      });
    }

    private RID ridOf(final String role) {
      final RID[] rid = new RID[1];
      database.transaction(() -> {
        try (final var rs = database.query("SQL", "SELECT FROM Poke WHERE role = ?", role)) {
          rid[0] = rs.next().getIdentity().orElseThrow();
        }
      });
      return rid[0];
    }

    /** Locates the marker string inside the page content, so the poke lands on real record bytes. */
    private int findMarker(final BasePage page) {
      return findMarkerOffset(page, MARKER.getBytes());
    }

    private int findMarkerOffset(final BasePage page, final byte[] needle) {
      for (int i = 0; i <= page.getContentSize() - needle.length; i++) {
        int k = 0;
        while (k < needle.length && page.readByte(i + k) == needle[k])
          ++k;
        if (k == needle.length)
          return i;
      }
      return -1;
    }
  }
}
