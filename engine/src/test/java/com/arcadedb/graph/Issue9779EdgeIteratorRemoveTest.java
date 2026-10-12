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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #9779: {@code Iterator.remove()} on the iterators a vertex hands out over its edge lists.
 * <ul>
 * <li>the untyped edge and entry iterators deleted the edge inside a {@code catch (Exception)} that logged a refused
 * delete and then unlinked the entry anyway: a LIGHTWEIGHT edge the user may not delete was gone from the source and
 * still listed on the target, a record-backed one survived reachable from one endpoint only;</li>
 * <li>the TYPED iterators ({@code getEdges(OUT, "T")}, {@code getVertices(OUT, "T")}) did not delete anything: they
 * unlinked the entry from the walked list only, with no {@code DELETE_RECORD} check at all, leaving the edge record and
 * the far endpoint's entry behind;</li>
 * <li>a second {@code remove()} on the untyped iterators dropped a SIBLING edge: the delete removed the entry from the
 * walked list by RID and the iterator then removed, by position, the entry that had moved into its place.</li>
 * </ul>
 * The user below resolves permissions as {@code ServerSecurityDatabaseUser} does (see
 * {@code Issue9619LightweightEdgeWriteGrantsTest}): only the edge type's buckets are refused, the vertex type stays writable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9779EdgeIteratorRemoveTest {
  private static final String PATH = "target/databases/Issue9779EdgeIteratorRemoveTest";

  /** Every iterator over one vertex's OUT list that supports {@code remove()}. */
  enum Walk {
    EDGES((v, type) -> v.getEdges(Vertex.DIRECTION.OUT).iterator()),
    TYPED_EDGES((v, type) -> v.getEdges(Vertex.DIRECTION.OUT, type).iterator()),
    VERTICES((v, type) -> v.getVertices(Vertex.DIRECTION.OUT).iterator()),
    TYPED_VERTICES((v, type) -> v.getVertices(Vertex.DIRECTION.OUT, type).iterator()),
    ENTRIES((v, type) -> outList(v).entryIterator()),
    TYPED_ENTRIES((v, type) -> outList(v).entryIterator(type));

    private final BiFunction<Vertex, String, Iterator<?>> open;

    Walk(final BiFunction<Vertex, String, Iterator<?>> open) {
      this.open = open;
    }

    Iterator<?> open(final Vertex vertex, final String type) {
      return open.apply(vertex, type);
    }

    private static EdgeLinkedList outList(final Vertex v) {
      return ((DatabaseInternal) v.getDatabase()).getGraphEngine().getEdgeHeadChunk((VertexInternal) v, Vertex.DIRECTION.OUT);
    }
  }

  private DatabaseFactory factory;
  private Database        database;
  private RID             a;
  private RID[]           targets;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE LW LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE HE");

    database.transaction(() -> {
      a = database.newVertex("V").set("id", "a").save().getIdentity();
      targets = new RID[3];
      for (int i = 0; i < targets.length; i++)
        targets[i] = database.newVertex("V").set("id", "t" + i).save().getIdentity();
    });
  }

  @AfterEach
  void tearDown() {
    unbindUser();
    if (database.isTransactionActive())
      database.rollback();
    database.drop();
    factory.close();
  }

  // ---------------------------------------------------------------- A REFUSED DELETE CHANGES NOTHING

  @ParameterizedTest
  @EnumSource(Walk.class)
  void refusedRemoveOfALightweightEdgeReachesTheCallerAndKeepsBothSides(final Walk walk) {
    connect("LW", 1);
    bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "LW");

    database.begin();
    final Iterator<?> it = walk.open(vertex(a), "LW");
    it.next();
    assertThat(catchThrowable(it::remove)).isInstanceOf(SecurityException.class);
    database.commit();

    unbindUser();
    assertConnected("LW", 1);
  }

  @ParameterizedTest
  @EnumSource(Walk.class)
  void refusedRemoveOfARecordBackedEdgeReachesTheCallerAndKeepsBothSides(final Walk walk) {
    connect("HE", 1);
    bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "HE");

    database.begin();
    final Iterator<?> it = walk.open(vertex(a), "HE");
    it.next();
    assertThat(catchThrowable(it::remove)).isInstanceOf(SecurityException.class);
    database.commit();

    unbindUser();
    assertConnected("HE", 1);
  }

  /** The issue's own repro: the state survives a close and reopen. */
  @Test
  void refusedRemoveLeavesNothingBehindAcrossAReopen() {
    connect("LW", 1);
    connect("HE", 1);
    bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "LW");
    database.begin();
    final Iterator<Edge> lw = vertex(a).getEdges(Vertex.DIRECTION.OUT, "LW").iterator();
    lw.next();
    assertThat(catchThrowable(lw::remove)).isInstanceOf(SecurityException.class);
    database.commit();
    unbindUser();

    bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "HE");
    database.begin();
    final Iterator<Edge> he = vertex(a).getEdges(Vertex.DIRECTION.OUT, "HE").iterator();
    he.next();
    assertThat(catchThrowable(he::remove)).isInstanceOf(SecurityException.class);
    database.commit();
    unbindUser();

    database.close();
    database = factory.open();

    assertConnected("LW", 1);
    assertConnected("HE", 1);
  }

  // ---------------------------------------------------------------- A GRANTED REMOVE DELETES EXACTLY THE EDGE RETURNED

  @ParameterizedTest
  @EnumSource(Walk.class)
  void twoConsecutiveRemovesOfLightweightEdgesDeleteExactlyThoseTwo(final Walk walk) {
    twoConsecutiveRemovesDeleteExactlyThoseTwo(walk, "LW");
  }

  @ParameterizedTest
  @EnumSource(Walk.class)
  void twoConsecutiveRemovesOfRecordBackedEdgesDeleteExactlyThoseTwo(final Walk walk) {
    twoConsecutiveRemovesDeleteExactlyThoseTwo(walk, "HE");
  }

  /** {@code Iterator.remove()} is about the element {@code next()} returned, even after a {@code hasNext()} peeked the following one. */
  @ParameterizedTest
  @EnumSource(Walk.class)
  void removeAfterHasNextDeletesTheReturnedEdgeNotThePeekedOne(final Walk walk) {
    connect("HE", 3);

    final Set<RID> removed = new HashSet<>();
    final Set<RID> visited = new HashSet<>();
    database.transaction(() -> {
      final Iterator<?> it = walk.open(vertex(a), "HE");
      final RID first = farEnd(it.next());
      visited.add(first);
      assertThat(it.hasNext()).isTrue();
      it.remove();
      removed.add(first);
      while (it.hasNext())
        visited.add(farEnd(it.next()));
    });

    // every edge was visited once, and the one removed is the one returned first
    assertThat(visited).hasSize(3);
    assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, "HE")).isEqualTo(2);
    assertThat(countType("HE")).isEqualTo(2);
    for (final RID target : targets)
      assertThat(vertex(target).countEdges(Vertex.DIRECTION.IN, "HE")).isEqualTo(removed.contains(target) ? 0 : 1);
    assertCheckIsClean();
  }

  /**
   * The same over a list of many segments, removing every other element after a {@code hasNext()}: the peek lands in the
   * next segment at each segment's last entry, where the cursor fix-up must not apply, and in the same segment everywhere
   * else, where it must.
   */
  @ParameterizedTest
  @EnumSource(Walk.class)
  void peekThenRemoveAcrossSegmentBoundariesDeletesExactlyTheReturnedEdges(final Walk walk) {
    final int degree = 300;
    final RID[] far = new RID[degree];
    database.transaction(() -> {
      for (int i = 0; i < degree; i++) {
        far[i] = database.newVertex("V").set("id", "f" + i).save().getIdentity();
        vertex(a).modify().newEdge("LW", far[i]);
      }
    });
    final EdgeSegment head = (EdgeSegment) database.lookupByRID(((VertexInternal) vertex(a)).getOutEdgesHeadChunk(), true);
    assertThat(head.getPrevious()).as("the list spans several segments").isNotNull();

    final Set<RID> removed = new HashSet<>();
    final Set<RID> visited = new HashSet<>();
    database.transaction(() -> {
      final Iterator<?> it = walk.open(vertex(a), "LW");
      boolean remove = true;
      while (it.hasNext()) {
        final RID target = farEnd(it.next());
        assertThat(visited.add(target)).as("each edge is visited once").isTrue();
        if (remove) {
          it.hasNext();
          it.remove();
          removed.add(target);
        }
        remove = !remove;
      }
    });

    assertThat(visited).hasSize(degree);
    assertThat(removed).hasSize(degree / 2);
    assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, "LW")).isEqualTo(degree / 2);
    for (final RID target : far)
      assertThat(vertex(target).countEdges(Vertex.DIRECTION.IN, "LW")).isEqualTo(removed.contains(target) ? 0 : 1);
    assertCheckIsClean();
  }

  /** A before-delete listener that vetoes the delete leaves the edge in place on both sides, as with {@code edge.delete()}. */
  @ParameterizedTest
  @EnumSource(Walk.class)
  void aVetoedDeleteKeepsTheEntry(final Walk walk) {
    connect("HE", 2);
    final BeforeRecordDeleteListener veto = record -> !(record instanceof Edge);
    database.getEvents().registerListener(veto);
    try {
      database.transaction(() -> {
        final Iterator<?> it = walk.open(vertex(a), "HE");
        it.next();
        it.remove();
        assertThat(catchThrowable(it::remove)).isInstanceOf(IllegalStateException.class);
      });
    } finally {
      database.getEvents().unregisterListener(veto);
    }
    assertConnected("HE", 2);
    assertCheckIsClean();
  }

  /**
   * A delete listener that writes to the walked list while {@code remove()} deletes - a cascade delete of a sibling edge,
   * a new edge from the same vertex - must not make the positional removal take out the wrong entry or overwrite the
   * listener's write. The graph stays consistent; an iterator whose view of the list went stale refuses to go on with a
   * {@link ConcurrentModificationException}, as java.util collections do.
   */
  @ParameterizedTest
  @EnumSource(Walk.class)
  void aDeleteListenerWritingToTheWalkedListKeepsTheGraphConsistent(final Walk walk) {
    for (final String type : new String[] { "HE", "LW" })
      for (final boolean cascade : new boolean[] { true, false }) {
        connect(type, 3);
        final boolean[] inListener = { false };
        final AfterRecordDeleteListener listener = record -> {
          if (inListener[0] || !(record instanceof Edge))
            return;
          inListener[0] = true;
          try {
            if (cascade) {
              // DELETE THE EDGE TO targets[2], WHICHEVER EDGE THE ITERATOR REMOVED
              for (final Edge edge : vertex(a).getEdges(Vertex.DIRECTION.OUT, type))
                if (edge.getIn().equals(targets[2])) {
                  edge.delete();
                  break;
                }
            } else
              vertex(a).modify().newEdge(type, targets[2]);
          } finally {
            inListener[0] = false;
          }
        };
        database.getEvents().registerListener(listener);
        final RID[] removed = new RID[1];
        try {
          database.transaction(() -> {
            final Iterator<?> it = walk.open(vertex(a), type);
            RID target = farEnd(it.next());
            if (target.equals(targets[2]))
              target = farEnd(it.next());
            it.remove();
            removed[0] = target;
            // THE LISTENER CHANGED THE ONE SEGMENT THE ITERATOR STANDS ON: ITS VIEW IS STALE, AND IT REFUSES TO GO ON
            assertThat(catchThrowable(it::hasNext)).isInstanceOf(ConcurrentModificationException.class);
            assertThat(catchThrowable(it::next)).isInstanceOf(ConcurrentModificationException.class);
            assertThat(catchThrowable(it::remove)).isInstanceOf(ConcurrentModificationException.class);
          });
        } finally {
          database.getEvents().unregisterListener(listener);
        }

        // 3 edges, one removed by the iterator, then one more removed (cascade) or added (create) by the listener
        final long expected = cascade ? 1 : 3;
        assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, type)).as(type + " cascade=" + cascade).isEqualTo(expected);
        assertThat(countType(type)).isEqualTo(expected);
        assertThat(vertex(removed[0]).countEdges(Vertex.DIRECTION.IN, type)).isZero();
        assertThat(vertex(targets[2]).countEdges(Vertex.DIRECTION.IN, type)).isEqualTo(cascade ? 0 : 2);
        assertCheckIsClean();

        // CLEAN UP FOR THE NEXT ROUND
        database.transaction(() -> database.command("sql", "DELETE FROM " + type));
      }
  }

  /** A self-loop walked on its OUT side is disconnected from its IN side too: only the walked list is skipped. */
  @Test
  void removingASelfLoopClearsBothListsOfTheVertex() {
    for (final String type : new String[] { "LW", "HE" }) {
      database.transaction(() -> vertex(a).modify().newEdge(type, a));

      database.transaction(() -> {
        final Iterator<Edge> it = vertex(a).getEdges(Vertex.DIRECTION.OUT, type).iterator();
        it.next();
        it.remove();
      });

      assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, type)).isZero();
      assertThat(vertex(a).countEdges(Vertex.DIRECTION.IN, type)).isZero();
      assertThat(countType(type)).isZero();
    }
    assertCheckIsClean();
  }

  @ParameterizedTest
  @EnumSource(Walk.class)
  void aSecondRemoveWithoutNextIsRefusedByTheIteratorContract(final Walk walk) {
    connect("HE", 2);

    database.begin();
    final Iterator<?> it = walk.open(vertex(a), "HE");
    it.next();
    it.remove();
    assertThat(catchThrowable(it::remove)).isInstanceOf(IllegalStateException.class);
    database.commit();

    assertConnected("HE", 1);
  }

  /** An entry whose edge record is gone names no edge to delete: removing the entry is the repair, and nothing else is touched. */
  @Test
  void anEntryWhoseEdgeRecordIsGoneIsUnlinked() {
    connect("HE", 2);
    final RID[] ghost = new RID[1];
    database.transaction(() -> {
      ghost[0] = vertex(a).getEdges(Vertex.DIRECTION.OUT, "HE").iterator().next().getIdentity();
      // THE RECORD ONLY, BYPASSING THE GRAPH: THE TWO ENTRIES POINTING AT IT ARE LEFT DANGLING
      ((LocalBucket) database.getSchema().getBucketById(ghost[0].getBucketId())).deleteRecord(ghost[0]);
    });

    database.transaction(() -> {
      final Iterator<Pair<RID, RID>> it = Walk.outList(vertex(a)).entryIterator();
      while (it.hasNext())
        if (it.next().getFirst().equals(ghost[0]))
          it.remove();
    });

    assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, "HE")).isEqualTo(1);
    assertThat(countType("HE")).isEqualTo(1);
  }

  // ---------------------------------------------------------------- helpers

  private void twoConsecutiveRemovesDeleteExactlyThoseTwo(final Walk walk, final String type) {
    connect(type, 3);

    final Set<RID> removed = new HashSet<>();
    database.transaction(() -> {
      final Iterator<?> it = walk.open(vertex(a), type);
      removed.add(farEnd(it.next()));
      it.remove();
      removed.add(farEnd(it.next()));
      it.remove();
    });

    assertThat(removed).hasSize(2);
    assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, type)).isEqualTo(1);
    assertThat(countType(type)).isEqualTo(1);
    for (final RID target : targets)
      assertThat(vertex(target).countEdges(Vertex.DIRECTION.IN, type)).isEqualTo(removed.contains(target) ? 0 : 1);
    assertCheckIsClean();
  }

  /** Through the CURRENT database handle, so a reopen does not leave the test reading through the closed one. */
  private Vertex vertex(final RID rid) {
    return database.lookupByRID(rid, false).asVertex();
  }

  /** The vertex at the far end of whatever an iterator hands out. */
  private RID farEnd(final Object element) {
    if (element instanceof Edge edge)
      return edge.getIn();
    if (element instanceof Vertex vertex)
      return vertex.getIdentity();
    return ((Pair<?, RID>) element).getSecond();
  }

  private void connect(final String type, final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        vertex(a).modify().newEdge(type, targets[i]);
    });
    assertConnected(type, count);
  }

  private void assertConnected(final String type, final long expected) {
    assertThat(countType(type)).isEqualTo(expected);
    assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, type)).isEqualTo(expected);
    long in = 0;
    for (final RID target : targets)
      in += vertex(target).countEdges(Vertex.DIRECTION.IN, type);
    assertThat(in).isEqualTo(expected);
  }

  private long countType(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + type)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private void assertCheckIsClean() {
    final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();
    assertThat((Long) result.get("totalWarnings")).isZero();
    assertThat((Long) result.get("totalCorruptedRecords")).isZero();
  }

  private void unbindUser() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  /** Same resolution as ServerSecurityDatabaseUser: only the type's involved bucket ids are listed, the rest is default-allow. */
  private void bindUserRefusing(final SecurityDatabaseUser.ACCESS refused, final String typeName) {
    final DocumentType type = database.getSchema().getType(typeName);
    final Set<Integer> gated = new HashSet<>();
    for (final Bucket bucket : type.getInvolvedBuckets())
      gated.add(bucket.getFileId());
    assertThat(gated).isNotEmpty();

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "restricted";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return access != refused || !gated.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String name, final ACCESS access) {
        return access != refused || !typeName.equals(name);
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    });
  }
}
