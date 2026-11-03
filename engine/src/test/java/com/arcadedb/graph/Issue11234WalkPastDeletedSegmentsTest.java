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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11234: a walk of an edge list that is still going when its own transaction empties and deletes a segment AHEAD of it
 * (an older one) must go on where the chain now goes. The relink is written on the transaction's copy of the segment in front,
 * and the walk held another one, whose pointer named the deleted segment: {@code RecordNotFoundException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11234WalkPastDeletedSegmentsTest extends TestHelper {
  private static final int EDGES = 3000;

  private Logger          previousLogger;
  private CapturingLogger captured;
  private RID             hub;
  private final List<RID> targets = new ArrayList<>();

  @BeforeEach
  void installCapturingLogger() {
    previousLogger = LogManager.instance().getLogger();
    captured = new CapturingLogger();
    LogManager.instance().setLogger(captured);
  }

  @AfterEach
  void restoreLogger() {
    LogManager.instance().setLogger(previousLogger);
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Entity");
      database.getSchema().createEdgeType("RELATES_TO");
    });
    // ONE EDGE PER TARGET, OLDEST FIRST, IN SEVERAL TRANSACTIONS AS A LOAD WOULD
    database.transaction(() -> hub = database.newVertex("Entity").set("id", "HUB").save().getIdentity());
    for (int from = 0; from < EDGES; from += 500) {
      final int begin = from;
      database.transaction(() -> {
        final MutableVertex h = hub.asVertex().modify();
        for (int i = begin; i < begin + 500; i++) {
          final MutableVertex target = database.newVertex("Entity").set("id", "E" + i).save();
          targets.add(target.getIdentity());
          h.newEdge("RELATES_TO", target);
        }
      });
    }
  }

  @Test
  void aFilteredWalkGoesOnPastTheOldSegmentsItsTransactionDeleted() {
    walkWhileDeletingTheOldest(true);
  }

  @Test
  void anUnfilteredWalkGoesOnPastTheOldSegmentsItsTransactionDeleted() {
    walkWhileDeletingTheOldest(false);
  }

  /**
   * The walk reaches the newest edge the transaction is going to keep of the old ones (to E1000), so it reads the segment in front
   * of those the deletes empty. The transaction deletes the 1000 oldest edges, oldest first, and the walk goes on: it meets none of
   * them, and does not step onto a segment that is gone.
   */
  private void walkWhileDeletingTheOldest(final boolean filtered) {
    database.transaction(() -> {
      final Vertex h = hub.asVertex();
      final Iterator<Edge> walk = (filtered ? h.getEdges(Vertex.DIRECTION.OUT, "RELATES_TO") : h.getEdges(Vertex.DIRECTION.OUT)).iterator();
      final Set<RID> seen = new HashSet<>();
      final RID keptOldest = targets.get(1000);
      while (!seen.contains(keptOldest))
        seen.add(walk.next().getIn());

      deleteEdgesTo(targets.subList(0, 1000));

      while (walk.hasNext())
        assertThat(seen.add(walk.next().getIn())).isTrue();

      assertThat(seen).hasSize(EDGES - 1000);
      assertThat(seen).doesNotContainAnyElementsOf(targets.subList(0, 1000));
    });
    assertThat(hub.asVertex().countEdges(Vertex.DIRECTION.OUT, "RELATES_TO")).isEqualTo(EDGES - 1000);
    assertThat(captured.loadErrors).isEmpty();
  }

  @Test
  void aWalkThatMeetsEdgesItsTransactionDeletedSkipsThemWithoutCallingThemCorrupted() {
    database.transaction(() -> {
      final Iterator<Edge> walk = hub.asVertex().getEdges(Vertex.DIRECTION.OUT, "RELATES_TO").iterator();
      final Edge first = walk.next();

      // EVERY OTHER EDGE GOES, THE ONES OF THE SEGMENT THE WALK IS READING TOO: IT STILL LISTS THEM
      final List<RID> others = new ArrayList<>(targets);
      others.remove(first.getIn());
      deleteEdgesTo(others);

      assertThat(walk.hasNext()).isFalse();
    });
    assertThat(hub.asVertex().countEdges(Vertex.DIRECTION.OUT, "RELATES_TO")).isEqualTo(1);
    assertThat(captured.loadErrors).isEmpty();
  }

  @Test
  void aFilteredWalkDoesNotHandOutTheEdgeItPeekedOnceItsTransactionDeletedIt() {
    peekThenDelete(true);
  }

  @Test
  void anUnfilteredWalkDoesNotHandOutTheEdgeItPeekedOnceItsTransactionDeletedIt() {
    peekThenDelete(false);
  }

  /** hasNext() has already read the next entry when the transaction deletes its edge: next() goes on to the one after it. */
  private void peekThenDelete(final boolean filtered) {
    database.transaction(() -> {
      final Vertex h = hub.asVertex();
      final List<Edge> order = new ArrayList<>();
      walkOf(h, filtered).forEachRemaining(order::add);

      final Iterator<Edge> walk = walkOf(h, filtered);
      assertThat(walk.next().getIdentity()).isEqualTo(order.get(0).getIdentity());
      assertThat(walk.hasNext()).isTrue();

      order.get(1).delete();

      assertThat(walk.hasNext()).isTrue();
      assertThat(walk.next().getIdentity()).isEqualTo(order.get(2).getIdentity());
    });
    assertThat(hub.asVertex().countEdges(Vertex.DIRECTION.OUT, "RELATES_TO")).isEqualTo(EDGES - 1);
  }

  private static Iterator<Edge> walkOf(final Vertex vertex, final boolean filtered) {
    return (filtered ? vertex.getEdges(Vertex.DIRECTION.OUT, "RELATES_TO") : vertex.getEdges(Vertex.DIRECTION.OUT)).iterator();
  }

  @Test
  void aWalkOfAListWhoseVertexItsTransactionDeletedEnds() {
    database.transaction(() -> {
      final Iterator<Edge> walk = hub.asVertex().getEdges(Vertex.DIRECTION.OUT, "RELATES_TO").iterator();
      walk.next();

      hub.asVertex().delete();

      while (walk.hasNext())
        walk.next();
    });
    assertThat(database.countType("RELATES_TO", false)).isZero();
    assertThat(captured.loadErrors).isEmpty();
  }

  private void deleteEdgesTo(final List<RID> targetsToDisconnect) {
    final Set<RID> wanted = new HashSet<>(targetsToDisconnect);
    final List<Edge> toDelete = new ArrayList<>();
    for (final Edge e : hub.asVertex().getEdges(Vertex.DIRECTION.OUT, "RELATES_TO"))
      if (wanted.contains(e.getIn()))
        toDelete.add(e);
    // OLDEST FIRST, AS THE STATEMENT OF THE ISSUE DOES
    for (int i = toDelete.size() - 1; i >= 0; i--)
      toDelete.get(i).delete();
  }

  /** Keeps the "Error on loading edge" records the edge iterators log when they take an entry for corruption. */
  private static final class CapturingLogger implements Logger {
    final List<String> loadErrors = new CopyOnWriteArrayList<>();

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
        final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
        final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
        final Object arg15, final Object arg16, final Object arg17) {
      capture(level, message);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(level, message);
    }

    private void capture(final Level level, final String message) {
      if (message != null && message.startsWith("Error on loading edge"))
        loadErrors.add(level + ":" + message);
    }

    @Override
    public void flush() {
      // NOTHING TO FLUSH
    }
  }
}
