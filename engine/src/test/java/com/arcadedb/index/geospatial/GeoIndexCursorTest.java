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
package com.arcadedb.index.geospatial;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.function.sql.geo.GeoUtils;
import com.arcadedb.index.EmptyIndexCursor;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.serializer.BinaryComparator;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.Shape;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for #5601 (2): the geospatial query cursor must chain the covering cells of a search shape LAZILY instead
 * of materialising every candidate RID before the caller sees the first row.
 * <p>
 * Driven through {@link GeoIndexCursor.CellCursorFactory} with a real {@link GeoCoveringCellWalk} over a real GeoHash
 * grid, so the cell sequence is the production one while the underlying scans are observable: the tests can count how
 * many were opened, how many were closed, and how far the walk got.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GeoIndexCursorTest {
  private static final Object[] KEYS = new Object[] { "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))" };

  /**
   * Records every cell scan handed out, so a test can assert on how much of the walk was consumed.
   */
  private static class RecordingCellCursorFactory implements GeoIndexCursor.CellCursorFactory {
    final List<String>             openedTokens = new ArrayList<>();
    final List<TrackingCursor>     opened       = new ArrayList<>();
    final Map<String, List<RID>>   contents     = new HashMap<>();

    @Override
    public IndexCursor open(final String token, final boolean frontier) {
      openedTokens.add(token);
      final List<RID> rids = contents.get(token);
      final TrackingCursor cursor = new TrackingCursor(rids == null ? List.of() : rids);
      opened.add(cursor);
      return cursor;
    }

    long closedCount() {
      return opened.stream().filter(c -> c.closed).count();
    }
  }

  /** A cell scan that records its own close(), so a test can assert the chaining released it. */
  private static class TrackingCursor implements IndexCursor {
    private final List<RID> rids;
    private       int       position;
    private       boolean   closed;
    private       RID       current;

    TrackingCursor(final List<RID> rids) {
      this.rids = rids;
    }

    @Override
    public boolean hasNext() {
      return position < rids.size();
    }

    @Override
    public Identifiable next() {
      current = rids.get(position++);
      return current;
    }

    @Override
    public Object[] getKeys() {
      return null;
    }

    @Override
    public Identifiable getRecord() {
      return current;
    }

    @Override
    public BinaryComparator getComparator() {
      return null;
    }

    @Override
    public byte[] getBinaryKeyTypes() {
      return new byte[0];
    }

    @Override
    public long estimateSize() {
      return rids.size();
    }

    @Override
    public void close() {
      closed = true;
    }

    @Override
    public Iterator<Identifiable> iterator() {
      return this;
    }
  }

  @Test
  void aSingleRowDoesNotWalkTheWholeCoveringSet() {
    final GeoCoveringCellWalk walk = newWalk("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
    final int totalCells = countCells("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
    assertThat(totalCells).as("the fixture must be a shape wide enough for laziness to matter").isGreaterThan(50);

    final RecordingCellCursorFactory factory = new RecordingCellCursorFactory();
    // only the very first cell of the walk answers, so one row can be produced without touching the rest
    factory.contents.put(firstToken("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"), List.of(new RID(1, 1)));

    final GeoIndexCursor cursor = new GeoIndexCursor(KEYS, walk, factory);

    assertThat(cursor.hasNext()).isTrue();
    assertThat(cursor.next().getIdentity()).isEqualTo(new RID(1, 1));

    assertThat(factory.openedTokens).as("exactly one cell scan must have been opened for the first row").hasSize(1);
    assertThat(factory.closedCount()).as("the cell still holding unread rows must stay open").isZero();
  }

  @Test
  void drainingTheCursorWalksEveryCellAndClosesEachScan() {
    final String wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
    final RecordingCellCursorFactory factory = new RecordingCellCursorFactory();
    final GeoIndexCursor cursor = new GeoIndexCursor(KEYS, newWalk(wkt), factory);

    while (cursor.hasNext())
      cursor.next();

    assertThat(factory.openedTokens).hasSize(countCells(wkt));
    assertThat(factory.closedCount()).as("every drained cell scan must be closed").isEqualTo(factory.opened.size());
  }

  @Test
  void theSameRecordReachedThroughSeveralCellsIsEmittedOnce() {
    final String wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
    final RID shared = new RID(3, 7);

    final RecordingCellCursorFactory factory = new RecordingCellCursorFactory() {
      @Override
      public IndexCursor open(final String token, final boolean frontier) {
        // every cell of the walk returns the SAME rid, as a polygon indexed under many cells would
        openedTokens.add(token);
        final TrackingCursor cursor = new TrackingCursor(List.of(shared));
        opened.add(cursor);
        return cursor;
      }
    };

    final GeoIndexCursor cursor = new GeoIndexCursor(KEYS, newWalk(wkt), factory);

    final List<Identifiable> emitted = new ArrayList<>();
    while (cursor.hasNext())
      emitted.add(cursor.next());

    assertThat(emitted).containsExactly(shared);
    assertThat(factory.openedTokens).as("dedup must not stop the walk early").hasSize(countCells(wkt));
  }

  @Test
  void anExhaustedCursorReleasesItsSeenSetWithoutWaitingForClose() {
    final String wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
    final RecordingCellCursorFactory factory = new RecordingCellCursorFactory();
    factory.contents.put(firstToken(wkt), List.of(new RID(1, 1)));

    final GeoIndexCursor cursor = new GeoIndexCursor(KEYS, newWalk(wkt), factory);
    while (cursor.hasNext())
      cursor.next();

    // the walk is over, so the dedup set has no reader left: on a wide-area query it holds one entry per candidate,
    // and it must not linger waiting for a close() that a caller holding a fully drained cursor may never issue
    assertThat(cursor.seenSize()).isZero();
    assertThat(cursor.hasNext()).isFalse();

    // closing an already-exhausted cursor stays a no-op
    cursor.close();
    assertThat(cursor.hasNext()).isFalse();
  }

  /**
   * #5635: the cell scans are driven STRICTLY - {@code next()} is only ever called after {@code hasNext()} answered
   * true - so they need no null guard. This replaces the #5609 test that fed a null through a cell scan to mimic the
   * optimistic {@code LSMTreeIndexCursor}: since #5635 an index cursor that answers {@code hasNext()} true always
   * yields a real element, and one that is exhausted throws. A cell scan built on that contract is used here, so an
   * over-eager {@code next()} anywhere in the chaining logic surfaces as a {@link NoSuchElementException} rather than
   * being silently absorbed.
   */
  @Test
  void cellScansAreDrivenStrictlyThroughHasNext() {
    final String wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
    final RID first = new RID(4, 9);
    final RID second = new RID(4, 10);

    final List<RID> remaining = new ArrayList<>(List.of(first, second));
    final GeoIndexCursor cursor = new GeoIndexCursor(KEYS, newWalk(wkt), (token, frontier) -> new IndexCursor() {
      @Override
      public boolean hasNext() {
        return !remaining.isEmpty();
      }

      @Override
      public Identifiable next() {
        if (remaining.isEmpty())
          throw new NoSuchElementException();
        return remaining.removeFirst();
      }

      @Override
      public Object[] getKeys() {
        return null;
      }

      @Override
      public Identifiable getRecord() {
        return null;
      }

      @Override
      public BinaryComparator getComparator() {
        return null;
      }

      @Override
      public byte[] getBinaryKeyTypes() {
        return new byte[0];
      }

      @Override
      public long estimateSize() {
        return -1L;
      }

      @Override
      public Iterator<Identifiable> iterator() {
        return this;
      }
    });

    final List<Identifiable> emitted = new ArrayList<>();
    while (cursor.hasNext())
      emitted.add(cursor.next());

    // the walk opens a cell scan per covering cell; only the first one holds anything, and the rest are empty
    assertThat(emitted).containsExactly(first, second);
  }

  @Test
  void nextThrowsWhenExhaustedAndNeverAnswersNull() {
    final GeoIndexCursor cursor = new GeoIndexCursor(KEYS, newWalk("POINT(1 1)"),
        (token, frontier) -> new EmptyIndexCursor());

    assertThat(cursor.hasNext()).isFalse();
    assertThatThrownBy(cursor::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void hasNextIsIdempotentAndDoesNotConsume() {
    final RID rid = new RID(5, 2);
    final RecordingCellCursorFactory factory = new RecordingCellCursorFactory();
    final String wkt = "POINT(1 1)";
    factory.contents.put(firstToken(wkt), List.of(rid));

    final GeoIndexCursor cursor = new GeoIndexCursor(KEYS, newWalk(wkt), factory);

    assertThat(cursor.hasNext()).isTrue();
    assertThat(cursor.hasNext()).isTrue();
    assertThat(cursor.hasNext()).isTrue();
    assertThat(cursor.next().getIdentity()).isEqualTo(rid);
    assertThat(factory.openedTokens).as("repeated hasNext() must not re-open or advance anything").hasSize(1);
  }

  @Test
  void closeReleasesTheOpenCellScanAndEndsTheCursor() {
    final String wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
    final RecordingCellCursorFactory factory = new RecordingCellCursorFactory();
    factory.contents.put(firstToken(wkt), List.of(new RID(1, 1), new RID(1, 2)));

    final GeoIndexCursor cursor = new GeoIndexCursor(KEYS, newWalk(wkt), factory);
    assertThat(cursor.hasNext()).isTrue();
    cursor.next();

    cursor.close();

    assertThat(factory.closedCount()).isEqualTo(1);
    assertThat(cursor.hasNext()).as("a closed cursor is exhausted").isFalse();
  }

  @Test
  void getKeysEchoesTheQueryKeysAndGetRecordFollowsTheEmittedRid() {
    final RID rid = new RID(6, 3);
    final String wkt = "POINT(1 1)";
    final RecordingCellCursorFactory factory = new RecordingCellCursorFactory();
    factory.contents.put(firstToken(wkt), List.of(rid));

    final GeoIndexCursor cursor = new GeoIndexCursor(KEYS, newWalk(wkt), factory);

    assertThat(cursor.hasNext()).isTrue();
    assertThat(cursor.next()).isEqualTo(rid);
    assertThat(cursor.getKeys()).isSameAs(KEYS);
    assertThat(cursor.getRecord()).isEqualTo(rid);
    assertThat(cursor.estimateSize()).as("a lazy cursor cannot know its size up front").isEqualTo(-1L);
  }

  @Test
  void anEmptyWalkProducesNothing() {
    final GeoIndexCursor cursor = new GeoIndexCursor(KEYS, newWalk("POINT(1 1)"),
        (token, frontier) -> new TempIndexCursor(List.<IndexCursorEntry>of()));

    assertThat(cursor.hasNext()).isFalse();
  }

  // ---- helpers ----

  private static GeohashPrefixTree grid() {
    return new GeohashPrefixTree(GeoUtils.getSpatialContext(), 11);
  }

  private static GeoCoveringCellWalk newWalk(final String wkt) {
    return new GeoCoveringCellWalk(grid().getTreeCellIterator(shape(wkt), 7));
  }

  private static Shape shape(final String wkt) {
    try {
      return GeoUtils.getSpatialContext().getFormats().getWktReader().read(wkt);
    } catch (final Exception e) {
      throw new IllegalArgumentException(wkt, e);
    }
  }

  private static int countCells(final String wkt) {
    final GeoCoveringCellWalk walk = newWalk(wkt);
    int total = 0;
    while (walk.advance())
      ++total;
    return total;
  }

  private static String firstToken(final String wkt) {
    final GeoCoveringCellWalk walk = newWalk(wkt);
    assertThat(walk.advance()).isTrue();
    return walk.getToken();
  }
}
