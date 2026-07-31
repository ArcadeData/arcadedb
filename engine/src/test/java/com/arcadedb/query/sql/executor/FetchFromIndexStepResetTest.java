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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5635: {@code FetchFromIndexStep.reset()} restarts the step - {@code inited} goes back to false and {@code init()}
 * rebuilds every cursor - so the cursors of the previous run have to be RELEASED, not merely dereferenced.
 * <p>
 * {@code close()} has said as much since #5601: a {@code LSMTreeIndexUnderlyingCompactedSeriesCursor} registers with its
 * file, and {@code LSMTreeIndex.dropRetiredCompactedIndexes} skips a retired file that still has one - nothing else will
 * ever close a cursor the step dropped, so the file stays on disk until the next database restart. {@code reset()} did
 * not do any of it, and it did not even CLEAR {@code nextCursors}: the pending cursors of the previous run survived into
 * the new one, and {@code init()} appended to them, so the restarted scan replayed the old, partly consumed cursors
 * first.
 * <p>
 * The sibling step, {@code FetchFromIndexedFunctionStep}, got the closing behaviour in #5609
 * ({@code FetchFromIndexedFunctionStepCloseTest}); this pins the same guarantee on the step every indexed query goes
 * through.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FetchFromIndexStepResetTest extends TestHelper {
  private static final String TYPE_NAME = "Reading";
  private static final int    TOTAL     = 200;

  /** Wraps a real index cursor so the test can observe whether the step closed it. */
  private static class TrackingCursor implements IndexCursor {
    private final IndexCursor delegate;
    private       boolean     closed;

    TrackingCursor(final IndexCursor delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public Identifiable next() {
      return delegate.next();
    }

    @Override
    public Object[] getKeys() {
      return delegate.getKeys();
    }

    @Override
    public Identifiable getRecord() {
      return delegate.getRecord();
    }

    @Override
    public BinaryComparator getComparator() {
      return delegate.getComparator();
    }

    @Override
    public byte[] getBinaryKeyTypes() {
      return delegate.getBinaryKeyTypes();
    }

    @Override
    public long estimateSize() {
      return delegate.estimateSize();
    }

    @Override
    public void close() {
      closed = true;
      delegate.close();
    }

    @Override
    public Iterator<Identifiable> iterator() {
      return this;
    }
  }

  @Test
  void resetClosesTheCurrentCursor() {
    createReadings();

    final ResultSet resultSet = database.query("sql", "SELECT id FROM " + TYPE_NAME + " WHERE id > 10 LIMIT 1");
    try {
      assertThat(resultSet.hasNext()).isTrue();
      resultSet.next();

      final FetchFromIndexStep step = indexStep(resultSet);
      // mid-scan: the step is holding an open cursor, which is exactly the state reset() has to release
      assertThat((Object) step.cursor).as("precondition: the scan must still be holding a cursor").isNotNull();

      final TrackingCursor tracker = new TrackingCursor(step.cursor);
      step.cursor = tracker;

      step.reset();

      assertThat(tracker.closed).as("a reset re-runs the scan, so the previous cursor must be released first").isTrue();
      assertThat((Object) step.cursor).isNull();
    } finally {
      resultSet.close();
    }
  }

  /**
   * The pending cursors of a cartesian-product key: not closed, and - worse - not even dropped, so they leaked into the
   * restarted scan.
   */
  @Test
  void resetClosesAndClearsThePendingCursors() {
    createReadings();

    final ResultSet resultSet = database.query("sql",
        "SELECT id FROM " + TYPE_NAME + " WHERE id IN [11, 12, 13] AND id > 0 LIMIT 1");
    try {
      assertThat(resultSet.hasNext()).isTrue();
      resultSet.next();

      final FetchFromIndexStep step = indexStep(resultSet);
      assertThat(step.nextCursors).as("precondition: a multi-value key must have planned more than one cursor")
          .isNotEmpty();

      final List<TrackingCursor> trackers = new ArrayList<>();
      for (int i = 0; i < step.nextCursors.size(); i++) {
        final TrackingCursor tracker = new TrackingCursor(step.nextCursors.get(i));
        trackers.add(tracker);
        step.nextCursors.set(i, tracker);
      }

      step.reset();

      assertThat(trackers).allMatch(t -> t.closed, "every pending cursor is released");
      assertThat(step.nextCursors).as("a pending cursor left here would be replayed by the restarted scan").isEmpty();
    } finally {
      resultSet.close();
    }
  }

  /**
   * The {@code key IN [...]} path - reachable with an index as the query target - hands its per-value cursors to
   * {@code customIterator}, which never closed them at all: not on {@code reset()}, not even on {@code close()}.
   */
  @Test
  void resetClosesTheCursorsOfAnInCondition() {
    createReadings();

    final ResultSet resultSet = database.query("sql",
        "SELECT FROM INDEX:`" + indexName() + "` WHERE key IN [11, 12, 13]");
    try {
      assertThat(resultSet.hasNext()).isTrue();
      resultSet.next();

      final FetchFromIndexStep step = indexStep(resultSet);
      assertThat(step.customCursors).as("precondition: an IN list must have opened one cursor per value").isNotEmpty();

      final List<TrackingCursor> trackers = new ArrayList<>();
      for (int i = 0; i < step.customCursors.size(); i++) {
        final TrackingCursor tracker = new TrackingCursor(step.customCursors.get(i));
        trackers.add(tracker);
        step.customCursors.set(i, tracker);
      }

      step.reset();

      assertThat(trackers).allMatch(t -> t.closed, "every IN-list cursor is released");
      assertThat(step.customCursors).isEmpty();
    } finally {
      resultSet.close();
    }
  }

  /**
   * The behavioural consequence of the un-cleared {@code nextCursors}: a restarted step must produce the same rows as a
   * fresh one, not the tail of the previous run followed by the new scan.
   */
  @Test
  void aRestartedStepReplaysTheScanFromTheBeginning() {
    createReadings();

    final ResultSet resultSet = database.query("sql",
        "SELECT id FROM " + TYPE_NAME + " WHERE id IN [11, 12, 13] AND id > 0 LIMIT 1");
    assertThat(resultSet.hasNext()).isTrue();
    resultSet.next();

    // NOT closed: the step is restarted with its pending cursors still loaded, which is the state reset() must handle
    // on its own. Closing the result set first would clear them through close() and make this hold vacuously.
    final FetchFromIndexStep step = indexStep(resultSet);
    assertThat(step.nextCursors).as("precondition: the restart must happen with a pending cursor loaded").isNotEmpty();

    final List<Object> firstRun = new ArrayList<>();
    step.reset();
    collectRids(step, firstRun);
    assertThat(firstRun).as("the restarted scan must see every matching key exactly once").hasSize(3);
    assertThat(firstRun).doesNotHaveDuplicates();

    final List<Object> secondRun = new ArrayList<>();
    step.reset();
    collectRids(step, secondRun);
    assertThat(secondRun).isEqualTo(firstRun);

    step.close();
  }

  private void collectRids(final FetchFromIndexStep step, final List<Object> out) {
    while (true) {
      final ResultSet rs = step.syncPull(step.context, 100);
      if (!rs.hasNext())
        return;
      while (rs.hasNext())
        out.add(rs.next().getProperty("rid"));
    }
  }

  private FetchFromIndexStep indexStep(final ResultSet resultSet) {
    final List<FetchFromIndexStep> found = new ArrayList<>();
    collect(resultSet.getExecutionPlan().orElseThrow().getSteps(), found);
    assertThat(found).as("the query must have planned an index fetch").hasSize(1);
    return found.getFirst();
  }

  private void collect(final List<ExecutionStep> steps, final List<FetchFromIndexStep> found) {
    for (final ExecutionStep step : steps) {
      if (step instanceof final FetchFromIndexStep fetch)
        found.add(fetch);
      collect(step.getSubSteps(), found);
    }
  }

  private String indexName() {
    return database.getSchema().getType(TYPE_NAME).getPolymorphicIndexByProperties("id").getName();
  }

  private void createReadings() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".id INTEGER");
    database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (id) UNIQUE");

    database.transaction(() -> {
      for (int i = 0; i < TOTAL; ++i)
        database.newDocument(TYPE_NAME).set("id", i).save();
    });
  }
}
