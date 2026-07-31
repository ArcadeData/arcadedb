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
import com.arcadedb.database.Record;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5601: the geospatial function now answers with a LAZY chain of index cursors, so a query that stops early can leave
 * an underlying scan open. A compacted-series cursor registers with its file - {@code LSMTreeIndex} skips a retired
 * file that still has one in {@code dropRetiredCompactedIndexes} - so that scan has to be released when the caller is
 * done, exactly as {@code FetchFromIndexStep.close()} already guarantees for the regular index path.
 * <p>
 * The guarantee has two halves and both are asserted here, because {@code releaseResult()} nulls its field
 * unconditionally: asserting only that the field became null would still pass if the iterator stopped being
 * {@link AutoCloseable} - the very regression this wiring exists to prevent. So these tests check that the production
 * iterator IS closeable, and drive a tracking iterator through the real plan to prove {@code close()} actually reaches
 * it.
 * <p>
 * Pinned on the propagation rather than on its downstream consequence: asserting on the retire-guard counter would
 * need a fixture large enough to produce a compacted series (tens of thousands of records), and would hold vacuously
 * on anything smaller.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FetchFromIndexedFunctionStepCloseTest extends TestHelper {
  private static final String SEARCH_BOX = "POLYGON ((5 35, 20 35, 20 48, 5 48, 5 35))";

  /** Wraps the real lazy chain so the test can observe whether the step actually closed it. */
  private static class TrackingIterator implements Iterator<Record>, AutoCloseable {
    private final Iterator<Record> delegate;
    private       boolean          closed;
    private       boolean          failOnClose;

    TrackingIterator(final Iterator<Record> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public Record next() {
      return delegate.next();
    }

    @Override
    public void close() throws Exception {
      closed = true;
      if (delegate instanceof final AutoCloseable closeable)
        closeable.close();
      if (failOnClose)
        throw new IllegalStateException("close failed on purpose");
    }
  }

  @Test
  void theLazyChainOfAGeoQueryIsCloseable() {
    createCities();

    final ResultSet resultSet = database.query("sql",
        "SELECT name FROM City WHERE geo.within(coords, geo.geomFromText('" + SEARCH_BOX + "')) = true LIMIT 1");
    try {
      assertThat(resultSet.hasNext()).isTrue();
      resultSet.next();

      // if the geo path ever stopped answering with a closeable iterator the release below would silently do nothing
      assertThat(indexedFunctionStep(resultSet).fullResult).isInstanceOf(AutoCloseable.class);
    } finally {
      resultSet.close();
    }
  }

  @Test
  void closingTheResultSetClosesTheIteratorOfAnAbandonedQuery() {
    createCities();

    final ResultSet resultSet = database.query("sql",
        "SELECT name FROM City WHERE geo.within(coords, geo.geomFromText('" + SEARCH_BOX + "')) = true LIMIT 1");

    assertThat(resultSet.hasNext()).isTrue();
    resultSet.next();

    final FetchFromIndexedFunctionStep step = indexedFunctionStep(resultSet);
    final TrackingIterator tracker = new TrackingIterator(step.fullResult);
    step.fullResult = tracker;

    // abandoned with the covering-cell walk still mid-flight - the shape that could not exist while get() drained
    resultSet.close();

    assertThat(tracker.closed).as("ResultSet.close() must reach the step and close the lazy chain").isTrue();
    assertThat(step.fullResult).isNull();
  }

  @Test
  void resetAlsoClosesTheIterator() {
    createCities();

    final ResultSet resultSet = database.query("sql",
        "SELECT name FROM City WHERE geo.within(coords, geo.geomFromText('" + SEARCH_BOX + "')) = true LIMIT 1");
    try {
      assertThat(resultSet.hasNext()).isTrue();
      resultSet.next();

      final FetchFromIndexedFunctionStep step = indexedFunctionStep(resultSet);
      final TrackingIterator tracker = new TrackingIterator(step.fullResult);
      step.fullResult = tracker;

      step.reset();

      assertThat(tracker.closed).as("a reset re-runs the search, so the previous chain must be closed first").isTrue();
      assertThat(step.fullResult).isNull();
    } finally {
      resultSet.close();
    }
  }

  @Test
  void aFailureWhileClosingTheIteratorDoesNotEscape() {
    createCities();

    final ResultSet resultSet = database.query("sql",
        "SELECT name FROM City WHERE geo.within(coords, geo.geomFromText('" + SEARCH_BOX + "')) = true LIMIT 1");

    assertThat(resultSet.hasNext()).isTrue();
    resultSet.next();

    final FetchFromIndexedFunctionStep step = indexedFunctionStep(resultSet);
    final TrackingIterator tracker = new TrackingIterator(step.fullResult);
    tracker.failOnClose = true;
    step.fullResult = tracker;

    // releasing a resource must not turn into a query failure: the close is best-effort and logged at FINE
    resultSet.close();

    assertThat(tracker.closed).isTrue();
    assertThat(step.fullResult).as("the chain is dropped even when its close() failed").isNull();
  }

  private FetchFromIndexedFunctionStep indexedFunctionStep(final ResultSet resultSet) {
    final List<FetchFromIndexedFunctionStep> found = new ArrayList<>();
    collect(resultSet.getExecutionPlan().orElseThrow().getSteps(), found);
    assertThat(found).as("the geo predicate must have planned an indexed-function fetch").hasSize(1);
    return found.getFirst();
  }

  private void collect(final List<ExecutionStep> steps, final List<FetchFromIndexedFunctionStep> found) {
    for (final ExecutionStep step : steps) {
      if (step instanceof final FetchFromIndexedFunctionStep fetch)
        found.add(fetch);
      collect(step.getSubSteps(), found);
    }
  }

  private void createCities() {
    database.command("sql", "CREATE DOCUMENT TYPE City");
    database.command("sql", "CREATE PROPERTY City.name STRING");
    database.command("sql", "CREATE PROPERTY City.coords STRING");
    database.command("sql", "CREATE INDEX ON City (coords) GEOSPATIAL");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO City SET name = 'Rome', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO City SET name = 'Naples', coords = 'POINT (14.3 40.8)'");
      database.command("sql", "INSERT INTO City SET name = 'Florence', coords = 'POINT (11.3 43.8)'");
      database.command("sql", "INSERT INTO City SET name = 'Bari', coords = 'POINT (16.9 41.1)'");
    });
  }
}
