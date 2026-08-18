package com.arcadedb.query.select;/*
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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.utility.StallAwareStopwatch;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #5065: the parallel select producers (async workers browsing one bucket each) must never
 * busy-spin forever when the consumer stops draining the hand-off queue (limit reached, early close, exception or
 * plain abandonment). The dataset is larger than the 4,096-slot hand-off queue on purpose, so the producers are
 * guaranteed to hit a full queue when the consumer stops.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectParallelIteratorTest extends TestHelper {
  private static final int TOTAL_RECORDS = 10_000;
  // FITS ENTIRELY IN THE 4,096-SLOT HAND-OFF QUEUE, SO THE PRODUCERS COMPLETE WITHOUT EVER STALLING ON A FULL QUEUE
  // AND THE TIMEOUT TESTS EXERCISE THE STREAMING (NON-EMPTY QUEUE) PATH ONLY
  private static final int SMALL_RECORDS = 1_000;
  private static final int BUCKETS       = 8;
  /**
   * A HANG DETECTOR, not a latency bound (issue #6324, item 3). Every wait below has to end, and only some of them
   * carry a claim about HOW SOON - so this number is deliberately far past anything healthy, and a run that reaches
   * it has a wedged producer rather than a slow one.
   */
  private static final int HANG_DETECTOR_MS = 120_000;
  /**
   * The tripwire between "the producers gave up" and "the producers are spinning on a full queue for ever", which is
   * the whole of #5065. Measured with the JVM-wide stall inside the window discounted, so it can stay this side of
   * unbounded on a machine running back-to-back suites; the raw form failed at 25.1 s against a 15 s budget on a busy
   * machine and passed 5/5 on an idle one, which is the coin flip CLAUDE.md's #6260 rule exists to remove.
   */
  private static final int GIVE_UP_TRIPWIRE_MS = 15_000;

  /**
   * The producers must stop rather than run to their unbounded end. Generous is free here: widening the tripwire
   * cannot turn a passing run red, while narrowing it is what would break.
   */
  private void assertProducersGaveUp(final String whatItSeparates) {
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThat(database.async().waitCompletion(HANG_DETECTOR_MS)).as("the producers must return at all").isTrue();
    watch.assertGaveUpWithin(GIVE_UP_TRIPWIRE_MS, whatItSeparates);
  }

  /** Liveness only: the scan has to finish. Nothing here claims how quickly, so nothing here is a bound. */
  private void assertProducersFinished() {
    assertThat(database.async().waitCompletion(HANG_DETECTOR_MS))
        .as("the parallel scan must finish rather than hang (liveness guard, not a latency bound)").isTrue();
  }

  public SelectParallelIteratorTest() {
    autoStartTx = false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Big", BUCKETS);
    database.getSchema().createVertexType("Small", BUCKETS);
    database.transaction(() -> {
      for (int i = 0; i < TOTAL_RECORDS; i++)
        database.newVertex("Big").set("id", i).save();
      for (int i = 0; i < SMALL_RECORDS; i++)
        database.newVertex("Small").set("id", i).save();
    });
  }

  @Test
  void fullParallelScanReturnsAllRecords() {
    final List<Vertex> result = database.select().fromType("Big").compile().parallel().<Vertex>vertices().toList();
    assertThat(result).hasSize(TOTAL_RECORDS);
    assertProducersFinished();
  }

  @Test
  void limitStopsProducersAndReturnsExactRecords() {
    final List<Vertex> result = database.select().fromType("Big").limit(10).compile().parallel().<Vertex>vertices().toList();

    // THE PRODUCERS MUST STOP ONCE THE LIMIT IS SATISFIED INSTEAD OF SPINNING FOREVER ON THE FULL QUEUE (#5065)
    assertProducersGaveUp("a producer that stops at the limit from one that spins on a full queue for ever");
    assertThat(result).hasSize(10);
  }

  @Test
  void abandonedConsumerReleasesWorkersWithinStallBound() {
    final SelectIterator<Vertex> iterator = database.select().fromType("Big")//
        .timeout(1, TimeUnit.SECONDS, false)//
        .compile().parallel().vertices();

    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isNotNull();

    // THE CONSUMER STOPS DRAINING WITHOUT CLOSING (E.G. AN EXCEPTION IN USER CODE): THE PRODUCERS MUST GIVE UP
    // WITHIN THE STALL BOUND (THE SELECT TIMEOUT HERE) INSTEAD OF PINNING THE ASYNC WORKERS AT 100% CPU (#5065)
    assertProducersGaveUp("a producer that honours the 1s stall bound from one that pins a worker at 100% CPU");
  }

  @Test
  void earlyCloseReleasesAsyncWorkers() {
    final SelectIterator<Vertex> iterator = database.select().fromType("Big").compile().parallel().vertices();

    for (int i = 0; i < 3; i++) {
      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.next()).isNotNull();
    }

    // CLOSE WHILE THE PRODUCERS ARE STILL STREAMING: THE ASYNC WORKERS MUST RETURN WITHIN A BOUND (#5065)
    iterator.close();

    assertProducersGaveUp("a producer that notices the close from one that keeps browsing its whole bucket");
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  void skipIsAppliedOnParallelScan() {
    final List<Vertex> result = database.select().fromType("Big").skip(100).compile().parallel().<Vertex>vertices().toList();
    assertThat(result).hasSize(TOTAL_RECORDS - 100);
    assertProducersFinished();
  }

  @Test
  void skipAndLimitTogetherReturnLimitRecordsOnParallelScan() {
    // STANDARD SEMANTICS: SKIP s RECORDS, THEN RETURN UP TO n. A LIMIT SMALLER THAN THE SKIP MUST NOT TRUNCATE THE
    // RESULT (THE INHERITED `returned` COUNTER ALREADY CONTAINS THE SKIPPED RECORDS WHEN THE STREAMING STARTS)
    final List<Vertex> result = database.select().fromType("Big").skip(100).limit(50).compile().parallel().<Vertex>vertices()
        .toList();
    assertThat(result).hasSize(50);
    assertProducersFinished();

    final List<Vertex> result2 = database.select().fromType("Big").skip(10).limit(50).compile().parallel().<Vertex>vertices()
        .toList();
    assertThat(result2).hasSize(50);
    assertProducersFinished();
  }

  @Test
  void skipAndLimitTogetherReturnLimitRecordsOnSerialScan() {
    // SAME SEMANTICS ON THE SERIAL PATH: THE LIMIT COUNTS THE RECORDS RETURNED AFTER THE SKIPPED ONES
    final List<Vertex> result = database.select().fromType("Big").skip(100).limit(50).compile().<Vertex>vertices().toList();
    assertThat(result).hasSize(50);

    final List<Vertex> result2 = database.select().fromType("Big").skip(10).limit(50).compile().<Vertex>vertices().toList();
    assertThat(result2).hasSize(50);
  }

  @Test
  void timeoutIsEnforcedWhileStreaming() throws InterruptedException {
    // THE DATASET FITS IN THE HAND-OFF QUEUE: THE PRODUCERS COMPLETE QUICKLY AND NEVER STALL, SO THE ONLY WAY THE
    // TIMEOUT CAN FIRE IS THE PER-FETCH CHECK ON THE STREAMING PATH (REGRESSION: THE CHECK WAS ONLY REACHED WHEN THE
    // QUEUE WAS EMPTY, SO A SLOW CONSUMER OF AN ALWAYS-READY QUEUE COULD RUN UNBOUNDED PAST ITS timeout())
    final SelectIterator<Vertex> iterator = database.select().fromType("Small")//
        .timeout(100, TimeUnit.MILLISECONDS, true)//
        .compile().parallel().vertices();

    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isNotNull();

    // LET THE TIMEOUT EXPIRE WHILE THE QUEUE IS STILL FULL OF RECORDS
    Thread.sleep(400);

    assertThatThrownBy(() -> {
      while (iterator.hasNext())
        iterator.next();
    }).isInstanceOf(TimeoutException.class);

    assertProducersFinished();
  }

  @Test
  void nonThrowingTimeoutTruncatesWhileStreaming() throws InterruptedException {
    // SAME SCENARIO WITH exceptionOnTimeout=false: THE ITERATION MUST END EARLY RETURNING WHAT WAS FETCHED SO FAR
    final SelectIterator<Vertex> iterator = database.select().fromType("Small")//
        .timeout(100, TimeUnit.MILLISECONDS, false)//
        .compile().parallel().vertices();

    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isNotNull();

    Thread.sleep(400);

    int fetchedAfterTimeout = 0;
    while (iterator.hasNext()) {
      iterator.next();
      ++fetchedAfterTimeout;
    }

    assertThat(fetchedAfterTimeout).isZero();
    assertProducersFinished();
  }
}
