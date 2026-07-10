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
    assertThat(database.async().waitCompletion(15_000)).isTrue();
  }

  @Test
  void limitStopsProducersAndReturnsExactRecords() {
    final List<Vertex> result = database.select().fromType("Big").limit(10).compile().parallel().<Vertex>vertices().toList();

    // THE PRODUCERS MUST STOP ONCE THE LIMIT IS SATISFIED INSTEAD OF SPINNING FOREVER ON THE FULL QUEUE (#5065)
    assertThat(database.async().waitCompletion(15_000)).isTrue();
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
    assertThat(database.async().waitCompletion(15_000)).isTrue();
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

    assertThat(database.async().waitCompletion(15_000)).isTrue();
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  void skipIsAppliedOnParallelScan() {
    final List<Vertex> result = database.select().fromType("Big").skip(100).compile().parallel().<Vertex>vertices().toList();
    assertThat(result).hasSize(TOTAL_RECORDS - 100);
    assertThat(database.async().waitCompletion(15_000)).isTrue();
  }

  @Test
  void skipAndLimitTogetherReturnLimitRecordsOnParallelScan() {
    // STANDARD SEMANTICS: SKIP s RECORDS, THEN RETURN UP TO n. A LIMIT SMALLER THAN THE SKIP MUST NOT TRUNCATE THE
    // RESULT (THE INHERITED `returned` COUNTER ALREADY CONTAINS THE SKIPPED RECORDS WHEN THE STREAMING STARTS)
    final List<Vertex> result = database.select().fromType("Big").skip(100).limit(50).compile().parallel().<Vertex>vertices()
        .toList();
    assertThat(result).hasSize(50);
    assertThat(database.async().waitCompletion(15_000)).isTrue();

    final List<Vertex> result2 = database.select().fromType("Big").skip(10).limit(50).compile().parallel().<Vertex>vertices()
        .toList();
    assertThat(result2).hasSize(50);
    assertThat(database.async().waitCompletion(15_000)).isTrue();
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
  void timeoutIsEnforcedWhileStreaming() throws Exception {
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

    assertThat(database.async().waitCompletion(15_000)).isTrue();
  }

  @Test
  void nonThrowingTimeoutTruncatesWhileStreaming() throws Exception {
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
    assertThat(database.async().waitCompletion(15_000)).isTrue();
  }
}
