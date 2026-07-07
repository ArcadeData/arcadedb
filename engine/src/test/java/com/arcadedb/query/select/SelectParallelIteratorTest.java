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
import com.arcadedb.graph.Vertex;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

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
  private static final int BUCKETS       = 8;

  public SelectParallelIteratorTest() {
    autoStartTx = false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Big", BUCKETS);
    database.transaction(() -> {
      for (int i = 0; i < TOTAL_RECORDS; i++)
        database.newVertex("Big").set("id", i).save();
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
}
