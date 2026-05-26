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
package com.arcadedb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the {@code arcadedb.profile} settings, focused on the {@code low-ram} profile.
 * The profile is supposed to shrink every component that allocates memory up-front; if a future
 * change introduces a new pool/cache/queue without tuning it here, this test should be updated
 * alongside it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LowRamProfileTest {

  @AfterEach
  void resetAll() {
    GlobalConfiguration.resetAll();
  }

  @Test
  void lowRamAppliesAllExpectedTunings() {
    GlobalConfiguration.PROFILE.setValue("low-ram");

    // PAGE CACHE / WAL / ASYNC
    assertThat(GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong()).isEqualTo(16L);
    assertThat(GlobalConfiguration.INDEX_COMPACTION_RAM_MB.getValueAsLong()).isEqualTo(16L);
    assertThat(GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.getValueAsInteger()).isEqualTo(256);
    assertThat(GlobalConfiguration.FREE_PAGE_RAM.getValueAsInteger()).isEqualTo(80);
    assertThat(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE.getValueAsInteger()).isEqualTo(8);
    assertThat(GlobalConfiguration.ASYNC_TX_BATCH_SIZE.getValueAsInteger()).isEqualTo(8);
    assertThat(GlobalConfiguration.PAGE_FLUSH_QUEUE.getValueAsInteger()).isEqualTo(8);
    assertThat(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_IMPL.getValueAsString()).isEqualTo("standard");
    assertThat(GlobalConfiguration.ASYNC_WORKER_THREADS.getValueAsInteger()).isEqualTo(1);
    assertThat(GlobalConfiguration.TX_WAL_FILES.getValueAsInteger()).isEqualTo(1);

    // STATEMENT/PLAN CACHES
    assertThat(GlobalConfiguration.SQL_STATEMENT_CACHE.getValueAsInteger()).isEqualTo(16);
    assertThat(GlobalConfiguration.OPENCYPHER_STATEMENT_CACHE.getValueAsInteger()).isEqualTo(16);
    assertThat(GlobalConfiguration.OPENCYPHER_PLAN_CACHE.getValueAsInteger()).isEqualTo(16);

    // SHARED THREAD POOLS
    assertThat(GlobalConfiguration.QUERY_PARALLELISM_POOL_THREADS.getValueAsInteger()).isEqualTo(2);
    assertThat(GlobalConfiguration.QUERY_PARALLELISM_QUEUE_SIZE.getValueAsInteger()).isEqualTo(64);
    assertThat(GlobalConfiguration.SPARSE_VECTOR_SCORING_POOL_THREADS.getValueAsInteger()).isEqualTo(1);
    assertThat(GlobalConfiguration.SPARSE_VECTOR_SCORING_QUEUE_SIZE.getValueAsInteger()).isEqualTo(64);

    // SERVER HTTP
    assertThat(GlobalConfiguration.SERVER_HTTP_WORKER_THREADS.getValueAsInteger()).isEqualTo(16);
    final int ioThreads = GlobalConfiguration.SERVER_HTTP_IO_THREADS.getValueAsInteger();
    assertThat(ioThreads).isIn(2, 4);

    // VECTOR INDEX CAPS
    assertThat(GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.getValueAsInteger()).isEqualTo(10_000);
    assertThat(GlobalConfiguration.VECTOR_INDEX_LOCATION_CACHE_SIZE.getValueAsInteger()).isEqualTo(10_000);

    // POLYGLOT ENGINE DISABLED -- avoids loading GraalVM Truffle + every language jar at startup
    assertThat(GlobalConfiguration.POLYGLOT_ENGINE_ENABLED.getValueAsBoolean()).isFalse();
  }

  @Test
  void defaultProfileLeavesPolyglotEnabled() {
    GlobalConfiguration.PROFILE.setValue("default");
    assertThat(GlobalConfiguration.POLYGLOT_ENGINE_ENABLED.getValueAsBoolean()).isTrue();
  }

  @Test
  void serverHttpWorkerThreadsDefaultsTo500() {
    GlobalConfiguration.SERVER_HTTP_WORKER_THREADS.reset();
    assertThat(GlobalConfiguration.SERVER_HTTP_WORKER_THREADS.getValueAsInteger()).isEqualTo(500);
  }
}
