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
package com.arcadedb.server.gremlin;

import com.arcadedb.database.ProtocolContext;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Issue #8363: under HA, {@code RaftReplicatedDatabase} refuses a CLIENT request on a database whose directory is
 * being replaced from the leader's snapshot, and tells a client from the engine's own threads by
 * {@link ProtocolContext}. The Gremlin Server runs every traversal on this pool and never set the tag, so a
 * traversal read as engine work and was served from the copy the cluster was discarding.
 */
class Issue8363GremlinWorkerTaggedAsClientTest {

  @Test
  void aTraversalRunsTaggedAsGremlinAndThePooledThreadIsHandedBackUntagged() throws Exception {
    final ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      final GremlinPrincipalPropagatingExecutorService executor =
          new GremlinPrincipalPropagatingExecutorService(pool, mock(ArcadeDBServer.class));

      assertThat(executor.submit(ProtocolContext::get).get(10, TimeUnit.SECONDS)).isEqualTo("gremlin");
      assertThat(pool.submit(ProtocolContext::get).get(10, TimeUnit.SECONDS))
          .as("the next, unrelated task on the same pooled thread is not attributed to Gremlin")
          .isEqualTo(ProtocolContext.INTERNAL);
    } finally {
      pool.shutdownNow();
    }
  }
}
