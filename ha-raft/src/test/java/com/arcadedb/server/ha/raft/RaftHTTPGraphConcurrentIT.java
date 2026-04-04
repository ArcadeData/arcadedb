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
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.*;

class RaftHTTPGraphConcurrentIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void oneEdgePerTxMultiThreads() throws Exception {
    testEachServer((serverIndex) -> {
      executeCommand(serverIndex, "sqlscript",
          "create vertex type RaftPhotos" + serverIndex + ";"
              + "create vertex type RaftUsers" + serverIndex + ";"
              + "create edge type RaftHasUploaded" + serverIndex + ";");

      waitForReplicationIsCompleted(serverIndex);

      executeCommand(serverIndex, "sql", "create vertex RaftUsers" + serverIndex + " set id = 'u1111'");
      waitForReplicationIsCompleted(serverIndex);

      final int THREADS = 4;
      final int SCRIPTS = 100;
      final AtomicInteger atomic = new AtomicInteger();

      final ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
      final List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < THREADS; i++) {
        final Future<?> future = executorService.submit(() -> {
          for (int j = 0; j < SCRIPTS; j++) {
            try {
              final JSONObject responseAsJson = executeCommand(serverIndex, "sqlscript",
                  "BEGIN ISOLATION REPEATABLE_READ;"
                      + "LET photo = CREATE vertex RaftPhotos" + serverIndex + " SET id = uuid(), name = \"downloadX.jpg\";"
                      + "LET user = SELECT FROM RaftUsers" + serverIndex + " WHERE id = \"u1111\";"
                      + "LET userEdge = Create edge RaftHasUploaded" + serverIndex
                      + " FROM $user to $photo set type = \"User_Photos\";"
                      + "commit retry 100;return $photo;");

              atomic.incrementAndGet();

              if (responseAsJson == null) {
                LogManager.instance().log(this, Level.SEVERE, "Error on execution from thread %d", Thread.currentThread().threadId());
                return;
              }

              assertThat(responseAsJson.getJSONObject("result").getJSONArray("records")).isNotNull();
            } catch (final Exception e) {
              fail(e);
            }
          }
        });
        futures.add(future);
      }

      for (final Future<?> future : futures)
        future.get(60, TimeUnit.SECONDS);

      executorService.shutdown();
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS))
        executorService.shutdownNow();

      assertThat(atomic.get()).isEqualTo(THREADS * SCRIPTS);

      final JSONObject select = executeCommand(serverIndex, "sql",
          "SELECT id FROM ( SELECT expand( outE('RaftHasUploaded" + serverIndex + "') ) FROM RaftUsers" + serverIndex
              + " WHERE id = \"u1111\" )");

      assertThat(select.getJSONObject("result").getJSONArray("records").length())
          .withFailMessage("Some edges were missing when executing from server " + serverIndex)
          .isEqualTo(THREADS * SCRIPTS);

      assertClusterConsistency();
    });
  }
}
