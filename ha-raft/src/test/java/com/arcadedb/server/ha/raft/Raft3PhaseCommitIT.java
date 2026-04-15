/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests that the 3-phase commit works correctly under concurrent load.
 * Verifies that multiple writers can make progress simultaneously
 * (the lock is released during Raft replication).
 */
class Raft3PhaseCommitIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * Runs multiple concurrent insert transactions against the leader using sqlscript
   * with "commit retry 100" to handle MVCC conflicts that arise when the read lock
   * is released during the Raft replication phase (between Phase 1 and Phase 2).
   * <p>
   * With 3-phase commit the replication gRPC round-trip runs without the database
   * read lock, allowing another transaction to complete between Phase 1 and Phase 2
   * of the first transaction. This produces MVCC ConcurrentModificationExceptions
   * that the client must retry. The "commit retry 100" directive handles that.
   * <p>
   * This test verifies correctness: after all retries every record is present on
   * every replica.
   */
  @Test
  void concurrentWritersReplicateCorrectly() throws Exception {
    final int leaderIndex = 0;
    executeCommand(leaderIndex, "sql", "CREATE document TYPE ConcurrentDoc");
    waitForReplicationIsCompleted(leaderIndex);

    final int THREADS = 8;
    final int INSERTS_PER_THREAD = 50;
    final AtomicInteger successCount = new AtomicInteger();

    final ExecutorService executor = Executors.newFixedThreadPool(THREADS);
    final List<Future<?>> futures = new ArrayList<>();

    for (int t = 0; t < THREADS; t++) {
      final int threadId = t;
      futures.add(executor.submit(() -> {
        for (int i = 0; i < INSERTS_PER_THREAD; i++) {
          try {
            // Use sqlscript with "commit retry 100" so that MVCC conflicts caused
            // by the lock-free Raft replication window are retried automatically.
            final JSONObject response = executeCommand(leaderIndex, "sqlscript",
                "BEGIN ISOLATION REPEATABLE_READ;"
                    + "INSERT INTO ConcurrentDoc SET threadId = " + threadId + ", seq = " + i + ";"
                    + "commit retry 100;");
            assertThat(response).withFailMessage(
                "Insert returned null: thread=%d seq=%d", threadId, i).isNotNull();
            successCount.incrementAndGet();
          } catch (final Exception e) {
            fail("Insert failed: thread=" + threadId + " seq=" + i + " error=" + e.getMessage());
          }
        }
      }));
    }

    for (final Future<?> f : futures)
      f.get(120, TimeUnit.SECONDS);

    executor.shutdown();
    assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

    final int expectedTotal = THREADS * INSERTS_PER_THREAD;
    assertThat(successCount.get()).isEqualTo(expectedTotal);

    // Verify all records replicated to every node
    assertClusterConsistency();

    for (int i = 0; i < getServerCount(); i++) {
      final JSONObject result = executeCommand(i, "sql", "SELECT count(*) as cnt FROM ConcurrentDoc");
      final long count = result.getJSONObject("result").getJSONArray("records")
          .getJSONObject(0).getLong("cnt");
      assertThat(count)
          .withFailMessage("Server %d has %d records, expected %d", i, count, expectedTotal)
          .isEqualTo(expectedTotal);
    }
  }

  /**
   * Verifies that a basic insert-and-read flow still works after the 3-phase refactor.
   * This is a simple smoke test to catch any regression in the commit path.
   */
  @Test
  void basicInsertReplicates() throws Exception {
    final int leaderIndex = 0;
    executeCommand(leaderIndex, "sql", "CREATE document TYPE BasicDoc");
    waitForReplicationIsCompleted(leaderIndex);

    executeCommand(leaderIndex, "sql", "INSERT INTO BasicDoc SET name = 'test1', value = 42");
    waitForReplicationIsCompleted(leaderIndex);

    for (int i = 0; i < getServerCount(); i++) {
      final JSONObject result = executeCommand(i, "sql", "SELECT FROM BasicDoc WHERE name = 'test1'");
      final int count = result.getJSONObject("result").getJSONArray("records").length();
      assertThat(count)
          .withFailMessage("Server %d should have 1 BasicDoc record but has %d", i, count)
          .isEqualTo(1);
    }
  }
}
