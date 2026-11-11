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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11458: with a one-byte apply-backlog budget the leader holds entries back from every follower whose applied
 * index it knows to be behind, resuming as the follower's applied index (carried on the capability poll) advances. The
 * cluster must still replicate every write and converge: back-pressure delays, it never loses or wedges.
 */
@Tag("slow")
class Issue11458ApplyBackPressureIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_APPLY_BACKLOG_MAX, "1");
  }

  @Test
  void writesReplicateAndConvergeWhileEntriesAreHeldBack() throws InterruptedException {
    final AtomicInteger held = new AtomicInteger();
    final Handler watcher = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        if (record != null && record.getMessage() != null && record.getMessage().contains("Holding Raft entries back"))
          held.incrementAndGet();
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };
    final Logger root = Logger.getLogger("");
    final Level previousLevel = root.getLevel();
    watcher.setLevel(Level.WARNING);
    root.addHandler(watcher);
    if (previousLevel == null || previousLevel.intValue() > Level.WARNING.intValue())
      root.setLevel(Level.WARNING);
    try {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
      final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
      leaderDb.transaction(() -> {
        if (!leaderDb.getSchema().existsType("Held"))
          leaderDb.getSchema().createVertexType("Held");
      });

      // Spread over more than one capability poll period (5 s), so the leader learns the followers' applied index
      // while there are still entries to hold back
      for (int round = 0; round < 8; round++) {
        final int base = round * 20;
        leaderDb.transaction(() -> {
          for (int i = 0; i < 20; i++)
            leaderDb.newVertex("Held").set("idx", base + i).save();
        });
        Thread.sleep(1_500);
      }

      assertClusterConsistency();
      for (int i = 0; i < getServerCount(); i++)
        assertThat(getServerDatabase(i, getDatabaseName()).countType("Held", true)).as("server %d", i).isEqualTo(160L);
      assertThat(held.get()).as("the leader must have held entries back at least once with a 1-byte budget").isGreaterThan(0);
    } finally {
      root.removeHandler(watcher);
      if (previousLevel != null)
        root.setLevel(previousLevel);
    }
  }
}
