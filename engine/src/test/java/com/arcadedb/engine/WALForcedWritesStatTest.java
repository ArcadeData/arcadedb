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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code forcedWALWrites} statistic counts the commits that forced their WAL record in their own write: the control of
 * {@code Issue11746LeaderWalNotForcedIT}, which asserts that an HA leader forces none.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class WALForcedWritesStatTest extends TestHelper {
  private static final int COMMITS = 20;

  @Test
  void countsTheCommitsThatForcedTheirWalRecord() throws Exception {
    database.getSchema().createDocumentType("Forced");

    // read when a thread's transaction context is created: the commits run on a thread that has none yet
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_GROUP_COMMIT, false);
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 1);
    try {
      final long before = forcedWrites();
      commitOnAFreshThread();
      // at least one per commit: an internal commit of the database (a statistics or schema save) may force its record too
      assertThat(forcedWrites() - before).isGreaterThanOrEqualTo(COMMITS);

      database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 0);
      final long unforced = forcedWrites();
      commitOnAFreshThread();
      assertThat(forcedWrites()).isEqualTo(unforced);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 0);
      database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_GROUP_COMMIT, true);
    }
  }

  private void commitOnAFreshThread() throws Exception {
    final Throwable[] failure = new Throwable[1];
    final Thread thread = new Thread(() -> {
      try {
        for (int i = 0; i < COMMITS; i++) {
          final int k = i;
          database.transaction(() -> database.newDocument("Forced").set("k", k).save());
        }
      } catch (final Throwable t) {
        failure[0] = t;
      }
    });
    thread.start();
    thread.join();
    assertThat(failure[0]).isNull();
  }

  private long forcedWrites() {
    return (Long) ((DatabaseInternal) database).getTransactionManager().getStats().get("forcedWALWrites");
  }
}
