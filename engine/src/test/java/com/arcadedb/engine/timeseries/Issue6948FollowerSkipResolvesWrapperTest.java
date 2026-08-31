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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6948: {@code runMaintenance}'s leader-only skip has to read {@code isReplicated()}/{@code isLeader()} off
 * the CURRENT wrapper, not off the reference the task was scheduled with.
 * <p>
 * Every {@code schedule()} call site hands over the instance {@code LocalSchema} was built with, and that field is
 * {@code final}, captured inside {@code LocalDatabase.openInternal()} - which runs before the server wraps the
 * database for HA, and is never updated afterwards. So the scheduled reference is the raw {@code LocalDatabase},
 * whose {@code isReplicated()} is the {@code DatabaseInternal} default {@code false}, and the skip could not fire
 * for anybody. The cost was wasted work rather than divergence, because the three operations underneath already
 * resolve the wrapper per call to reach {@code runWithCompactionReplication} - but the skip exists precisely to
 * avoid that wasted work, and it was doing nothing.
 * <p>
 * The wrapper here is installed exactly the way the HA server installs {@code RaftReplicatedDatabase}: through
 * {@code LocalDatabase.setWrappedDatabaseInstance()}, with the wrapper answering itself for
 * {@code getWrappedDatabaseInstance()} and delegating everything else to the real database. The two flags are the
 * only behaviour it invents, because they are the only ones the engine module cannot get from a real Raft node.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6948FollowerSkipResolvesWrapperTest extends TestHelper {

  private static final String TYPE_NAME = "Reading";

  /**
   * Runs BEFORE {@code TestHelper.afterTest()} (JUnit runs a subclass's {@code @AfterEach} first), which matters:
   * the harness's closing integrity check reports a warning for a database that says it is replicated, and this
   * test's whole point is a database that says exactly that.
   */
  @AfterEach
  void removeWrapper() {
    if (database != null && database.isOpen())
      ((LocalDatabase) database).setWrappedDatabaseInstance((LocalDatabase) database);
  }

  /**
   * A follower does no maintenance at all. Observed through compaction: the mutable rows are still in the mutable
   * bucket afterwards, because nothing sealed them.
   */
  @Test
  void aFollowerSkipsMaintenanceEvenThoughItWasScheduledWithTheRawInstance() throws Exception {
    final LocalTimeSeriesType tsType = createTypeWithMutableRows();
    installWrapper(true, false);

    TimeSeriesMaintenanceScheduler.runMaintenance(database, tsType, TYPE_NAME);

    assertThat(tsType.getEngine().getShard(0).getSealedStore().getBlockCount())
        .as("a follower must not compact: the leader ships the sealed store").isZero();
  }

  /**
   * The arm that proves the one above is not passing because maintenance is broken outright: the same call on a
   * leader does compact. Also covers the standalone case indirectly - {@code isReplicated() == false} takes the
   * same branch as a leader.
   */
  @Test
  void aLeaderStillRunsMaintenance() throws Exception {
    final LocalTimeSeriesType tsType = createTypeWithMutableRows();
    installWrapper(true, true);

    TimeSeriesMaintenanceScheduler.runMaintenance(database, tsType, TYPE_NAME);

    assertThat(tsType.getEngine().getShard(0).getSealedStore().getBlockCount())
        .as("a leader compacts, and ships the result").isGreaterThan(0);
  }

  /** A standalone database has no wrapper at all and must keep maintaining itself. */
  @Test
  void aStandaloneDatabaseStillRunsMaintenance() throws Exception {
    final LocalTimeSeriesType tsType = createTypeWithMutableRows();

    TimeSeriesMaintenanceScheduler.runMaintenance(database, tsType, TYPE_NAME);

    assertThat(tsType.getEngine().getShard(0).getSealedStore().getBlockCount()).isGreaterThan(0);
  }

  // ---- Helpers ----

  private LocalTimeSeriesType createTypeWithMutableRows() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME
        + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);

    final int rows = 3_000;
    final long[] timestamps = new long[rows];
    final Object[][] columns = new Object[2][rows];
    for (int i = 0; i < rows; i++) {
      timestamps[i] = 1_700_000_000_000L + i * 1_000L;
      columns[0][i] = "host_" + (i % 7);
      columns[1][i] = (double) i;
    }
    tsType.getEngine().appendBatch(timestamps, columns);

    assertThat(tsType.getEngine().getShard(0).getSealedStore().getBlockCount())
        .as("nothing is sealed yet, so the assertions below measure this call alone").isZero();
    return tsType;
  }

  /**
   * Installs a wrapper the way the HA server does, so "raw instance" and "wrapped instance" are two different
   * objects with two different answers to the replication flags.
   */
  private void installWrapper(final boolean replicated, final boolean leader) {
    final LocalDatabase real = (LocalDatabase) database;
    final DatabaseInternal wrapper = (DatabaseInternal) Proxy.newProxyInstance(
        DatabaseInternal.class.getClassLoader(), new Class<?>[] { DatabaseInternal.class },
        (proxy, method, args) -> {
          switch (method.getName()) {
          // Mirrors RaftReplicatedDatabase: the wrapper is its own wrapped instance.
          case "getWrappedDatabaseInstance":
            if (method.getParameterCount() == 0)
              return proxy;
            break;
          case "isReplicated":
            return replicated;
          case "isLeader":
            return leader;
          default:
            break;
          }
          try {
            return method.invoke(real, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
    real.setWrappedDatabaseInstance(wrapper);
  }
}
