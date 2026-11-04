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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11031: the periodic refresh of a materialized view rewrites its backing type, and under HA the leader replicates its
 * own rewrite of the same records, so a follower must not recompute the view on its own timer. The scheduler is handed the raw
 * {@code LocalDatabase} the schema was loaded with, built before the server wraps the database for HA, so the guard has to
 * resolve the wrapper the node is currently served through: asked of the scheduled reference itself it could never fire.
 * <p>
 * The wrapper is installed the way the HA server installs {@code RaftReplicatedDatabase}, through
 * {@code LocalDatabase.setWrappedDatabaseInstance()}, answering itself for {@code getWrappedDatabaseInstance()}: the two
 * replication flags are the only behaviour it invents.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11031MaterializedViewRefreshLeaderOnlyTest extends TestHelper {

  /** Runs before {@code TestHelper.afterTest()}, whose integrity check would otherwise meet a database that says it is replicated. */
  @AfterEach
  void removeWrapper() {
    if (database != null && database.isOpen())
      ((LocalDatabase) database).setWrappedDatabaseInstance((LocalDatabase) database);
  }

  @Test
  void aFollowerSkipsThePeriodicRefreshAndTheLeaderRunsIt() {
    final MaterializedViewImpl view = createPeriodicViewOverTwoRecords();
    database.transaction(() -> database.newDocument("Source11031").set("id", 3).save());

    final MaterializedViewScheduler scheduler = new MaterializedViewScheduler(database.getName());
    try {
      final long refreshesBefore = view.getRefreshCount();

      installWrapper(false);
      // Handed the raw instance, as LocalSchema hands it
      scheduler.runOneRefresh(database, view);
      assertThat(view.getRefreshCount()).as("a follower does not refresh: the leader replicates its own rewrite")
          .isEqualTo(refreshesBefore);
      assertThat(database.countType(view.getBackingTypeName(), false)).isEqualTo(2);

      installWrapper(true);
      scheduler.runOneRefresh(database, view);
      assertThat(view.getRefreshCount()).as("the leader refreshes").isEqualTo(refreshesBefore + 1);
      assertThat(database.countType(view.getBackingTypeName(), false)).isEqualTo(3);
    } finally {
      scheduler.shutdown();
    }
  }

  @Test
  void aStandaloneDatabaseStillRefreshes() {
    final MaterializedViewImpl view = createPeriodicViewOverTwoRecords();
    database.transaction(() -> database.newDocument("Source11031").set("id", 3).save());

    final MaterializedViewScheduler scheduler = new MaterializedViewScheduler(database.getName());
    try {
      scheduler.runOneRefresh(database, view);
      assertThat(database.countType(view.getBackingTypeName(), false)).isEqualTo(3);
    } finally {
      scheduler.shutdown();
    }
  }

  @Test
  void theFollowerTestResolvesTheCurrentWrapperOnEveryCall() {
    final DatabaseInternal raw = (DatabaseInternal) database;
    assertThat(raw.isFollower()).as("standalone").isFalse();
    installWrapper(false);
    assertThat(raw.isReplicated()).as("the raw instance never sees the wrapper by itself").isFalse();
    assertThat(raw.isFollower()).as("a follower, through the wrapper").isTrue();
    installWrapper(true);
    assertThat(raw.isFollower()).as("promoted: the next call sees it").isFalse();
  }

  private MaterializedViewImpl createPeriodicViewOverTwoRecords() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Source11031");
      database.newDocument("Source11031").set("id", 1).save();
      database.newDocument("Source11031").set("id", 2).save();
    });
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName("View11031")
        .withQuery("SELECT id FROM Source11031")
        .withRefreshMode(MaterializedViewRefreshMode.PERIODIC)
        // Long enough that the scheduler's own tick never fires during the test: the passes are driven by hand
        .withRefreshInterval(TimeUnit.HOURS.toMillis(1))
        .create());
    final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("View11031");
    assertThat(database.countType(view.getBackingTypeName(), false)).isEqualTo(2);
    return view;
  }

  private void installWrapper(final boolean leader) {
    final LocalDatabase real = (LocalDatabase) database;
    final DatabaseInternal wrapper = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if (method.getParameterCount() == 0)
            switch (method.getName()) {
            // Mirrors RaftReplicatedDatabase: the wrapper is its own wrapped instance
            case "getWrappedDatabaseInstance" -> {
              return proxy;
            }
            case "isReplicated" -> {
              return Boolean.TRUE;
            }
            case "isLeader" -> {
              return leader;
            }
            case "isFollower" -> {
              return InvocationHandler.invokeDefault(proxy, method);
            }
            default -> {
            }
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
