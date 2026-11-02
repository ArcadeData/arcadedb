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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Background work of a graph analytical view, on two edges.
 * <p>
 * Issue #11032: a deferred restore from disk that failed with nothing to serve set FAILED and stopped there, while the identical
 * failure of an asynchronous build also unregistered the view and freed its name. After a restore that died of an
 * {@link OutOfMemoryError} a CREATE of the same name was refused as already existing. Both now share one teardown.
 * <p>
 * Issue #11021: the leader-only guards of the view read the replication flags off the database the view was built with, which
 * for a view restored at open is the raw {@code LocalDatabase}, built before the server wraps it for HA: they could never fire,
 * and a follower wrote at close the freshness certificate its own code calls untrustworthy. The wrapper here is installed the
 * way the HA server installs {@code RaftReplicatedDatabase}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11032Issue11021GavBackgroundWorkTest extends TestHelper {
  private static final String VIEW     = "roads11032";
  private static final int    VERTICES = 50;

  /** Runs before {@code TestHelper.afterTest()}, whose integrity check would otherwise meet a database that says it is replicated. */
  @AfterEach
  void removeWrapper() {
    if (database != null && database.isOpen())
      ((LocalDatabase) database).setWrappedDatabaseInstance((LocalDatabase) database);
  }

  @Test
  void aDeferredRestoreThatFailsFreesTheNameAsAFailedBuildDoes() {
    createGraphAndView();
    reopenDatabase();

    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, VIEW);
    assertThat(view.isBuilt()).as("restored lazily: nothing is read before something needs the view").isFalse();
    final OutOfMemoryError failure = new OutOfMemoryError("simulated while sizing the columns");
    view.setBeforeDiskRestoreForTest(() -> {
      throw failure;
    });

    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isFalse();

    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.FAILED);
    assertThat(view.getBuildError()).isSameAs(failure);
    assertThat(GraphAnalyticalViewRegistry.get(database, VIEW)).as("the name is freed, as after a failed build").isNull();
    assertThat(GraphAnalyticalViewRegistry.getFailure(database, VIEW)).as("and the reason kept for the schema row")
        .isSameAs(failure);
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:graphAnalyticalViews WHERE name = ?", VIEW)) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("status")).isEqualTo("FAILED");
    }

    // The same name can be created again, and builds
    final GraphAnalyticalView rebuilt = newViewBuilder().build();
    assertThat(rebuilt.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(rebuilt.getNodeCount()).isEqualTo(VERTICES);
    assertThat(GraphAnalyticalViewRegistry.getFailure(database, VIEW)).isNull();
  }

  @Test
  void aFailedViewDoesNotUnregisterTheViewThatReplacedItUnderTheSameName() {
    createGraphAndView();
    final GraphAnalyticalView first = GraphAnalyticalViewRegistry.get(database, VIEW);
    GraphAnalyticalViewRegistry.unregister(database, VIEW);
    final GraphAnalyticalView second = new GraphAnalyticalView(database, VIEW, new String[] { "City" }, new String[] { "ROAD" },
        null, null, GraphAnalyticalView.UpdateMode.OFF);
    GraphAnalyticalViewRegistry.register(database, VIEW, second);
    try {
      final RuntimeException oldFailure = new RuntimeException("the old view's build");
      assertThat(GraphAnalyticalViewRegistry.unregisterFailed(database, VIEW, first, oldFailure))
          .as("the old view no longer owns the name").isFalse();
      assertThat(GraphAnalyticalViewRegistry.get(database, VIEW)).isSameAs(second);
      assertThat(GraphAnalyticalViewRegistry.getFailure(database, VIEW)).as("nor does it leave its failure on the successor")
          .isNull();

      final RuntimeException failure = new RuntimeException("the successor's build");
      assertThat(GraphAnalyticalViewRegistry.unregisterFailed(database, VIEW, second, failure)).isTrue();
      assertThat(GraphAnalyticalViewRegistry.get(database, VIEW)).isNull();
      assertThat(GraphAnalyticalViewRegistry.getFailure(database, VIEW)).isSameAs(failure);
    } finally {
      GraphAnalyticalViewRegistry.clearFailure(database, VIEW);
      first.shutdown();
      second.shutdown();
    }
  }

  @Test
  void aFollowerDoesNotPersistTheCertificateItCannotTrust() {
    createGraphAndView();
    final File csr = GraphAnalyticalViewCSRPersistence.fileFor(database, VIEW);
    // Whatever the build wrote goes: the close below is the only writer this test looks at
    deleteCsr(csr);

    installWrapper(false);
    reopenDatabase();
    assertThat(csr).as("a follower's certificate freezes against replicated commits: it must not be written").doesNotExist();

    // The arm that proves the one above is not passing because nothing is ever written: a leader writes it
    GraphAnalyticalViewRegistry.get(database, VIEW).awaitReady(30, TimeUnit.SECONDS);
    deleteCsr(csr);
    installWrapper(true);
    reopenDatabase();
    assertThat(csr).as("the leader persists it").exists();
  }

  private static void deleteCsr(final File csr) {
    if (csr.exists())
      assertThat(csr.delete()).isTrue();
  }

  private void createGraphAndView() {
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("ROAD");
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.newVertex("City").set("id", i).save();
    });
    final List<RID> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM City ORDER BY id")) {
      while (rs.hasNext())
        ids.add(rs.next().getIdentity().orElseThrow());
    }
    database.transaction(() -> {
      for (int i = 0; i < ids.size(); i++)
        ids.get(i).asVertex().modify().newEdge("ROAD", ids.get((i + 1) % ids.size()).asVertex());
    });

    newViewBuilder().build();
    assertThat(GraphAnalyticalViewRegistry.get(database, VIEW).getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
  }

  private GraphAnalyticalViewBuilder newViewBuilder() {
    return GraphAnalyticalView.builder(database)
        .withName(VIEW)
        .withVertexTypes("City")
        .withEdgeTypes("ROAD")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF);
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
