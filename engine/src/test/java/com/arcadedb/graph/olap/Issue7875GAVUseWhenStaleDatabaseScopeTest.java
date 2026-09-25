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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #7875: {@code arcadedb.gavUseWhenStale} is {@code SCOPE.DATABASE}, so {@code ALTER DATABASE} on it must
 * reach every view of that database - views already built, and views restored when the database is reopened - while an
 * explicit per-view override still wins and survives the restart.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7875GAVUseWhenStaleDatabaseScopeTest extends TestHelper {

  @Test
  void alterDatabaseReachesAnAlreadyBuiltView() {
    final GraphAnalyticalView gav = buildStaleView("gav7875", null);

    assertThat(gav.isStale()).isTrue();
    assertThat(gav.isUseWhenStale()).isFalse();
    assertThat(gav.isReady()).isFalse();
    assertThat(GraphTraversalProviderRegistry.findProvider(database, new String[] { "FOLLOWS" })).isNull();

    database.command("sql", "ALTER DATABASE `arcadedb.gavUseWhenStale` true").close();

    assertThat(gav.getUseWhenStaleOverride()).isNull();
    assertThat(gav.isUseWhenStale()).isTrue();
    assertThat(gav.isReady()).isTrue();
    assertThat(GraphTraversalProviderRegistry.findProvider(database, new String[] { "FOLLOWS" })).isNotNull();

    database.command("sql", "ALTER DATABASE `arcadedb.gavUseWhenStale` false").close();
    assertThat(gav.isReady()).isFalse();

    // The JVM-global value is untouched: the setting lives in this database's overlay only
    assertThat(GlobalConfiguration.GAV_USE_WHEN_STALE.getValueAsBoolean()).isFalse();
    gav.drop();
  }

  @Test
  void restoredViewFollowsTheDatabaseSetting() {
    database.command("sql", "ALTER DATABASE `arcadedb.gavUseWhenStale` true").close();
    buildStaleView("gav7875r", null);

    reopenDatabase();

    final GraphAnalyticalView restored = awaitRestored("gav7875r");
    assertThat(restored.getUseWhenStaleOverride()).isNull();
    assertThat(database.getConfiguration().getValueAsBoolean(GlobalConfiguration.GAV_USE_WHEN_STALE)).isTrue();
    assertThat(restored.isUseWhenStale()).isTrue();

    // Go stale again on the restored view: the database setting keeps it serving
    commitOneVertex();
    assertThat(restored.isStale()).isTrue();
    assertThat(restored.isReady()).isTrue();
    restored.drop();
  }

  @Test
  void explicitOverrideWinsAndSurvivesRestart() {
    database.command("sql", "ALTER DATABASE `arcadedb.gavUseWhenStale` true").close();
    final GraphAnalyticalView gav = buildStaleView("gav7875o", false);
    assertThat(gav.getUseWhenStaleOverride()).isFalse();
    assertThat(gav.isReady()).isFalse();

    reopenDatabase();

    final GraphAnalyticalView restored = awaitRestored("gav7875o");
    assertThat(restored.getUseWhenStaleOverride()).isFalse();
    commitOneVertex();
    assertThat(restored.isStale()).isTrue();
    assertThat(restored.isReady()).isFalse();

    // Clearing the override goes back to the database setting
    restored.setUseWhenStale(null);
    assertThat(restored.isReady()).isTrue();
    restored.drop();
  }

  private GraphAnalyticalView buildStaleView(final String name, final Boolean useWhenStale) {
    if (!database.getSchema().existsType("Person")) {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("FOLLOWS");
    }
    commitOneVertex();

    final GraphAnalyticalViewBuilder builder = GraphAnalyticalView.builder(database)
        .withName(name)
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF);
    if (useWhenStale != null)
      builder.withUseWhenStale(useWhenStale);
    final GraphAnalyticalView gav = builder.build();
    assertThat(gav.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);

    // OFF mode: any commit marks the view STALE
    commitOneVertex();
    return gav;
  }

  private GraphAnalyticalView awaitRestored(final String name) {
    final GraphAnalyticalView restored = GraphAnalyticalViewRegistry.get(database, name);
    assertThat(restored).isNotNull();
    assertThat(restored.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(restored.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    return restored;
  }

  private void commitOneVertex() {
    database.transaction(() -> database.newVertex("Person").set("name", "p").save());
  }
}
