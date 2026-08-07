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
package com.arcadedb.gremlin.consumer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.gremlin.ArcadeGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs on the exact classpath a third-party Maven project gets from a single, no-classifier
 * {@code arcadedb-gremlin} dependency declaration (this module's pom.xml mirrors that
 * reproducer). Guards the embedded, fluent-API consumption path against issue #5879: before the
 * fix, {@code arcadedb-network} was reachable only through the {@code provided}-scoped
 * {@code arcadedb-server}, so {@code com.arcadedb.remote.RemoteDatabase} - referenced by an
 * {@code instanceof} check on the ordinary embedded query path in {@code ArcadeGraph},
 * {@code ArcadeGraphFactory} and {@code ArcadeGremlin} - threw {@code NoClassDefFoundError} on the
 * first query for every consumer of the published artifact, embedded-only or not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinPlainJarConsumerIT {

  @Test
  void fluentTraversalWorksWithOnlyThePlainCoordinateOnTheClasspath(@TempDir final Path dir) {
    try (final DatabaseFactory factory = new DatabaseFactory(dir.resolve("db").toString());
        final Database db = factory.create()) {
      db.getSchema().createVertexType("V");
      db.begin();
      db.newVertex("V").save();
      db.commit();

      try (final ArcadeGraph graph = ArcadeGraph.open(db)) {
        final GraphTraversalSource g = graph.traversal();
        assertThat(g.V().count().next()).isEqualTo(1L);
      }
    }
  }

  /**
   * Documents a known, separate limitation (issue #5937), left untouched by #5879 on purpose: the
   * secure default {@code "java"} Gremlin engine (used by {@code analyze()}/
   * {@code command("gremlin", ...)}, i.e. ArcadeDB's textual QueryEngine SPI) drives TinkerPop's
   * ANTLR-4.9.1-precompiled grammar parser, which cannot initialize once the engine's ANTLR
   * 4.13.2 runtime is also on the classpath - as it always is here, since arcadedb-integration
   * pulls in arcadedb-engine. Only the relocated {@code arcadedb-gremlin:shaded} classifier
   * avoids that clash. Re-enable this once #5937 lands.
   */
  @Disabled("known limitation, tracked separately as issue #5937 - not fixed by #5879")
  @Test
  void textualQueryEngineOnThePlainCoordinateStillCrashesUntilShaded() {
    // final Path dir = ...
    // db.getQueryEngine("gremlin").analyze("g.V()"); // ExceptionInInitializerError: ATN v3 vs v4
  }
}
