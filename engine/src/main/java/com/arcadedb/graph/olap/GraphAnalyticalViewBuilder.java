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

import com.arcadedb.database.Database;

/**
 * Fluent builder for {@link GraphAnalyticalView}.
 * <p>
 * Allows fine-grained configuration of which vertex types, edge types, and properties
 * to include in the analytical view, saving RAM by materializing only what's needed.
 * <p>
 * Usage:
 * <pre>
 *   // Synchronous build
 *   GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
 *       .withName("social-graph")
 *       .withVertexTypes("Person", "Company")
 *       .withEdgeTypes("FOLLOWS", "WORKS_AT")
 *       .withProperties("name", "age")
 *       .withAutoUpdate(true)
 *       .build();
 *
 *   // Asynchronous build
 *   GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
 *       .withName("social-graph")
 *       .withVertexTypes("Person")
 *       .buildAsync();
 *   gav.awaitReady(10, TimeUnit.SECONDS);
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphAnalyticalViewBuilder {
  private final Database database;
  private       String   name;
  private       String[] vertexTypes;
  private       String[] edgeTypes;
  private       String[] properties;
  private       boolean  autoUpdate;
  private       int      compactionThreshold = -1;

  GraphAnalyticalViewBuilder(final Database database) {
    this.database = database;
  }

  /**
   * Sets the name for this analytical view. When a name is set, the view is automatically
   * registered in the {@link GraphAnalyticalViewRegistry} and can be looked up by name.
   */
  public GraphAnalyticalViewBuilder withName(final String name) {
    this.name = name;
    return this;
  }

  /**
   * Specifies the vertex types to include. If not called or null, all vertex types are included.
   */
  public GraphAnalyticalViewBuilder withVertexTypes(final String... vertexTypes) {
    this.vertexTypes = vertexTypes;
    return this;
  }

  /**
   * Specifies the edge types to include. If not called or null, all edge types are included.
   */
  public GraphAnalyticalViewBuilder withEdgeTypes(final String... edgeTypes) {
    this.edgeTypes = edgeTypes;
    return this;
  }

  /**
   * Specifies the vertex properties to materialize in columnar storage.
   * If not called or null, all properties are included.
   * Pass an empty array to skip property materialization entirely.
   */
  public GraphAnalyticalViewBuilder withProperties(final String... properties) {
    this.properties = properties;
    return this;
  }

  /**
   * Enables automatic rebuild of the analytical view after each transaction commit.
   * When enabled, record listeners are registered on the database to detect changes,
   * and a post-commit callback triggers an asynchronous rebuild.
   */
  public GraphAnalyticalViewBuilder withAutoUpdate(final boolean autoUpdate) {
    this.autoUpdate = autoUpdate;
    return this;
  }

  /**
   * Sets the compaction threshold — the number of delta edges accumulated before
   * triggering a full background rebuild of the CSR. Default is 10,000.
   * Only meaningful when {@link #withAutoUpdate(boolean)} is enabled.
   */
  public GraphAnalyticalViewBuilder withCompactionThreshold(final int compactionThreshold) {
    this.compactionThreshold = compactionThreshold;
    return this;
  }

  /**
   * Builds the analytical view synchronously with the configured settings.
   * This triggers the initial full build (CSR + columnar storage) and blocks until complete.
   * Status will be READY when this method returns.
   */
  public GraphAnalyticalView build() {
    final GraphAnalyticalView view = new GraphAnalyticalView(database, name, vertexTypes, edgeTypes, properties, autoUpdate);
    if (compactionThreshold > 0)
      view.setCompactionThreshold(compactionThreshold);
    if (name != null) {
      GraphAnalyticalViewRegistry.register(database, name, view);
      GraphAnalyticalViewPersistence.save(database, view);
    }
    view.registerAsTraversalProvider();
    view.build();
    return view;
  }

  /**
   * Builds the analytical view asynchronously in a background thread.
   * Returns immediately with the view in BUILDING status.
   * Use {@link GraphAnalyticalView#awaitReady} or {@link GraphAnalyticalView#getStatus()} to check completion.
   */
  public GraphAnalyticalView buildAsync() {
    final GraphAnalyticalView view = new GraphAnalyticalView(database, name, vertexTypes, edgeTypes, properties, autoUpdate);
    if (compactionThreshold > 0)
      view.setCompactionThreshold(compactionThreshold);
    if (name != null) {
      GraphAnalyticalViewRegistry.register(database, name, view);
      GraphAnalyticalViewPersistence.save(database, view);
    }
    view.registerAsTraversalProvider();
    view.buildAsync();
    return view;
  }
}
