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
import com.arcadedb.log.LogManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Fluent builder for {@link GraphAnalyticalView}.
 * <p>
 * Allows fine-grained configuration of which vertex types, edge types, and properties
 * to include in the analytical view, saving RAM by materializing only what's needed.
 * <p>
 * Usage:
 * <pre>
 *   // Synchronous update mode (overlay, no stale window)
 *   GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
 *       .withName("social-graph")
 *       .withVertexTypes("Person", "Company")
 *       .withEdgeTypes("FOLLOWS", "WORKS_AT")
 *       .withProperties("name", "age")
 *       .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
 *       .build();
 *
 *   // Asynchronous update mode (async rebuild after commit)
 *   GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
 *       .withName("social-graph")
 *       .withVertexTypes("Person")
 *       .withUpdateMode(GraphAnalyticalView.UpdateMode.ASYNCHRONOUS)
 *       .build();
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphAnalyticalViewBuilder {
  private final Database                        database;
  private       String                          name;
  private       String[]                        vertexTypes;
  private       String[]                        edgeTypes;
  private       String[]                        properties;
  private       String[]                        edgeProperties;
  private       GraphAnalyticalView.UpdateMode  updateMode = GraphAnalyticalView.UpdateMode.OFF;
  private       int                             compactionThreshold = -1;
  private       int                             propertySampleSize  = -1;
  private       Boolean                         useWhenStale;
  private       boolean                         skipPersistence;
  // A view restored from its saved definition (database open, type rename) is never refused: see createView()
  private       boolean                         fromDefinition;
  private final List<String[]>                  contractionHierarchies = new ArrayList<>();

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
   * Specifies the edge properties to materialize in columnar storage alongside the CSR adjacency.
   * If not called or null, no edge properties are stored (default — zero overhead).
   * When specified, edge properties are stored aligned to the forward CSR arrays and accessible
   * via {@link GraphTraversalProvider#edgeWeightsForSlice} for both forward and backward traversals.
   * <p>
   * Example: {@code .withEdgeProperties("weight")} to store edge weights for Dijkstra/SSSP.
   */
  public GraphAnalyticalViewBuilder withEdgeProperties(final String... edgeProperties) {
    this.edgeProperties = edgeProperties;
    return this;
  }

  /**
   * Sets the update mode for the analytical view:
   * <ul>
   *   <li>{@link GraphAnalyticalView.UpdateMode#OFF OFF} — no auto-update; view becomes STALE on commit (default)</li>
   *   <li>{@link GraphAnalyticalView.UpdateMode#SYNCHRONOUS SYNCHRONOUS} — applies changes via overlay on commit; no stale window</li>
   *   <li>{@link GraphAnalyticalView.UpdateMode#ASYNCHRONOUS ASYNCHRONOUS} — triggers async rebuild on commit; brief BUILDING window</li>
   * </ul>
   */
  public GraphAnalyticalViewBuilder withUpdateMode(final GraphAnalyticalView.UpdateMode updateMode) {
    this.updateMode = updateMode;
    return this;
  }

  /**
   * Sets the compaction threshold — the number of delta edges accumulated before
   * triggering a full background rebuild of the CSR. Default is 10,000.
   * Only meaningful when update mode is {@link GraphAnalyticalView.UpdateMode#SYNCHRONOUS}.
   */
  public GraphAnalyticalViewBuilder withCompactionThreshold(final int compactionThreshold) {
    this.compactionThreshold = compactionThreshold;
    return this;
  }

  /**
   * Controls whether this view is used by the query planner when its data is stale.
   * When true (default), stale CSR data is used for faster traversals even though it may not
   * reflect the latest committed changes. When false, the query planner falls back to OLTP
   * traversal if the view is stale.
   */
  public GraphAnalyticalViewBuilder withUseWhenStale(final boolean useWhenStale) {
    this.useWhenStale = useWhenStale;
    return this;
  }

  /**
   * Sets the number of records to sample for detecting property types in schemaless databases.
   * Default is 100. Properties that first appear beyond this limit will be excluded from the
   * columnar store. Has no effect when vertex types have schema-defined properties.
   */
  public GraphAnalyticalViewBuilder withPropertySampleSize(final int propertySampleSize) {
    this.propertySampleSize = propertySampleSize;
    return this;
  }

  /**
   * Keeps a Customizable Contraction Hierarchy on the view for fast point-to-point shortest paths weighted by
   * {@code weightProperty}, over {@code edgeTypes} or, when none are given, every edge type of the view. The weight is
   * added to the materialized edge properties when it is not there already. See {@link ContractionHierarchy}.
   */
  public GraphAnalyticalViewBuilder withContractionHierarchy(final String weightProperty, final String... edgeTypes) {
    if (weightProperty == null || weightProperty.isEmpty())
      throw new IllegalArgumentException("A contraction hierarchy needs a weight property");
    final String[] spec = new String[1 + (edgeTypes == null ? 0 : edgeTypes.length)];
    spec[0] = weightProperty;
    if (edgeTypes != null)
      System.arraycopy(edgeTypes, 0, spec, 1, edgeTypes.length);
    contractionHierarchies.add(spec);
    return this;
  }

  /**
   * Builds the analytical view synchronously with the configured settings.
   * This triggers the initial full build (CSR + columnar storage) and blocks until complete.
   * Status will be READY when this method returns.
   */
  public GraphAnalyticalView build() {
    final GraphAnalyticalView view = createView();
    try {
      view.build();
    } catch (final Exception | Error e) {
      view.shutdown();
      // The definition stays: the schema row reports the view FAILED, and why, until it is rebuilt or dropped (issue #11011)
      if (name != null)
        GraphAnalyticalViewRegistry.recordFailure(database, name, e);
      throw e;
    }
    return view;
  }

  /**
   * Builds the analytical view asynchronously in a background thread.
   * Returns immediately with the view in BUILDING status.
   * Use {@link GraphAnalyticalView#awaitReady} or {@link GraphAnalyticalView#getStatus()} to check completion.
   */
  public GraphAnalyticalView buildAsync() {
    final GraphAnalyticalView view = createView();
    try {
      view.buildAsync();
    } catch (final Exception e) {
      view.shutdown();
      throw e;
    }
    return view;
  }

  /**
   * Skips persisting the definition to schema. Used during restore to avoid redundant writes.
   */
  public GraphAnalyticalViewBuilder skipPersistence() {
    this.skipPersistence = true;
    return this;
  }

  /** Marks a builder of a saved definition, which is rebuilt as it was defined rather than refused (see createView()). */
  GraphAnalyticalViewBuilder fromDefinition() {
    this.fromDefinition = true;
    return this;
  }

  /**
   * Used by {@link GraphAnalyticalViewPersistence#restoreAll} on database open (see #6583, #6632): when a
   * persisted CSR plausibly applies, marks the view READY immediately and defers the actual disk read to
   * whatever touches it first (a real query, or an explicit {@link GraphAnalyticalView#awaitReady} call) —
   * see {@link GraphAnalyticalView#restoreFromDiskOrBuildAsync()} — rather than reading it eagerly right
   * here. Falls back to the usual {@link #buildAsync()} full graph scan immediately when there is no
   * plausibly-usable file. Either way the view is registered and returned immediately; check {@link
   * GraphAnalyticalView#getStatus()} / {@link GraphAnalyticalView#awaitReady} for completion exactly as
   * with {@link #buildAsync()}.
   */
  GraphAnalyticalView restoreFromDiskOrBuildAsync() {
    final GraphAnalyticalView view = createView();
    try {
      view.restoreFromDiskOrBuildAsync();
    } catch (final Exception e) {
      view.shutdown();
      throw e;
    }
    return view;
  }

  private GraphAnalyticalView createView() {
    final String[] allEdgeProperties = withHierarchyWeights(edgeProperties);
    checkEdgePropertiesHaveColumns(allEdgeProperties);
    final GraphAnalyticalView view = new GraphAnalyticalView(database, name, vertexTypes, edgeTypes, properties,
        allEdgeProperties, updateMode);
    for (final String[] spec : contractionHierarchies)
      view.addContractionHierarchy(spec[0], Arrays.copyOfRange(spec, 1, spec.length));
    if (compactionThreshold >= 0)
      view.setCompactionThreshold(compactionThreshold);
    if (propertySampleSize >= 0)
      view.setPropertySampleSize(propertySampleSize);
    if (useWhenStale != null)
      view.setUseWhenStale(useWhenStale);
    if (name != null) {
      GraphAnalyticalViewRegistry.register(database, name, view);
      if (!skipPersistence)
        GraphAnalyticalViewPersistence.save(database, view);
    }
    view.registerAsTraversalProvider();
    return view;
  }

  /**
   * Refuses an edge property the view would store no column for (issue #12177): one the schema does not declare on the edge
   * types of the view, or declares only with a type no column holds, or with types that differ across them. The view would
   * list it among its edge properties while holding no value of it, and every call weighted by it would read the edge records,
   * as without the view. The type of an undeclared property is not inferred from the values the build meets: a view created
   * before its edges, or kept up to date as edges with other values arrive, would have no stable type to infer, while a
   * declaration gives every build and every update the same one.
   * <p>
   * A saved definition is rebuilt as it was defined, a refusal there would drop it: the properties without a column are
   * reported in the log instead.
   */
  private void checkEdgePropertiesHaveColumns(final String[] allEdgeProperties) {
    if (allEdgeProperties == null || allEdgeProperties.length == 0)
      return;
    final List<String> withoutColumn = new CSRBuilder(database, null, allEdgeProperties, CSRBuilder.DEFAULT_PROPERTY_SAMPLE_SIZE)
        .edgePropertiesWithoutColumn(edgeTypes);
    if (withoutColumn.isEmpty())
      return;

    final String typeNames = edgeTypes == null || edgeTypes.length == 0 ? "any edge type" : String.join(", ", edgeTypes);
    final String exampleType = edgeTypes != null && edgeTypes.length > 0 ? edgeTypes[0] : "<EdgeType>";
    final String names = withoutColumn.stream().map(n -> "'" + n + "'").collect(Collectors.joining(", "));
    final String message = "Graph Analytical View" + (name != null ? " '" + name + "'" : "") + ": edge propert"
        + (withoutColumn.size() == 1 ? "y " + names + " is" : "ies " + names + " are")
        + " not declared on " + typeNames + " with a type a column can hold (INTEGER, SHORT, BYTE, LONG, FLOAT, DOUBLE or STRING,"
        + " the same on every edge type), so the view would hold no value of it and every call weighted by it would read the edge"
        + " records. Declare it before creating the view, for example CREATE PROPERTY " + exampleType + "." + withoutColumn.getFirst()
        + " DOUBLE";
    if (!fromDefinition)
      throw new IllegalArgumentException(message);
    LogManager.instance().log(this, Level.WARNING, message);
  }

  /** The edge properties to materialize, with every hierarchy's weight among them. */
  private String[] withHierarchyWeights(final String[] configured) {
    if (contractionHierarchies.isEmpty())
      return configured;
    final Set<String> all = new LinkedHashSet<>();
    if (configured != null)
      all.addAll(Arrays.asList(configured));
    for (final String[] spec : contractionHierarchies)
      all.add(spec[0]);
    return all.toArray(new String[0]);
  }
}
