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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Locale;

/**
 * The create, rebuild and drop of a Graph Analytical View as a command does them: the permission check, the exists check
 * and the build. Shared by the SQL statements ({@code CREATE/REBUILD/DROP GRAPH ANALYTICAL VIEW}) and the Cypher
 * procedures ({@code db.analyticalView.*}, issue #11438), so the two languages cannot drift apart.
 */
public final class GraphAnalyticalViewCommands {

  /**
   * What a view is created with, in the terms of the SQL clauses.
   *
   * @param vertexTypes         null or empty for every vertex type
   * @param edgeTypes           null or empty for every edge type
   * @param properties          null copies every vertex property, an empty array none (issue #9831)
   * @param edgeProperties      null or empty for none beyond the hierarchy weights
   * @param updateMode          null for {@link GraphAnalyticalView.UpdateMode#OFF}
   * @param compactionThreshold negative for the default
   * @param cchWeights          the weight properties to keep a contraction hierarchy for, null or empty for none
   */
  public record Definition(String name, String[] vertexTypes, String[] edgeTypes, String[] properties, String[] edgeProperties,
                           GraphAnalyticalView.UpdateMode updateMode, int compactionThreshold, String[] cchWeights) {
  }

  private GraphAnalyticalViewCommands() {
  }

  /**
   * Creates the view and starts its build in the background: it is BUILDING when this returns.
   *
   * @return the view, or null when one with the name exists already and {@code ifNotExists} is set
   *
   * @throws AlreadyExistsException when one with the name exists already and {@code ifNotExists} is not set
   */
  public static GraphAnalyticalView create(final DatabaseInternal database, final Definition definition, final boolean ifNotExists) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);
    final String viewName = definition.name();

    if (exists(database, viewName)) {
      if (ifNotExists)
        return null;
      throw new AlreadyExistsException(AlreadyExistsException.Kind.GRAPH_ANALYTICAL_VIEW,
          "Graph Analytical View '" + viewName + "' already exists");
    }

    // The builder persists the definition through GraphAnalyticalViewPersistence.save()
    final GraphAnalyticalViewBuilder builder = GraphAnalyticalView.builder(database).withName(viewName);
    if (isNotEmpty(definition.vertexTypes()))
      builder.withVertexTypes(definition.vertexTypes());
    if (isNotEmpty(definition.edgeTypes()))
      builder.withEdgeTypes(definition.edgeTypes());
    // An empty array copies no vertex property while an absent one copies all (issue #9831)
    if (definition.properties() != null)
      builder.withProperties(definition.properties());
    if (isNotEmpty(definition.edgeProperties()))
      builder.withEdgeProperties(definition.edgeProperties());
    builder.withUpdateMode(definition.updateMode() != null ? definition.updateMode() : GraphAnalyticalView.UpdateMode.OFF);
    if (definition.compactionThreshold() >= 0)
      builder.withCompactionThreshold(definition.compactionThreshold());
    if (definition.cchWeights() != null)
      for (final String weight : definition.cchWeights())
        builder.withContractionHierarchy(weight);
    return builder.buildAsync();
  }

  /**
   * Rebuilds the view synchronously, or builds it again from its definition when its last build failed and left the
   * registry (issue #10368), so the reason it fails, if it still does, reaches the caller rather than a log line.
   *
   * @throws SchemaException when no view has the name
   */
  public static GraphAnalyticalView rebuild(final DatabaseInternal database, final String viewName) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    GraphAnalyticalView liveView = GraphAnalyticalViewRegistry.get(database, viewName);
    if (liveView != null)
      liveView.build();
    else {
      liveView = GraphAnalyticalViewPersistence.rebuildFromDefinition(database, viewName);
      if (liveView == null)
        throw new SchemaException("Graph Analytical View '" + viewName + "' does not exist");
    }
    return liveView;
  }

  /**
   * Drops the view and its definition.
   *
   * @return false when no view has the name and {@code ifExists} is set
   *
   * @throws SchemaException when no view has the name and {@code ifExists} is not set
   */
  public static boolean drop(final DatabaseInternal database, final String viewName, final boolean ifExists) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (!exists(database, viewName)) {
      if (ifExists)
        return false;
      throw new SchemaException("Graph Analytical View '" + viewName + "' does not exist");
    }

    // Dropping the in-memory view removes its definition from the schema too
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, viewName);
    if (view != null)
      view.drop();
    else {
      // No in-memory view (its build failed, say): remove the definition, and the failure recorded for it, from the schema
      GraphAnalyticalViewPersistence.remove(database, viewName);

      // Unregister any orphaned traversal provider with this name
      for (final GraphTraversalProvider provider : GraphTraversalProviderRegistry.getProviders(database))
        if (viewName.equals(provider.getName()))
          GraphTraversalProviderRegistry.unregister(database, provider);
    }
    return true;
  }

  /** Whether the schema holds a definition with this name. */
  public static boolean exists(final Database database, final String viewName) {
    final JSONObject allGavs = database.getSchema().getExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY);
    return allGavs != null && allGavs.has(viewName);
  }

  /**
   * The status a view with this name reports, as {@code SELECT FROM schema:graphAnalyticalViews} reads it: the live view's,
   * FAILED for a view whose build failed and left the registry (issue #11011), NOT_BUILT for one with no live view.
   */
  public static String statusOf(final Database database, final String viewName) {
    final GraphAnalyticalView liveView = GraphAnalyticalViewRegistry.get(database, viewName);
    if (liveView != null) {
      liveView.startPendingRestore();
      return liveView.getStatus().name();
    }
    return GraphAnalyticalViewRegistry.getFailure(database, viewName) != null ?
        GraphAnalyticalView.Status.FAILED.name() :
        GraphAnalyticalView.Status.NOT_BUILT.name();
  }

  /**
   * Parses an update mode name, ignoring its case.
   *
   * @throws CommandExecutionException naming the value and the valid ones
   */
  public static GraphAnalyticalView.UpdateMode parseUpdateMode(final String updateMode) {
    try {
      return GraphAnalyticalView.UpdateMode.valueOf(updateMode.toUpperCase(Locale.ROOT));
    } catch (final IllegalArgumentException e) {
      throw new CommandExecutionException("Unknown update mode: '" + updateMode + "'. Valid values: OFF, SYNCHRONOUS, ASYNCHRONOUS");
    }
  }

  private static boolean isNotEmpty(final String[] array) {
    return array != null && array.length > 0;
  }
}
