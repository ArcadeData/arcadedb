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
package com.arcadedb.server.gremlin;

import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;

import javax.script.Bindings;
import javax.script.SimpleBindings;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * Custom GraphManager for ArcadeDB that supports dynamic, on-demand registration
 * of databases as Gremlin graphs. When a client requests a graph/traversal source
 * by database name, this manager checks if the database exists in ArcadeDBServer
 * and automatically registers it.
 * <p>
 * This enables ArcadeDB's multi-database architecture to work seamlessly with
 * Gremlin Server without requiring static configuration of each database.
 */
public class ArcadeGraphManager implements GraphManager {

  private static ArcadeDBServer serverInstance;

  private final Map<String, Graph>           graphs           = new ConcurrentHashMap<>();
  private final Map<String, TraversalSource> traversalSources = new ConcurrentHashMap<>();

  public ArcadeGraphManager(final Settings settings) {
    // Settings can define pre-configured graphs, but we primarily use dynamic registration
    if (settings.graphs != null) {
      settings.graphs.forEach((name, path) -> {
        // For pre-configured graphs, we'll create them on first access
        LogManager.instance().log(this, Level.INFO,
            "Graph '%s' configured in settings - will be created on first access", name);
      });
    }
  }

  /**
   * Sets the ArcadeDBServer instance to be used for database lookups.
   * This must be called before the GraphManager is used.
   */
  public static void setServer(final ArcadeDBServer server) {
    serverInstance = server;
  }

  @Override
  public Set<String> getGraphNames() {
    return graphs.keySet();
  }

  @Override
  public Graph getGraph(final String graphName) {
    Graph graph = graphs.get(graphName);
    if (graph == null) {
      graph = getOrCreateArcadeGraph(graphName);
    }
    return graph;
  }

  @Override
  public void putGraph(final String graphName, final Graph graph) {
    graphs.put(graphName, graph);
  }

  @Override
  public Set<String> getTraversalSourceNames() {
    // Return both registered traversal sources and available databases
    // This is needed because validateTraversalSourceAlias checks this set
    // before calling getTraversalSource
    final Set<String> names = new HashSet<>(traversalSources.keySet());
    if (serverInstance != null) {
      names.addAll(serverInstance.getDatabaseNames());
      // Also include "g" alias for script-based queries
      if (!serverInstance.getDatabaseNames().isEmpty()) {
        names.add("g");
      }
    }
    return names;
  }

  @Override
  public TraversalSource getTraversalSource(final String traversalSourceName) {
    TraversalSource ts = traversalSources.get(traversalSourceName);
    if (ts == null) {
      String dbName = traversalSourceName;

      // Handle "g" as alias for the default/first available database
      if ("g".equals(traversalSourceName) && serverInstance != null) {
        final Set<String> dbNames = serverInstance.getDatabaseNames();
        if (!dbNames.isEmpty()) {
          // Use "graph" if available, otherwise use the first database
          if (dbNames.contains("graph")) {
            dbName = "graph";
          } else {
            dbName = dbNames.iterator().next();
          }
          LogManager.instance().log(this, Level.INFO,
              "Mapping 'g' alias to database '%s'", dbName);
        }
      }

      // Try to create from database
      final Graph graph = getOrCreateArcadeGraph(dbName);
      if (graph != null) {
        ts = graph.traversal();
        traversalSources.put(traversalSourceName, ts);
      }
    }
    return ts;
  }

  @Override
  public void putTraversalSource(final String tsName, final TraversalSource ts) {
    traversalSources.put(tsName, ts);
  }

  @Override
  public TraversalSource removeTraversalSource(final String tsName) {
    return traversalSources.remove(tsName);
  }

  @Override
  public Graph removeGraph(final String graphName) {
    final Graph graph = graphs.remove(graphName);
    traversalSources.remove(graphName);
    if (graph instanceof ArcadeGraph) {
      try {
        ((ArcadeGraph) graph).close();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Error closing ArcadeGraph for '%s': %s", graphName, e.getMessage());
      }
    }
    return graph;
  }

  @Override
  public Bindings getAsBindings() {
    // Ensure 'g' is available for script-based queries before returning bindings
    if (!traversalSources.containsKey("g") && serverInstance != null) {
      final Set<String> dbNames = serverInstance.getDatabaseNames();
      if (!dbNames.isEmpty()) {
        // This will create and cache the traversal source for 'g'
        getTraversalSource("g");
      }
    }

    final Bindings bindings = new SimpleBindings();
    graphs.forEach(bindings::put);
    traversalSources.forEach(bindings::put);
    return bindings;
  }

  @Override
  public void rollbackAll() {
    graphs.values().forEach(graph -> {
      if (graph.features().graph().supportsTransactions() && graph.tx().isOpen()) {
        graph.tx().rollback();
      }
    });
  }

  @Override
  public void rollback(final Set<String> graphSourceNamesToCloseTxOn) {
    closeTx(graphSourceNamesToCloseTxOn, Transaction.Status.ROLLBACK);
  }

  @Override
  public void commitAll() {
    graphs.values().forEach(graph -> {
      if (graph.features().graph().supportsTransactions() && graph.tx().isOpen()) {
        graph.tx().commit();
      }
    });
  }

  @Override
  public void commit(final Set<String> graphSourceNamesToCloseTxOn) {
    closeTx(graphSourceNamesToCloseTxOn, Transaction.Status.COMMIT);
  }

  @Override
  public Graph openGraph(final String graphName, final Function<String, Graph> supplier) {
    Graph graph = graphs.get(graphName);
    if (graph != null) {
      return graph;
    }

    // First try our dynamic creation
    graph = getOrCreateArcadeGraph(graphName);
    if (graph != null) {
      return graph;
    }

    // Fall back to supplier if provided
    if (supplier != null) {
      graph = supplier.apply(graphName);
      if (graph != null) {
        graphs.put(graphName, graph);
      }
    }
    return graph;
  }

  /**
   * Creates or retrieves an ArcadeGraph for the given database name.
   */
  private synchronized Graph getOrCreateArcadeGraph(final String databaseName) {
    if (serverInstance == null) {
      LogManager.instance().log(this, Level.WARNING,
          "ArcadeDBServer not set in ArcadeGraphManager. Cannot dynamically register database '%s'", databaseName);
      return null;
    }

    // Check if already created
    Graph graph = graphs.get(databaseName);
    if (graph != null) {
      return graph;
    }

    // Check if database exists
    if (!serverInstance.existsDatabase(databaseName)) {
      LogManager.instance().log(this, Level.FINE,
          "Database '%s' does not exist in ArcadeDBServer", databaseName);
      return null;
    }

    try {
      // Create ArcadeGraph wrapper for the database
      final ArcadeGraph arcadeGraph = ArcadeGraph.open(serverInstance.getDatabase(databaseName));
      graphs.put(databaseName, arcadeGraph);
      traversalSources.put(databaseName, arcadeGraph.traversal());

      LogManager.instance().log(this, Level.INFO,
          "Dynamically registered database '%s' as Gremlin graph", databaseName);

      return arcadeGraph;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE,
          "Failed to create ArcadeGraph for database '%s': %s", databaseName, e.getMessage());
      return null;
    }
  }

  private void closeTx(final Set<String> graphSourceNamesToCloseTxOn, final Transaction.Status status) {
    graphSourceNamesToCloseTxOn.forEach(name -> {
      final Graph graph = graphs.get(name);
      if (graph != null && graph.features().graph().supportsTransactions() && graph.tx().isOpen()) {
        if (status == Transaction.Status.COMMIT) {
          graph.tx().commit();
        } else {
          graph.tx().rollback();
        }
      }
    });
  }

  /**
   * Closes all dynamically created ArcadeGraph instances.
   */
  public void closeAll() {
    for (final Map.Entry<String, Graph> entry : graphs.entrySet()) {
      if (entry.getValue() instanceof ArcadeGraph) {
        try {
          ((ArcadeGraph) entry.getValue()).close();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING,
              "Error closing ArcadeGraph for '%s': %s", entry.getKey(), e.getMessage());
        }
      }
    }
    graphs.clear();
    traversalSources.clear();
  }
}
