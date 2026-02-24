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
package com.arcadedb.query.opencypher.procedures;

import com.arcadedb.function.procedure.ProcedureRegistry;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.opencypher.procedures.algo.AlgoAPSP;
import com.arcadedb.query.opencypher.procedures.algo.AlgoAStar;
import com.arcadedb.query.opencypher.procedures.algo.AlgoAdamicAdar;
import com.arcadedb.query.opencypher.procedures.algo.AlgoAllSimplePaths;
import com.arcadedb.query.opencypher.procedures.algo.AlgoArticulationPoints;
import com.arcadedb.query.opencypher.procedures.algo.AlgoBellmanFord;
import com.arcadedb.query.opencypher.procedures.algo.AlgoBetweenness;
import com.arcadedb.query.opencypher.procedures.algo.AlgoArticleRank;
import com.arcadedb.query.opencypher.procedures.algo.AlgoBFS;
import com.arcadedb.query.opencypher.procedures.algo.AlgoBiconnectedComponents;
import com.arcadedb.query.opencypher.procedures.algo.AlgoBipartiteCheck;
import com.arcadedb.query.opencypher.procedures.algo.AlgoBipartiteMatching;
import com.arcadedb.query.opencypher.procedures.algo.AlgoBridges;
import com.arcadedb.query.opencypher.procedures.algo.AlgoClique;
import com.arcadedb.query.opencypher.procedures.algo.AlgoClosenessCentrality;
import com.arcadedb.query.opencypher.procedures.algo.AlgoCommonNeighbors;
import com.arcadedb.query.opencypher.procedures.algo.AlgoDFS;
import com.arcadedb.query.opencypher.procedures.algo.AlgoDijkstraSingleSource;
import com.arcadedb.query.opencypher.procedures.algo.AlgoCycleDetection;
import com.arcadedb.query.opencypher.procedures.algo.AlgoDensestSubgraph;
import com.arcadedb.query.opencypher.procedures.algo.AlgoEccentricity;
import com.arcadedb.query.opencypher.procedures.algo.AlgoGraphColoring;
import com.arcadedb.query.opencypher.procedures.algo.AlgoKNN;
import com.arcadedb.query.opencypher.procedures.algo.AlgoLocalClusteringCoefficient;
import com.arcadedb.query.opencypher.procedures.algo.AlgoLongestPathDAG;
import com.arcadedb.query.opencypher.procedures.algo.AlgoPreferentialAttachment;
import com.arcadedb.query.opencypher.procedures.algo.AlgoResourceAllocation;
import com.arcadedb.query.opencypher.procedures.algo.AlgoSLPA;
import com.arcadedb.query.opencypher.procedures.algo.AlgoSameCommunity;
import com.arcadedb.query.opencypher.procedures.algo.AlgoTotalNeighbors;
import com.arcadedb.query.opencypher.procedures.algo.AlgoDegreeCentrality;
import com.arcadedb.query.opencypher.procedures.algo.AlgoDijkstra;
import com.arcadedb.query.opencypher.procedures.algo.AlgoEigenvectorCentrality;
import com.arcadedb.query.opencypher.procedures.algo.AlgoGraphSummary;
import com.arcadedb.query.opencypher.procedures.algo.AlgoHITS;
import com.arcadedb.query.opencypher.procedures.algo.AlgoHarmonicCentrality;
import com.arcadedb.query.opencypher.procedures.algo.AlgoJaccardSimilarity;
import com.arcadedb.query.opencypher.procedures.algo.AlgoAssortativity;
import com.arcadedb.query.opencypher.procedures.algo.AlgoConductance;
import com.arcadedb.query.opencypher.procedures.algo.AlgoHierarchicalClustering;
import com.arcadedb.query.opencypher.procedures.algo.AlgoInfluenceMaximization;
import com.arcadedb.query.opencypher.procedures.algo.AlgoKCore;
import com.arcadedb.query.opencypher.procedures.algo.AlgoKShortestPaths;
import com.arcadedb.query.opencypher.procedures.algo.AlgoKTruss;
import com.arcadedb.query.opencypher.procedures.algo.AlgoLeiden;
import com.arcadedb.query.opencypher.procedures.algo.AlgoPersonalizedPageRank;
import com.arcadedb.query.opencypher.procedures.algo.AlgoRichClub;
import com.arcadedb.query.opencypher.procedures.algo.AlgoKatz;
import com.arcadedb.query.opencypher.procedures.algo.AlgoLabelPropagation;
import com.arcadedb.query.opencypher.procedures.algo.AlgoLouvain;
import com.arcadedb.query.opencypher.procedures.algo.AlgoMST;
import com.arcadedb.query.opencypher.procedures.algo.AlgoMaxFlow;
import com.arcadedb.query.opencypher.procedures.algo.AlgoModularityScore;
import com.arcadedb.query.opencypher.procedures.algo.AlgoPageRank;
import com.arcadedb.query.opencypher.procedures.algo.AlgoRandomWalk;
import com.arcadedb.query.opencypher.procedures.algo.AlgoSCC;
import com.arcadedb.query.opencypher.procedures.algo.AlgoSimRank;
import com.arcadedb.query.opencypher.procedures.algo.AlgoTopologicalSort;
import com.arcadedb.query.opencypher.procedures.algo.AlgoTriangleCount;
import com.arcadedb.query.opencypher.procedures.algo.AlgoVoteRank;
import com.arcadedb.query.opencypher.procedures.algo.AlgoWCC;
import com.arcadedb.query.opencypher.procedures.merge.MergeNode;
import com.arcadedb.query.opencypher.procedures.merge.MergeRelationship;
import com.arcadedb.query.opencypher.procedures.meta.MetaGraph;
import com.arcadedb.query.opencypher.procedures.meta.MetaNodeTypeProperties;
import com.arcadedb.query.opencypher.procedures.meta.MetaRelTypeProperties;
import com.arcadedb.query.opencypher.procedures.meta.MetaSchema;
import com.arcadedb.query.opencypher.procedures.meta.MetaStats;
import com.arcadedb.query.opencypher.procedures.path.PathExpand;
import com.arcadedb.query.opencypher.procedures.path.PathExpandConfig;
import com.arcadedb.query.opencypher.procedures.path.PathSpanningTree;
import com.arcadedb.query.opencypher.procedures.path.PathSubgraphAll;
import com.arcadedb.query.opencypher.procedures.path.PathSubgraphNodes;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Registry for namespaced Cypher procedures.
 * <p>
 * Procedures are registered by their fully qualified name (e.g., "merge.relationship", "algo.dijkstra").
 * The registry provides thread-safe access to procedure lookup and registration.
 * </p>
 * <p>
 * For Neo4j/APOC compatibility, procedures can also be accessed using the "apoc." prefix.
 * For example, "apoc.merge.relationship" will automatically resolve to "merge.relationship".
 * </p>
 * <p>
 * Example usage:
 * <pre>
 * CypherProcedureRegistry.register(new MergeRelationship());
 * CypherProcedure proc = CypherProcedureRegistry.get("merge.relationship");
 * // APOC compatibility - same procedure
 * CypherProcedure proc2 = CypherProcedureRegistry.get("apoc.merge.relationship");
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public final class CypherProcedureRegistry {
  private static final String APOC_PREFIX = "apoc.";
  private static final Map<String, CypherProcedure> PROCEDURES = new ConcurrentHashMap<>();

  // Static initialization block to register built-in procedures
  static {
    registerBuiltInProcedures();
  }

  private CypherProcedureRegistry() {
    // Utility class - prevent instantiation
  }

  /**
   * Registers a procedure in the registry.
   * <p>
   * Also registers the procedure in the unified {@link ProcedureRegistry} for cross-engine access.
   * </p>
   *
   * @param procedure the procedure to register
   * @throws IllegalArgumentException if a procedure with the same name is already registered
   */
  public static void register(final CypherProcedure procedure) {
    final String name = procedure.getName().toLowerCase();
    final CypherProcedure existing = PROCEDURES.putIfAbsent(name, procedure);
    if (existing != null) {
      LogManager.instance().log(CypherProcedureRegistry.class, Level.WARNING,
          "Procedure already registered, ignoring: " + name);
    } else {
      // Also register in the unified ProcedureRegistry for cross-engine access
      ProcedureRegistry.register(procedure);
    }
  }

  /**
   * Registers a procedure, replacing any existing procedure with the same name.
   * <p>
   * Also registers the procedure in the unified {@link ProcedureRegistry} for cross-engine access.
   * </p>
   *
   * @param procedure the procedure to register
   */
  public static void registerOrReplace(final CypherProcedure procedure) {
    final String name = procedure.getName().toLowerCase();
    PROCEDURES.put(name, procedure);
    // Also register in the unified ProcedureRegistry for cross-engine access
    ProcedureRegistry.registerOrReplace(procedure);
  }

  /**
   * Retrieves a procedure by its fully qualified name.
   * <p>
   * For APOC compatibility, the "apoc." prefix is automatically stripped.
   * For example, "apoc.merge.relationship" resolves to "merge.relationship".
   * </p>
   *
   * @param name the procedure name (case-insensitive)
   * @return the procedure, or null if not found
   */
  public static CypherProcedure get(final String name) {
    return PROCEDURES.get(normalizeApocName(name));
  }

  /**
   * Checks if a procedure is registered.
   * <p>
   * For APOC compatibility, the "apoc." prefix is automatically stripped.
   * </p>
   *
   * @param name the procedure name (case-insensitive)
   * @return true if the procedure is registered
   */
  public static boolean hasProcedure(final String name) {
    return PROCEDURES.containsKey(normalizeApocName(name));
  }

  /**
   * Normalizes a procedure name by stripping the "apoc." prefix if present.
   * This provides compatibility with Neo4j APOC procedure calls.
   *
   * @param name the procedure name
   * @return the normalized name (lowercase, without apoc. prefix)
   */
  private static String normalizeApocName(final String name) {
    final String lowerName = name.toLowerCase();
    if (lowerName.startsWith(APOC_PREFIX)) {
      return lowerName.substring(APOC_PREFIX.length());
    }
    return lowerName;
  }

  /**
   * Returns all registered procedure names.
   *
   * @return unmodifiable set of procedure names
   */
  public static Set<String> getProcedureNames() {
    return Collections.unmodifiableSet(PROCEDURES.keySet());
  }

  /**
   * Returns all registered procedures.
   *
   * @return unmodifiable collection of procedures
   */
  public static Collection<CypherProcedure> getAllProcedures() {
    return Collections.unmodifiableCollection(PROCEDURES.values());
  }

  /**
   * Returns the number of registered procedures.
   *
   * @return procedure count
   */
  public static int size() {
    return PROCEDURES.size();
  }

  /**
   * Unregisters a procedure by name.
   * <p>
   * For APOC compatibility, the "apoc." prefix is automatically stripped.
   * </p>
   *
   * @param name the procedure name to unregister
   * @return the unregistered procedure, or null if not found
   */
  public static CypherProcedure unregister(final String name) {
    return PROCEDURES.remove(normalizeApocName(name));
  }

  /**
   * Clears all registered procedures (for testing purposes).
   */
  public static void clear() {
    PROCEDURES.clear();
  }

  /**
   * Resets the registry to its initial state with built-in procedures.
   */
  public static void reset() {
    PROCEDURES.clear();
    registerBuiltInProcedures();
  }

  /**
   * Registers all built-in procedures.
   * Called during static initialization and reset.
   */
  private static void registerBuiltInProcedures() {
    // Merge procedures
    registerMergeProcedures();
    // Algorithm procedures
    registerAlgorithmProcedures();
    // Path expansion procedures
    registerPathProcedures();
    // Meta/schema procedures
    registerMetaProcedures();
  }

  private static void registerMergeProcedures() {
    register(new MergeRelationship());
    register(new MergeNode());
  }

  private static void registerAlgorithmProcedures() {
    register(new AlgoDijkstra());
    register(new AlgoAStar());
    register(new AlgoAllSimplePaths());
    register(new AlgoBellmanFord());
    register(new AlgoPageRank());
    register(new AlgoBetweenness());
    register(new AlgoWCC());
    register(new AlgoLouvain());
    register(new AlgoLabelPropagation());
    register(new AlgoClosenessCentrality());
    register(new AlgoDegreeCentrality());
    register(new AlgoTriangleCount());
    register(new AlgoKCore());
    register(new AlgoSCC());
    register(new AlgoMST());
    register(new AlgoJaccardSimilarity());
    register(new AlgoRandomWalk());
    register(new AlgoHITS());
    register(new AlgoHarmonicCentrality());
    register(new AlgoEigenvectorCentrality());
    register(new AlgoArticulationPoints());
    register(new AlgoBridges());
    register(new AlgoTopologicalSort());
    register(new AlgoAPSP());
    register(new AlgoAdamicAdar());
    register(new AlgoKatz());
    register(new AlgoVoteRank());
    register(new AlgoMaxFlow());
    register(new AlgoKShortestPaths());
    register(new AlgoSimRank());
    register(new AlgoClique());
    register(new AlgoGraphSummary());
    register(new AlgoModularityScore());
    register(new AlgoLeiden());
    register(new AlgoPersonalizedPageRank());
    register(new AlgoKTruss());
    register(new AlgoAssortativity());
    register(new AlgoRichClub());
    register(new AlgoInfluenceMaximization());
    register(new AlgoHierarchicalClustering());
    register(new AlgoConductance());
    register(new AlgoBipartiteCheck());
    register(new AlgoCommonNeighbors());
    register(new AlgoCycleDetection());
    register(new AlgoDensestSubgraph());
    register(new AlgoEccentricity());
    register(new AlgoGraphColoring());
    register(new AlgoPreferentialAttachment());
    register(new AlgoResourceAllocation());
    register(new AlgoBFS());
    register(new AlgoDFS());
    register(new AlgoArticleRank());
    register(new AlgoLocalClusteringCoefficient());
    register(new AlgoTotalNeighbors());
    register(new AlgoSameCommunity());
    register(new AlgoBipartiteMatching());
    register(new AlgoKNN());
    register(new AlgoDijkstraSingleSource());
    register(new AlgoLongestPathDAG());
    register(new AlgoSLPA());
    register(new AlgoBiconnectedComponents());
  }

  private static void registerPathProcedures() {
    register(new PathExpand());
    register(new PathExpandConfig());
    register(new PathSubgraphNodes());
    register(new PathSubgraphAll());
    register(new PathSpanningTree());
  }

  private static void registerMetaProcedures() {
    register(new MetaGraph());
    register(new MetaSchema());
    register(new MetaStats());
    register(new MetaNodeTypeProperties());
    register(new MetaRelTypeProperties());
  }
}
