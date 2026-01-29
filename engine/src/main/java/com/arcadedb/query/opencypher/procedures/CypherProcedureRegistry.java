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

import com.arcadedb.log.LogManager;
import com.arcadedb.query.opencypher.procedures.merge.MergeNode;
import com.arcadedb.query.opencypher.procedures.merge.MergeRelationship;

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
 * Example usage:
 * <pre>
 * CypherProcedureRegistry.register(new MergeRelationship());
 * CypherProcedure proc = CypherProcedureRegistry.get("merge.relationship");
 * </pre>
 * </p>
 *
 * @author ArcadeDB Team
 */
public final class CypherProcedureRegistry {
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
    }
  }

  /**
   * Registers a procedure, replacing any existing procedure with the same name.
   *
   * @param procedure the procedure to register
   */
  public static void registerOrReplace(final CypherProcedure procedure) {
    final String name = procedure.getName().toLowerCase();
    PROCEDURES.put(name, procedure);
  }

  /**
   * Retrieves a procedure by its fully qualified name.
   *
   * @param name the procedure name (case-insensitive)
   * @return the procedure, or null if not found
   */
  public static CypherProcedure get(final String name) {
    return PROCEDURES.get(name.toLowerCase());
  }

  /**
   * Checks if a procedure is registered.
   *
   * @param name the procedure name (case-insensitive)
   * @return true if the procedure is registered
   */
  public static boolean hasProcedure(final String name) {
    return PROCEDURES.containsKey(name.toLowerCase());
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
   *
   * @param name the procedure name to unregister
   * @return the unregistered procedure, or null if not found
   */
  public static CypherProcedure unregister(final String name) {
    return PROCEDURES.remove(name.toLowerCase());
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
    // Algorithm procedures will be registered here as they are implemented
  }

  private static void registerPathProcedures() {
    // Path procedures will be registered here as they are implemented
  }

  private static void registerMetaProcedures() {
    // Meta procedures will be registered here as they are implemented
  }
}
