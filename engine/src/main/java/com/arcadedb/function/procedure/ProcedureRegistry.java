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
package com.arcadedb.function.procedure;

import com.arcadedb.log.LogManager;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Unified registry for all ArcadeDB procedures.
 * <p>
 * Procedures are operations that can return multiple rows and/or modify the database.
 * They are called using CALL statements in query languages that support them.
 * </p>
 * <p>
 * For Neo4j/APOC compatibility, procedures can also be accessed using the "apoc." prefix.
 * For example, "apoc.merge.relationship" will automatically resolve to "merge.relationship".
 * </p>
 * <p>
 * Thread-safe: All operations use a ConcurrentHashMap for safe concurrent access.
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 * @see Procedure
 */
public final class ProcedureRegistry {
  private static final String APOC_PREFIX = "apoc.";
  private static final Map<String, Procedure> PROCEDURES = new ConcurrentHashMap<>();

  private ProcedureRegistry() {
    // Utility class - prevent instantiation
  }

  /**
   * Registers a procedure in the registry.
   *
   * @param procedure the procedure to register
   * @return true if registered successfully, false if a procedure with the same name already exists
   */
  public static boolean register(final Procedure procedure) {
    final String name = procedure.getName().toLowerCase();
    final Procedure existing = PROCEDURES.putIfAbsent(name, procedure);
    if (existing != null) {
      LogManager.instance().log(ProcedureRegistry.class, Level.WARNING,
          "Procedure already registered, ignoring: " + name);
      return false;
    }
    return true;
  }

  /**
   * Registers a procedure, replacing any existing procedure with the same name.
   *
   * @param procedure the procedure to register
   */
  public static void registerOrReplace(final Procedure procedure) {
    final String name = procedure.getName().toLowerCase();
    PROCEDURES.put(name, procedure);
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
  public static Procedure get(final String name) {
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
  public static String normalizeApocName(final String name) {
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
  public static Collection<Procedure> getAllProcedures() {
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
  public static Procedure unregister(final String name) {
    return PROCEDURES.remove(normalizeApocName(name));
  }

  /**
   * Clears all registered procedures (for testing purposes).
   */
  public static void clear() {
    PROCEDURES.clear();
  }
}
