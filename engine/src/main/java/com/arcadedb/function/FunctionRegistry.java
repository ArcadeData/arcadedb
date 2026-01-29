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
package com.arcadedb.function;

import com.arcadedb.log.LogManager;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Unified registry for all ArcadeDB functions.
 * <p>
 * This registry holds both {@link StatelessFunction} and {@link RecordFunction} implementations,
 * making them available to all query engines (Cypher, SQL, etc.).
 * </p>
 * <p>
 * For Neo4j/APOC compatibility, functions can also be accessed using the "apoc." prefix.
 * For example, "apoc.text.indexOf" will automatically resolve to "text.indexOf".
 * </p>
 * <p>
 * Thread-safe: All operations use a ConcurrentHashMap for safe concurrent access.
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 * @see Function
 * @see StatelessFunction
 * @see RecordFunction
 */
public final class FunctionRegistry {
  private static final String APOC_PREFIX = "apoc.";
  private static final Map<String, Function> FUNCTIONS = new ConcurrentHashMap<>();

  private FunctionRegistry() {
    // Utility class - prevent instantiation
  }

  /**
   * Registers a function in the registry.
   *
   * @param function the function to register
   * @return true if registered successfully, false if a function with the same name already exists
   */
  public static boolean register(final Function function) {
    final String name = function.getName().toLowerCase();
    final Function existing = FUNCTIONS.putIfAbsent(name, function);
    if (existing != null) {
      LogManager.instance().log(FunctionRegistry.class, Level.WARNING,
          "Function already registered, ignoring: " + name);
      return false;
    }
    return true;
  }

  /**
   * Registers a function, replacing any existing function with the same name.
   *
   * @param function the function to register
   */
  public static void registerOrReplace(final Function function) {
    final String name = function.getName().toLowerCase();
    FUNCTIONS.put(name, function);
  }

  /**
   * Retrieves a function by its fully qualified name.
   * <p>
   * For APOC compatibility, the "apoc." prefix is automatically stripped.
   * For example, "apoc.text.indexOf" resolves to "text.indexOf".
   * </p>
   *
   * @param name the function name (case-insensitive)
   * @return the function, or null if not found
   */
  public static Function get(final String name) {
    return FUNCTIONS.get(normalizeApocName(name));
  }

  /**
   * Retrieves a stateless function by name.
   *
   * @param name the function name (case-insensitive)
   * @return the stateless function, or null if not found or not a StatelessFunction
   */
  public static StatelessFunction getStateless(final String name) {
    final Function fn = get(name);
    return fn instanceof StatelessFunction ? (StatelessFunction) fn : null;
  }

  /**
   * Retrieves a record function by name.
   *
   * @param name the function name (case-insensitive)
   * @return the record function, or null if not found or not a RecordFunction
   */
  public static RecordFunction getRecord(final String name) {
    final Function fn = get(name);
    return fn instanceof RecordFunction ? (RecordFunction) fn : null;
  }

  /**
   * Checks if a function is registered.
   * <p>
   * For APOC compatibility, the "apoc." prefix is automatically stripped.
   * </p>
   *
   * @param name the function name (case-insensitive)
   * @return true if the function is registered
   */
  public static boolean hasFunction(final String name) {
    return FUNCTIONS.containsKey(normalizeApocName(name));
  }

  /**
   * Checks if a stateless function is registered with the given name.
   *
   * @param name the function name (case-insensitive)
   * @return true if a StatelessFunction is registered with this name
   */
  public static boolean hasStatelessFunction(final String name) {
    final Function fn = get(name);
    return fn instanceof StatelessFunction;
  }

  /**
   * Checks if a record function is registered with the given name.
   *
   * @param name the function name (case-insensitive)
   * @return true if a RecordFunction is registered with this name
   */
  public static boolean hasRecordFunction(final String name) {
    final Function fn = get(name);
    return fn instanceof RecordFunction;
  }

  /**
   * Normalizes a function name by stripping the "apoc." prefix if present.
   * This provides compatibility with Neo4j APOC procedure calls.
   *
   * @param name the function name
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
   * Returns all registered function names.
   *
   * @return unmodifiable set of function names
   */
  public static Set<String> getFunctionNames() {
    return Collections.unmodifiableSet(FUNCTIONS.keySet());
  }

  /**
   * Returns all registered stateless function names.
   *
   * @return unmodifiable set of stateless function names
   */
  public static Set<String> getStatelessFunctionNames() {
    return FUNCTIONS.entrySet().stream()
        .filter(e -> e.getValue() instanceof StatelessFunction)
        .map(Map.Entry::getKey)
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Returns all registered record function names.
   *
   * @return unmodifiable set of record function names
   */
  public static Set<String> getRecordFunctionNames() {
    return FUNCTIONS.entrySet().stream()
        .filter(e -> e.getValue() instanceof RecordFunction)
        .map(Map.Entry::getKey)
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Returns all registered functions.
   *
   * @return unmodifiable collection of functions
   */
  public static Collection<Function> getAllFunctions() {
    return Collections.unmodifiableCollection(FUNCTIONS.values());
  }

  /**
   * Returns all registered stateless functions.
   *
   * @return unmodifiable collection of stateless functions
   */
  public static Collection<StatelessFunction> getAllStatelessFunctions() {
    return FUNCTIONS.values().stream()
        .filter(f -> f instanceof StatelessFunction)
        .map(f -> (StatelessFunction) f)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Returns all registered record functions.
   *
   * @return unmodifiable collection of record functions
   */
  public static Collection<RecordFunction> getAllRecordFunctions() {
    return FUNCTIONS.values().stream()
        .filter(f -> f instanceof RecordFunction)
        .map(f -> (RecordFunction) f)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Returns the number of registered functions.
   *
   * @return function count
   */
  public static int size() {
    return FUNCTIONS.size();
  }

  /**
   * Unregisters a function by name.
   * <p>
   * For APOC compatibility, the "apoc." prefix is automatically stripped.
   * </p>
   *
   * @param name the function name to unregister
   * @return the unregistered function, or null if not found
   */
  public static Function unregister(final String name) {
    return FUNCTIONS.remove(normalizeApocName(name));
  }

  /**
   * Clears all registered functions (for testing purposes).
   */
  public static void clear() {
    FUNCTIONS.clear();
  }
}
