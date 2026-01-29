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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.function.FunctionRegistry;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.opencypher.functions.agg.*;
import com.arcadedb.query.opencypher.functions.convert.*;
import com.arcadedb.query.opencypher.functions.create.*;
import com.arcadedb.query.opencypher.functions.date.*;
import com.arcadedb.query.opencypher.functions.map.*;
import com.arcadedb.query.opencypher.functions.math.*;
import com.arcadedb.query.opencypher.functions.node.*;
import com.arcadedb.query.opencypher.functions.path.*;
import com.arcadedb.query.opencypher.functions.rel.*;
import com.arcadedb.query.opencypher.functions.text.*;
import com.arcadedb.query.opencypher.functions.util.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Registry for namespaced Cypher functions.
 * <p>
 * Functions are registered by their fully qualified name (e.g., "text.indexOf", "map.merge").
 * The registry provides thread-safe access to function lookup and registration.
 * </p>
 * <p>
 * For Neo4j/APOC compatibility, functions can also be accessed using the "apoc." prefix.
 * For example, "apoc.text.indexOf" will automatically resolve to "text.indexOf".
 * </p>
 * <p>
 * Example usage:
 * <pre>
 * CypherFunctionRegistry.register(new TextIndexOf());
 * CypherFunction fn = CypherFunctionRegistry.get("text.indexOf");
 * // APOC compatibility - same function
 * CypherFunction fn2 = CypherFunctionRegistry.get("apoc.text.indexOf");
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public final class CypherFunctionRegistry {
  private static final String APOC_PREFIX = "apoc.";
  private static final Map<String, CypherFunction> FUNCTIONS = new ConcurrentHashMap<>();

  // Static initialization block to register built-in functions
  static {
    registerBuiltInFunctions();
  }

  private CypherFunctionRegistry() {
    // Utility class - prevent instantiation
  }

  /**
   * Registers a function in the registry.
   * <p>
   * Also registers the function in the unified {@link FunctionRegistry} for cross-engine access.
   * </p>
   *
   * @param function the function to register
   * @throws IllegalArgumentException if a function with the same name is already registered
   */
  public static void register(final CypherFunction function) {
    final String name = function.getName().toLowerCase();
    final CypherFunction existing = FUNCTIONS.putIfAbsent(name, function);
    if (existing != null) {
      LogManager.instance().log(CypherFunctionRegistry.class, Level.WARNING,
          "Function already registered, ignoring: " + name);
    } else {
      // Also register in the unified FunctionRegistry for cross-engine access
      FunctionRegistry.register(function);
    }
  }

  /**
   * Registers a function, replacing any existing function with the same name.
   * <p>
   * Also registers the function in the unified {@link FunctionRegistry} for cross-engine access.
   * </p>
   *
   * @param function the function to register
   */
  public static void registerOrReplace(final CypherFunction function) {
    final String name = function.getName().toLowerCase();
    FUNCTIONS.put(name, function);
    // Also register in the unified FunctionRegistry for cross-engine access
    FunctionRegistry.registerOrReplace(function);
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
  public static CypherFunction get(final String name) {
    return FUNCTIONS.get(normalizeApocName(name));
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
   * Normalizes a function name by stripping the "apoc." prefix if present.
   * This provides compatibility with Neo4j APOC procedure calls.
   *
   * @param name the function name
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
   * Returns all registered function names.
   *
   * @return unmodifiable set of function names
   */
  public static Set<String> getFunctionNames() {
    return Collections.unmodifiableSet(FUNCTIONS.keySet());
  }

  /**
   * Returns all registered functions.
   *
   * @return unmodifiable collection of functions
   */
  public static Collection<CypherFunction> getAllFunctions() {
    return Collections.unmodifiableCollection(FUNCTIONS.values());
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
  public static CypherFunction unregister(final String name) {
    return FUNCTIONS.remove(normalizeApocName(name));
  }

  /**
   * Clears all registered functions (for testing purposes).
   */
  public static void clear() {
    FUNCTIONS.clear();
  }

  /**
   * Resets the registry to its initial state with built-in functions.
   */
  public static void reset() {
    FUNCTIONS.clear();
    registerBuiltInFunctions();
  }

  /**
   * Registers all built-in functions.
   * Called during static initialization and reset.
   */
  private static void registerBuiltInFunctions() {
    // Text functions
    registerTextFunctions();
    // Map functions
    registerMapFunctions();
    // Math functions
    registerMathFunctions();
    // Convert functions
    registerConvertFunctions();
    // Date functions
    registerDateFunctions();
    // Util functions
    registerUtilFunctions();
    // Agg functions
    registerAggFunctions();
    // Node functions
    registerNodeFunctions();
    // Relationship functions
    registerRelFunctions();
    // Path functions
    registerPathFunctions();
    // Create functions
    registerCreateFunctions();
  }

  private static void registerTextFunctions() {
    register(new TextIndexOf());
    register(new TextJoin());
    register(new TextSplit());
    register(new TextReplace());
    register(new TextRegexReplace());
    register(new TextCapitalize());
    register(new TextCapitalizeAll());
    register(new TextDecapitalize());
    register(new TextDecapitalizeAll());
    register(new TextCamelCase());
    register(new TextSnakeCase());
    register(new TextUpperCamelCase());
    register(new TextLpad());
    register(new TextRpad());
    register(new TextFormat());
    register(new TextSlug());
    register(new TextRandom());
    register(new TextHexValue());
    register(new TextByteCount());
    register(new TextCharAt());
    register(new TextCode());
    register(new TextLevenshteinDistance());
    register(new TextLevenshteinSimilarity());
    register(new TextSorensenDiceSimilarity());
    register(new TextJaroWinklerDistance());
    register(new TextHammingDistance());
  }

  private static void registerMapFunctions() {
    register(new MapMerge());
    register(new MapMergeList());
    register(new MapFromLists());
    register(new MapFromPairs());
    register(new MapSetKey());
    register(new MapRemoveKey());
    register(new MapRemoveKeys());
    register(new MapClean());
    register(new MapFlatten());
    register(new MapUnflatten());
    register(new MapSubmap());
    register(new MapValues());
    register(new MapGroupBy());
    register(new MapSortedProperties());
  }

  private static void registerMathFunctions() {
    register(new MathSigmoid());
    register(new MathSigmoidPrime());
    register(new MathTanh());
    register(new MathCosh());
    register(new MathSinh());
    register(new MathMaxLong());
    register(new MathMinLong());
    register(new MathMaxDouble());
  }

  private static void registerConvertFunctions() {
    register(new ConvertToJson());
    register(new ConvertFromJsonMap());
    register(new ConvertFromJsonList());
    register(new ConvertToMap());
    register(new ConvertToList());
    register(new ConvertToSet());
    register(new ConvertToBoolean());
    register(new ConvertToInteger());
    register(new ConvertToFloat());
  }

  private static void registerDateFunctions() {
    register(new DateFormat());
    register(new DateParse());
    register(new DateAdd());
    register(new DateConvert());
    register(new DateField());
    register(new DateFields());
    register(new DateCurrentTimestamp());
    register(new DateToISO8601());
    register(new DateFromISO8601());
    register(new DateSystemTimezone());
  }

  private static void registerUtilFunctions() {
    register(new UtilMd5());
    register(new UtilSha1());
    register(new UtilSha256());
    register(new UtilSha512());
    register(new UtilCompress());
    register(new UtilDecompress());
    register(new UtilSleep());
    register(new UtilValidate());
  }

  private static void registerAggFunctions() {
    register(new AggFirst());
    register(new AggLast());
    register(new AggNth());
    register(new AggSlice());
    register(new AggMedian());
    register(new AggPercentiles());
    register(new AggStatistics());
    register(new AggProduct());
    register(new AggMinItems());
    register(new AggMaxItems());
  }

  private static void registerNodeFunctions() {
    register(new NodeDegree());
    register(new NodeDegreeIn());
    register(new NodeDegreeOut());
    register(new NodeLabels());
    register(new NodeId());
    register(new NodeRelationshipExists());
    register(new NodeRelationshipTypes());
  }

  private static void registerRelFunctions() {
    register(new RelId());
    register(new RelType());
    register(new RelStartNode());
    register(new RelEndNode());
  }

  private static void registerPathFunctions() {
    register(new PathCreate());
    register(new PathCombine());
    register(new PathSlice());
    register(new PathElements());
  }

  private static void registerCreateFunctions() {
    register(new CreateUuid());
    register(new CreateUuidBase64());
    register(new CreateVNode());
    register(new CreateVRelationship());
  }
}
