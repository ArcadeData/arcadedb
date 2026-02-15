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
package com.arcadedb.query.opencypher.parser;

import java.util.*;

/**
 * Validates Cypher function calls for correct argument counts and types.
 * Provides information about built-in Cypher functions and their signatures.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FunctionValidator {

  /**
   * Function signature defining name, argument requirements, and description.
   */
  public static class FunctionSignature {
    private final String name;
    private final int minArgs;
    private final int maxArgs;  // -1 means unlimited
    private final String description;
    private final boolean aggregation;

    public FunctionSignature(final String name, final int minArgs, final int maxArgs,
                             final String description, final boolean aggregation) {
      this.name = name.toLowerCase();
      this.minArgs = minArgs;
      this.maxArgs = maxArgs;
      this.description = description;
      this.aggregation = aggregation;
    }

    public String getName() {
      return name;
    }

    public int getMinArgs() {
      return minArgs;
    }

    public int getMaxArgs() {
      return maxArgs;
    }

    public String getDescription() {
      return description;
    }

    public boolean isAggregation() {
      return aggregation;
    }

    public boolean acceptsArgCount(final int count) {
      if (count < minArgs)
        return false;
      if (maxArgs == -1)
        return true;
      return count <= maxArgs;
    }

    public String getExpectedArgsDescription() {
      if (minArgs == maxArgs)
        return minArgs + " argument" + (minArgs == 1 ? "" : "s");
      if (maxArgs == -1)
        return "at least " + minArgs + " argument" + (minArgs == 1 ? "" : "s");
      return minArgs + "-" + maxArgs + " arguments";
    }
  }

  // Registry of known Cypher functions
  private static final Map<String, FunctionSignature> FUNCTIONS = new HashMap<>();

  static {
    // Aggregation functions
    registerFunction("count", 1, 1, "Count of values", true);
    registerFunction("sum", 1, 1, "Sum of values", true);
    registerFunction("avg", 1, 1, "Average of values", true);
    registerFunction("min", 1, 1, "Minimum value", true);
    registerFunction("max", 1, 1, "Maximum value", true);
    registerFunction("collect", 1, 1, "Collect values into list", true);
    registerFunction("stdev", 1, 1, "Standard deviation", true);
    registerFunction("stdevp", 1, 1, "Population standard deviation", true);
    registerFunction("percentilecont", 2, 2, "Continuous percentile", true);
    registerFunction("percentiledisc", 2, 2, "Discrete percentile", true);

    // Scalar functions
    registerFunction("id", 1, 1, "Internal ID of node/relationship", false);
    registerFunction("type", 1, 1, "Type of relationship", false);
    registerFunction("labels", 1, 1, "Labels of node", false);
    registerFunction("keys", 1, 1, "Property keys of entity", false);
    registerFunction("properties", 1, 1, "All properties of entity", false);
    registerFunction("size", 1, 1, "Size of list/string", false);
    registerFunction("length", 1, 1, "Length of path", false);
    registerFunction("reverse", 1, 1, "Reverse list/string", false);

    // String functions
    registerFunction("tostring", 1, 1, "Convert to string", false);
    registerFunction("tolower", 1, 1, "Convert to lowercase", false);
    registerFunction("toupper", 1, 1, "Convert to uppercase", false);
    registerFunction("trim", 1, 3, "Trim whitespace", false);
    registerFunction("ltrim", 1, 1, "Trim left whitespace", false);
    registerFunction("rtrim", 1, 1, "Trim right whitespace", false);
    registerFunction("substring", 2, 3, "Extract substring", false);
    registerFunction("replace", 3, 3, "Replace string", false);
    registerFunction("split", 2, 2, "Split string", false);
    registerFunction("left", 2, 2, "Left substring", false);
    registerFunction("right", 2, 2, "Right substring", false);

    // Math functions
    registerFunction("abs", 1, 1, "Absolute value", false);
    registerFunction("ceil", 1, 1, "Ceiling", false);
    registerFunction("floor", 1, 1, "Floor", false);
    registerFunction("round", 1, 2, "Round to integer or precision", false);
    registerFunction("sign", 1, 1, "Sign of number", false);
    registerFunction("rand", 0, 0, "Random number", false);
    registerFunction("sqrt", 1, 1, "Square root", false);
    registerFunction("log", 1, 1, "Natural logarithm", false);
    registerFunction("log10", 1, 1, "Base-10 logarithm", false);
    registerFunction("exp", 1, 1, "Exponential", false);
    registerFunction("sin", 1, 1, "Sine", false);
    registerFunction("cos", 1, 1, "Cosine", false);
    registerFunction("tan", 1, 1, "Tangent", false);
    registerFunction("asin", 1, 1, "Arcsine", false);
    registerFunction("acos", 1, 1, "Arccosine", false);
    registerFunction("atan", 1, 1, "Arctangent", false);
    registerFunction("atan2", 2, 2, "Arctangent of y/x", false);

    // Type conversion
    registerFunction("tointeger", 1, 1, "Convert to integer", false);
    registerFunction("tofloat", 1, 1, "Convert to float", false);
    registerFunction("toboolean", 1, 1, "Convert to boolean", false);

    // List functions
    registerFunction("head", 1, 1, "First element of list", false);
    registerFunction("last", 1, 1, "Last element of list", false);
    registerFunction("tail", 1, 1, "List without first element", false);
    registerFunction("range", 2, 3, "Generate range of numbers", false);

    // Predicate functions
    registerFunction("exists", 1, 1, "Check if property exists", false);

    // String aliases (Neo4j 5+)
    registerFunction("lower", 1, 1, "Convert to lowercase (alias for toLower)", false);
    registerFunction("upper", 1, 1, "Convert to uppercase (alias for toUpper)", false);
    registerFunction("btrim", 1, 3, "Trim both sides (alias for trim)", false);

    // List conversion functions
    registerFunction("tobooleanlist", 1, 1, "Convert list elements to booleans", false);
    registerFunction("tofloatlist", 1, 1, "Convert list elements to floats", false);
    registerFunction("tointegerlist", 1, 1, "Convert list elements to integers", false);
    registerFunction("tostringlist", 1, 1, "Convert list elements to strings", false);

    // Vector functions
    registerFunction("vector", 1, 2, "Create vector from list (alias for vector_create)", false);
    registerFunction("vector_dimension_count", 1, 1, "Return dimension count of vector", false);
    registerFunction("vector_distance", 2, 3, "Calculate distance between vectors", false);
    registerFunction("vector.distance.euclidean", 2, 2, "Euclidean distance between vectors", false);
    registerFunction("vector.norm", 1, 1, "L2 norm (magnitude) of vector", false);
    registerFunction("coalesce", 1, -1, "First non-null value", false);
    registerFunction("isempty", 1, 1, "Check if list/map/string is empty", false);
    registerFunction("isnan", 1, 1, "Check if value is NaN", false);

    // Temporal functions
    registerFunction("timestamp", 0, 0, "Current timestamp", false);
    registerFunction("datetime", 0, 1, "Current or parsed datetime", false);
    registerFunction("date", 0, 1, "Current or parsed date", false);
    registerFunction("time", 0, 1, "Current or parsed time", false);
    registerFunction("localtime", 0, 1, "Current or parsed local time", false);
    registerFunction("localdatetime", 0, 1, "Current or parsed local datetime", false);
    registerFunction("duration", 1, 1, "Duration value", false);

    // Temporal format function
    registerFunction("format", 1, 2, "Format temporal value as string", false);

    // Vector similarity functions
    registerFunction("vector.similarity.cosine", 2, 2, "Cosine similarity between vectors", false);
    registerFunction("vector.similarity.euclidean", 2, 2, "Euclidean similarity between vectors", false);

    // Vector construction and distance functions (Cypher-specific; others delegate to SQL functions)
    registerFunction("vector_create", 1, 2, "Create vector from list", false);
    registerFunction("vector_distance_manhattan", 2, 2, "Manhattan distance between vectors", false);
    registerFunction("vector_distance_cosine", 2, 2, "Cosine distance between vectors", false);

    // Geo-spatial functions
    registerFunction("point.withinbbox", 3, 3, "Check if point is within bounding box", false);

    // Path/Graph functions
    registerFunction("nodes", 1, 1, "Nodes in path", false);
    registerFunction("relationships", 1, 1, "Relationships in path", false);
    registerFunction("startnode", 1, 1, "Start node of relationship", false);
    registerFunction("endnode", 1, 1, "End node of relationship", false);

    // Additional type conversions
    registerFunction("tostringornull", 1, 1, "Convert to string or null", false);
    registerFunction("tointegerornull", 1, 1, "Convert to integer or null", false);
    registerFunction("tofloatornull", 1, 1, "Convert to float or null", false);
    registerFunction("tobooleanornull", 1, 1, "Convert to boolean or null", false);

    // Additional functions
    registerFunction("randomuuid", 0, 0, "Random UUID", false);
    registerFunction("pi", 0, 0, "Pi constant", false);
    registerFunction("e", 0, 0, "Euler's number", false);
    registerFunction("point", 1, 1, "Create point", false);
    registerFunction("distance", 2, 2, "Distance between points", false);
    registerFunction("degrees", 1, 1, "Radians to degrees", false);
    registerFunction("radians", 1, 1, "Degrees to radians", false);
    registerFunction("haversin", 1, 1, "Haversine", false);
    registerFunction("cosh", 1, 1, "Hyperbolic cosine", false);
    registerFunction("sinh", 1, 1, "Hyperbolic sine", false);
    registerFunction("tanh", 1, 1, "Hyperbolic tangent", false);
    registerFunction("cot", 1, 1, "Cotangent", false);
    registerFunction("coth", 1, 1, "Hyperbolic cotangent", false);
    registerFunction("charlength", 1, 1, "Character length", false);
    registerFunction("charat", 2, 2, "Character at position", false);
    registerFunction("normalize", 1, 2, "Normalize string", false);
    registerFunction("isnormalized", 1, 2, "Check if normalized", false);
    registerFunction("char_length", 1, 1, "Character length", false);
    registerFunction("character_length", 1, 1, "Character length", false);
    registerFunction("nullif", 2, 2, "Null if equal", false);
    registerFunction("valuetype", 1, 1, "Value type string", false);
    registerFunction("elementid", 1, 1, "Element ID", false);
  }

  private static void registerFunction(final String name, final int minArgs, final int maxArgs,
                                       final String description, final boolean aggregation) {
    FUNCTIONS.put(name.toLowerCase(), new FunctionSignature(name, minArgs, maxArgs, description, aggregation));
  }

  /**
   * Check if a function is known.
   */
  public static boolean isKnownFunction(final String functionName) {
    return FUNCTIONS.containsKey(functionName.toLowerCase());
  }

  /**
   * Get function signature.
   */
  public static FunctionSignature getSignature(final String functionName) {
    return FUNCTIONS.get(functionName.toLowerCase());
  }

  /**
   * Validate function call argument count.
   *
   * @return error message if invalid, null if valid
   */
  public static String validateArgumentCount(final String functionName, final int actualArgs) {
    final FunctionSignature sig = getSignature(functionName);
    if (sig == null)
      return null; // Unknown function, can't validate (may be user-defined)

    if (!sig.acceptsArgCount(actualArgs)) {
      return "Function '" + functionName + "' expects " + sig.getExpectedArgsDescription() +
          " but got " + actualArgs;
    }

    return null;
  }

  /**
   * Check if function is an aggregation function.
   */
  public static boolean isAggregationFunction(final String functionName) {
    final FunctionSignature sig = getSignature(functionName);
    return sig != null && sig.isAggregation();
  }

  /**
   * Get all known function names.
   */
  public static Set<String> getKnownFunctionNames() {
    return new HashSet<>(FUNCTIONS.keySet());
  }

  /**
   * Get function description for error messages/help.
   */
  public static String getFunctionDescription(final String functionName) {
    final FunctionSignature sig = getSignature(functionName);
    return sig != null ? sig.getDescription() : "Unknown function";
  }
}
