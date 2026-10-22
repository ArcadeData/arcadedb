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

import java.util.HashSet;
import java.util.Set;

/**
 * The Cypher scalar/aggregate function names that are hardcoded into the engine rather than looked up through
 * {@link CypherFunctionRegistry} or a database's function library - {@code toUpper}, {@code abs}, {@code coalesce}
 * and the like. Every one of them is a pure, built-in Java implementation with no path to a schema-registered
 * {@code DEFINE FUNCTION} body, so a name in this list can never be the write-capable custom function issue #6418
 * is about.
 * <p>
 * The single source of truth for that name list, shared by the executor that resolves a call
 * ({@code CypherFunctionFactory.isCypherSpecificFunction}) and the parse-time classifier that has to tell a call to
 * one of these apart from a call into a {@code library.function} custom function ({@code SimpleCypherStatement}'s
 * read-only analysis) - both need the same answer to "is this name closed and pure", and duplicating the list
 * between them is exactly how the two would drift. It is a set rather than a switch so that a test can enumerate the
 * functions instead of naming them (issue #10523).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CypherBuiltinFunctions {
  private static final Set<String> NAMES = buildNames();

  private CypherBuiltinFunctions() {
  }

  /**
   * Whether {@code functionName} - already lower-cased, matching {@code FunctionCallExpression.getFunctionName()} -
   * is one of the engine's hardcoded Cypher scalar/aggregate functions.
   */
  public static boolean isBuiltin(final String functionName) {
    return NAMES.contains(functionName);
  }

  /**
   * Every name {@link #isBuiltin} answers true for, lower-cased.
   */
  public static Set<String> getNames() {
    return NAMES;
  }

  private static Set<String> buildNames() {
    final Set<String> names = new HashSet<>();
    // Graph functions
    add(names, "id", "elementid", "labels", "type", "keys", "properties", "startnode", "endnode");
    // Path functions
    add(names, "nodes", "relationships", "length", "path_length");
    // Math functions
    add(names, "rand", "sign", "ceil", "ceiling", "floor", "abs", "sqrt", "round", "isnan",
        "cosh", "sinh", "tanh", "cot", "coth", "pi", "e", "randomuuid",
        "acos", "asin", "atan", "atan2", "cos", "sin", "tan",
        "degrees", "radians", "haversin", "exp", "log", "ln", "log10");
    // General functions
    add(names, "coalesce");
    // Predicate functions
    add(names, "isempty", "exists");
    // List functions
    add(names, "size", "head", "tail", "last", "range");
    // String functions
    add(names, "left", "right", "reverse", "split", "substring", "tolower", "toupper", "lower", "upper", "ltrim", "rtrim", "btrim");
    // String functions (additional)
    add(names, "trim", "replace", "char.length", "character.length", "char_length", "character_length", "charlength",
        "normalize", "isnormalized");
    // Type conversion functions
    add(names, "tostring", "tointeger", "tofloat", "toboolean",
        "tostringornull", "tointegerornull", "tofloatornull", "tobooleanornull",
        "tobooleanlist", "tofloatlist", "tointegerlist", "tostringlist");
    // Scalar functions
    add(names, "nullif", "valuetype");
    // Aggregation functions
    add(names, "collect", "collect_list", "percentiledisc", "percentile_disc", "percentilecont", "percentile_cont", "min", "max",
        "avg");
    // Temporal functions
    add(names, "timestamp");
    // Temporal constructor functions
    add(names, "date", "localtime", "local_time", "time", "zoned_time", "localdatetime", "local_datetime", "datetime", "zoned_datetime",
        "duration", "duration_between");
    // Temporal truncation functions
    add(names, "date.truncate", "localtime.truncate", "time.truncate", "localdatetime.truncate", "datetime.truncate");
    // Temporal epoch functions
    add(names, "datetime.fromepoch", "datetime.fromepochmillis");
    // Temporal format function
    add(names, "format");
    // Duration calculation functions
    add(names, "duration.between", "duration.inmonths", "duration.indays", "duration.inseconds");
    // LOAD CSV context functions
    add(names, "file", "linenumber");
    // Vector similarity functions
    add(names, "vector.similarity.cosine", "vector.similarity.euclidean");
    // Vector construction and distance functions (used by Cypher vector(), vector_norm(), vector_distance())
    // Note: vector_norm and vector_distance with EUCLIDEAN/DOT metrics delegate to SQL functions
    // (vector.magnitude, vector.l1Norm, vector.l2Distance, vector.dotProduct) via the SQL bridge
    add(names, "vector.create", "vector.distance.manhattan", "vector.distance.cosine",
        "vector", "vector.dimension.count", "vector_dimension_count", "vector.distance");
    // Vector distance functions
    add(names, "vector.distance.euclidean");
    // Vector norm function
    add(names, "vector.norm");
    // Geo-spatial functions
    add(names, "point", "distance", "point.withinbbox", "point.distance");
    // Temporal clock functions (realtime/statement/transaction are aliases for current instant)
    add(names, "date.realtime", "date.statement", "date.transaction");
    add(names, "localtime.realtime", "localtime.statement", "localtime.transaction");
    add(names, "time.realtime", "time.statement", "time.transaction");
    add(names, "localdatetime.realtime", "localdatetime.statement", "localdatetime.transaction");
    add(names, "datetime.realtime", "datetime.statement", "datetime.transaction");
    return Set.copyOf(names);
  }

  private static void add(final Set<String> names, final String... group) {
    for (final String name : group)
      names.add(name);
  }
}
