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
 * between them is exactly how the two would drift.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CypherBuiltinFunctions {

  private CypherBuiltinFunctions() {
  }

  /**
   * Whether {@code functionName} - already lower-cased, matching {@code FunctionCallExpression.getFunctionName()} -
   * is one of the engine's hardcoded Cypher scalar/aggregate functions.
   */
  public static boolean isBuiltin(final String functionName) {
    return switch (functionName) {
      // Graph functions
      case "id", "elementid", "labels", "type", "keys", "properties", "startnode", "endnode" -> true;
      // Path functions
      case "nodes", "relationships", "length", "path_length" -> true;
      // Math functions
      case "rand", "sign", "ceil", "ceiling", "floor", "abs", "sqrt", "round", "isnan",
           "cosh", "sinh", "tanh", "cot", "coth", "pi", "e", "randomuuid",
           "acos", "asin", "atan", "atan2", "cos", "sin", "tan",
           "degrees", "radians", "haversin", "exp", "log", "ln", "log10" -> true;
      // General functions
      case "coalesce" -> true;
      // Predicate functions
      case "isempty", "exists" -> true;
      // List functions
      case "size", "head", "tail", "last", "range" -> true;
      // String functions
      case "left", "right", "reverse", "split", "substring", "tolower", "toupper", "lower", "upper", "ltrim", "rtrim", "btrim" ->
          true;
      // String functions (additional)
      case "trim", "replace", "char.length", "character.length", "char_length", "character_length", "charlength",
           "normalize", "isnormalized" -> true;
      // Type conversion functions
      case "tostring", "tointeger", "tofloat", "toboolean",
           "tostringornull", "tointegerornull", "tofloatornull", "tobooleanornull",
           "tobooleanlist", "tofloatlist", "tointegerlist", "tostringlist" -> true;
      // Scalar functions
      case "nullif", "valuetype" -> true;
      // Aggregation functions
      case "collect", "collect_list", "percentiledisc", "percentile_disc", "percentilecont", "percentile_cont", "min", "max",
           "avg" -> true;
      // Temporal functions
      case "timestamp" -> true;
      // Temporal constructor functions
      case "date", "localtime", "local_time", "time", "zoned_time", "localdatetime", "local_datetime", "datetime", "zoned_datetime",
           "duration", "duration_between" -> true;
      // Temporal truncation functions
      case "date.truncate", "localtime.truncate", "time.truncate", "localdatetime.truncate", "datetime.truncate" -> true;
      // Temporal epoch functions
      case "datetime.fromepoch", "datetime.fromepochmillis" -> true;
      // Temporal format function
      case "format" -> true;
      // Duration calculation functions
      case "duration.between", "duration.inmonths", "duration.indays", "duration.inseconds" -> true;
      // LOAD CSV context functions
      case "file", "linenumber" -> true;
      // Vector similarity functions
      case "vector.similarity.cosine", "vector.similarity.euclidean" -> true;
      // Vector construction and distance functions (used by Cypher vector(), vector_norm(), vector_distance())
      // Note: vector_norm and vector_distance with EUCLIDEAN/DOT metrics delegate to SQL functions
      // (vector.magnitude, vector.l1Norm, vector.l2Distance, vector.dotProduct) via the SQL bridge
      case "vector.create", "vector.distance.manhattan", "vector.distance.cosine",
           "vector", "vector.dimension.count", "vector_dimension_count", "vector.distance" -> true;
      // Vector distance functions
      case "vector.distance.euclidean" -> true;
      // Vector norm function
      case "vector.norm" -> true;
      // Geo-spatial functions
      case "point", "distance", "point.withinbbox", "point.distance" -> true;
      // Temporal clock functions (realtime/statement/transaction are aliases for current instant)
      case "date.realtime", "date.statement", "date.transaction" -> true;
      case "localtime.realtime", "localtime.statement", "localtime.transaction" -> true;
      case "time.realtime", "time.statement", "time.transaction" -> true;
      case "localdatetime.realtime", "localdatetime.statement", "localdatetime.transaction" -> true;
      case "datetime.realtime", "datetime.statement", "datetime.transaction" -> true;
      default -> false;
    };
  }
}
