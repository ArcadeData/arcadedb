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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.vector.*;
import com.arcadedb.function.text.*;
import com.arcadedb.function.convert.*;
import com.arcadedb.function.coll.*;
import com.arcadedb.function.graph.*;
import com.arcadedb.function.temporal.*;
import com.arcadedb.function.agg.*;
import com.arcadedb.function.misc.*;
import com.arcadedb.function.geo.*;
import com.arcadedb.function.sql.geo.SQLFunctionST_Distance;
import com.arcadedb.function.cypher.*;
import com.arcadedb.function.math.*;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.query.sql.executor.SQLFunction;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory for Cypher functions.
 * Provides a bridge between Cypher function names and ArcadeDB SQL functions.
 * Also implements Cypher-specific functions that don't have SQL equivalents.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class CypherFunctionFactory {
  private static final String SQL_PREFIX = "sql.";

  private final DefaultSQLFunctionFactory sqlFunctionFactory;
  private final Map<String, String>       cypherToSqlMapping;

  public CypherFunctionFactory(final DefaultSQLFunctionFactory sqlFunctionFactory) {
    this.sqlFunctionFactory = sqlFunctionFactory;
    this.cypherToSqlMapping = createFunctionMapping();
  }

  /**
   * Returns the underlying SQL function factory.
   */
  public DefaultSQLFunctionFactory getSQLFunctionFactory() {
    return sqlFunctionFactory;
  }

  /**
   * Create a mapping from Cypher function names to SQL function names.
   * Many functions have the same name, but some need mapping.
   */
  private Map<String, String> createFunctionMapping() {
    final Map<String, String> mapping = new HashMap<>();

    // Direct mappings (same name in Cypher and SQL)
    // Math functions
    mapping.put("abs", "abs");
    mapping.put("ceil", "ceil");
    mapping.put("floor", "floor");
    mapping.put("round", "round");
    mapping.put("sqrt", "sqrt");
    // rand() is handled as Cypher-specific (returns float 0.0-1.0)

    // Aggregation functions
    mapping.put("count", "count");
    mapping.put("sum", "sum");
    // avg is handled as Cypher-specific (always returns Double, matching Neo4j)
    // min/max handled as Cypher-specific to support mixed-type comparison
    mapping.put("stdev", "stddev");
    mapping.put("stdevp", "stddevp");

    // String functions - need to check if SQL has these
    mapping.put("toupper", "upper");
    mapping.put("tolower", "lower");
    // trim and replace are handled as Cypher-specific functions

    // Date/Time functions
    // Note: timestamp() is handled as Cypher-specific (returns millis since epoch, not a date object)

    // Cypher-specific functions (no SQL equivalent, handled specially)
    // id(), labels(), type(), keys(), properties(), startNode(), endNode()
    // nodes(), relationships(), length() (for paths)

    return mapping;
  }

  /**
   * Check if a function is available (either as SQL function or Cypher-specific).
   */
  public boolean hasFunction(final String cypherFunctionName) {
    final String functionName = cypherFunctionName.toLowerCase();

    // Check for sql. prefix - explicit SQL function access
    if (functionName.startsWith(SQL_PREFIX)) {
      final String sqlFunctionName = functionName.substring(SQL_PREFIX.length());
      return sqlFunctionFactory.hasFunction(sqlFunctionName);
    }

    // Check CypherFunctionRegistry for namespaced functions (text.*, map.*, util.*, date.*, etc.)
    if (CypherFunctionRegistry.hasFunction(functionName)) {
      return true;
    }

    // Check Cypher-specific functions
    if (isCypherSpecificFunction(functionName)) {
      return true;
    }

    // Note: Custom functions (library.function pattern) cannot be checked here
    // because we don't have database access. They will be validated during getFunctionExecutor().

    // Check if we have a mapping to SQL
    if (cypherToSqlMapping.containsKey(functionName)) {
      final String sqlFunctionName = cypherToSqlMapping.get(functionName);
      return sqlFunctionFactory.hasFunction(sqlFunctionName);
    }

    // Try direct lookup in SQL functions
    return sqlFunctionFactory.hasFunction(functionName);
  }

  /**
   * Get a function executor for the given Cypher function name.
   */
  public StatelessFunction getFunctionExecutor(final String cypherFunctionName) {
    return getFunctionExecutor(cypherFunctionName, false);
  }

  /**
   * Get a function executor for a Cypher function with optional DISTINCT handling.
   *
   * @param cypherFunctionName the function name
   * @param distinct           true if DISTINCT keyword was used
   */
  public StatelessFunction getFunctionExecutor(final String cypherFunctionName, final boolean distinct) {
    final String functionName = cypherFunctionName.toLowerCase();

    // Handle sql. prefix - explicit SQL function access
    if (functionName.startsWith(SQL_PREFIX)) {
      final String sqlFunctionName = functionName.substring(SQL_PREFIX.length());
      if (sqlFunctionFactory.hasFunction(sqlFunctionName)) {
        final SQLFunction sqlFunction = sqlFunctionFactory.getFunctionInstance(sqlFunctionName);
        return new SQLFunctionBridge(sqlFunction, cypherFunctionName);
      }
      throw new CommandExecutionException("Unknown SQL function: " + sqlFunctionName);
    }

    // Check CypherFunctionRegistry for namespaced functions (text.*, map.*, util.*, date.*, etc.)
    final StatelessFunction registryFunction = CypherFunctionRegistry.get(functionName);
    if (registryFunction != null) {
      return registryFunction;
    }

    // Handle Cypher-specific functions
    if (isCypherSpecificFunction(functionName)) {
      return createCypherSpecificExecutor(functionName, distinct);
    }

    // Get SQL function (either mapped or direct) - checked BEFORE custom functions
    // so that registered SQL functions (e.g., vector.magnitude) are found
    final String sqlFunctionName = cypherToSqlMapping.getOrDefault(functionName, functionName);
    if (sqlFunctionFactory.hasFunction(sqlFunctionName)) {
      final SQLFunction sqlFunction = sqlFunctionFactory.getFunctionInstance(sqlFunctionName);
      final SQLFunctionBridge bridge = new SQLFunctionBridge(sqlFunction, functionName);
      if (distinct && bridge.aggregateResults())
        return new DistinctAggregationWrapper(bridge);
      return bridge;
    }

    // Check for custom functions (library.function pattern) - checked AFTER SQL functions
    if (functionName.contains(".")) {
      final StatelessFunction customFunction = getOrCreateCustomFunctionAdapter(functionName);
      if (customFunction != null)
        return customFunction;
    }

    throw new CommandExecutionException("Unknown function: " + cypherFunctionName);
  }

  private StatelessFunction getOrCreateCustomFunctionAdapter(final String functionName) {
    // Parse library.function
    final int dotIndex = functionName.lastIndexOf('.');
    if (dotIndex <= 0 || dotIndex >= functionName.length() - 1)
      return null;

    final String libraryName = functionName.substring(0, dotIndex);
    final String simpleName = functionName.substring(dotIndex + 1);

    // Create adapter (it will do lazy schema lookup on execute each time)
    // Not cached to avoid stale references when databases are dropped/recreated
    return new CustomFunctionAdapter(libraryName, simpleName);
  }

  /**
   * Check if this is a Cypher-specific function (not available in SQL).
   */
  private boolean isCypherSpecificFunction(final String functionName) {
    return switch (functionName) {
      // Graph functions
      case "id", "elementid", "labels", "type", "keys", "properties", "startnode", "endnode" -> true;
      // Path functions
      case "nodes", "relationships", "length" -> true;
      // Math functions
      case "rand", "sign", "ceil", "floor", "abs", "sqrt", "round", "isnan",
           "cosh", "sinh", "tanh", "cot", "coth", "pi", "e", "randomuuid",
           "acos", "asin", "atan", "atan2", "cos", "sin", "tan",
           "degrees", "radians", "haversin", "exp", "log", "log10" -> true;
      // General functions
      case "coalesce" -> true;
      // Predicate functions
      case "isempty", "exists" -> true;
      // List functions
      case "size", "head", "tail", "last", "range" -> true;
      // String functions
      case "left", "right", "reverse", "split", "substring", "tolower", "toupper", "lower", "upper", "ltrim", "rtrim", "btrim" -> true;
      // String functions (additional)
      case "trim", "replace", "char_length", "character_length", "normalize" -> true;
      // Type conversion functions
      case "tostring", "tointeger", "tofloat", "toboolean",
           "tostringornull", "tointegerornull", "tofloatornull", "tobooleanornull",
           "tobooleanlist", "tofloatlist", "tointegerlist", "tostringlist" -> true;
      // Scalar functions
      case "nullif", "valuetype" -> true;
      // Aggregation functions
      case "collect", "percentiledisc", "percentilecont", "min", "max", "avg" -> true;
      // Temporal functions
      case "timestamp" -> true;
      // Temporal constructor functions
      case "date", "localtime", "time", "localdatetime", "datetime", "duration" -> true;
      // Temporal truncation functions
      case "date.truncate", "localtime.truncate", "time.truncate", "localdatetime.truncate", "datetime.truncate" ->
          true;
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
      case "vector_create", "vector_distance_manhattan", "vector_distance_cosine",
           "vector", "vector_dimension_count", "vector_distance" -> true;
      // Vector distance functions
      case "vector.distance.euclidean" -> true;
      // Vector norm function
      case "vector.norm" -> true;
      // Geo-spatial functions
      case "point", "distance", "point.withinbbox" -> true;
      // Temporal clock functions (realtime/statement/transaction are aliases for current instant)
      case "date.realtime", "date.statement", "date.transaction" -> true;
      case "localtime.realtime", "localtime.statement", "localtime.transaction" -> true;
      case "time.realtime", "time.statement", "time.transaction" -> true;
      case "localdatetime.realtime", "localdatetime.statement", "localdatetime.transaction" -> true;
      case "datetime.realtime", "datetime.statement", "datetime.transaction" -> true;
      default -> false;
    };
  }

  /**
   * Create executor for Cypher-specific functions.
   *
   * @param functionName the function name (lowercase)
   * @param distinct     true if DISTINCT keyword was used (for aggregation functions)
   */
  private StatelessFunction createCypherSpecificExecutor(final String functionName, final boolean distinct) {
    return switch (functionName) {
      // Math functions
      case "rand" -> new RandFunction();
      case "randomuuid" -> new RandomUuidFunction();
      case "sign" -> new SignFunction();
      case "ceil" -> new MathUnaryFunction("ceil", Math::ceil);
      case "floor" -> new MathUnaryFunction("floor", Math::floor);
      case "abs" -> new MathUnaryFunction("abs", Math::abs);
      case "sqrt" -> new MathUnaryFunction("sqrt", Math::sqrt);
      case "round" -> new RoundFunction();
      case "isnan" -> new IsNaNFunction();
      case "cosh" -> new MathUnaryFunction("cosh", Math::cosh);
      case "sinh" -> new MathUnaryFunction("sinh", Math::sinh);
      case "tanh" -> new MathUnaryFunction("tanh", Math::tanh);
      case "cot" -> new MathUnaryFunction("cot", v -> Math.cos(v) / Math.sin(v));
      case "coth" -> new MathUnaryFunction("coth", v -> Math.cosh(v) / Math.sinh(v));
      case "pi" -> new ConstantFunction("pi", Math.PI);
      case "e" -> new ConstantFunction("e", Math.E);
      // Trigonometric functions
      case "acos" -> new MathUnaryFunction("acos", Math::acos);
      case "asin" -> new MathUnaryFunction("asin", Math::asin);
      case "atan" -> new MathUnaryFunction("atan", Math::atan);
      case "atan2" -> new MathBinaryFunction("atan2", Math::atan2);
      case "cos" -> new MathUnaryFunction("cos", Math::cos);
      case "sin" -> new MathUnaryFunction("sin", Math::sin);
      case "tan" -> new MathUnaryFunction("tan", Math::tan);
      case "degrees" -> new MathUnaryFunction("degrees", Math::toDegrees);
      case "radians" -> new MathUnaryFunction("radians", Math::toRadians);
      case "haversin" -> new MathUnaryFunction("haversin", v -> (1.0 - Math.cos(v)) / 2.0);
      // Logarithmic functions
      case "exp" -> new MathUnaryFunction("exp", Math::exp);
      case "log" -> new MathUnaryFunction("log", Math::log);
      case "log10" -> new MathUnaryFunction("log10", Math::log10);
      // General functions
      case "coalesce" -> new CoalesceFunction();
      // Graph functions
      case "id" -> new IdFunction();
      case "elementid" -> new ElementIdFunction();
      case "labels" -> new LabelsFunction();
      case "type" -> new TypeFunction();
      case "keys" -> new KeysFunction();
      case "properties" -> new PropertiesFunction();
      case "startnode" -> new StartNodeFunction();
      case "endnode" -> new EndNodeFunction();
      // Path functions
      case "nodes" -> new NodesFunction();
      case "relationships" -> new RelationshipsFunction();
      case "length" -> new LengthFunction();
      // List functions
      case "size" -> new SizeFunction();
      case "head" -> new HeadFunction();
      case "tail" -> new TailFunction();
      case "last" -> new LastFunction();
      case "range" -> new RangeFunction();
      // Predicate functions
      case "isempty" -> new IsEmptyFunction();
      case "exists" -> new ExistsFunction();
      // String functions
      case "left" -> new LeftFunction();
      case "right" -> new RightFunction();
      case "reverse" -> new ReverseFunction();
      case "split" -> new SplitFunction();
      case "substring" -> new SubstringFunction();
      case "tolower", "lower" -> new ToLowerFunction();
      case "toupper", "upper" -> new ToUpperFunction();
      case "ltrim" -> new LTrimFunction();
      case "rtrim" -> new RTrimFunction();
      case "trim", "btrim" -> new TrimFunction();
      case "replace" -> new ReplaceFunction();
      case "char_length", "character_length" -> new CharLengthFunction();
      case "normalize" -> new NormalizeFunction();
      // Type conversion functions
      case "tostring" -> new ToStringFunction();
      case "tointeger" -> new ToIntegerFunction();
      case "tofloat" -> new ToFloatFunction();
      case "toboolean" -> new ToBooleanFunction();
      case "tostringornull" -> new OrNullFunction("toStringOrNull", new ToStringFunction());
      case "tointegerornull" -> new OrNullFunction("toIntegerOrNull", new ToIntegerFunction());
      case "tofloatornull" -> new OrNullFunction("toFloatOrNull", new ToFloatFunction());
      case "tobooleanornull" -> new OrNullFunction("toBooleanOrNull", new ToBooleanFunction());
      // List conversion functions
      case "tobooleanlist" -> new ToBooleanListFunction();
      case "tofloatlist" -> new ToFloatListFunction();
      case "tointegerlist" -> new ToIntegerListFunction();
      case "tostringlist" -> new ToStringListFunction();
      // Scalar functions
      case "nullif" -> new NullIfFunction();
      case "valuetype" -> new ValueTypeFunction();
      // Aggregation functions
      case "avg" -> distinct ? new DistinctAggregationWrapper(new CypherAvgFunction()) : new CypherAvgFunction();
      case "collect" -> distinct ? new CollectDistinctFunction() : new CollectFunction();
      case "min" -> distinct ? new DistinctAggregationWrapper(new CypherMinFunction()) : new CypherMinFunction();
      case "max" -> distinct ? new DistinctAggregationWrapper(new CypherMaxFunction()) : new CypherMaxFunction();
      case "percentiledisc" -> new PercentileDiscFunction();
      case "percentilecont" -> new PercentileContFunction();
      // Temporal functions
      case "timestamp" -> new TimestampFunction();
      // Temporal format function
      case "format" -> new FormatFunction();
      // LOAD CSV context functions
      case "file" -> new LoadCSVFileFunction();
      case "linenumber" -> new LoadCSVLineNumberFunction();
      // Vector similarity functions
      case "vector.similarity.cosine" -> new VectorSimilarityCosineFunction();
      case "vector.similarity.euclidean" -> new VectorSimilarityEuclideanFunction();
      // Vector construction and distance functions
      case "vector_create" -> new VectorCreateFunction();
      case "vector_distance_manhattan" -> new VectorDistanceManhattanFunction();
      case "vector_distance_cosine" -> new VectorDistanceCosineFunction();
      case "vector" -> new VectorCreateFunction();
      case "vector_dimension_count" -> new VectorDimensionCountFunction();
      case "vector_distance" -> new VectorDistanceFunction();
      // Vector distance functions
      case "vector.distance.euclidean" -> new VectorDistanceEuclideanFunction();
      // Vector norm function
      case "vector.norm" -> new VectorNormFunction();
      // Geo-spatial functions
      case "point" -> new CypherPointFunction();
      case "distance" -> new SQLFunctionBridge(sqlFunctionFactory.getFunctionInstance(SQLFunctionST_Distance.NAME), "distance");
      case "point.withinbbox" -> new PointWithinBBoxFunction();
      // Temporal constructor functions
      case "date" -> new DateConstructorFunction();
      case "localtime" -> new LocalTimeConstructorFunction();
      case "time" -> new TimeConstructorFunction();
      case "localdatetime" -> new LocalDateTimeConstructorFunction();
      case "datetime" -> new DateTimeConstructorFunction();
      case "duration" -> new DurationConstructorFunction();
      // Temporal truncation functions
      case "date.truncate" -> new DateTruncateFunction();
      case "localtime.truncate" -> new LocalTimeTruncateFunction();
      case "time.truncate" -> new TimeTruncateFunction();
      case "localdatetime.truncate" -> new LocalDateTimeTruncateFunction();
      case "datetime.truncate" -> new DateTimeTruncateFunction();
      // Temporal epoch functions
      case "datetime.fromepoch" -> new DateTimeFromEpochFunction();
      case "datetime.fromepochmillis" -> new DateTimeFromEpochMillisFunction();
      // Duration calculation functions
      case "duration.between" -> new DurationBetweenFunction();
      case "duration.inmonths" -> new DurationInMonthsFunction();
      case "duration.indays" -> new DurationInDaysFunction();
      case "duration.inseconds" -> new DurationInSecondsFunction();
      // Temporal clock functions (aliases for no-arg constructors)
      case "date.realtime", "date.statement", "date.transaction" -> new DateConstructorFunction();
      case "localtime.realtime", "localtime.statement", "localtime.transaction" -> new LocalTimeConstructorFunction();
      case "time.realtime", "time.statement", "time.transaction" -> new TimeConstructorFunction();
      case "localdatetime.realtime", "localdatetime.statement", "localdatetime.transaction" ->
          new LocalDateTimeConstructorFunction();
      case "datetime.realtime", "datetime.statement", "datetime.transaction" -> new DateTimeConstructorFunction();
      default -> throw new CommandExecutionException("Cypher function not implemented: " + functionName);
    };
  }
}
