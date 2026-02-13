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
import com.arcadedb.query.opencypher.function.*;
import com.arcadedb.query.opencypher.functions.CypherFunctionRegistry;
import com.arcadedb.query.sql.executor.SQLFunction;
import com.arcadedb.query.sql.function.DefaultSQLFunctionFactory;

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
    mapping.put("avg", "avg");
    // min/max handled as Cypher-specific to support mixed-type comparison
    // stdev/stdevp handled as Cypher-specific (sample vs population)

    // String functions - need to check if SQL has these
    mapping.put("toupper", "upper");
    mapping.put("tolower", "lower");
    mapping.put("trim", "trim");
    mapping.put("replace", "replace");

    // Date/Time functions
    mapping.put("timestamp", "sysdate");

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

    // Check for custom functions (library.function pattern)
    if (functionName.contains(".")) {
      final StatelessFunction customFunction = getOrCreateCustomFunctionAdapter(functionName);
      if (customFunction != null)
        return customFunction;
    }

    // Get SQL function (either mapped or direct)
    final String sqlFunctionName = cypherToSqlMapping.getOrDefault(functionName, functionName);
    if (sqlFunctionFactory.hasFunction(sqlFunctionName)) {
      final SQLFunction sqlFunction = sqlFunctionFactory.getFunctionInstance(sqlFunctionName);
      final SQLFunctionBridge bridge = new SQLFunctionBridge(sqlFunction, functionName);
      if (distinct && bridge.aggregateResults())
        return new DistinctAggregationWrapper(bridge);
      return bridge;
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
      case "id", "labels", "type", "keys", "properties", "startnode", "endnode" -> true;
      // Path functions
      case "nodes", "relationships", "length" -> true;
      // Math functions
      case "rand", "sign", "ceil", "floor", "abs", "sqrt", "round", "isnan",
           "cosh", "sinh", "tanh", "cot", "coth", "pi", "e", "randomuuid" -> true;
      // General functions
      case "coalesce" -> true;
      // Predicate functions
      case "isempty" -> true;
      // List functions
      case "size", "head", "tail", "last", "range" -> true;
      // String functions
      case "left", "right", "reverse", "split", "substring", "tolower", "toupper", "ltrim", "rtrim" -> true;
      // Type conversion functions
      case "tostring", "tointeger", "tofloat", "toboolean" -> true;
      // Aggregation functions
      case "collect", "percentiledisc", "percentilecont", "min", "max", "stdev", "stdevp" -> true;
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
      // Vector similarity functions
      case "vector.similarity.cosine", "vector.similarity.euclidean" -> true;
      // Geo-spatial functions
      case "point.withinbbox" -> true;
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
      case "round" -> new MathUnaryFunction("round", v -> (double) Math.round(v));
      case "isnan" -> new IsNaNFunction();
      case "cosh" -> new MathUnaryFunction("cosh", Math::cosh);
      case "sinh" -> new MathUnaryFunction("sinh", Math::sinh);
      case "tanh" -> new MathUnaryFunction("tanh", Math::tanh);
      case "cot" -> new MathUnaryFunction("cot", v -> Math.cos(v) / Math.sin(v));
      case "coth" -> new MathUnaryFunction("coth", v -> Math.cosh(v) / Math.sinh(v));
      case "pi" -> new ConstantFunction("pi", Math.PI);
      case "e" -> new ConstantFunction("e", Math.E);
      // General functions
      case "coalesce" -> new CoalesceFunction();
      // Graph functions
      case "id" -> new IdFunction();
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
      // String functions
      case "left" -> new LeftFunction();
      case "right" -> new RightFunction();
      case "reverse" -> new ReverseFunction();
      case "split" -> new SplitFunction();
      case "substring" -> new SubstringFunction();
      case "tolower" -> new ToLowerFunction();
      case "toupper" -> new ToUpperFunction();
      case "ltrim" -> new LTrimFunction();
      case "rtrim" -> new RTrimFunction();
      // Type conversion functions
      case "tostring" -> new ToStringFunction();
      case "tointeger" -> new ToIntegerFunction();
      case "tofloat" -> new ToFloatFunction();
      case "toboolean" -> new ToBooleanFunction();
      // Aggregation functions
      case "collect" -> distinct ? new CollectDistinctFunction() : new CollectFunction();
      case "min" -> distinct ? new DistinctAggregationWrapper(new CypherMinFunction()) : new CypherMinFunction();
      case "max" -> distinct ? new DistinctAggregationWrapper(new CypherMaxFunction()) : new CypherMaxFunction();
      case "percentiledisc" -> new PercentileDiscFunction();
      case "percentilecont" -> new PercentileContFunction();
      case "stdev" -> distinct ? new DistinctAggregationWrapper(new StDevFunction()) : new StDevFunction();
      case "stdevp" -> distinct ? new DistinctAggregationWrapper(new StDevPFunction()) : new StDevPFunction();
      // Temporal format function
      case "format" -> new FormatFunction();
      // Vector similarity functions
      case "vector.similarity.cosine" -> new VectorSimilarityCosineFunction();
      case "vector.similarity.euclidean" -> new VectorSimilarityEuclideanFunction();
      // Geo-spatial functions
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
