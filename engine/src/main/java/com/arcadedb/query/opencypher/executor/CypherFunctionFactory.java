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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.functions.CypherFunctionRegistry;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.SQLFunction;
import com.arcadedb.query.sql.function.DefaultSQLFunctionFactory;

import com.arcadedb.query.opencypher.temporal.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Factory for Cypher functions.
 * Provides a bridge between Cypher function names and ArcadeDB SQL functions.
 * Also implements Cypher-specific functions that don't have SQL equivalents.
 */
public class CypherFunctionFactory {
  private static final String SQL_PREFIX = "sql.";

  private final DefaultSQLFunctionFactory sqlFunctionFactory;
  private final Map<String, String> cypherToSqlMapping;

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
    mapping.put("min", "min");
    mapping.put("max", "max");
    mapping.put("stdev", "stddev");
    mapping.put("stdevp", "stddev");

    // String functions - need to check if SQL has these
    mapping.put("toupper", "upper");
    mapping.put("tolower", "lower");
    mapping.put("trim", "trim");
    mapping.put("substring", "substring");
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
      case "rand", "sign" -> true;
      // General functions
      case "coalesce" -> true;
      // List functions
      case "size", "head", "tail", "last", "range" -> true;
      // String functions
      case "left", "right", "reverse", "split" -> true;
      // Type conversion functions
      case "tostring", "tointeger", "tofloat", "toboolean" -> true;
      // Aggregation functions
      case "collect" -> true;
      // Temporal constructor functions
      case "date", "localtime", "time", "localdatetime", "datetime", "duration" -> true;
      // Temporal truncation functions
      case "date.truncate", "localtime.truncate", "time.truncate", "localdatetime.truncate", "datetime.truncate" -> true;
      // Temporal epoch functions
      case "datetime.fromepoch", "datetime.fromepochmillis" -> true;
      // Duration calculation functions
      case "duration.between", "duration.inmonths", "duration.indays", "duration.inseconds" -> true;
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
      case "sign" -> new SignFunction();
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
      // String functions
      case "left" -> new LeftFunction();
      case "right" -> new RightFunction();
      case "reverse" -> new ReverseFunction();
      case "split" -> new SplitFunction();
      // Type conversion functions
      case "tostring" -> new ToStringFunction();
      case "tointeger" -> new ToIntegerFunction();
      case "tofloat" -> new ToFloatFunction();
      case "toboolean" -> new ToBooleanFunction();
      // Aggregation functions
      case "collect" -> distinct ? new CollectDistinctFunction() : new CollectFunction();
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
      case "localdatetime.realtime", "localdatetime.statement", "localdatetime.transaction" -> new LocalDateTimeConstructorFunction();
      case "datetime.realtime", "datetime.statement", "datetime.transaction" -> new DateTimeConstructorFunction();
      default -> throw new CommandExecutionException("Cypher function not implemented: " + functionName);
    };
  }

  // Cypher-specific function implementations

  /**
   * rand() function - returns a random float between 0.0 (inclusive) and 1.0 (exclusive).
   */
  private static class RandFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "rand";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      return Math.random();
    }
  }

  /**
   * sign() function - returns the signum of a number: -1, 0, or 1.
   */
  private static class SignFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "sign";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1)
        throw new CommandExecutionException("sign() requires exactly one argument");
      if (args[0] == null)
        return null;
      if (args[0] instanceof Number) {
        final double value = ((Number) args[0]).doubleValue();
        if (value > 0)
          return 1L;
        else if (value < 0)
          return -1L;
        else
          return 0L;
      }
      throw new CommandExecutionException("sign() requires a numeric argument");
    }
  }

  /**
   * coalesce() function - returns the first non-null argument.
   */
  private static class CoalesceFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "coalesce";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      for (final Object arg : args)
        if (arg != null)
          return arg;
      return null;
    }
  }

  /**
   * id() function - returns the internal ID of a node or relationship.
   */
  private static class IdFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "id";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("id() requires exactly one argument");
      }
      if (args[0] instanceof Identifiable) {
        return ((Identifiable) args[0]).getIdentity().toString();
      }
      return null;
    }
  }

  /**
   * labels() function - returns the labels of a node.
   * <p>
   * For vertices with multiple labels (composite types), returns all labels
   * sorted alphabetically. For single-label vertices, returns a list with
   * the type name.
   */
  private static class LabelsFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "labels";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("labels() requires exactly one argument");
      }
      if (args[0] instanceof Vertex vertex) {
        // Use the Labels helper class which handles composite types
        return Labels.getLabels(vertex);
      }
      return Collections.emptyList();
    }
  }

  /**
   * type() function - returns the type of a relationship.
   */
  private static class TypeFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "type";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("type() requires exactly one argument");
      }
      if (args[0] instanceof Edge) {
        return ((Edge) args[0]).getTypeName();
      }
      return null;
    }
  }

  /**
   * keys() function - returns the property keys of a node or relationship.
   */
  private static class KeysFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "keys";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("keys() requires exactly one argument");
      }
      if (args[0] == null)
        return null;
      if (args[0] instanceof Document) {
        final Document doc = (Document) args[0];
        return new ArrayList<>(doc.getPropertyNames());
      }
      if (args[0] instanceof Map) {
        return new ArrayList<>(((Map<?, ?>) args[0]).keySet());
      }
      return Collections.emptyList();
    }
  }

  /**
   * properties() function - returns all properties as a map.
   */
  private static class PropertiesFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "properties";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("properties() requires exactly one argument");
      }
      if (args[0] == null)
        return null;
      if (args[0] instanceof Document) {
        final Document doc = (Document) args[0];
        final Map<String, Object> props = new LinkedHashMap<>();
        for (final String propName : doc.getPropertyNames())
          props.put(propName, doc.get(propName));
        return props;
      }
      if (args[0] instanceof Map)
        return new LinkedHashMap<>((Map<?, ?>) args[0]);
      return Collections.emptyMap();
    }
  }

  /**
   * startNode() function - returns the start node of a relationship.
   */
  private static class StartNodeFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "startNode";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("startNode() requires exactly one argument");
      }
      if (args[0] instanceof Edge) {
        final Edge edge = (Edge) args[0];
        // getOutVertex() returns the actual Vertex, not just a RID
        return edge.getOutVertex();
      }
      return null;
    }
  }

  /**
   * endNode() function - returns the end node of a relationship.
   */
  private static class EndNodeFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "endNode";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("endNode() requires exactly one argument");
      }
      if (args[0] instanceof Edge) {
        final Edge edge = (Edge) args[0];
        // getInVertex() returns the actual Vertex, not just a RID
        return edge.getInVertex();
      }
      return null;
    }
  }

  // Path Functions

  /**
   * nodes() function - returns all nodes in a path.
   */
  private static class NodesFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "nodes";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1)
        throw new CommandExecutionException("nodes() requires exactly one argument");
      if (args[0] == null)
        return null;
      if (args[0] instanceof List) {
        final List<?> path = (List<?>) args[0];
        final List<Vertex> nodes = new ArrayList<>();
        for (final Object element : path)
          if (element instanceof Vertex)
            nodes.add((Vertex) element);
        return nodes;
      }
      return Collections.emptyList();
    }
  }

  /**
   * relationships() function - returns all relationships in a path.
   */
  private static class RelationshipsFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "relationships";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1)
        throw new CommandExecutionException("relationships() requires exactly one argument");
      if (args[0] == null)
        return null;
      if (args[0] instanceof List) {
        final List<?> path = (List<?>) args[0];
        final List<Edge> edges = new ArrayList<>();
        for (final Object element : path)
          if (element instanceof Edge)
            edges.add((Edge) element);
        return edges;
      }
      return Collections.emptyList();
    }
  }

  /**
   * length() function - returns the length of a path (number of relationships).
   */
  private static class LengthFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "length";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("length() requires exactly one argument");
      }
      if (args[0] instanceof List) {
        // Path is represented as a list of alternating vertices and edges
        // Length = number of edges
        final List<?> path = (List<?>) args[0];
        long edgeCount = 0;
        for (final Object element : path) {
          if (element instanceof Edge) {
            edgeCount++;
          }
        }
        return edgeCount;
      } else if (args[0] instanceof String) {
        // Also support string length for compatibility
        return (long) ((String) args[0]).length();
      }
      return 0L;
    }
  }

  // List Functions

  /**
   * size() function - returns the size of a list or string.
   */
  private static class SizeFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "size";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("size() requires exactly one argument");
      }
      if (args[0] == null)
        return null;
      if (args[0] instanceof List) {
        return (long) ((List<?>) args[0]).size();
      } else if (args[0] instanceof String) {
        return (long) ((String) args[0]).length();
      }
      return null;
    }
  }

  /**
   * head() function - returns the first element of a list.
   */
  private static class HeadFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "head";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("head() requires exactly one argument");
      }
      if (args[0] instanceof List) {
        final List<?> list = (List<?>) args[0];
        return list.isEmpty() ? null : list.get(0);
      }
      return null;
    }
  }

  /**
   * tail() function - returns all elements except the first.
   */
  private static class TailFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "tail";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("tail() requires exactly one argument");
      }
      if (args[0] instanceof List) {
        final List<?> list = (List<?>) args[0];
        return list.size() <= 1 ? Collections.emptyList() : list.subList(1, list.size());
      }
      return Collections.emptyList();
    }
  }

  /**
   * last() function - returns the last element of a list.
   */
  private static class LastFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "last";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("last() requires exactly one argument");
      }
      if (args[0] instanceof List) {
        final List<?> list = (List<?>) args[0];
        return list.isEmpty() ? null : list.get(list.size() - 1);
      }
      return null;
    }
  }

  /**
   * range() function - creates a list of numbers from start to end (optionally with step).
   */
  private static class RangeFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "range";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length < 2 || args.length > 3) {
        throw new CommandExecutionException("range() requires 2 or 3 arguments: range(start, end) or range(start, end, step)");
      }
      final long start = ((Number) args[0]).longValue();
      final long end = ((Number) args[1]).longValue();
      final long step = args.length == 3 ? ((Number) args[2]).longValue() : 1L;

      if (step == 0) {
        throw new CommandExecutionException("range() step cannot be zero");
      }

      final List<Long> result = new ArrayList<>();
      if (step > 0) {
        for (long i = start; i <= end; i += step) {
          result.add(i);
        }
      } else {
        for (long i = start; i >= end; i += step) {
          result.add(i);
        }
      }
      return result;
    }
  }

  // String Functions

  /**
   * left() function - returns the leftmost n characters of a string.
   */
  private static class LeftFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "left";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 2) {
        throw new CommandExecutionException("left() requires exactly 2 arguments: left(string, length)");
      }
      if (args[0] == null) {
        return null;
      }
      final String str = args[0].toString();
      final int length = ((Number) args[1]).intValue();
      return str.substring(0, Math.min(length, str.length()));
    }
  }

  /**
   * right() function - returns the rightmost n characters of a string.
   */
  private static class RightFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "right";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 2) {
        throw new CommandExecutionException("right() requires exactly 2 arguments: right(string, length)");
      }
      if (args[0] == null) {
        return null;
      }
      final String str = args[0].toString();
      final int length = ((Number) args[1]).intValue();
      return str.substring(Math.max(0, str.length() - length));
    }
  }

  /**
   * reverse() function - reverses a string or list.
   */
  private static class ReverseFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "reverse";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("reverse() requires exactly one argument");
      }
      if (args[0] == null) {
        return null;
      }
      if (args[0] instanceof String) {
        final String str = (String) args[0];
        return new StringBuilder(str).reverse().toString();
      } else if (args[0] instanceof List) {
        final List<?> list = (List<?>) args[0];
        final List<Object> reversed = new ArrayList<>(list);
        Collections.reverse(reversed);
        return reversed;
      }
      return null;
    }
  }

  /**
   * split() function - splits a string by a delimiter.
   */
  private static class SplitFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "split";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 2) {
        throw new CommandExecutionException("split() requires exactly 2 arguments: split(string, delimiter)");
      }
      if (args[0] == null) {
        return null;
      }
      final String str = args[0].toString();
      final String delimiter = args[1].toString();
      return List.of(str.split(Pattern.quote(delimiter)));
    }
  }

  // Type Conversion Functions

  /**
   * toString() function - converts a value to a string.
   */
  private static class ToStringFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "toString";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1)
        throw new CommandExecutionException("toString() requires exactly one argument");
      if (args[0] == null)
        return null;
      if (args[0] instanceof String || args[0] instanceof Number || args[0] instanceof Boolean)
        return args[0].toString();
      // Temporal types
      if (args[0] instanceof CypherTemporalValue)
        return args[0].toString();
      // Unsupported types: List, Map, Node, Relationship, Path
      if (args[0] instanceof List || args[0] instanceof Map
          || args[0] instanceof Vertex || args[0] instanceof Edge
          || args[0] instanceof Document)
        throw new CommandExecutionException("TypeError: InvalidArgumentValue - toString() cannot convert " + args[0].getClass().getSimpleName());
      return args[0].toString();
    }
  }

  /**
   * toInteger() function - converts a value to an integer.
   */
  private static class ToIntegerFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "toInteger";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1)
        throw new CommandExecutionException("toInteger() requires exactly one argument");
      if (args[0] == null)
        return null;
      if (args[0] instanceof Boolean)
        return ((Boolean) args[0]) ? 1L : 0L;
      if (args[0] instanceof Number)
        return ((Number) args[0]).longValue();
      if (args[0] instanceof String) {
        try {
          // Try integer first, then float (truncating)
          final String s = ((String) args[0]).trim();
          if (s.contains(".") || s.contains("e") || s.contains("E"))
            return (long) Double.parseDouble(s);
          return Long.parseLong(s);
        } catch (final NumberFormatException e) {
          return null;
        }
      }
      throw new CommandExecutionException("TypeError: InvalidArgumentValue - toInteger() cannot convert " + args[0].getClass().getSimpleName());
    }
  }

  /**
   * toFloat() function - converts a value to a float.
   */
  private static class ToFloatFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "toFloat";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1)
        throw new CommandExecutionException("toFloat() requires exactly one argument");
      if (args[0] == null)
        return null;
      if (args[0] instanceof Number)
        return ((Number) args[0]).doubleValue();
      if (args[0] instanceof String) {
        try {
          return Double.parseDouble((String) args[0]);
        } catch (final NumberFormatException e) {
          return null;
        }
      }
      throw new CommandExecutionException("TypeError: InvalidArgumentValue - toFloat() cannot convert " + args[0].getClass().getSimpleName());
    }
  }

  /**
   * toBoolean() function - converts a value to a boolean.
   * Numbers: 0 is false, non-zero is true.
   * Strings: "true" is true, "false" is false (case-insensitive).
   */
  private static class ToBooleanFunction implements StatelessFunction {
    @Override
    public String getName() {
      return "toBoolean";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1)
        throw new CommandExecutionException("toBoolean() requires exactly one argument");
      if (args[0] == null)
        return null;
      if (args[0] instanceof Boolean)
        return args[0];
      if (args[0] instanceof String) {
        final String str = ((String) args[0]).toLowerCase();
        if ("true".equals(str))
          return Boolean.TRUE;
        else if ("false".equals(str))
          return Boolean.FALSE;
        return null;
      }
      throw new CommandExecutionException("TypeError: InvalidArgumentValue - toBoolean() cannot convert " + args[0].getClass().getSimpleName());
    }
  }

  /**
   * collect() aggregation function - collects values into a list.
   * Example: MATCH (n:Person) RETURN collect(n.name)
   */
  private static class CollectFunction implements StatelessFunction {
    private final List<Object> collectedValues = new ArrayList<>();

    @Override
    public String getName() {
      return "collect";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("collect() requires exactly one argument");
      }
      // Collect the value (skip nulls per OpenCypher spec)
      if (args[0] != null)
        collectedValues.add(args[0]);
      return null; // Intermediate result doesn't matter
    }

    @Override
    public boolean aggregateResults() {
      return true;
    }

    @Override
    public Object getAggregatedResult() {
      return new ArrayList<>(collectedValues);
    }
  }

  /**
   * collect(DISTINCT ...) aggregation function - collects unique values into a list.
   * Uses LinkedHashSet to maintain insertion order while eliminating duplicates.
   * Example: MATCH (n:Person) RETURN collect(DISTINCT n.name)
   */
  private static class CollectDistinctFunction implements StatelessFunction {
    private final Set<Object> distinctValues = new LinkedHashSet<>();

    @Override
    public String getName() {
      return "collect";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("collect(DISTINCT ...) requires exactly one argument");
      }
      final Object value = args[0];
      // Only add non-null values, and use identity for Identifiable objects
      if (value != null) {
        if (value instanceof Identifiable) {
          // Use RID as the key for deduplication to handle proxies and loaded records
          distinctValues.add(new IdentifiableWrapper((Identifiable) value));
        } else {
          distinctValues.add(value);
        }
      }
      return null; // Intermediate result doesn't matter
    }

    @Override
    public boolean aggregateResults() {
      return true;
    }

    @Override
    public Object getAggregatedResult() {
      // Unwrap IdentifiableWrapper back to the original objects
      final List<Object> result = new ArrayList<>(distinctValues.size());
      for (final Object value : distinctValues) {
        if (value instanceof IdentifiableWrapper) {
          result.add(((IdentifiableWrapper) value).getIdentifiable());
        } else {
          result.add(value);
        }
      }
      return result;
    }
  }

  /**
   * Wrapper for Identifiable objects that uses RID for equals/hashCode.
   * This ensures proper deduplication even when the same record is loaded multiple times.
   */
  private static class IdentifiableWrapper {
    private final Identifiable identifiable;

    IdentifiableWrapper(final Identifiable identifiable) {
      this.identifiable = identifiable;
    }

    Identifiable getIdentifiable() {
      return identifiable;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj)
        return true;
      if (obj == null || getClass() != obj.getClass())
        return false;
      final IdentifiableWrapper other = (IdentifiableWrapper) obj;
      return identifiable.getIdentity().equals(other.identifiable.getIdentity());
    }

    @Override
    public int hashCode() {
      return identifiable.getIdentity().hashCode();
    }
  }

  // ======================== Temporal Functions ========================

  @SuppressWarnings("unchecked")
  private static class DateConstructorFunction implements StatelessFunction {
    @Override public String getName() { return "date"; }
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length == 0)
        return CypherDate.now();
      if (args[0] instanceof String)
        return CypherDate.parse((String) args[0]);
      if (args[0] instanceof Map)
        return CypherDate.fromMap((Map<String, Object>) args[0]);
      if (args[0] instanceof CypherDate)
        return args[0];
      if (args[0] instanceof CypherLocalDateTime)
        return new CypherDate(((CypherLocalDateTime) args[0]).getValue().toLocalDate());
      if (args[0] instanceof CypherDateTime)
        return new CypherDate(((CypherDateTime) args[0]).getValue().toLocalDate());
      if (args[0] instanceof LocalDate)
        return new CypherDate((LocalDate) args[0]);
      if (args[0] instanceof LocalDateTime)
        return new CypherDate(((LocalDateTime) args[0]).toLocalDate());
      if (args[0] instanceof CypherTemporalValue) {
        // Generic temporal → extract date part
        final CypherTemporalValue tv = (CypherTemporalValue) args[0];
        final Object year = tv.getTemporalProperty("year");
        if (year != null)
          return new CypherDate(LocalDate.of(((Number) year).intValue(),
              ((Number) tv.getTemporalProperty("month")).intValue(),
              ((Number) tv.getTemporalProperty("day")).intValue()));
      }
      throw new CommandExecutionException("date() expects a string, map, or temporal argument");
    }
  }

  @SuppressWarnings("unchecked")
  private static class LocalTimeConstructorFunction implements StatelessFunction {
    @Override public String getName() { return "localtime"; }
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length == 0)
        return CypherLocalTime.now();
      if (args[0] instanceof String)
        return CypherLocalTime.parse((String) args[0]);
      if (args[0] instanceof Map)
        return CypherLocalTime.fromMap((Map<String, Object>) args[0]);
      if (args[0] instanceof CypherLocalTime)
        return args[0];
      if (args[0] instanceof CypherTime)
        return new CypherLocalTime(((CypherTime) args[0]).getValue().toLocalTime());
      if (args[0] instanceof CypherLocalDateTime)
        return new CypherLocalTime(((CypherLocalDateTime) args[0]).getValue().toLocalTime());
      if (args[0] instanceof CypherDateTime)
        return new CypherLocalTime(((CypherDateTime) args[0]).getValue().toLocalTime());
      if (args[0] instanceof LocalDateTime)
        return new CypherLocalTime(((LocalDateTime) args[0]).toLocalTime());
      throw new CommandExecutionException("localtime() expects a string, map, or temporal argument");
    }
  }

  @SuppressWarnings("unchecked")
  private static class TimeConstructorFunction implements StatelessFunction {
    @Override public String getName() { return "time"; }
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length == 0)
        return CypherTime.now();
      if (args[0] instanceof String)
        return CypherTime.parse((String) args[0]);
      if (args[0] instanceof Map)
        return CypherTime.fromMap((Map<String, Object>) args[0]);
      if (args[0] instanceof CypherTime)
        return args[0];
      if (args[0] instanceof CypherDateTime)
        return new CypherTime(((CypherDateTime) args[0]).getValue().toOffsetDateTime().toOffsetTime());
      if (args[0] instanceof CypherLocalTime)
        return new CypherTime(((CypherLocalTime) args[0]).getValue().atOffset(ZoneOffset.UTC));
      if (args[0] instanceof CypherLocalDateTime)
        return new CypherTime(((CypherLocalDateTime) args[0]).getValue().toLocalTime().atOffset(ZoneOffset.UTC));
      throw new CommandExecutionException("time() expects a string, map, or temporal argument");
    }
  }

  @SuppressWarnings("unchecked")
  private static class LocalDateTimeConstructorFunction implements StatelessFunction {
    @Override public String getName() { return "localdatetime"; }
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length == 0)
        return CypherLocalDateTime.now();
      if (args[0] instanceof String)
        return CypherLocalDateTime.parse((String) args[0]);
      if (args[0] instanceof Map)
        return CypherLocalDateTime.fromMap((Map<String, Object>) args[0]);
      if (args[0] instanceof CypherLocalDateTime)
        return args[0];
      if (args[0] instanceof CypherDateTime)
        return new CypherLocalDateTime(((CypherDateTime) args[0]).getValue().toLocalDateTime());
      if (args[0] instanceof CypherDate)
        return new CypherLocalDateTime(((CypherDate) args[0]).getValue().atStartOfDay());
      if (args[0] instanceof LocalDateTime)
        return new CypherLocalDateTime((LocalDateTime) args[0]);
      if (args[0] instanceof LocalDate)
        return new CypherLocalDateTime(((LocalDate) args[0]).atStartOfDay());
      if (args[0] instanceof CypherTime)
        return new CypherLocalDateTime(LocalDateTime.of(LocalDate.now(), ((CypherTime) args[0]).getValue().toLocalTime()));
      throw new CommandExecutionException("localdatetime() expects a string, map, or temporal argument");
    }
  }

  @SuppressWarnings("unchecked")
  private static class DateTimeConstructorFunction implements StatelessFunction {
    @Override public String getName() { return "datetime"; }
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length == 0)
        return CypherDateTime.now();
      if (args[0] instanceof String)
        return CypherDateTime.parse((String) args[0]);
      if (args[0] instanceof Map)
        return CypherDateTime.fromMap((Map<String, Object>) args[0]);
      if (args[0] instanceof CypherDateTime)
        return args[0];
      if (args[0] instanceof CypherLocalDateTime)
        return new CypherDateTime(((CypherLocalDateTime) args[0]).getValue().atZone(ZoneOffset.UTC));
      if (args[0] instanceof CypherDate)
        return new CypherDateTime(((CypherDate) args[0]).getValue().atStartOfDay(ZoneOffset.UTC));
      if (args[0] instanceof LocalDateTime)
        return new CypherDateTime(((LocalDateTime) args[0]).atZone(ZoneOffset.UTC));
      if (args[0] instanceof LocalDate)
        return new CypherDateTime(((LocalDate) args[0]).atStartOfDay(ZoneOffset.UTC));
      throw new CommandExecutionException("datetime() expects a string, map, or temporal argument");
    }
  }

  @SuppressWarnings("unchecked")
  private static class DurationConstructorFunction implements StatelessFunction {
    @Override public String getName() { return "duration"; }
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1)
        throw new CommandExecutionException("duration() requires exactly one argument");
      if (args[0] instanceof String)
        return CypherDuration.parse((String) args[0]);
      if (args[0] instanceof Map)
        return CypherDuration.fromMap((Map<String, Object>) args[0]);
      if (args[0] instanceof CypherDuration)
        return args[0];
      throw new CommandExecutionException("duration() expects a string or map argument");
    }
  }

  // Truncation functions

  private static class DateTruncateFunction implements StatelessFunction {
    @Override public String getName() { return "date.truncate"; }
    @SuppressWarnings("unchecked")
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length < 2)
        throw new CommandExecutionException("date.truncate() requires at least 2 arguments: unit, temporal");
      final String unit = args[0].toString();
      final LocalDate date;
      if (args[1] instanceof CypherDate)
        date = ((CypherDate) args[1]).getValue();
      else if (args[1] instanceof CypherLocalDateTime)
        date = ((CypherLocalDateTime) args[1]).getValue().toLocalDate();
      else if (args[1] instanceof CypherDateTime)
        date = ((CypherDateTime) args[1]).getValue().toLocalDate();
      else if (args[1] instanceof LocalDate)
        date = (LocalDate) args[1];
      else if (args[1] instanceof LocalDateTime)
        date = ((LocalDateTime) args[1]).toLocalDate();
      else
        throw new CommandExecutionException("date.truncate() second argument must be a temporal value with a date");
      LocalDate truncated = TemporalUtil.truncateDate(date, unit);
      // Apply optional map adjustment
      if (args.length >= 3 && args[2] instanceof Map) {
        truncated = applyDateMap(truncated, (Map<String, Object>) args[2]);
      }
      return new CypherDate(truncated);
    }
  }

  private static class LocalTimeTruncateFunction implements StatelessFunction {
    @Override public String getName() { return "localtime.truncate"; }
    @SuppressWarnings("unchecked")
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length < 2)
        throw new CommandExecutionException("localtime.truncate() requires at least 2 arguments");
      final String unit = args[0].toString();
      final LocalTime time;
      if (args[1] instanceof CypherLocalTime)
        time = ((CypherLocalTime) args[1]).getValue();
      else if (args[1] instanceof CypherTime)
        time = ((CypherTime) args[1]).getValue().toLocalTime();
      else if (args[1] instanceof CypherLocalDateTime)
        time = ((CypherLocalDateTime) args[1]).getValue().toLocalTime();
      else if (args[1] instanceof CypherDateTime)
        time = ((CypherDateTime) args[1]).getValue().toLocalTime();
      else
        throw new CommandExecutionException("localtime.truncate() second argument must be a temporal value with a time");
      LocalTime truncated = TemporalUtil.truncateLocalTime(time, unit);
      if (args.length >= 3 && args[2] instanceof Map)
        truncated = applyTimeMap(truncated, (Map<String, Object>) args[2]);
      return new CypherLocalTime(truncated);
    }
  }

  private static class TimeTruncateFunction implements StatelessFunction {
    @Override public String getName() { return "time.truncate"; }
    @SuppressWarnings("unchecked")
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length < 2)
        throw new CommandExecutionException("time.truncate() requires at least 2 arguments");
      final String unit = args[0].toString();
      final OffsetTime time;
      if (args[1] instanceof CypherTime)
        time = ((CypherTime) args[1]).getValue();
      else if (args[1] instanceof CypherDateTime)
        time = ((CypherDateTime) args[1]).getValue().toOffsetDateTime().toOffsetTime();
      else if (args[1] instanceof CypherLocalTime)
        time = ((CypherLocalTime) args[1]).getValue().atOffset(ZoneOffset.UTC);
      else if (args[1] instanceof CypherLocalDateTime)
        time = ((CypherLocalDateTime) args[1]).getValue().toLocalTime().atOffset(ZoneOffset.UTC);
      else
        throw new CommandExecutionException("time.truncate() second argument must be a temporal value with a time");
      LocalTime truncated = TemporalUtil.truncateLocalTime(time.toLocalTime(), unit);
      if (args.length >= 3 && args[2] instanceof Map)
        truncated = applyTimeMap(truncated, (Map<String, Object>) args[2]);
      return new CypherTime(OffsetTime.of(truncated, time.getOffset()));
    }
  }

  private static class LocalDateTimeTruncateFunction implements StatelessFunction {
    @Override public String getName() { return "localdatetime.truncate"; }
    @SuppressWarnings("unchecked")
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length < 2)
        throw new CommandExecutionException("localdatetime.truncate() requires at least 2 arguments");
      final String unit = args[0].toString();
      final LocalDateTime dt;
      if (args[1] instanceof CypherLocalDateTime)
        dt = ((CypherLocalDateTime) args[1]).getValue();
      else if (args[1] instanceof CypherDateTime)
        dt = ((CypherDateTime) args[1]).getValue().toLocalDateTime();
      else if (args[1] instanceof CypherDate)
        dt = ((CypherDate) args[1]).getValue().atStartOfDay();
      else if (args[1] instanceof LocalDateTime)
        dt = (LocalDateTime) args[1];
      else if (args[1] instanceof LocalDate)
        dt = ((LocalDate) args[1]).atStartOfDay();
      else
        throw new CommandExecutionException("localdatetime.truncate() second argument must be a temporal value");
      LocalDateTime truncated = TemporalUtil.truncateLocalDateTime(dt, unit);
      if (args.length >= 3 && args[2] instanceof Map)
        truncated = applyDateTimeMap(truncated, (Map<String, Object>) args[2]);
      return new CypherLocalDateTime(truncated);
    }
  }

  private static class DateTimeTruncateFunction implements StatelessFunction {
    @Override public String getName() { return "datetime.truncate"; }
    @SuppressWarnings("unchecked")
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length < 2)
        throw new CommandExecutionException("datetime.truncate() requires at least 2 arguments");
      final String unit = args[0].toString();
      final ZonedDateTime dt;
      if (args[1] instanceof CypherDateTime)
        dt = ((CypherDateTime) args[1]).getValue();
      else if (args[1] instanceof CypherLocalDateTime)
        dt = ((CypherLocalDateTime) args[1]).getValue().atZone(ZoneOffset.UTC);
      else if (args[1] instanceof CypherDate)
        dt = ((CypherDate) args[1]).getValue().atStartOfDay(ZoneOffset.UTC);
      else if (args[1] instanceof LocalDateTime)
        dt = ((LocalDateTime) args[1]).atZone(ZoneOffset.UTC);
      else if (args[1] instanceof LocalDate)
        dt = ((LocalDate) args[1]).atStartOfDay(ZoneOffset.UTC);
      else
        throw new CommandExecutionException("datetime.truncate() second argument must be a temporal value");
      LocalDateTime truncated = TemporalUtil.truncateLocalDateTime(dt.toLocalDateTime(), unit);
      if (args.length >= 3 && args[2] instanceof Map)
        truncated = applyDateTimeMap(truncated, (Map<String, Object>) args[2]);
      return new CypherDateTime(ZonedDateTime.of(truncated, dt.getZone()));
    }
  }

  // Epoch functions

  private static class DateTimeFromEpochFunction implements StatelessFunction {
    @Override public String getName() { return "datetime.fromepoch"; }
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 2)
        throw new CommandExecutionException("datetime.fromepoch() requires 2 arguments: seconds, nanoseconds");
      final long seconds = ((Number) args[0]).longValue();
      final long nanos = ((Number) args[1]).longValue();
      return CypherDateTime.fromEpoch(seconds, nanos);
    }
  }

  private static class DateTimeFromEpochMillisFunction implements StatelessFunction {
    @Override public String getName() { return "datetime.fromepochmillis"; }
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1)
        throw new CommandExecutionException("datetime.fromepochmillis() requires 1 argument: milliseconds");
      final long millis = ((Number) args[0]).longValue();
      return CypherDateTime.fromEpochMillis(millis);
    }
  }

  // Duration calculation functions

  private static class DurationBetweenFunction implements StatelessFunction {
    @Override public String getName() { return "duration.between"; }
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 2)
        throw new CommandExecutionException("duration.between() requires 2 arguments");
      if (args[0] == null || args[1] == null)
        return null;
      return TemporalUtil.durationBetween(wrapTemporal(args[0]), wrapTemporal(args[1]));
    }
  }

  private static class DurationInMonthsFunction implements StatelessFunction {
    @Override public String getName() { return "duration.inMonths"; }
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 2)
        throw new CommandExecutionException("duration.inMonths() requires 2 arguments");
      if (args[0] == null || args[1] == null)
        return null;
      return TemporalUtil.durationInMonths(wrapTemporal(args[0]), wrapTemporal(args[1]));
    }
  }

  private static class DurationInDaysFunction implements StatelessFunction {
    @Override public String getName() { return "duration.inDays"; }
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 2)
        throw new CommandExecutionException("duration.inDays() requires 2 arguments");
      if (args[0] == null || args[1] == null)
        return null;
      return TemporalUtil.durationInDays(wrapTemporal(args[0]), wrapTemporal(args[1]));
    }
  }

  private static class DurationInSecondsFunction implements StatelessFunction {
    @Override public String getName() { return "duration.inSeconds"; }
    @Override public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 2)
        throw new CommandExecutionException("duration.inSeconds() requires 2 arguments");
      if (args[0] == null || args[1] == null)
        return null;
      return TemporalUtil.durationInSeconds(wrapTemporal(args[0]), wrapTemporal(args[1]));
    }
  }

  /**
   * Wrap a java.time value from ArcadeDB storage into the corresponding CypherTemporalValue.
   */
  private static CypherTemporalValue wrapTemporal(final Object val) {
    if (val instanceof CypherTemporalValue)
      return (CypherTemporalValue) val;
    if (val instanceof LocalDate)
      return new CypherDate((LocalDate) val);
    if (val instanceof LocalDateTime)
      return new CypherLocalDateTime((LocalDateTime) val);
    if (val instanceof java.time.ZonedDateTime)
      return new CypherDateTime((java.time.ZonedDateTime) val);
    throw new CommandExecutionException("Expected temporal value but got: " + (val == null ? "null" : val.getClass().getSimpleName()));
  }

  /**
   * Apply a map of adjustments to a truncated date.
   * The map can contain: year, month, day, dayOfWeek, ordinalDay, dayOfQuarter.
   */
  private static LocalDate applyDateMap(LocalDate date, final Map<String, Object> map) {
    if (map == null || map.isEmpty())
      return date;
    if (map.containsKey("year"))
      date = date.withYear(((Number) map.get("year")).intValue());
    if (map.containsKey("month"))
      date = date.withMonth(((Number) map.get("month")).intValue());
    if (map.containsKey("day"))
      date = date.withDayOfMonth(((Number) map.get("day")).intValue());
    if (map.containsKey("dayOfWeek"))
      date = date.with(java.time.temporal.WeekFields.ISO.dayOfWeek(), ((Number) map.get("dayOfWeek")).longValue());
    return date;
  }

  /**
   * Apply a map of adjustments to a truncated time.
   */
  private static LocalTime applyTimeMap(LocalTime time, final Map<String, Object> map) {
    if (map == null || map.isEmpty())
      return time;
    if (map.containsKey("hour"))
      time = time.withHour(((Number) map.get("hour")).intValue());
    if (map.containsKey("minute"))
      time = time.withMinute(((Number) map.get("minute")).intValue());
    if (map.containsKey("second"))
      time = time.withSecond(((Number) map.get("second")).intValue());
    if (map.containsKey("nanosecond"))
      time = time.withNano(((Number) map.get("nanosecond")).intValue());
    else if (map.containsKey("microsecond"))
      time = time.withNano(((Number) map.get("microsecond")).intValue() * 1000);
    else if (map.containsKey("millisecond"))
      time = time.withNano(((Number) map.get("millisecond")).intValue() * 1_000_000);
    return time;
  }

  /**
   * Apply a map of adjustments to a truncated local datetime.
   */
  private static LocalDateTime applyDateTimeMap(LocalDateTime dt, final Map<String, Object> map) {
    if (map == null || map.isEmpty())
      return dt;
    final LocalDate date = applyDateMap(dt.toLocalDate(), map);
    final LocalTime time = applyTimeMap(dt.toLocalTime(), map);
    return LocalDateTime.of(date, time);
  }

  /**
   * Bridge from Cypher function to SQL function.
   * For aggregation functions, we configure them to enable proper state accumulation.
   */
  private static class SQLFunctionBridge implements StatelessFunction {
    private final SQLFunction sqlFunction;
    private final String cypherFunctionName;
    private final boolean isAggregation;

    SQLFunctionBridge(final SQLFunction sqlFunction, final String cypherFunctionName) {
      this.sqlFunction = sqlFunction;
      this.cypherFunctionName = cypherFunctionName;

      // Configure the function to enable aggregation mode
      // SQL aggregation functions check configuredParameters in aggregateResults()
      // We configure with a dummy parameter since the actual values come through execute() args
      sqlFunction.config(new Object[]{"dummy"});

      // Cache whether this is an aggregation function
      this.isAggregation = sqlFunction.aggregateResults();
    }

    @Override
    public String getName() {
      return cypherFunctionName;
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      // SQL functions expect (self, currentRecord, currentResult, params, context)
      // For now, we pass nulls for self, currentRecord, currentResult
      return sqlFunction.execute(null, null, null, args, context);
    }

    @Override
    public boolean aggregateResults() {
      return isAggregation;
    }

    @Override
    public Object getAggregatedResult() {
      return sqlFunction.getResult();
    }
  }

  /**
   * Wrapper that applies DISTINCT semantics to an aggregation function.
   * Tracks seen argument values and only delegates to the wrapped function for unique values.
   */
  private static class DistinctAggregationWrapper implements StatelessFunction {
    private final StatelessFunction delegate;
    private final Set<List<Object>> seenValues = new HashSet<>();

    DistinctAggregationWrapper(final StatelessFunction delegate) {
      this.delegate = delegate;
    }

    @Override
    public String getName() {
      return delegate.getName();
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (!seenValues.add(java.util.Arrays.asList(args)))
        return null;
      return delegate.execute(args, context);
    }

    @Override
    public boolean aggregateResults() {
      return true;
    }

    @Override
    public Object getAggregatedResult() {
      return delegate.getAggregatedResult();
    }
  }
}
