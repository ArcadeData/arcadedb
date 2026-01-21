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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.SQLFunction;
import com.arcadedb.query.sql.function.DefaultSQLFunctionFactory;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Factory for Cypher functions.
 * Provides a bridge between Cypher function names and ArcadeDB SQL functions.
 * Also implements Cypher-specific functions that don't have SQL equivalents.
 */
public class CypherFunctionFactory {
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
    mapping.put("rand", "randomInt");

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
  public CypherFunctionExecutor getFunctionExecutor(final String cypherFunctionName) {
    final String functionName = cypherFunctionName.toLowerCase();

    // Handle Cypher-specific functions
    if (isCypherSpecificFunction(functionName)) {
      return createCypherSpecificExecutor(functionName);
    }

    // Get SQL function (either mapped or direct)
    final String sqlFunctionName = cypherToSqlMapping.getOrDefault(functionName, functionName);
    if (sqlFunctionFactory.hasFunction(sqlFunctionName)) {
      final SQLFunction sqlFunction = sqlFunctionFactory.getFunctionInstance(sqlFunctionName);
      return new SQLFunctionBridge(sqlFunction, functionName);
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
      // List functions
      case "size", "head", "tail", "last", "range" -> true;
      // String functions
      case "left", "right", "reverse", "split" -> true;
      // Type conversion functions
      case "tostring", "tointeger", "tofloat", "toboolean" -> true;
      // Aggregation functions
      case "collect" -> true;
      default -> false;
    };
  }

  /**
   * Create executor for Cypher-specific functions.
   */
  private CypherFunctionExecutor createCypherSpecificExecutor(final String functionName) {
    return switch (functionName) {
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
      case "collect" -> new CollectFunction();
      default -> throw new CommandExecutionException("Cypher function not implemented: " + functionName);
    };
  }

  // Cypher-specific function implementations

  /**
   * id() function - returns the internal ID of a node or relationship.
   */
  private static class IdFunction implements CypherFunctionExecutor {
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

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * labels() function - returns the labels of a node.
   */
  private static class LabelsFunction implements CypherFunctionExecutor {
    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("labels() requires exactly one argument");
      }
      if (args[0] instanceof Vertex) {
        final Vertex vertex = (Vertex) args[0];
        return List.of(vertex.getTypeName());
      }
      return Collections.emptyList();
    }

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * type() function - returns the type of a relationship.
   */
  private static class TypeFunction implements CypherFunctionExecutor {
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

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * keys() function - returns the property keys of a node or relationship.
   */
  private static class KeysFunction implements CypherFunctionExecutor {
    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("keys() requires exactly one argument");
      }
      if (args[0] instanceof Document) {
        final Document doc = (Document) args[0];
        return new ArrayList<>(doc.getPropertyNames());
      }
      return Collections.emptyList();
    }

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * properties() function - returns all properties as a map.
   */
  private static class PropertiesFunction implements CypherFunctionExecutor {
    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("properties() requires exactly one argument");
      }
      if (args[0] instanceof Document) {
        final Document doc = (Document) args[0];
        final Map<String, Object> props = new HashMap<>();
        for (final String propName : doc.getPropertyNames()) {
          props.put(propName, doc.get(propName));
        }
        return props;
      }
      return Collections.emptyMap();
    }

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * startNode() function - returns the start node of a relationship.
   */
  private static class StartNodeFunction implements CypherFunctionExecutor {
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

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * endNode() function - returns the end node of a relationship.
   */
  private static class EndNodeFunction implements CypherFunctionExecutor {
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

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  // Path Functions

  /**
   * nodes() function - returns all nodes in a path.
   */
  private static class NodesFunction implements CypherFunctionExecutor {
    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("nodes() requires exactly one argument");
      }
      if (args[0] instanceof List) {
        // Path is represented as a list of alternating vertices and edges
        final List<?> path = (List<?>) args[0];
        final List<Vertex> nodes = new ArrayList<>();
        for (final Object element : path) {
          if (element instanceof Vertex) {
            nodes.add((Vertex) element);
          }
        }
        return nodes;
      }
      return Collections.emptyList();
    }

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * relationships() function - returns all relationships in a path.
   */
  private static class RelationshipsFunction implements CypherFunctionExecutor {
    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("relationships() requires exactly one argument");
      }
      if (args[0] instanceof List) {
        // Path is represented as a list of alternating vertices and edges
        final List<?> path = (List<?>) args[0];
        final List<Edge> edges = new ArrayList<>();
        for (final Object element : path) {
          if (element instanceof Edge) {
            edges.add((Edge) element);
          }
        }
        return edges;
      }
      return Collections.emptyList();
    }

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * length() function - returns the length of a path (number of relationships).
   */
  private static class LengthFunction implements CypherFunctionExecutor {
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

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  // List Functions

  /**
   * size() function - returns the size of a list or string.
   */
  private static class SizeFunction implements CypherFunctionExecutor {
    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("size() requires exactly one argument");
      }
      if (args[0] instanceof List) {
        return (long) ((List<?>) args[0]).size();
      } else if (args[0] instanceof String) {
        return (long) ((String) args[0]).length();
      }
      return 0L;
    }

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * head() function - returns the first element of a list.
   */
  private static class HeadFunction implements CypherFunctionExecutor {
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

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * tail() function - returns all elements except the first.
   */
  private static class TailFunction implements CypherFunctionExecutor {
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

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * last() function - returns the last element of a list.
   */
  private static class LastFunction implements CypherFunctionExecutor {
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

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * range() function - creates a list of numbers from start to end (optionally with step).
   */
  private static class RangeFunction implements CypherFunctionExecutor {
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

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  // String Functions

  /**
   * left() function - returns the leftmost n characters of a string.
   */
  private static class LeftFunction implements CypherFunctionExecutor {
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

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * right() function - returns the rightmost n characters of a string.
   */
  private static class RightFunction implements CypherFunctionExecutor {
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

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * reverse() function - reverses a string or list.
   */
  private static class ReverseFunction implements CypherFunctionExecutor {
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

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * split() function - splits a string by a delimiter.
   */
  private static class SplitFunction implements CypherFunctionExecutor {
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

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  // Type Conversion Functions

  /**
   * toString() function - converts a value to a string.
   */
  private static class ToStringFunction implements CypherFunctionExecutor {
    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("toString() requires exactly one argument");
      }
      return args[0] == null ? null : args[0].toString();
    }

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * toInteger() function - converts a value to an integer.
   */
  private static class ToIntegerFunction implements CypherFunctionExecutor {
    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("toInteger() requires exactly one argument");
      }
      if (args[0] == null) {
        return null;
      }
      if (args[0] instanceof Number) {
        return ((Number) args[0]).longValue();
      }
      if (args[0] instanceof String) {
        try {
          return Long.parseLong((String) args[0]);
        } catch (final NumberFormatException e) {
          return null;
        }
      }
      return null;
    }

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * toFloat() function - converts a value to a float.
   */
  private static class ToFloatFunction implements CypherFunctionExecutor {
    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("toFloat() requires exactly one argument");
      }
      if (args[0] == null) {
        return null;
      }
      if (args[0] instanceof Number) {
        return ((Number) args[0]).doubleValue();
      }
      if (args[0] instanceof String) {
        try {
          return Double.parseDouble((String) args[0]);
        } catch (final NumberFormatException e) {
          return null;
        }
      }
      return null;
    }

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * toBoolean() function - converts a value to a boolean.
   * Numbers: 0 is false, non-zero is true.
   * Strings: "true" is true, "false" is false (case-insensitive).
   */
  private static class ToBooleanFunction implements CypherFunctionExecutor {
    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("toBoolean() requires exactly one argument");
      }
      if (args[0] == null) {
        return null;
      }
      if (args[0] instanceof Boolean) {
        return args[0];
      }
      if (args[0] instanceof Number) {
        // In Cypher: 0 is false, non-zero is true
        final double value = ((Number) args[0]).doubleValue();
        return value != 0.0;
      }
      if (args[0] instanceof String) {
        final String str = ((String) args[0]).toLowerCase();
        if ("true".equals(str)) {
          return Boolean.TRUE;
        } else if ("false".equals(str)) {
          return Boolean.FALSE;
        }
      }
      return null;
    }

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * collect() aggregation function - collects values into a list.
   * Example: MATCH (n:Person) RETURN collect(n.name)
   */
  private static class CollectFunction implements CypherFunctionExecutor {
    private final List<Object> collectedValues = new ArrayList<>();

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      if (args.length != 1) {
        throw new CommandExecutionException("collect() requires exactly one argument");
      }
      // Collect the value (including nulls)
      collectedValues.add(args[0]);
      return null; // Intermediate result doesn't matter
    }

    @Override
    public boolean isAggregation() {
      return true;
    }

    @Override
    public Object getAggregatedResult() {
      return new ArrayList<>(collectedValues);
    }
  }

  /**
   * Bridge from Cypher function to SQL function.
   * For aggregation functions, we configure them to enable proper state accumulation.
   */
  private static class SQLFunctionBridge implements CypherFunctionExecutor {
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
    public Object execute(final Object[] args, final CommandContext context) {
      // SQL functions expect (self, currentRecord, currentResult, params, context)
      // For now, we pass nulls for self, currentRecord, currentResult
      return sqlFunction.execute(null, null, null, args, context);
    }

    @Override
    public boolean isAggregation() {
      return isAggregation;
    }

    @Override
    public Object getAggregatedResult() {
      return sqlFunction.getResult();
    }
  }
}
