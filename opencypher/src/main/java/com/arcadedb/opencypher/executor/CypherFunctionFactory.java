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
package com.arcadedb.opencypher.executor;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.SQLFunction;
import com.arcadedb.query.sql.function.DefaultSQLFunctionFactory;

import java.util.*;

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
      case "id", "labels", "type", "keys", "properties", "startnode", "endnode", "nodes", "relationships" -> true;
      default -> false;
    };
  }

  /**
   * Create executor for Cypher-specific functions.
   */
  private CypherFunctionExecutor createCypherSpecificExecutor(final String functionName) {
    return switch (functionName) {
      case "id" -> new IdFunction();
      case "labels" -> new LabelsFunction();
      case "type" -> new TypeFunction();
      case "keys" -> new KeysFunction();
      case "properties" -> new PropertiesFunction();
      case "startnode" -> new StartNodeFunction();
      case "endnode" -> new EndNodeFunction();
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
      if (args[0] instanceof com.arcadedb.database.Document) {
        final com.arcadedb.database.Document doc = (com.arcadedb.database.Document) args[0];
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
      if (args[0] instanceof com.arcadedb.database.Document) {
        final com.arcadedb.database.Document doc = (com.arcadedb.database.Document) args[0];
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
        return ((Edge) args[0]).getOut();
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
        return ((Edge) args[0]).getIn();
      }
      return null;
    }

    @Override
    public boolean isAggregation() {
      return false;
    }
  }

  /**
   * Bridge from Cypher function to SQL function.
   */
  private static class SQLFunctionBridge implements CypherFunctionExecutor {
    private final SQLFunction sqlFunction;
    private final String cypherFunctionName;

    SQLFunctionBridge(final SQLFunction sqlFunction, final String cypherFunctionName) {
      this.sqlFunction = sqlFunction;
      this.cypherFunctionName = cypherFunctionName;
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      // SQL functions expect (self, currentRecord, currentResult, params, context)
      // For now, we pass nulls for self, currentRecord, currentResult
      return sqlFunction.execute(null, null, null, args, context);
    }

    @Override
    public boolean isAggregation() {
      return sqlFunction.aggregateResults();
    }

    @Override
    public Object getAggregatedResult() {
      return sqlFunction.getResult();
    }
  }
}
