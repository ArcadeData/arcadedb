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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.query.opencypher.ast.CallClause;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.functions.CypherFunction;
import com.arcadedb.query.opencypher.functions.CypherFunctionRegistry;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.*;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Execution step for CALL clause.
 * Invokes ArcadeDB SQL functions and procedures.
 * <p>
 * Syntax:
 * <pre>
 * CALL functionName(arg1, arg2, ...)
 * CALL functionName() YIELD result
 * </pre>
 * <p>
 * Maps Cypher procedure names to ArcadeDB SQL functions:
 * - db.labels() -> returns all type names
 * - db.relationshipTypes() -> returns all edge type names
 * - db.propertyKeys() -> returns all property keys
 * - Any SQL function can be called directly
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CallStep extends AbstractExecutionStep {
  private final CallClause callClause;
  private final CypherFunctionFactory functionFactory;
  private final ExpressionEvaluator evaluator;

  public CallStep(final CallClause callClause, final CommandContext context,
                  final CypherFunctionFactory functionFactory) {
    super(context);
    this.callClause = callClause;
    this.functionFactory = functionFactory;
    this.evaluator = new ExpressionEvaluator(functionFactory);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    // Check if this is a chained CALL with a previous step
    if (prev != null) {
      return executeChainedCall(context, nRecords);
    }

    // Standalone CALL - execute once
    final Object result = executeCall(context);

    // Convert result to ResultSet
    return convertToResultSet(result, context);
  }

  /**
   * Executes the CALL for each row from the previous step (chained CALL).
   * This enables patterns like:
   * <pre>
   * UNWIND $batch AS row
   * MATCH (a), (b) WHERE elementId(a) = row.source_id AND elementId(b) = row.target_id
   * CALL merge.relationship(a, row.rel_type, {}, row.props, b) YIELD rel
   * RETURN elementId(rel) as id
   * </pre>
   */
  private ResultSet executeChainedCall(final CommandContext context, final int nRecords) {
    final List<ResultInternal> results = new ArrayList<>();

    // Pull rows from previous step
    final ResultSet prevResults = prev.syncPull(context, nRecords);

    while (prevResults.hasNext()) {
      final Result inputRow = prevResults.next();

      // Execute the call with this row's context
      final Object callResult = executeCall(context, inputRow);

      // Convert and add results, merging with input row properties
      if (callResult == null) {
        // OPTIONAL CALL returned null - pass through input row
        if (callClause.isOptional()) {
          results.add(mergeWithInputRow(inputRow, null));
        }
        continue;
      }

      if (callResult instanceof Collection) {
        for (final Object item : (Collection<?>) callResult) {
          results.add(mergeWithInputRow(inputRow, convertItemToResult(item)));
        }
      } else if (callResult instanceof Iterator) {
        final Iterator<?> iter = (Iterator<?>) callResult;
        while (iter.hasNext()) {
          results.add(mergeWithInputRow(inputRow, convertItemToResult(iter.next())));
        }
      } else {
        results.add(mergeWithInputRow(inputRow, convertItemToResult(callResult)));
      }
    }

    // Apply YIELD filtering if specified
    if (callClause.hasYield() && !callClause.isYieldAll()) {
      return applyYieldFiltering(results);
    }

    return new IteratorResultSet(results.iterator());
  }

  /**
   * Merges the call result with the input row properties.
   * The input row properties are preserved, and call result properties are added.
   */
  private ResultInternal mergeWithInputRow(final Result inputRow, final ResultInternal callResult) {
    final ResultInternal merged = new ResultInternal();

    // Copy input row properties
    if (inputRow != null) {
      for (final String prop : inputRow.getPropertyNames()) {
        merged.setProperty(prop, inputRow.getProperty(prop));
      }
    }

    // Add call result properties
    if (callResult != null) {
      for (final String prop : callResult.getPropertyNames()) {
        merged.setProperty(prop, callResult.getProperty(prop));
      }
    }

    return merged;
  }

  /**
   * Executes the CALL and returns the raw result.
   */
  private Object executeCall(final CommandContext context) {
    return executeCall(context, null);
  }

  /**
   * Executes the CALL with optional input row context.
   */
  private Object executeCall(final CommandContext context, final Result inputRow) {
    final String procedureName = callClause.getProcedureName();
    final String simpleName = callClause.getSimpleName();
    final String namespace = callClause.getNamespace();

    // Evaluate arguments (with input row context for variable resolution)
    final List<Object> args = new ArrayList<>();
    for (final Expression argExpr : callClause.getArguments()) {
      args.add(evaluator.evaluate(argExpr, inputRow, context));
    }

    // Handle built-in Cypher procedures
    switch (procedureName.toLowerCase()) {
      case "db.labels":
        return getLabels(context);
      case "db.relationshiptypes":
        return getRelationshipTypes(context);
      case "db.propertykeys":
        return getPropertyKeys(context);
      case "db.schema":
      case "db.schema.visualization":
        return getSchemaVisualization(context);
      default:
        // Check the procedure registry first
        final CypherProcedure procedure = CypherProcedureRegistry.get(procedureName);
        if (procedure != null) {
          return executeProcedure(procedure, args.toArray(), inputRow, context);
        }

        // Check the function registry
        final CypherFunction function = CypherFunctionRegistry.get(procedureName);
        if (function != null) {
          return executeFunction(function, args.toArray(), context);
        }

        // Try to call as custom function (DEFINE FUNCTION) first
        if (!namespace.isEmpty()) {
          final Object customResult = callCustomFunction(namespace, simpleName, args, context);
          if (customResult != null)
            return customResult;
        }

        // Fall back to ArcadeDB SQL function
        return callSQLFunction(simpleName, args, context);
    }
  }

  /**
   * Executes a registered procedure.
   * Returns an Iterator for lazy evaluation to avoid materializing large result sets into memory.
   */
  private Object executeProcedure(final CypherProcedure procedure, final Object[] args,
                                   final Result inputRow, final CommandContext context) {
    try {
      procedure.validateArgs(args);
      // Return iterator for lazy evaluation instead of collecting to list
      // This prevents memory exhaustion for procedures that yield many results
      return procedure.execute(args, inputRow, context)
          .map(this::convertProcedureResultToInternal)
          .iterator();
    } catch (final IllegalArgumentException e) {
      if (callClause.isOptional())
        return null;
      throw new CommandExecutionException("Error executing procedure: " + procedure.getName(), e);
    } catch (final Exception e) {
      if (callClause.isOptional())
        return null;
      throw new CommandExecutionException("Error executing procedure: " + procedure.getName(), e);
    }
  }

  /**
   * Converts a procedure Result to ResultInternal for consistency.
   */
  private ResultInternal convertProcedureResultToInternal(final Result result) {
    if (result instanceof ResultInternal) {
      return (ResultInternal) result;
    }
    final ResultInternal internal = new ResultInternal();
    for (final String prop : result.getPropertyNames()) {
      internal.setProperty(prop, result.getProperty(prop));
    }
    return internal;
  }

  /**
   * Executes a registered function.
   */
  private Object executeFunction(final CypherFunction function, final Object[] args,
                                  final CommandContext context) {
    try {
      function.validateArgs(args);
      return function.execute(args, context);
    } catch (final IllegalArgumentException e) {
      if (callClause.isOptional())
        return null;
      throw new CommandExecutionException("Error executing function: " + function.getName(), e);
    } catch (final Exception e) {
      if (callClause.isOptional())
        return null;
      throw new CommandExecutionException("Error executing function: " + function.getName(), e);
    }
  }

  /**
   * Calls a custom function defined via DEFINE FUNCTION.
   * Custom functions are stored in the schema's function library.
   *
   * @return the function result, or null if function not found
   */
  private Object callCustomFunction(final String libraryName, final String functionName,
                                     final List<Object> args, final CommandContext context) {
    try {
      // Check if the library exists
      if (!context.getDatabase().getSchema().hasFunctionLibrary(libraryName))
        return null;

      // Get the function from the library
      final FunctionDefinition function =
          context.getDatabase().getSchema().getFunction(libraryName, functionName);
      if (function == null)
        return null;

      // Execute the function with arguments
      return function.execute(args.toArray());
    } catch (final Exception e) {
      if (callClause.isOptional())
        return null;
      throw new CommandExecutionException("Error executing custom function: " + libraryName + "." + functionName, e);
    }
  }

  /**
   * Returns all vertex type names.
   */
  private List<Map<String, Object>> getLabels(final CommandContext context) {
    final List<Map<String, Object>> results = new ArrayList<>();
    for (final DocumentType type : context.getDatabase().getSchema().getTypes()) {
      if (type instanceof VertexType) {
        results.add(Map.of("label", type.getName()));
      }
    }
    return results;
  }

  /**
   * Returns all edge type names.
   */
  private List<Map<String, Object>> getRelationshipTypes(final CommandContext context) {
    final List<Map<String, Object>> results = new ArrayList<>();
    for (final DocumentType type : context.getDatabase().getSchema().getTypes()) {
      if (type instanceof EdgeType) {
        results.add(Map.of("relationshipType", type.getName()));
      }
    }
    return results;
  }

  /**
   * Returns all property keys across all types.
   */
  private List<Map<String, Object>> getPropertyKeys(final CommandContext context) {
    final Set<String> propertyKeys = new HashSet<>();
    for (final DocumentType type : context.getDatabase().getSchema().getTypes()) {
      for (final String propName : type.getPropertyNames()) {
        propertyKeys.add(propName);
      }
    }
    final List<Map<String, Object>> results = new ArrayList<>();
    for (final String key : propertyKeys) {
      results.add(Map.of("propertyKey", key));
    }
    return results;
  }

  /**
   * Returns schema visualization data.
   */
  private List<Map<String, Object>> getSchemaVisualization(final CommandContext context) {
    final List<Map<String, Object>> results = new ArrayList<>();
    for (final DocumentType type : context.getDatabase().getSchema().getTypes()) {
      final Map<String, Object> typeInfo = new HashMap<>();
      typeInfo.put("name", type.getName());
      if (type instanceof VertexType) {
        typeInfo.put("type", "node");
      } else if (type instanceof EdgeType) {
        typeInfo.put("type", "relationship");
      } else {
        typeInfo.put("type", "document");
      }
      typeInfo.put("properties", new ArrayList<>(type.getPropertyNames()));
      results.add(typeInfo);
    }
    return results;
  }

  /**
   * Calls an ArcadeDB SQL function.
   */
  private Object callSQLFunction(final String functionName, final List<Object> args,
                                  final CommandContext context) {
    // Try to get the function from factory
    if (!functionFactory.getSQLFunctionFactory().hasFunction(functionName)) {
      if (callClause.isOptional())
        return null;
      throw new CommandExecutionException("Unknown procedure/function: " + callClause.getProcedureName());
    }
    final SQLFunction function = functionFactory.getSQLFunctionFactory().getFunctionInstance(functionName);
    if (function == null) {
      if (callClause.isOptional())
        return null;
      throw new CommandExecutionException("Unknown procedure/function: " + callClause.getProcedureName());
    }

    // Execute the function
    return function.execute(null, null, null, args.toArray(), context);
  }

  /**
   * Converts the call result to a ResultSet.
   */
  private ResultSet convertToResultSet(final Object result, final CommandContext context) {
    final List<ResultInternal> results = new ArrayList<>();

    if (result == null) {
      // OPTIONAL CALL returned null - return empty result
      return createEmptyResultSet();
    }

    if (result instanceof Collection) {
      // Multiple results
      for (final Object item : (Collection<?>) result) {
        results.add(convertItemToResult(item));
      }
    } else if (result instanceof Iterator) {
      // Iterator of results
      final Iterator<?> iter = (Iterator<?>) result;
      while (iter.hasNext()) {
        results.add(convertItemToResult(iter.next()));
      }
    } else if (result instanceof ResultSet) {
      // Already a ResultSet
      return (ResultSet) result;
    } else {
      // Single result
      results.add(convertItemToResult(result));
    }

    // Apply YIELD filtering if specified
    if (callClause.hasYield() && !callClause.isYieldAll()) {
      return applyYieldFiltering(results);
    }

    return new IteratorResultSet(results.iterator());
  }

  /**
   * Converts a single item to a Result.
   */
  @SuppressWarnings("unchecked")
  private ResultInternal convertItemToResult(final Object item) {
    final ResultInternal result = new ResultInternal();

    if (item instanceof Map) {
      // Map - copy all entries
      for (final Map.Entry<?, ?> entry : ((Map<?, ?>) item).entrySet()) {
        result.setProperty(String.valueOf(entry.getKey()), entry.getValue());
      }
    } else if (item instanceof Document) {
      // Document - copy all properties
      final Document doc = (Document) item;
      for (final String prop : doc.getPropertyNames()) {
        result.setProperty(prop, doc.get(prop));
      }
    } else if (item instanceof Identifiable) {
      // RID or record
      result.setProperty("value", item);
    } else {
      // Scalar value
      result.setProperty("value", item);
    }

    return result;
  }

  /**
   * Applies YIELD field filtering to results.
   */
  private ResultSet applyYieldFiltering(final List<ResultInternal> results) {
    final List<ResultInternal> filteredResults = new ArrayList<>();

    for (final ResultInternal input : results) {
      final ResultInternal output = new ResultInternal();

      for (final CallClause.YieldItem yieldItem : callClause.getYieldItems()) {
        final Object value = input.getProperty(yieldItem.getFieldName());
        output.setProperty(yieldItem.getOutputName(), value);
      }

      // Apply YIELD WHERE if present
      if (callClause.getYieldWhere() != null) {
        final boolean matches = callClause.getYieldWhere().getConditionExpression().evaluate(output, context);
        if (!matches)
          continue;
      }

      filteredResults.add(output);
    }

    return new IteratorResultSet(filteredResults.iterator());
  }

  private ResultSet createEmptyResultSet() {
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Result next() {
        throw new NoSuchElementException();
      }

      @Override
      public void close() {
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    builder.append(ind);
    builder.append("+ CALL ").append(callClause.getProcedureName());

    if (!callClause.getArguments().isEmpty())
      builder.append("(").append(callClause.getArguments().size()).append(" args)");
    else
      builder.append("()");

    if (callClause.hasYield()) {
      builder.append(" YIELD ");
      if (callClause.isYieldAll())
        builder.append("*");
      else
        builder.append(callClause.getYieldItems().size()).append(" fields");
    }

    if (context.isProfiling())
      builder.append(" (").append(getCostFormatted()).append(")");

    return builder.toString();
  }
}
