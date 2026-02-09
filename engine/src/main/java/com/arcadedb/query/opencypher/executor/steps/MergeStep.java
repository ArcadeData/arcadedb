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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.MergeClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Execution step for MERGE clause.
 * Ensures a pattern exists: matches if present, creates if not.
 * <p>
 * Examples:
 * - MERGE (n:Person {name: 'Alice'}) - finds or creates person
 * - MERGE (a)-[r:KNOWS]->(b) - finds or creates relationship
 * <p>
 * MERGE is an "upsert" operation (update or insert).
 * TODO: Support ON CREATE SET and ON MATCH SET sub-clauses
 */
public class MergeStep extends AbstractExecutionStep {
  private final MergeClause mergeClause;
  private final ExpressionEvaluator evaluator;

  public MergeStep(final MergeClause mergeClause, final CommandContext context,
                   final CypherFunctionFactory functionFactory) {
    super(context);
    this.mergeClause = mergeClause;
    this.evaluator = new ExpressionEvaluator(functionFactory);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final boolean hasInput = prev != null;

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      private boolean mergedStandalone = false;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size()) {
          return true;
        }

        if (finished) {
          return false;
        }

        // Fetch more results
        fetchMore(nRecords);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        if (hasInput) {
          // Chained MERGE: merge for each input result
          if (prevResults == null) {
            prevResults = prev.syncPull(context, nRecords);
          }

          while (buffer.size() < n && prevResults.hasNext()) {
            final Result inputResult = prevResults.next();
            final List<Result> mergedResults = executeMerge(inputResult);
            buffer.addAll(mergedResults);
          }

          if (!prevResults.hasNext()) {
            finished = true;
          }
        } else {
          // Standalone MERGE: merge once
          if (!mergedStandalone) {
            final List<Result> mergedResults = executeMerge(null);
            buffer.addAll(mergedResults);
            mergedStandalone = true;
          }
          finished = true;
        }
      }

      @Override
      public void close() {
        MergeStep.this.close();
      }
    };
  }

  /**
   * Executes the MERGE operation: tries to match all, creates if none found.
   * In Cypher, MERGE returns ALL matching elements (one row per match),
   * or creates one element if no match exists.
   *
   * @param inputResult input result from previous step (may be null for standalone MERGE)
   * @return list of results containing matched or created elements
   */
  private List<Result> executeMerge(final Result inputResult) {
    final PathPattern pathPattern = mergeClause.getPathPattern();

    // Check if we're already in a transaction
    final boolean wasInTransaction = context.getDatabase().isTransactionActive();

    try {
      // Begin transaction if not already active
      if (!wasInTransaction) {
        context.getDatabase().begin();
      }

      // Create base result and copy input properties if present
      final ResultInternal baseResult = new ResultInternal();
      if (inputResult != null) {
        for (final String prop : inputResult.getPropertyNames()) {
          baseResult.setProperty(prop, inputResult.getProperty(prop));
        }
      }

      final List<Result> results;
      if (pathPattern.isSingleNode()) {
        results = mergeSingleNodeAll(pathPattern.getFirstNode(), baseResult);
        // Add path binding for single-node MERGE (e.g., MERGE p = (a {num: 1}))
        if (pathPattern.hasPathVariable()) {
          for (final Result r : results)
            addPathBinding((ResultInternal) r, pathPattern);
        }
      } else {
        results = mergePathAll(pathPattern, baseResult);
      }

      // Apply ON CREATE SET or ON MATCH SET to each result
      for (final Result r : results) {
        final boolean wasCreated = Boolean.TRUE.equals(r.getProperty("  wasCreated"));
        // Remove internal flag
        if (r instanceof ResultInternal)
          ((ResultInternal) r).removeProperty("  wasCreated");
        if (wasCreated && mergeClause.hasOnCreateSet())
          applySetClause(mergeClause.getOnCreateSet(), (ResultInternal) r);
        else if (!wasCreated && mergeClause.hasOnMatchSet())
          applySetClause(mergeClause.getOnMatchSet(), (ResultInternal) r);
      }

      // Commit transaction if we started it
      if (!wasInTransaction) {
        context.getDatabase().commit();
      }

      return results;
    } catch (final Exception e) {
      // Rollback if we started the transaction
      if (!wasInTransaction && context.getDatabase().isTransactionActive()) {
        context.getDatabase().rollback();
      }
      throw e;
    }
  }

  /**
   * Merges a single node, returning ALL matching nodes or creating one.
   * MERGE returns one row per matching node, or creates if none match.
   */
  private List<Result> mergeSingleNodeAll(final NodePattern nodePattern, final ResultInternal baseResult) {
    final String variable = nodePattern.getVariable() != null ? nodePattern.getVariable() : "n";

    // Only check for already-bound variable when explicitly named by the user.
    // Anonymous MERGE nodes (no variable) should always search for matches.
    if (nodePattern.getVariable() != null) {
      final Object existing = baseResult.getProperty(variable);
      if (existing instanceof Vertex) {
        baseResult.setProperty("  wasCreated", false);
        return List.of(baseResult);
      }
    }

    // Find ALL matching nodes
    final List<Vertex> matches = findAllNodes(nodePattern, baseResult);

    if (!matches.isEmpty()) {
      final List<Result> results = new ArrayList<>();
      for (final Vertex v : matches) {
        final ResultInternal r = copyResult(baseResult);
        r.setProperty(variable, v);
        r.setProperty("  wasCreated", false);
        results.add(r);
      }
      return results;
    }

    // No match - create one
    final Vertex vertex = createVertex(nodePattern, baseResult);
    baseResult.setProperty(variable, vertex);
    baseResult.setProperty("  wasCreated", true);
    return List.of(baseResult);
  }

  /**
   * Merges a path with relationships.
   * For now, this is a simplified implementation that creates if any part doesn't exist.
   *
   * @param pathPattern path pattern to merge
   * @param result result to store merged elements in
   * @return true if any element was created, false if all were matched
   */
  /**
   * Merges a path, returning ALL matching paths or creating one.
   * When vertices are bound, finds all matching edges; creates if none match.
   */
  private List<Result> mergePathAll(final PathPattern pathPattern, final ResultInternal baseResult) {
    final List<Vertex> vertices = new ArrayList<>();
    boolean anyNodeCreated = false;

    // Resolve all vertices in the path
    for (int i = 0; i <= pathPattern.getRelationshipCount(); i++) {
      final NodePattern nodePattern = pathPattern.getNode(i);
      Vertex vertex = null;

      if (nodePattern.getVariable() != null) {
        final Object existing = baseResult.getProperty(nodePattern.getVariable());
        if (existing instanceof Vertex)
          vertex = (Vertex) existing;
      }

      if (vertex == null) {
        vertex = findNode(nodePattern, baseResult);
        if (vertex == null) {
          vertex = createVertex(nodePattern, baseResult);
          anyNodeCreated = true;
        }
        if (nodePattern.getVariable() != null)
          baseResult.setProperty(nodePattern.getVariable(), vertex);
      }

      vertices.add(vertex);
    }

    // Merge relationships — find ALL matching edges or create
    for (int i = 0; i < pathPattern.getRelationshipCount(); i++) {
      final RelationshipPattern relPattern = pathPattern.getRelationship(i);
      final Vertex fromVertex;
      final Vertex toVertex;

      if (relPattern.getDirection() == Direction.IN) {
        fromVertex = vertices.get(i + 1);
        toVertex = vertices.get(i);
      } else {
        fromVertex = vertices.get(i);
        toVertex = vertices.get(i + 1);
      }

      final List<Edge> matchingEdges = findAllEdges(fromVertex, toVertex, relPattern, baseResult);

      if (!matchingEdges.isEmpty()) {
        // Return one result per matching edge
        final List<Result> results = new ArrayList<>();
        for (final Edge edge : matchingEdges) {
          final ResultInternal r = copyResult(baseResult);
          if (relPattern.getVariable() != null)
            r.setProperty(relPattern.getVariable(), edge);
          r.setProperty("  wasCreated", false);
          addPathBinding(r, pathPattern);
          results.add(r);
        }
        return results;
      }

      // No matching edge - create one
      final Edge edge = createEdge(fromVertex, toVertex, relPattern, baseResult);
      if (relPattern.getVariable() != null)
        baseResult.setProperty(relPattern.getVariable(), edge);
    }

    addPathBinding(baseResult, pathPattern);
    baseResult.setProperty("  wasCreated", anyNodeCreated || true);
    return List.of(baseResult);
  }

  private void addPathBinding(final ResultInternal result, final PathPattern pathPattern) {
    if (!pathPattern.hasPathVariable())
      return;
    final String pathVar = pathPattern.getPathVariable();
    final List<Object> pathElements = new ArrayList<>();
    for (int i = 0; i <= pathPattern.getRelationshipCount(); i++) {
      final NodePattern np = pathPattern.getNode(i);
      if (np.getVariable() != null) {
        final Object v = result.getProperty(np.getVariable());
        if (v != null)
          pathElements.add(v);
      }
      if (i < pathPattern.getRelationshipCount()) {
        final RelationshipPattern rp = pathPattern.getRelationship(i);
        if (rp.getVariable() != null) {
          final Object e = result.getProperty(rp.getVariable());
          if (e != null)
            pathElements.add(e);
        }
      }
    }
    result.setProperty(pathVar, pathElements);
  }

  private ResultInternal copyResult(final Result source) {
    final ResultInternal copy = new ResultInternal();
    for (final String prop : source.getPropertyNames())
      copy.setProperty(prop, source.getProperty(prop));
    return copy;
  }

  /**
   * Finds a node matching the pattern.
   *
   * @param nodePattern node pattern to find
   * @param result current result context for evaluating property expressions
   * @return matching vertex or null
   */
  private Vertex findNode(final NodePattern nodePattern, final Result result) {
    // Evaluate property expressions against current result context (may be empty)
    final Map<String, Object> evaluatedProperties = nodePattern.hasProperties()
        ? evaluateProperties(nodePattern.getProperties(), result)
        : null;

    if (!nodePattern.hasLabels()) {
      // No labels specified: MERGE (a) or MERGE (a {prop: val})
      // Match any vertex (optionally with matching properties)
      for (final com.arcadedb.schema.DocumentType type : context.getDatabase().getSchema().getTypes()) {
        if (type instanceof com.arcadedb.schema.VertexType) {
          @SuppressWarnings("unchecked")
          final Iterator<Identifiable> iter = (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(type.getName(), false);
          while (iter.hasNext()) {
            final Identifiable id = iter.next();
            if (id instanceof Vertex vertex) {
              if (evaluatedProperties == null || matchesProperties(vertex, evaluatedProperties))
                return vertex;
            }
          }
        }
      }
      return null;
    }

    final List<String> labels = nodePattern.getLabels();

    // For multi-label MERGE, use composite type if available, otherwise check all labels
    if (labels.size() > 1) {
      // Try to find a vertex that has ALL specified labels
      final String firstLabel = labels.get(0);
      if (!context.getDatabase().getSchema().existsType(firstLabel))
        return null;

      @SuppressWarnings("unchecked")
      final Iterator<Identifiable> iterator = (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(firstLabel, true);
      while (iterator.hasNext()) {
        final Identifiable identifiable = iterator.next();
        if (identifiable instanceof Vertex vertex) {
          // Check that the vertex has ALL required labels
          final List<String> vertexLabels = com.arcadedb.query.opencypher.Labels.getLabels(vertex);
          if (vertexLabels.containsAll(labels)) {
            if (evaluatedProperties == null || matchesProperties(vertex, evaluatedProperties))
              return vertex;
          }
        }
      }
      return null;
    }

    final String label = labels.get(0);

    // Check if the type exists in the schema before iterating
    if (!context.getDatabase().getSchema().existsType(label))
      return null;

    @SuppressWarnings("unchecked")
    final Iterator<Identifiable> iterator = (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(label, true);

    // Find first vertex matching all properties (or any vertex if no properties specified)
    while (iterator.hasNext()) {
      final Identifiable identifiable = iterator.next();
      if (identifiable instanceof Vertex vertex) {
        if (evaluatedProperties == null || matchesProperties(vertex, evaluatedProperties))
          return vertex;
      }
    }

    return null;
  }

  /**
   * Finds an edge matching the pattern between two vertices.
   *
   * @param from source vertex
   * @param to   target vertex
   * @param relPattern relationship pattern
   * @param result current result context for evaluating property expressions
   * @return matching edge or null
   */
  /**
   * Finds ALL nodes matching the pattern.
   */
  private List<Vertex> findAllNodes(final NodePattern nodePattern, final Result result) {
    final List<Vertex> matches = new ArrayList<>();
    final Map<String, Object> evaluatedProperties = nodePattern.hasProperties()
        ? evaluateProperties(nodePattern.getProperties(), result)
        : null;

    if (!nodePattern.hasLabels()) {
      for (final com.arcadedb.schema.DocumentType type : context.getDatabase().getSchema().getTypes()) {
        if (type instanceof com.arcadedb.schema.VertexType) {
          @SuppressWarnings("unchecked")
          final Iterator<Identifiable> iter = (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(type.getName(), false);
          while (iter.hasNext()) {
            final Identifiable id = iter.next();
            if (id instanceof Vertex vertex)
              if (evaluatedProperties == null || matchesProperties(vertex, evaluatedProperties))
                matches.add(vertex);
          }
        }
      }
      return matches;
    }

    final List<String> labels = nodePattern.getLabels();

    if (labels.size() > 1) {
      final String firstLabel = labels.get(0);
      if (!context.getDatabase().getSchema().existsType(firstLabel))
        return matches;
      @SuppressWarnings("unchecked")
      final Iterator<Identifiable> iterator = (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(firstLabel, true);
      while (iterator.hasNext()) {
        final Identifiable identifiable = iterator.next();
        if (identifiable instanceof Vertex vertex) {
          final List<String> vertexLabels = com.arcadedb.query.opencypher.Labels.getLabels(vertex);
          if (vertexLabels.containsAll(labels))
            if (evaluatedProperties == null || matchesProperties(vertex, evaluatedProperties))
              matches.add(vertex);
        }
      }
      return matches;
    }

    final String label = labels.get(0);
    if (!context.getDatabase().getSchema().existsType(label))
      return matches;
    @SuppressWarnings("unchecked")
    final Iterator<Identifiable> iterator = (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(label, true);
    while (iterator.hasNext()) {
      final Identifiable identifiable = iterator.next();
      if (identifiable instanceof Vertex vertex)
        if (evaluatedProperties == null || matchesProperties(vertex, evaluatedProperties))
          matches.add(vertex);
    }
    return matches;
  }

  /**
   * Finds ALL edges matching the pattern between two vertices.
   */
  private List<Edge> findAllEdges(final Vertex from, final Vertex to, final RelationshipPattern relPattern, final Result result) {
    final List<Edge> matches = new ArrayList<>();
    if (!relPattern.hasTypes())
      return matches;

    final String type = relPattern.getFirstType();
    final Map<String, Object> evaluatedProperties = relPattern.hasProperties()
        ? evaluateProperties(relPattern.getProperties(), result)
        : null;

    // Check outgoing edges from 'from' to 'to'
    final Iterator<Edge> outEdges = from.getEdges(Vertex.DIRECTION.OUT, type).iterator();
    while (outEdges.hasNext()) {
      final Edge edge = outEdges.next();
      if (edge.getIn().equals(to))
        if (evaluatedProperties == null || matchesProperties(edge, evaluatedProperties))
          matches.add(edge);
    }

    // For undirected (BOTH) patterns, also check reverse direction
    if (relPattern.getDirection() == Direction.BOTH) {
      final Iterator<Edge> reverseEdges = to.getEdges(Vertex.DIRECTION.OUT, type).iterator();
      while (reverseEdges.hasNext()) {
        final Edge edge = reverseEdges.next();
        if (edge.getIn().equals(from))
          if (evaluatedProperties == null || matchesProperties(edge, evaluatedProperties))
            matches.add(edge);
      }
    }

    return matches;
  }

  private Edge findEdge(final Vertex from, final Vertex to, final RelationshipPattern relPattern, final Result result) {
    if (!relPattern.hasTypes()) {
      return null;
    }

    final String type = relPattern.getFirstType();

    // Evaluate property expressions against current result context
    final Map<String, Object> evaluatedProperties = relPattern.hasProperties()
        ? evaluateProperties(relPattern.getProperties(), result)
        : null;

    // Check outgoing edges from 'from' to 'to'
    final Iterator<Edge> outEdges = from.getEdges(Vertex.DIRECTION.OUT, type).iterator();
    while (outEdges.hasNext()) {
      final Edge edge = outEdges.next();
      if (edge.getIn().equals(to)) {
        if (evaluatedProperties == null || matchesProperties(edge, evaluatedProperties))
          return edge;
      }
    }

    // For undirected (BOTH) patterns, also check reverse: edges from 'to' to 'from'
    if (relPattern.getDirection() == Direction.BOTH) {
      final Iterator<Edge> reverseEdges = to.getEdges(Vertex.DIRECTION.OUT, type).iterator();
      while (reverseEdges.hasNext()) {
        final Edge edge = reverseEdges.next();
        if (edge.getIn().equals(from)) {
          if (evaluatedProperties == null || matchesProperties(edge, evaluatedProperties))
            return edge;
        }
      }
    }

    return null;
  }

  /**
   * Checks if a vertex/edge matches all property filters.
   *
   * @param doc        document to check
   * @param properties expected properties
   * @return true if all properties match
   */
  private boolean matchesProperties(final Document doc, final Map<String, Object> properties) {
    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final String key = entry.getKey();
      Object expectedValue = entry.getValue();

      // Handle string literals: remove quotes
      if (expectedValue instanceof String) {
        final String strValue = (String) expectedValue;
        if (strValue.startsWith("'") && strValue.endsWith("'"))
          expectedValue = strValue.substring(1, strValue.length() - 1);
        else if (strValue.startsWith("\"") && strValue.endsWith("\""))
          expectedValue = strValue.substring(1, strValue.length() - 1);
      }

      final Object actualValue = doc.get(key);
      if (actualValue == null)
        return false;
      // Use numeric-safe comparison (Integer vs Long, etc.)
      if (!valuesEqual(actualValue, expectedValue))
        return false;
    }
    return true;
  }

  private static boolean valuesEqual(final Object a, final Object b) {
    if (a == null)
      return b == null;
    if (a.equals(b))
      return true;
    // Numeric type-safe comparison: Integer(1) should equal Long(1)
    if (a instanceof Number && b instanceof Number)
      return ((Number) a).longValue() == ((Number) b).longValue()
          && Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue()) == 0;
    return false;
  }

  /**
   * Creates a vertex from a node pattern.
   *
   * @param nodePattern node pattern to create
   * @param result current result context for evaluating property expressions
   * @return created vertex
   */
  private Vertex createVertex(final NodePattern nodePattern, final Result result) {
    final List<String> labels = nodePattern.hasLabels()
        ? nodePattern.getLabels()
        : List.of("Vertex");

    // Get or create the appropriate type (composite if multiple labels)
    final String typeName = com.arcadedb.query.opencypher.Labels.ensureCompositeType(
        context.getDatabase().getSchema(), labels);

    final MutableVertex vertex = context.getDatabase().newVertex(typeName);

    if (nodePattern.hasProperties()) {
      // Evaluate property expressions against current result context
      final Map<String, Object> evaluatedProperties = evaluateProperties(nodePattern.getProperties(), result);
      setProperties(vertex, evaluatedProperties);
    }

    vertex.save();
    return vertex;
  }

  /**
   * Creates an edge between two vertices.
   *
   * @param fromVertex source vertex
   * @param toVertex target vertex
   * @param relPattern relationship pattern
   * @param result current result context for evaluating property expressions
   * @return created edge
   */
  private Edge createEdge(final Vertex fromVertex, final Vertex toVertex, final RelationshipPattern relPattern,
                          final Result result) {
    final String type = relPattern.hasTypes() ? relPattern.getFirstType() : "EDGE";

    // Ensure edge type exists (Cypher auto-creates types)
    context.getDatabase().getSchema().getOrCreateEdgeType(type);

    final MutableEdge edge = fromVertex.newEdge(type, toVertex);

    if (relPattern.hasProperties()) {
      // Evaluate property expressions against current result context
      final Map<String, Object> evaluatedProperties = evaluateProperties(relPattern.getProperties(), result);
      setProperties(edge, evaluatedProperties);
    }

    edge.save();
    return edge;
  }

  /**
   * Sets properties on a document from a property map.
   */
  private void setProperties(final MutableDocument document, final Map<String, Object> properties) {
    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final String key = entry.getKey();
      Object value = entry.getValue();

      // Handle string literals: remove quotes
      if (value instanceof String) {
        final String strValue = (String) value;
        if (strValue.startsWith("'") && strValue.endsWith("'")) {
          value = strValue.substring(1, strValue.length() - 1);
        } else if (strValue.startsWith("\"") && strValue.endsWith("\"")) {
          value = strValue.substring(1, strValue.length() - 1);
        }
      }

      document.set(key, value);
    }
  }

  /**
   * Evaluates property expressions against the current result context.
   * This handles cases like: {subtype: BatchEntry.subtype, name: BatchEntry.name}
   * where the property values are expressions that need to be evaluated.
   *
   * @param properties raw property map from the pattern
   * @param result current result context containing variables
   * @return evaluated property map with actual values
   */
  private Map<String, Object> evaluateProperties(final Map<String, Object> properties, final Result result) {
    final Map<String, Object> evaluated = new HashMap<>();

    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final String key = entry.getKey();
      Object value = entry.getValue();

      // If the value is an Expression object, evaluate it in the current result context
      if (value instanceof Expression) {
        value = evaluator.evaluate((Expression) value, result, context);
      }
      // Legacy support: If the value looks like a property access (e.g., "BatchEntry.subtype"),
      // try to evaluate it against the current result context
      else if (value instanceof String) {
        final String strValue = (String) value;

        // Check if it's a property access pattern: variable.property
        if (strValue.contains(".") && !strValue.startsWith("'") && !strValue.startsWith("\"")) {
          final String[] parts = strValue.split("\\.", 2);
          if (parts.length == 2) {
            final String variable = parts[0];
            final String property = parts[1];

            // Try to get the variable from the result
            final Object obj = result.getProperty(variable);
            if (obj != null) {
              // If it's a map (like unwound data), get the property
              if (obj instanceof Map) {
                value = ((Map<?, ?>) obj).get(property);
              } else if (obj instanceof Document) {
                value = ((Document) obj).get(property);
              }
            }
          }
        } else if (!strValue.startsWith("'") && !strValue.startsWith("\"")) {
          // It might be a simple variable reference
          final Object obj = result.getProperty(strValue);
          if (obj != null) {
            value = obj;
          }
        }
      }

      evaluated.put(key, value);
    }

    return evaluated;
  }

  /**
   * Applies a SET clause to the result (used for ON CREATE SET / ON MATCH SET).
   *
   * @param setClause the SET clause to apply
   * @param result the result containing variables to update
   */
  @SuppressWarnings("unchecked")
  private void applySetClause(final SetClause setClause, final Result result) {
    if (setClause == null || setClause.isEmpty())
      return;

    for (final SetClause.SetItem item : setClause.getItems()) {
      final String variable = item.getVariable();
      final Object obj = result.getProperty(variable);
      if (obj == null)
        continue;

      switch (item.getType()) {
        case PROPERTY: {
          if (!(obj instanceof Document doc))
            break;
          final MutableDocument mutableDoc = doc.modify();
          final Object value = evaluator.evaluate(item.getValueExpression(), result, context);
          if (value == null)
            mutableDoc.remove(item.getProperty());
          else
            mutableDoc.set(item.getProperty(), value);
          mutableDoc.save();
          ((ResultInternal) result).setProperty(variable, mutableDoc);
          break;
        }
        case REPLACE_MAP: {
          if (!(obj instanceof Document doc))
            break;
          final Object mapValue = evaluator.evaluate(item.getValueExpression(), result, context);
          final java.util.Map<String, Object> map;
          if (mapValue instanceof java.util.Map)
            map = (java.util.Map<String, Object>) mapValue;
          else if (mapValue instanceof Document srcDoc)
            map = srcDoc.propertiesAsMap();
          else
            break;
          final MutableDocument mutableDoc = doc.modify();
          for (final String prop : new java.util.HashSet<>(mutableDoc.getPropertyNames()))
            if (!prop.startsWith("@"))
              mutableDoc.remove(prop);
          for (final java.util.Map.Entry<String, Object> entry : map.entrySet())
            if (entry.getValue() != null)
              mutableDoc.set(entry.getKey(), entry.getValue());
          mutableDoc.save();
          ((ResultInternal) result).setProperty(variable, mutableDoc);
          break;
        }
        case MERGE_MAP: {
          if (!(obj instanceof Document doc))
            break;
          final Object mapValue = evaluator.evaluate(item.getValueExpression(), result, context);
          final java.util.Map<String, Object> map;
          if (mapValue instanceof java.util.Map)
            map = (java.util.Map<String, Object>) mapValue;
          else if (mapValue instanceof Document srcDoc)
            map = srcDoc.propertiesAsMap();
          else
            break;
          final MutableDocument mutableDoc = doc.modify();
          for (final java.util.Map.Entry<String, Object> entry : map.entrySet())
            if (entry.getValue() == null)
              mutableDoc.remove(entry.getKey());
            else
              mutableDoc.set(entry.getKey(), entry.getValue());
          mutableDoc.save();
          ((ResultInternal) result).setProperty(variable, mutableDoc);
          break;
        }
        case LABELS: {
          if (!(obj instanceof Vertex vertex))
            break;
          final java.util.List<String> existingLabels = com.arcadedb.query.opencypher.Labels.getLabels(vertex);
          final java.util.List<String> allLabels = new java.util.ArrayList<>(existingLabels);
          for (final String label : item.getLabels())
            if (!allLabels.contains(label))
              allLabels.add(label);
          final String newTypeName = com.arcadedb.query.opencypher.Labels.ensureCompositeType(
              context.getDatabase().getSchema(), allLabels);
          if (!vertex.getTypeName().equals(newTypeName)) {
            final MutableVertex newVertex = context.getDatabase().newVertex(newTypeName);
            for (final String prop : vertex.getPropertyNames())
              newVertex.set(prop, vertex.get(prop));
            newVertex.save();
            for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT))
              newVertex.newEdge(edge.getTypeName(), edge.getVertex(Vertex.DIRECTION.IN));
            for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.IN))
              edge.getVertex(Vertex.DIRECTION.OUT).newEdge(edge.getTypeName(), newVertex);
            vertex.delete();
            ((ResultInternal) result).setProperty(variable, newVertex);
          }
          break;
        }
      }
    }
  }

  /**
   * Evaluates a simple expression for SET clauses.
   * Currently supports:
   * - String literals: 'Alice', "Bob"
   * - Numbers: 42, 3.14
   * - Booleans: true, false
   * - null
   * - Variable references
   * - Property access: variable.property
   */
  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ MERGE");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
