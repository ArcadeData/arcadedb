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
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.MergeClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.parser.CypherASTBuilder;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (context.isProfiling())
                rowCount++;

              final List<Result> mergedResults = executeMerge(inputResult);
              buffer.addAll(mergedResults);
            } finally {
              if (context.isProfiling())
                cost += (System.nanoTime() - begin);
            }
          }

          if (!prevResults.hasNext()) {
            finished = true;
          }
        } else {
          // Standalone MERGE: merge once
          if (!mergedStandalone) {
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (context.isProfiling())
                rowCount++;

              final List<Result> mergedResults = executeMerge(null);
              buffer.addAll(mergedResults);
              mergedStandalone = true;
            } finally {
              if (context.isProfiling())
                cost += (System.nanoTime() - begin);
            }
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
   * Merges a path, returning ALL matching paths or creating one.
   * Unbound endpoint variables are resolved via edge traversal only — never by
   * independent node lookup — so an unbound label-only endpoint is not silently
   * mapped to an unrelated existing node (fix for issue #3998).
   */
  private List<Result> mergePathAll(final PathPattern pathPattern, final ResultInternal baseResult) {
    final List<Result> found = findAllMatchingPaths(pathPattern, baseResult);
    if (!found.isEmpty())
      return found;
    return createNewPath(pathPattern, baseResult);
  }

  /**
   * Finds all complete existing paths that match {@code pathPattern}.
   * Traversal starts from any already-bound anchor node and follows edges to
   * discover the remaining pattern; unbound intermediate and endpoint nodes are
   * matched by label/property only after being reached via a qualifying edge.
   */
  private List<Result> findAllMatchingPaths(final PathPattern pathPattern, final ResultInternal baseResult) {
    final List<Result> results = new ArrayList<>();
    traverseFromNode(pathPattern, 0, null, copyResult(baseResult), results);
    return results;
  }

  /**
   * Recursive DFS walker.  {@code nodeIndex} is the current position in the
   * path (0 = start node, {@code pathPattern.getRelationshipCount()} = terminal
   * node).  {@code forcedVertex} is non-null when the node was reached via a
   * preceding edge traversal; otherwise the node is resolved from
   * {@code currentResult} or left as "unbound" for a full-scan.
   */
  private void traverseFromNode(final PathPattern pathPattern, final int nodeIndex,
      final Vertex forcedVertex, final ResultInternal currentResult, final List<Result> results) {

    final NodePattern nodePattern = pathPattern.getNode(nodeIndex);

    // Determine the vertex at this position
    Vertex vertex = forcedVertex;
    if (nodePattern.getVariable() != null) {
      final Object bound = currentResult.getProperty(nodePattern.getVariable());
      if (bound instanceof Vertex v) {
        if (vertex != null && !vertex.equals(v))
          return;
        vertex = v;
      }
    }

    if (nodeIndex == pathPattern.getRelationshipCount()) {
      // Terminal node: the full path has been matched up to here
      if (vertex == null)
        return;

      // If the node variable was pre-bound (from MATCH), the reached vertex must be identical
      if (nodePattern.getVariable() != null) {
        final Object preBound = currentResult.getProperty(nodePattern.getVariable());
        if (preBound instanceof Vertex bv && !bv.equals(vertex))
          return;
      }

      if (!matchesNodePattern(vertex, nodePattern, currentResult))
        return;

      final ResultInternal r = copyResult(currentResult);
      if (nodePattern.getVariable() != null)
        r.setProperty(nodePattern.getVariable(), vertex);
      r.setProperty("  wasCreated", false);
      addPathBinding(r, pathPattern);
      results.add(r);
      return;
    }

    final RelationshipPattern relPattern = pathPattern.getRelationship(nodeIndex);

    if (vertex != null) {
      // Anchor is known — verify it matches its own pattern, then traverse edges
      if (!matchesNodePattern(vertex, nodePattern, currentResult))
        return;

      final ResultInternal stepResult = copyResult(currentResult);
      if (nodePattern.getVariable() != null)
        stepResult.setProperty(nodePattern.getVariable(), vertex);

      traverseEdgesFromVertex(pathPattern, nodeIndex, vertex, relPattern, stepResult, results);
    } else {
      // Anchor is unbound — scan all edges of the required type; both endpoints
      // of each edge must satisfy their respective node patterns
      if (!relPattern.hasTypes())
        return;
      final String relType = relPattern.getFirstType();
      if (!context.getDatabase().getSchema().existsType(relType))
        return;

      final Map<String, Object> relProps = relPattern.hasProperties()
          ? evaluateProperties(relPattern.getProperties(), currentResult) : null;

      @SuppressWarnings("unchecked")
      final Iterator<Identifiable> it = (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(relType, true);
      while (it.hasNext()) {
        final Identifiable id = it.next();
        if (!(id instanceof Edge edge))
          continue;
        if (relProps != null && !matchesProperties(edge, relProps))
          continue;

        final Direction dir = relPattern.getDirection();
        final NodePattern nextPattern = pathPattern.getNode(nodeIndex + 1);

        if (dir == Direction.OUT || dir == Direction.BOTH) {
          final Vertex sourceV = edge.getVertex(Vertex.DIRECTION.OUT);
          final Vertex targetV = edge.getVertex(Vertex.DIRECTION.IN);
          if (matchesNodePattern(sourceV, nodePattern, currentResult) && matchesNodePattern(targetV, nextPattern, currentResult)) {
            final ResultInternal stepResult = copyResult(currentResult);
            if (nodePattern.getVariable() != null)
              stepResult.setProperty(nodePattern.getVariable(), sourceV);
            if (relPattern.getVariable() != null)
              stepResult.setProperty(relPattern.getVariable(), edge);
            traverseFromNode(pathPattern, nodeIndex + 1, targetV, stepResult, results);
          }
        }
        if (dir == Direction.IN || dir == Direction.BOTH) {
          final Vertex sourceV = edge.getVertex(Vertex.DIRECTION.IN);
          final Vertex targetV = edge.getVertex(Vertex.DIRECTION.OUT);
          if (matchesNodePattern(sourceV, nodePattern, currentResult) && matchesNodePattern(targetV, nextPattern, currentResult)) {
            final ResultInternal stepResult = copyResult(currentResult);
            if (nodePattern.getVariable() != null)
              stepResult.setProperty(nodePattern.getVariable(), sourceV);
            if (relPattern.getVariable() != null)
              stepResult.setProperty(relPattern.getVariable(), edge);
            traverseFromNode(pathPattern, nodeIndex + 1, targetV, stepResult, results);
          }
        }
      }
    }
  }

  /**
   * Traverses the edges of {@code vertex} that match {@code relPattern} and
   * recurses into each reachable next node.
   */
  private void traverseEdgesFromVertex(final PathPattern pathPattern, final int nodeIndex,
      final Vertex vertex, final RelationshipPattern relPattern,
      final ResultInternal currentResult, final List<Result> results) {

    if (!relPattern.hasTypes())
      return;

    final String relType = relPattern.getFirstType();
    final Map<String, Object> relProps = relPattern.hasProperties()
        ? evaluateProperties(relPattern.getProperties(), currentResult) : null;

    final boolean inbound = relPattern.getDirection() == Direction.IN;
    final Vertex.DIRECTION edgeDir = inbound ? Vertex.DIRECTION.IN : Vertex.DIRECTION.OUT;
    final Vertex.DIRECTION otherEnd = inbound ? Vertex.DIRECTION.OUT : Vertex.DIRECTION.IN;

    for (final Edge edge : vertex.getEdges(edgeDir, relType)) {
      if (relProps != null && !matchesProperties(edge, relProps))
        continue;

      final Vertex nextV = edge.getVertex(otherEnd);
      final ResultInternal stepResult = copyResult(currentResult);
      if (relPattern.getVariable() != null)
        stepResult.setProperty(relPattern.getVariable(), edge);

      traverseFromNode(pathPattern, nodeIndex + 1, nextV, stepResult, results);
    }

    // For undirected patterns also traverse the reverse direction
    if (relPattern.getDirection() == Direction.BOTH) {
      for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.IN, relType)) {
        if (relProps != null && !matchesProperties(edge, relProps))
          continue;

        final Vertex nextV = edge.getVertex(Vertex.DIRECTION.OUT);
        final ResultInternal stepResult = copyResult(currentResult);
        if (relPattern.getVariable() != null)
          stepResult.setProperty(relPattern.getVariable(), edge);

        traverseFromNode(pathPattern, nodeIndex + 1, nextV, stepResult, results);
      }
    }
  }

  /**
   * Creates all nodes and edges in the path that are not already bound from a
   * preceding MATCH.  Never calls {@code findNode()} — every missing node is
   * created fresh.
   */
  private List<Result> createNewPath(final PathPattern pathPattern, final ResultInternal baseResult) {
    final List<Vertex> vertices = new ArrayList<>();

    for (int i = 0; i <= pathPattern.getRelationshipCount(); i++) {
      final NodePattern nodePattern = pathPattern.getNode(i);
      Vertex vertex = null;

      if (nodePattern.getVariable() != null) {
        final Object existing = baseResult.getProperty(nodePattern.getVariable());
        if (existing instanceof Vertex v)
          vertex = v;
      }

      if (vertex == null) {
        vertex = createVertex(nodePattern, baseResult);
        if (nodePattern.getVariable() != null)
          baseResult.setProperty(nodePattern.getVariable(), vertex);
      }

      vertices.add(vertex);
    }

    for (int i = 0; i < pathPattern.getRelationshipCount(); i++) {
      final RelationshipPattern relPattern = pathPattern.getRelationship(i);
      final Vertex fromVertex = relPattern.getDirection() == Direction.IN
          ? vertices.get(i + 1) : vertices.get(i);
      final Vertex toVertex = relPattern.getDirection() == Direction.IN
          ? vertices.get(i) : vertices.get(i + 1);

      final Edge edge = createEdge(fromVertex, toVertex, relPattern, baseResult);
      if (relPattern.getVariable() != null)
        baseResult.setProperty(relPattern.getVariable(), edge);
    }

    addPathBinding(baseResult, pathPattern);
    baseResult.setProperty("  wasCreated", true);
    return List.of(baseResult);
  }

  /**
   * Returns true if {@code vertex} satisfies {@code nodePattern}'s label and
   * property constraints.
   */
  private boolean matchesNodePattern(final Vertex vertex, final NodePattern nodePattern, final Result result) {
    if (nodePattern.hasLabels()) {
      for (final String label : nodePattern.getLabels()) {
        if (!Labels.hasLabel(vertex, label))
          return false;
      }
    }
    if (nodePattern.hasProperties()) {
      final Map<String, Object> props = evaluateProperties(nodePattern.getProperties(), result);
      if (!matchesProperties(vertex, props))
        return false;
    }
    return true;
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
   * Tries to find and use an index for the evaluated property constraints.
   * Returns an iterator of matching identifiables, or null if no suitable index found.
   * Applies leftmost-prefix matching for composite indexes.
   */
  private Iterator<Identifiable> tryFindByIndex(final DocumentType type, final String label,
      final Map<String, Object> evaluatedProperties) {
    TypeIndex bestIndex = null;
    int bestMatchCount = 0;
    List<String> bestMatchedProperties = null;

    for (final TypeIndex index : type.getAllIndexes(false)) {
      final List<String> indexProperties = index.getPropertyNames();
      int matchCount = 0;
      final List<String> matchedProperties = new ArrayList<>();

      for (final String indexProp : indexProperties) {
        if (evaluatedProperties.containsKey(indexProp)) {
          matchCount++;
          matchedProperties.add(indexProp);
        } else
          break; // Leftmost prefix only
      }

      // Require full index match (all index properties covered) - lookupByKey needs exact match
      if (matchCount > 0 && matchCount == indexProperties.size() && matchCount > bestMatchCount) {
        bestMatchCount = matchCount;
        bestIndex = index;
        bestMatchedProperties = matchedProperties;
      }
    }

    if (bestIndex == null || bestMatchedProperties == null || bestMatchedProperties.isEmpty())
      return null;

    final String[] propertyNames = bestMatchedProperties.toArray(new String[0]);
    final Object[] propertyValues = new Object[propertyNames.length];
    for (int i = 0; i < propertyNames.length; i++)
      propertyValues[i] = evaluatedProperties.get(propertyNames[i]);

    final Iterator<Identifiable> iter = context.getDatabase().lookupByKey(label, propertyNames, propertyValues);
    return iter;
  }

  /**
   * Finds ALL nodes matching the pattern.
   */
  private List<Vertex> findAllNodes(final NodePattern nodePattern, final Result result) {
    final List<Vertex> matches = new ArrayList<>();
    final Map<String, Object> evaluatedProperties = nodePattern.hasProperties()
        ? evaluateProperties(nodePattern.getProperties(), result)
        : null;

    if (!nodePattern.hasLabels()) {
      for (final DocumentType type : context.getDatabase().getSchema().getTypes()) {
        if (type instanceof VertexType) {
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
          final List<String> vertexLabels = Labels.getLabels(vertex);
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

    // OPTIMIZATION: try index before full scan
    if (evaluatedProperties != null && !evaluatedProperties.isEmpty()) {
      final DocumentType type = context.getDatabase().getSchema().getType(label);
      if (type != null) {
        final Iterator<Identifiable> indexIter = tryFindByIndex(type, label, evaluatedProperties);
        if (indexIter != null) {
          while (indexIter.hasNext()) {
            final Identifiable identifiable = indexIter.next();
            if (identifiable instanceof Vertex vertex && matchesProperties(vertex, evaluatedProperties))
              matches.add(vertex);
          }
          return matches;
        }
      }
    }

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
    final String typeName = Labels.ensureCompositeType(
        context.getDatabase().getSchema(), labels);

    final MutableVertex vertex = context.getDatabase().newVertex(typeName);

    if (nodePattern.hasProperties()) {
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
      // Resolve parameter references (e.g., $username -> actual value from context)
      else if (value instanceof CypherASTBuilder.ParameterReference) {
        final String paramName = ((CypherASTBuilder.ParameterReference) value).getName();
        if (context.getInputParameters() != null)
          value = context.getInputParameters().get(paramName);
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
          final Map<String, Object> map;
          if (mapValue instanceof Map)
            map = (Map<String, Object>) mapValue;
          else if (mapValue instanceof Document srcDoc)
            map = srcDoc.propertiesAsMap();
          else
            break;
          final MutableDocument mutableDoc = doc.modify();
          for (final String prop : new HashSet<>(mutableDoc.getPropertyNames()))
            if (!prop.startsWith("@"))
              mutableDoc.remove(prop);
          for (final Map.Entry<String, Object> entry : map.entrySet())
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
          final Map<String, Object> map;
          if (mapValue instanceof Map)
            map = (Map<String, Object>) mapValue;
          else if (mapValue instanceof Document srcDoc)
            map = srcDoc.propertiesAsMap();
          else
            break;
          final MutableDocument mutableDoc = doc.modify();
          for (final Map.Entry<String, Object> entry : map.entrySet())
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
          final List<String> existingLabels = Labels.getLabels(vertex);
          final List<String> allLabels = new ArrayList<>(existingLabels);
          for (final String label : item.getLabels())
            if (!allLabels.contains(label))
              allLabels.add(label);
          final String newTypeName = Labels.ensureCompositeType(
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
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
