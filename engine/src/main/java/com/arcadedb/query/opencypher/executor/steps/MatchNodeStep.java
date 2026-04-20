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
import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.parser.CypherASTBuilder;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Execution step for matching node patterns.
 * Fetches vertices from the database by type (label).
 * <p>
 * Example: MATCH (n:Person)
 * - Iterates all vertices of type "Person"
 * - Binds each vertex to variable 'n'
 * <p>
 * Supports optional inline WHERE filter (predicate pushdown) to evaluate
 * predicates during scanning rather than in a separate FilterPropertiesStep.
 */
public class MatchNodeStep extends AbstractExecutionStep {
  private final String              variable;
  private final NodePattern         pattern;
  private final String              idFilter;    // Optional ID filter to apply (e.g., "#1:0")
  private final BooleanExpression   whereFilter; // Optional inline WHERE predicate (pushdown)
  private final ExpressionEvaluator evaluator;   // Shared evaluator for WHERE/ID expression resolution
  private final Expression          dynamicIdExpression; // Pre-analyzed expression for runtime RID resolution (issue #3864)
  private       String              usedIndexName; // Track which index was used (if any)

  /**
   * Creates a match node step.
   *
   * @param variable variable name to bind vertices to
   * @param pattern  node pattern to match
   * @param context  command context
   */
  public MatchNodeStep(final String variable, final NodePattern pattern, final CommandContext context) {
    this(variable, pattern, context, null, null);
  }

  /**
   * Creates a match node step with ID filter optimization.
   *
   * @param variable variable name to bind vertices to
   * @param pattern  node pattern to match
   * @param context  command context
   * @param idFilter optional ID filter to apply (e.g., "#1:0")
   */
  public MatchNodeStep(final String variable, final NodePattern pattern, final CommandContext context,
                       final String idFilter) {
    this(variable, pattern, context, idFilter, null);
  }

  /**
   * Creates a match node step with ID filter and inline WHERE predicate pushdown.
   *
   * @param variable    variable name to bind vertices to
   * @param pattern     node pattern to match
   * @param context     command context
   * @param idFilter    optional ID filter to apply (e.g., "#1:0")
   * @param whereFilter optional inline WHERE predicate for pushdown filtering
   */
  public MatchNodeStep(final String variable, final NodePattern pattern, final CommandContext context,
                       final String idFilter, final BooleanExpression whereFilter) {
    super(context);
    this.variable = variable;
    this.pattern = pattern;
    this.idFilter = idFilter;
    this.whereFilter = whereFilter;
    this.evaluator = new ExpressionEvaluator(new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance()));
    this.dynamicIdExpression = (whereFilter != null) ? findIdValueExpression(whereFilter) : null;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final boolean hasInput = prev != null;

    return new ResultSet() {
      private       ResultSet              prevResults        = null;
      private       Iterator<Identifiable> iterator           = null;
      private final List<Result>           buffer             = new ArrayList<>();
      private       int                    bufferIndex        = 0;
      private       boolean                finished           = false;
      private       Result                 currentInputResult = null;

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
          // Chained mode: for each input result, add matched nodes
          if (prevResults == null) {
            prevResults = prev.syncPull(context, nRecords);
          }

          // Process input results and add our matched nodes
          while (buffer.size() < n) {
            // If we've exhausted nodes for current input, get next input
            if (iterator == null || !iterator.hasNext()) {
              if (!prevResults.hasNext()) {
                finished = true;
                break;
              }
              currentInputResult = prevResults.next();

              // Check if the variable is already bound in the input result.
              // This happens in OPTIONAL MATCH when a variable from a previous MATCH
              // is reused (e.g., MATCH (a)...(c) OPTIONAL MATCH (a)-[r]->(c)).
              // In this case, use the bound vertex directly instead of scanning all vertices.
              if (variable != null && currentInputResult.getPropertyNames().contains(variable)) {
                final Object boundValue = currentInputResult.getProperty(variable);
                if (boundValue instanceof Vertex) {
                  final Vertex boundVertex = (Vertex) boundValue;
                  if (matchesAllLabelsBound(boundVertex, currentInputResult) && matchesProperties(boundVertex, currentInputResult))
                    iterator = Collections.singletonList((Identifiable) boundVertex).iterator();
                  else
                    iterator = Collections.<Identifiable>emptyList().iterator();
                } else {
                  iterator = Collections.<Identifiable>emptyList().iterator();
                }
              } else {
                iterator = getVertexIterator(currentInputResult);
              }
            }

            // Match nodes and add to input result
            if (iterator.hasNext()) {
              final long begin = context.isProfiling() ? System.nanoTime() : 0;
              try {
                if (context.isProfiling())
                  rowCount++;

                final Identifiable identifiable = iterator.next();
                // Load the record if it's not already loaded
                final Document record = identifiable.asDocument();
                if (record instanceof Vertex) {
                  final Vertex vertex = (Vertex) record;

                  // Apply label and property filters
                  if (!matchesAllLabels(vertex, currentInputResult) || !matchesProperties(vertex, currentInputResult))
                    continue;

                  // Copy input result and add our vertex
                  final ResultInternal result = new ResultInternal();
                  if (currentInputResult != null) {
                    for (final String prop : currentInputResult.getPropertyNames()) {
                      result.setProperty(prop, currentInputResult.getProperty(prop));
                    }
                  }
                  result.setProperty(variable, vertex);

                  // Apply inline WHERE filter (predicate pushdown)
                  if (whereFilter != null && !whereFilter.evaluate(result, context))
                    continue;

                  buffer.add(result);
                }
              } finally {
                if (context.isProfiling())
                  cost += (System.nanoTime() - begin);
              }
            }
          }
        } else {
          // Standalone mode: no input, create fresh results
          // Initialize iterator on first call
          if (iterator == null) {
            iterator = getVertexIterator();
          }

          // Fetch up to n vertices
          while (buffer.size() < n && iterator.hasNext()) {
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (context.isProfiling())
                rowCount++;

              final Identifiable identifiable = iterator.next();

              // Load the record if it's not already loaded
              final Document record = identifiable.asDocument();
              if (record instanceof Vertex) {
                final Vertex vertex = (Vertex) record;

                // Apply label and property filters
                if (!matchesAllLabels(vertex) || !matchesProperties(vertex))
                  continue;

                // Create result with vertex bound to variable
                final ResultInternal result = new ResultInternal();
                result.setProperty(variable, vertex);

                // Apply inline WHERE filter (predicate pushdown)
                if (whereFilter != null && !whereFilter.evaluate(result, context))
                  continue;

                buffer.add(result);
              }
            } finally {
              if (context.isProfiling())
                cost += (System.nanoTime() - begin);
            }
          }

          if (!iterator.hasNext()) {
            finished = true;
          }
        }
      }

      @Override
      public void close() {
        MatchNodeStep.this.close();
      }
    };
  }

  /**
   * Gets an iterator for vertices matching the pattern.
   * OPTIMIZATION: Uses indexes for property equality constraints when available.
   * Supports composite indexes with partial key matching (leftmost prefix).
   * OPTIMIZATION: Uses ID filter when available to return single vertex.
   */
  private Iterator<Identifiable> getVertexIterator() {
    return getVertexIterator(null);
  }

  private Iterator<Identifiable> getVertexIterator(final Result currentInputResult) {
    // OPTIMIZATION: Resolve ID filter - either static (from plan time) or dynamic (from runtime).
    // Static idFilter handles literals/parameters; dynamicIdExpression handles expressions like
    // BatchEntry.destRID that can only be resolved with the current input row (issue #3864).
    String effectiveIdFilter = this.idFilter;
    if ((effectiveIdFilter == null || effectiveIdFilter.isEmpty())
        && dynamicIdExpression != null && currentInputResult != null) {
      final Object resolved = evaluator.evaluate(dynamicIdExpression, currentInputResult, context);
      if (resolved != null)
        effectiveIdFilter = resolved instanceof Identifiable
            ? ((Identifiable) resolved).getIdentity().toString()
            : resolved.toString();
    }

    // If ID filter is present, look up the specific vertex by ID.
    // This is critical for performance when matching by ID (e.g., WHERE ID(a) = "#1:0")
    // Without this optimization, MATCH (a),(b) WHERE ID(a) = x AND ID(b) = y
    // would create a Cartesian product of ALL vertices before filtering
    if (effectiveIdFilter != null && !effectiveIdFilter.isEmpty()) {
      try {
        final RID rid = context.getDatabase().newRID(effectiveIdFilter);
        final Identifiable vertex = context.getDatabase().lookupByRID(rid, true);
        return List.of(vertex).iterator();
      } catch (final Exception e) {
        // Invalid ID format or record not found - return empty iterator
        return List.<Identifiable>of().iterator();
      }
    }

    final List<String> labels = resolveEffectiveLabels(currentInputResult);

    if (!labels.isEmpty()) {
      if (labels.size() == 1) {
        // Single label - polymorphic iteration (existing behavior)
        final String label = labels.get(0);

        // OPTIMIZATION: Check if we can use an index for property lookup
        if (pattern.hasProperties() && !pattern.getProperties().isEmpty()) {
          final DocumentType type = context.getDatabase().getSchema().getType(label);
          if (type != null) {
            // Try to find an index that matches the property constraints
            // Support composite indexes with partial keys (leftmost prefix matching)
            final Iterator<Identifiable> indexedIter = tryFindAndUseIndex(type, label, currentInputResult);
            if (indexedIter != null)
              return indexedIter;
          }
        }

        // OPTIMIZATION: Check if WHERE clause has equality predicates that can use an index
        // This is critical for UNWIND...MATCH...WHERE patterns where the predicate references
        // an UNWIND variable (e.g., WHERE a.id = e.src_id)
        if (whereFilter != null && currentInputResult != null) {
          final DocumentType type = context.getDatabase().getSchema().getType(label);
          if (type != null) {
            final Iterator<Identifiable> indexedIter = tryFindAndUseIndexFromWhere(type, label, currentInputResult);
            if (indexedIter != null)
              return indexedIter;
          }
        }

        // No index available - fall back to full type scan
        if (context.getDatabase().getSchema().existsType(label)) {
          @SuppressWarnings("unchecked") final Iterator<Identifiable> iter =
              (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(label, true);
          return iter;
        }
        return Collections.emptyIterator();
      }

      // Multiple labels - iterate all vertex types that extend ALL required labels.
      // We can't use simple polymorphic iteration because A~B~C extends A, B, C
      // individually but iterateType("A", true) may not include A~B~C.
      // Instead, find all types that are instanceOf ALL labels and iterate them.
      final List<Iterator<Identifiable>> iterators = new ArrayList<>();
      for (final DocumentType type : context.getDatabase().getSchema().getTypes()) {
        if (type instanceof VertexType) {
          boolean matchesAll = true;
          for (final String label : labels) {
            if (!type.instanceOf(label)) {
              matchesAll = false;
              break;
            }
          }
          if (matchesAll) {
            @SuppressWarnings("unchecked") final Iterator<Identifiable> iter =
                (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(type.getName(), false);
            iterators.add(iter);
          }
        }
      }
      return new ChainedIterator(iterators);
    } else {
      // No label specified - iterate ALL vertex types
      // Get all vertex types from schema and chain their iterators
      final List<Iterator<Identifiable>> iterators = new ArrayList<>();

      for (final DocumentType type : context.getDatabase().getSchema().getTypes()) {
        // Only include vertex types (not edge types or document types)
        if (type instanceof VertexType) {
          @SuppressWarnings("unchecked") final Iterator<Identifiable> iter =
              (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(type.getName(), false);
          iterators.add(iter);
        }
      }

      // Chain all iterators together
      return new ChainedIterator(iterators);
    }
  }

  /**
   * Iterator that chains multiple iterators together.
   */
  private static class ChainedIterator implements Iterator<Identifiable> {
    private final List<Iterator<Identifiable>> iterators;
    private       int                          currentIndex = 0;

    public ChainedIterator(final List<Iterator<Identifiable>> iterators) {
      this.iterators = iterators;
    }

    @Override
    public boolean hasNext() {
      while (currentIndex < iterators.size()) {
        if (iterators.get(currentIndex).hasNext()) {
          return true;
        }
        currentIndex++;
      }
      return false;
    }

    @Override
    public Identifiable next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return iterators.get(currentIndex).next();
    }
  }

  /**
   * Tries to find and use an index for the property constraints.
   * Supports composite indexes with partial key matching (leftmost prefix).
   *
   * @param type  the document type
   * @param label the type label
   * @return iterator from index lookup, or null if no suitable index found
   */
  private Iterator<Identifiable> tryFindAndUseIndex(final DocumentType type, final String label,
      final Result currentInputResult) {
    // Prepare property names and values from the pattern
    final Map<String, Object> properties = new LinkedHashMap<>();
    for (final Map.Entry<String, Object> entry : pattern.getProperties().entrySet()) {
      final String propertyName = entry.getKey();
      Object propertyValue = entry.getValue();

      // Resolve parameter references
      if (propertyValue instanceof CypherASTBuilder.ParameterReference) {
        final String paramName = ((CypherASTBuilder.ParameterReference) propertyValue).getName();
        if (context.getInputParameters() != null) {
          final Object paramValue = context.getInputParameters().get(paramName);
          if (paramValue != null)
            propertyValue = paramValue;
        }
      }
      // Resolve dynamic expressions (e.g., e.src_id from UNWIND) against the current input result
      else if (propertyValue instanceof Expression) {
        if (currentInputResult != null)
          propertyValue = evaluator.evaluate((Expression) propertyValue, currentInputResult, context);
        else
          return null; // Cannot resolve expression without input row — skip index
      }

      properties.put(propertyName, propertyValue);
    }

    // Find the best index (longest leftmost prefix match)
    TypeIndex bestIndex = null;
    int bestMatchCount = 0;
    List<String> bestMatchedProperties = null;

    for (final TypeIndex index : type.getAllIndexes(false)) {
      final List<String> indexProperties = index.getPropertyNames();

      // Check how many properties match as a leftmost prefix
      // For composite indexes, we can only use a partial key if we have values for all
      // properties from the beginning (leftmost prefix)
      // Example: Index [a,b,c] can be used for [a], [a,b], or [a,b,c] but not [b] or [a,c]
      int matchCount = 0;
      final List<String> matchedProperties = new ArrayList<>();

      for (int i = 0; i < indexProperties.size(); i++) {
        final String indexProp = indexProperties.get(i);
        if (properties.containsKey(indexProp)) {
          // This property is available in the query
          matchCount++;
          matchedProperties.add(indexProp);
        } else {
          // Missing property - can't use further properties from this index
          break;
        }
      }

      // Update best match if this index covers all its properties (full match required for lookupByKey)
      if (matchCount > 0 && matchCount == indexProperties.size() && matchCount > bestMatchCount) {
        bestMatchCount = matchCount;
        bestIndex = index;
        bestMatchedProperties = matchedProperties;
      }
    }

    // If we found a suitable index, use it
    if (bestIndex != null && bestMatchedProperties != null && !bestMatchedProperties.isEmpty()) {
      final String[] propertyNames = bestMatchedProperties.toArray(new String[0]);
      final Object[] propertyValues = new Object[propertyNames.length];

      for (int i = 0; i < propertyNames.length; i++)
        propertyValues[i] = properties.get(propertyNames[i]);

      // Track which index was used for profiling output
      usedIndexName = label + "[" + String.join(", ", propertyNames) + "]";

      final Iterator<Identifiable> iter = context.getDatabase().lookupByKey(label, propertyNames, propertyValues);
      return iter;
    }

    return null;
  }

  /**
   * Extracts equality predicates from the WHERE clause pushdown filter and tries to use
   * an index for lookup. This is critical for UNWIND...MATCH...WHERE patterns where
   * the WHERE references an UNWIND variable (e.g., WHERE a.id = e.src_id).
   * Without this, each UNWIND row triggers a full type scan - O(N) per row.
   * With index lookup, it's O(log N) per row.
   */
  private Iterator<Identifiable> tryFindAndUseIndexFromWhere(final DocumentType type, final String label,
      final Result currentInputResult) {
    // Extract equality predicates: variable.property = <expression>
    final Map<String, Object> equalityPredicates = new LinkedHashMap<>();
    extractEqualityPredicates(whereFilter, equalityPredicates, currentInputResult);

    if (equalityPredicates.isEmpty())
      return null;

    // Find the best matching index
    TypeIndex bestIndex = null;
    int bestMatchCount = 0;
    List<String> bestMatchedProperties = null;

    for (final TypeIndex index : type.getAllIndexes(false)) {
      final List<String> indexProperties = index.getPropertyNames();
      int matchCount = 0;
      final List<String> matchedProperties = new ArrayList<>();

      for (final String indexProp : indexProperties) {
        if (equalityPredicates.containsKey(indexProp)) {
          matchCount++;
          matchedProperties.add(indexProp);
        } else
          break; // Leftmost prefix matching
      }

      // Require full index match (all index properties covered) - lookupByKey needs exact match
      if (matchCount > 0 && matchCount == indexProperties.size() && matchCount > bestMatchCount) {
        bestMatchCount = matchCount;
        bestIndex = index;
        bestMatchedProperties = matchedProperties;
      }
    }

    if (bestIndex != null && bestMatchedProperties != null && !bestMatchedProperties.isEmpty()) {
      final String[] propertyNames = bestMatchedProperties.toArray(new String[0]);
      final Object[] propertyValues = new Object[propertyNames.length];
      for (int i = 0; i < propertyNames.length; i++)
        propertyValues[i] = equalityPredicates.get(propertyNames[i]);

      usedIndexName = label + "[" + String.join(", ", propertyNames) + "]";

      final Iterator<Identifiable> iter = context.getDatabase().lookupByKey(label, propertyNames, propertyValues);
      return iter;
    }

    return null;
  }

  /**
   * Pre-analyzes the WHERE filter AST to find an expression providing the RID value for
   * ID(variable) = &lt;expression&gt; patterns. Called once in the constructor so the AST
   * is not re-traversed on every input row. Supports both id() and elementId().
   *
   * @return the Expression that evaluates to the RID, or null if no ID pattern found
   */
  private Expression findIdValueExpression(final BooleanExpression expr) {
    if (expr instanceof ComparisonExpression) {
      final ComparisonExpression comp = (ComparisonExpression) expr;
      if (comp.getOperator() != ComparisonExpression.Operator.EQUALS)
        return null;

      // Check for pattern: id(variable) = <expression> or elementId(variable) = <expression>
      if (isIdFunctionOnVariable(comp.getLeft()))
        return comp.getRight();
      if (isIdFunctionOnVariable(comp.getRight()))
        return comp.getLeft();
    } else if (expr instanceof LogicalExpression) {
      final LogicalExpression logical = (LogicalExpression) expr;
      if (logical.getOperator() == LogicalExpression.Operator.AND) {
        final Expression left = findIdValueExpression(logical.getLeft());
        if (left != null)
          return left;
        return findIdValueExpression(logical.getRight());
      }
    }
    return null;
  }

  /**
   * Checks if an expression is a call to id() or elementId() on this step's variable.
   */
  private boolean isIdFunctionOnVariable(final Expression expr) {
    if (expr instanceof FunctionCallExpression) {
      final FunctionCallExpression func = (FunctionCallExpression) expr;
      final String name = func.getFunctionName();
      if (("id".equalsIgnoreCase(name) || "elementid".equalsIgnoreCase(name)) && func.getArguments().size() == 1) {
        final Expression arg = func.getArguments().get(0);
        return arg instanceof VariableExpression && variable.equals(((VariableExpression) arg).getVariableName());
      }
    }
    return false;
  }

  /**
   * Extracts equality predicates of the form variable.property = value from a boolean expression.
   * Supports AND conjunctions. Resolves dynamic expressions against the current input result.
   */
  private void extractEqualityPredicates(final BooleanExpression expr,
      final Map<String, Object> predicates, final Result currentInputResult) {
    if (expr instanceof ComparisonExpression) {
      final ComparisonExpression comp = (ComparisonExpression) expr;
      if (comp.getOperator() != ComparisonExpression.Operator.EQUALS)
        return;

      // Check for pattern: variable.property = <expression>
      String propertyName = null;
      Expression valueExpr = null;

      if (comp.getLeft() instanceof PropertyAccessExpression) {
        final PropertyAccessExpression propAccess = (PropertyAccessExpression) comp.getLeft();
        if (variable.equals(propAccess.getVariableName())) {
          propertyName = propAccess.getPropertyName();
          valueExpr = comp.getRight();
        }
      }
      // Also check reversed: <expression> = variable.property
      if (propertyName == null && comp.getRight() instanceof PropertyAccessExpression) {
        final PropertyAccessExpression propAccess = (PropertyAccessExpression) comp.getRight();
        if (variable.equals(propAccess.getVariableName())) {
          propertyName = propAccess.getPropertyName();
          valueExpr = comp.getLeft();
        }
      }

      if (propertyName != null && valueExpr != null) {
        // Resolve the value expression
        final Object resolvedValue = evaluator.evaluate(valueExpr, currentInputResult, context);
        if (resolvedValue != null)
          predicates.put(propertyName, resolvedValue);
      }
    } else if (expr instanceof LogicalExpression) {
      final LogicalExpression logical = (LogicalExpression) expr;
      if (logical.getOperator() == LogicalExpression.Operator.AND) {
        extractEqualityPredicates(logical.getLeft(), predicates, currentInputResult);
        extractEqualityPredicates(logical.getRight(), predicates, currentInputResult);
      }
    }
  }

  /**
   * Checks if a vertex matches the property filters in the pattern.
   *
   * @param vertex vertex to check
   * @return true if matches or no properties specified
   */
  /**
   * Checks if a vertex has ALL labels specified in the pattern.
   * For single-label patterns, this is handled by type iteration.
   * For multi-label patterns (e.g., :A:B:C), checks type hierarchy.
   */
  private boolean matchesAllLabels(final Vertex vertex) {
    return matchesAllLabels(vertex, null);
  }

  private boolean matchesAllLabels(final Vertex vertex, final Result currentResult) {
    final List<String> labels = resolveEffectiveLabels(currentResult);
    if (labels.size() <= 1)
      return true; // Single label already filtered by iterator
    for (final String label : labels)
      if (!Labels.hasLabel(vertex, label))
        return false;
    return true;
  }

  /**
   * Checks all labels including single labels for bound variables.
   * Unlike matchesAllLabels, this doesn't skip the check for single-label patterns
   * because bound variables bypass the type-filtered iterator.
   */
  private boolean matchesAllLabelsBound(final Vertex vertex) {
    return matchesAllLabelsBound(vertex, null);
  }

  private boolean matchesAllLabelsBound(final Vertex vertex, final Result currentResult) {
    final List<String> labels = resolveEffectiveLabels(currentResult);
    if (labels.isEmpty())
      return true;
    for (final String label : labels)
      if (!Labels.hasLabel(vertex, label))
        return false;
    return true;
  }

  /**
   * Returns the effective labels for this pattern, combining static labels with the results of
   * evaluating any Cypher 25 dynamic {@code $(expression)} labels against the current binding.
   * A dynamic label expression may yield a string (single label) or a list/iterable of strings
   * (multiple labels, all required).
   */
  private List<String> resolveEffectiveLabels(final Result currentInputResult) {
    final List<String> staticLabels = pattern.getLabels();
    if (!pattern.hasDynamicLabels())
      return staticLabels;

    final List<String> result = new ArrayList<>(staticLabels.size() + pattern.getDynamicLabels().size());
    result.addAll(staticLabels);
    for (final Expression dynExpr : pattern.getDynamicLabels()) {
      final Object resolved = evaluator.evaluate(dynExpr, currentInputResult, context);
      appendResolvedLabels(result, resolved);
    }
    return result;
  }

  private static void appendResolvedLabels(final List<String> labels, final Object resolved) {
    if (resolved == null)
      return;
    if (resolved instanceof String) {
      labels.add((String) resolved);
    } else if (resolved instanceof Iterable) {
      for (final Object item : (Iterable<?>) resolved) {
        if (item != null)
          labels.add(item.toString());
      }
    } else {
      labels.add(resolved.toString());
    }
  }

  private boolean matchesProperties(final Vertex vertex) {
    return matchesProperties(vertex, null);
  }

  private boolean matchesProperties(final Vertex vertex, final Result currentResult) {
    if (!pattern.hasProperties()) {
      return true; // No property filters
    }

    // Check each property filter
    for (final Map.Entry<String, Object> entry : pattern.getProperties().entrySet()) {
      final String key = entry.getKey();
      Object expectedValue = entry.getValue();

      // Evaluate Expression-based property values (e.g., event.year)
      if (expectedValue instanceof Expression && currentResult != null)
        expectedValue = evaluator.evaluate((Expression) expectedValue, currentResult, context);

      // Resolve parameter references (e.g., $username -> actual value from context)
      if (expectedValue instanceof CypherASTBuilder.ParameterReference) {
        final String paramName = ((CypherASTBuilder.ParameterReference) expectedValue).getName();
        if (context.getInputParameters() != null)
          expectedValue = context.getInputParameters().get(paramName);
      } else if (expectedValue instanceof String) {
        final String strValue = (String) expectedValue;

        // Check if it's a parameter reference
        if (strValue.startsWith("$")) {
          final String paramName = strValue.substring(1);
          if (context.getInputParameters() != null) {
            final Object paramValue = context.getInputParameters().get(paramName);
            if (paramValue != null)
              expectedValue = paramValue;
          }
        }
        // Handle string literals: remove quotes
        else if (strValue.startsWith("'") && strValue.endsWith("'")) {
          expectedValue = strValue.substring(1, strValue.length() - 1);
        } else if (strValue.startsWith("\"") && strValue.endsWith("\"")) {
          expectedValue = strValue.substring(1, strValue.length() - 1);
        }
      }

      final Object actualValue = vertex.get(key);

      // Compare values with numeric coercion
      if (actualValue == null)
        return false;
      if (!actualValue.equals(expectedValue)) {
        // Numeric type-safe comparison (Integer vs Long)
        if (actualValue instanceof Number && expectedValue instanceof Number) {
          if (((Number) actualValue).longValue() != ((Number) expectedValue).longValue())
            return false;
        } else
          return false;
      }
    }

    return true;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ MATCH NODE ");
    builder.append("(").append(variable);
    if (pattern.hasLabels()) {
      builder.append(":").append(String.join("|", pattern.getLabels()));
    }
    builder.append(")");
    if (usedIndexName != null)
      builder.append(" [index: ").append(usedIndexName).append("]");
    if (whereFilter != null)
      builder.append(" [filter: ").append(whereFilter.getText()).append("]");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
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
