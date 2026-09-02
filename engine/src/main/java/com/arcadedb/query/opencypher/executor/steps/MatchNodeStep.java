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
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.graph.IdFunction;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.executor.PartitionPruning;
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
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
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
  // Read-only empty result used to evaluate context-only pattern property expressions (e.g. a parameter
  // map field like $edge_data.uuid) when the pattern has no input row. Row-variable lookups against it
  // return null gracefully. Shared static because it is only ever read; the immutable backing map makes
  // any accidental write fail fast (UnsupportedOperationException) instead of racing shared state across
  // concurrent queries (issue #4909).
  private static final Result       EMPTY_RESULT = new ResultInternal(Collections.emptyMap());
  private final Expression          dynamicIdExpression; // Pre-analyzed expression for runtime RID resolution (issue #3864)
  // Display-only diagnostic fields surfaced through {@link #prettyPrint}. Mirrors the
  // {@code usedIndexName} pattern: written once during the first iterator setup and read by the
  // pretty printer at plan-print time. Not safe to share a step instance across concurrent MATCH
  // branches - if a future change drives parallel sub-plans through the same step, all three fields
  // (including cachedFullScanCandidates below) need to move to a per-execution scope or be guarded.
  private       String              usedIndexName; // Track which index was used (if any)
  private       String              usedPartitionBucket; // Track partition bucket pruning (if any) - same write-once-per-execution contract as usedIndexName
  // Full snapshot of a row-independent full-type-scan's candidates, populated (via recordingIterator) only
  // once the first getVertexIterator() call of a CHAINED match (prev != null) has been fully drained by the
  // caller, and replayed from then on for every later re-open instead of re-opening a live
  // database.iterateType() cursor per outer input row (a chained MATCH, e.g. MATCH (), p0 = (), re-opens
  // that scan once per outer input row). Without this, a vertex a downstream MERGE/CREATE creates
  // mid-query - whether of a brand-new type or of a type that already existed - is visible to a later
  // re-open and retroactively grows this MATCH's cardinality after earlier rows already reached WHERE/MERGE
  // (issue #6602). Only enabled for a pattern this class can prove is 100% independent of the current input
  // row: no ID filter (static or dynamic), no dynamic labels, and (for a single-label pattern) no properties
  // or a WHERE-driven equality predicate on this pattern's own variable that could route to a row-dependent
  // index/partition lookup - see {@link #isRowIndependentFullScan()}. Scoped to prev != null: a standalone
  // MATCH calls getVertexIterator() exactly once regardless, so caching there would only add eager
  // materialization cost (and memory) for large scans with no correctness benefit.
  // <p>
  // Performance: staying null until the first pass is fully drained (rather than eagerly draining on the
  // first call) matters beyond just deferring the cost - it preserves LIMIT push-down/early-termination for
  // the common case where the outer input produces exactly one row (this scan is opened once, satisfies a
  // LIMIT after a few elements, and is never re-opened): the bug this field guards against can only occur
  // from a SECOND re-open onward, so a query shape that never re-opens this scan pays none of the eager-scan
  // cost, exactly like the pre-fix behaviour. See {@link #recordingIterator}.
  // <p>
  // Memory/CPU tradeoff once a genuine second re-open DOES happen: for a chained label-less or multi-label
  // pattern over a very large vertex type, this then holds every candidate's Identifiable in memory for the
  // rest of the step's lifetime instead of streaming a live cursor per re-open. That is a clear win when the
  // pattern is re-opened many times (turns O(N) per outer row into one O(N) resolve), but it does mean the
  // full candidate set is resident rather than GC-able as it streams past - deliberate, since re-opening a
  // live cursor per row is exactly the mechanism issue #6602 exploits, and the RID list is far lighter than
  // the vertices it names.
  private       List<Identifiable>  cachedFullScanCandidates;

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
    this.evaluator = new ExpressionEvaluator(new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance()));
    this.dynamicIdExpression = whereFilter != null ? findIdValueExpression(whereFilter) : null;
    // When the dynamic ID expression handles {@code id(variable) = X} via runtime RID lookup, the
    // same predicate is still present in the WHERE pushdown filter. Re-evaluating it row-by-row
    // would be wasted work, and after id() became Neo4j-compatible (returns Long instead of an RID
    // string, issue #4183) the cross-type comparison Long vs. String/Identifiable would drop the
    // row entirely. Strip the predicate here so the pushdown only carries predicates that the
    // dynamic ID lookup does not already cover. The static idFilter case is handled upstream in
    // {@code CypherExecutionPlan.extractPushdownFilter}.
    this.whereFilter = dynamicIdExpression != null
        ? stripIdEqualityForVariable(whereFilter, variable)
        : whereFilter;
  }

  /**
   * Removes every top-level {@code id(variable) = X} or {@code elementId(variable) = X} equality predicate (in either argument order) from an AND chain.
   * Returns {@code null} when the entire expression was the stripped predicate. Used together with the dynamic ID lookup so the same predicate is not
   * re-evaluated in the row-by-row WHERE filter.
   */
  private static BooleanExpression stripIdEqualityForVariable(final BooleanExpression expr, final String variable) {
    if (expr == null)
      return null;

    if (expr instanceof ComparisonExpression comp
        && comp.getOperator() == ComparisonExpression.Operator.EQUALS
        && (isIdFunctionOn(comp.getLeft(), variable) || isIdFunctionOn(comp.getRight(), variable)))
      return null;

    if (expr instanceof LogicalExpression logical && logical.getOperator() == LogicalExpression.Operator.AND) {
      final BooleanExpression leftStripped = stripIdEqualityForVariable(logical.getLeft(), variable);
      final BooleanExpression rightStripped = stripIdEqualityForVariable(logical.getRight(), variable);
      if (leftStripped != null && rightStripped != null)
        return new LogicalExpression(LogicalExpression.Operator.AND, leftStripped, rightStripped);
      if (leftStripped != null)
        return leftStripped;
      return rightStripped;
    }

    return expr;
  }

  private static boolean isIdFunctionOn(final Expression expr, final String variable) {
    if (!(expr instanceof FunctionCallExpression func))
      return false;
    final String name = func.getFunctionName();
    if (!("id".equalsIgnoreCase(name) || "elementid".equalsIgnoreCase(name)) || func.getArguments().size() != 1)
      return false;
    final Expression arg = func.getArguments().get(0);
    return arg instanceof VariableExpression varExpr && variable.equals(varExpr.getVariableName());
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final boolean hasInput = prev != null;
    // A scan whose filters reject every record spends the whole scan inside a single hasNext(), so the command
    // deadline has to be tested inside the scan loop rather than between two batches (issue #6266).
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

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
            guard.check();
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
                // Load the record if it's not already loaded. A RID drawn from
                // cachedFullScanCandidates (see getVertexIterator) can outlive the vertex it names -
                // a downstream DELETE for an earlier outer row can remove it before a later re-open
                // replays the cached list - so, like every other step in this codebase that resolves
                // a previously-captured RID (FetchFromRidsStep, MatchRelationshipStep, ...), a vertex
                // that no longer exists is skipped rather than surfaced as an uncaught exception.
                final Document record;
                try {
                  record = identifiable.asDocument();
                } catch (final RecordNotFoundException e) {
                  continue;
                }
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
                  cost += System.nanoTime() - begin;
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
            guard.check();
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
                cost += System.nanoTime() - begin;
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
    if (cachedFullScanCandidates != null)
      return cachedFullScanCandidates.iterator();

    final Iterator<Identifiable> computed = computeVertexIterator(currentInputResult);

    // prev != null: only a chained match re-opens this scan per outer input row, which is the only
    // situation where a live re-open can observe a downstream write made mid-query (issue #6602). The
    // bug this guards against can only occur from the SECOND re-open onward - the first open can never
    // observe a downstream write, since nothing downstream has run yet - so the first pass stays fully
    // lazy/streaming (recordingIterator only records what the caller actually consumes) rather than
    // eagerly draining the whole candidate set. That preserves LIMIT push-down/short-circuiting for the
    // common case where the outer input produces exactly one row (e.g.
    // MATCH (a:Anchor {id:$x}), (n) RETURN a, n LIMIT 5) - this scan is opened once, caching would buy
    // it nothing, and eagerly draining it would turn an O(LIMIT) scan into an O(N) one.
    if (prev != null && isRowIndependentFullScan())
      return recordingIterator(computed);

    return computed;
  }

  /**
   * Wraps {@code source} - a freshly computed, row-independent candidate iterator - so it streams lazily
   * exactly like the pre-fix behaviour, while recording every element as the caller consumes it. Only once
   * {@code source} reports {@code hasNext() == false} (fully drained) does the recording get promoted to
   * {@link #cachedFullScanCandidates}, so a genuine second {@link #getVertexIterator} call - the only
   * situation issue #6602 can occur in - replays the cache instead of re-opening a live cursor. If the
   * caller never drains {@code source} to exhaustion (e.g. a LIMIT satisfied after one row, with no second
   * outer row ever produced), no full scan ever happens and no cache is ever populated - exactly the
   * pre-fix cost profile for that case.
   */
  private Iterator<Identifiable> recordingIterator(final Iterator<Identifiable> source) {
    final List<Identifiable> recorded = new ArrayList<>();
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        final boolean hasNext = source.hasNext();
        if (!hasNext && cachedFullScanCandidates == null)
          cachedFullScanCandidates = recorded;
        return hasNext;
      }

      @Override
      public Identifiable next() {
        final Identifiable next = source.next();
        recorded.add(next);
        return next;
      }
    };
  }

  /**
   * True when this pattern's candidate set cannot vary from one input row to the next, so the iterator
   * {@link #computeVertexIterator} returns is safe to materialize once and replay for every later re-open
   * (see {@link #cachedFullScanCandidates}): no ID filter (static or dynamic) and no dynamic labels - either
   * of those can resolve to a different, row-dependent target regardless of label count.
   * <p>
   * Properties and a WHERE-driven equality predicate on <b>this pattern's own variable</b> only disqualify a
   * <b>single-label</b> pattern: that is the only branch of {@code computeVertexIterator}
   * ({@code tryFindAndUseIndex}, {@code tryFindAndUseIndexFromWhere}, {@code tryPartitionPrunedIterator})
   * where a property value or such a predicate can steer the scan itself to a row-dependent index/partition
   * lookup. A label-less or multi-label pattern never takes any of those branches regardless of
   * properties/whereFilter - it always resolves the same full type scan - and both are applied there only as
   * post-fetch per-row filters (see {@code matchesProperties}/the {@code whereFilter.evaluate} call in this
   * step's chained-mode fetch loop), which does not affect what the candidate set is.
   * <p>
   * {@code whereFilter} itself is checked structurally rather than just for {@code != null}:
   * {@code tryFindAndUseIndexFromWhere} only ever acts on a {@code variable.property = <expr>} (or reversed)
   * equality it can find inside {@code whereFilter}'s top-level AND-chain (see
   * {@link #extractEqualityPredicates}, which recurses through AND only, exactly like this check). A pushed
   * -down predicate that mentions unrelated variables - e.g. an UNWIND alias a WHERE clause filters on, with
   * nothing to do with this pattern's own variable - can never route there, so it must not disable caching.
   */
  private boolean isRowIndependentFullScan() {
    if ((idFilter != null && !idFilter.isEmpty()) || dynamicIdExpression != null || pattern.hasDynamicLabels())
      return false;
    final List<String> labels = pattern.getLabels();
    if (labels != null && labels.size() == 1) {
      if (pattern.getProperties() != null && !pattern.getProperties().isEmpty())
        return false;
      if (whereFilterHasEqualityPredicateOnOwnVariable(whereFilter))
        return false;
    }
    return true;
  }

  /**
   * Structural (not value-resolving) mirror of {@link #extractEqualityPredicates}'s predicate recognition:
   * true if {@code expr}'s top-level AND-chain contains a {@code variable.property = <expr>} (or reversed)
   * comparison against this step's own {@link #variable}, regardless of whether that expression could
   * actually be resolved for any given row. Recursing through AND only (not OR/NOT/XOR) matches
   * {@code extractEqualityPredicates} exactly: a predicate {@code extractEqualityPredicates} can never reach
   * is not something {@code tryFindAndUseIndexFromWhere} can act on either, so it must not disqualify caching.
   */
  private boolean whereFilterHasEqualityPredicateOnOwnVariable(final BooleanExpression expr) {
    if (expr instanceof ComparisonExpression comp) {
      if (comp.getOperator() != ComparisonExpression.Operator.EQUALS)
        return false;
      return isPropertyAccessOnOwnVariable(comp.getLeft()) || isPropertyAccessOnOwnVariable(comp.getRight());
    }
    if (expr instanceof LogicalExpression logical && logical.getOperator() == LogicalExpression.Operator.AND)
      return whereFilterHasEqualityPredicateOnOwnVariable(logical.getLeft())
          || whereFilterHasEqualityPredicateOnOwnVariable(logical.getRight());
    return false;
  }

  private boolean isPropertyAccessOnOwnVariable(final Expression expr) {
    return expr instanceof PropertyAccessExpression propAccess && variable.equals(propAccess.getVariableName());
  }

  private Iterator<Identifiable> computeVertexIterator(final Result currentInputResult) {
    // OPTIMIZATION: Resolve ID filter - either static (from plan time) or dynamic (from runtime).
    // Static idFilter handles literals/parameters; dynamicIdExpression handles expressions like
    // BatchEntry.destRID that can only be resolved with the current input row (issue #3864).
    String effectiveIdFilter = this.idFilter;
    if ((effectiveIdFilter == null || effectiveIdFilter.isEmpty())
        && dynamicIdExpression != null && currentInputResult != null) {
      final Object resolved = evaluator.evaluate(dynamicIdExpression, currentInputResult, context);
      if (resolved != null) {
        if (resolved instanceof Identifiable identifiable)
          effectiveIdFilter = identifiable.getIdentity().toString();
        else if (resolved instanceof Number number)
          effectiveIdFilter = IdFunction.decodeLongToRidString(number.longValue());
        else
          effectiveIdFilter = resolved.toString();
      }
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
        // Single label - polymorphic iteration (existing behavior). Resolve the type once and
        // reuse the reference: every {@code getSchema().getType(label)} walks the type map, and
        // this block runs per MATCH iteration, so the redundant lookups landed on a hot path.
        final String label = labels.get(0);

        // If the label does not exist in the schema, the match yields no rows
        // (matches Neo4j semantics, issue #4090). Skip all index/iteration logic.
        if (!context.getDatabase().getSchema().existsType(label))
          return Collections.emptyIterator();

        final DocumentType type = context.getDatabase().getSchema().getType(label);

        // A non-vertex type with the same name (edge/document type) matches no node pattern: labels
        // and relationship types are separate namespaces in Cypher, so yield 0 rows instead of
        // failing with a ClassCastException while casting edges to vertices (issue #5194)
        if (!(type instanceof VertexType))
          return Collections.emptyIterator();

        // OPTIMIZATION: Check if we can use an index for property lookup
        if (type != null && pattern.hasProperties() && !pattern.getProperties().isEmpty()) {
          // Try to find an index that matches the property constraints
          // Support composite indexes with partial keys (leftmost prefix matching)
          final Iterator<Identifiable> indexedIter = tryFindAndUseIndex(type, label, currentInputResult);
          if (indexedIter != null)
            return indexedIter;
        }

        // OPTIMIZATION: Check if WHERE clause has equality predicates that can use an index.
        // Runs for both input-driven MATCH (e.g. UNWIND...MATCH...WHERE a.id = e.src_id, where the
        // predicate references an UNWIND variable) and a leading/seed MATCH with a constant or
        // parameter predicate (WHERE p.id = 42), which has no input row. Without this, a write
        // statement whose leading MATCH is routed to the legacy path (e.g. MATCH...CREATE...CREATE)
        // would full-scan the type instead of using the unique index (issue #5107). Predicate values
        // that cannot be resolved without an input row are skipped inside extractEqualityPredicates,
        // falling back to the full scan + row-level whereFilter, so correctness is preserved.
        if (type != null && whereFilter != null) {
          final Iterator<Identifiable> indexedIter = tryFindAndUseIndexFromWhere(type, label, currentInputResult);
          if (indexedIter != null)
            return indexedIter;
        }

        // OPTIMIZATION: Partition-aware bucket pruning. When the type uses a partitioned bucket
        // strategy and the node-pattern's inline properties bind every partition property to a
        // literal, switch the full-scan fallback to a single-bucket iteration. The partition
        // strategy invariant guarantees every match for those property values lives in the
        // hash-target bucket, so iterating other buckets is wasted work. Skipped when the type's
        // {@code needsRepartition} flag is set (with a throttled WARNING).
        // <p>
        // Use {@code !getProperties().isEmpty()} directly: {@link NodePattern#hasProperties}
        // is true when there are inline properties OR a parameter-form ({$props}); the latter
        // can't be evaluated at plan time and pruning would have to bail anyway.
        // <p>
        // <b>Asymmetry vs. SQL.</b> The SQL planner (see
        // {@code SelectExecutionPlanner#derivePartitionPrunedClusters}) prunes the bucket set
        // before the index-vs-scan decision, so even index-based fetch steps inherit the pruned
        // cluster filter. This Cypher path runs as a full-scan fallback only - the two
        // {@code tryFindAndUseIndex*} branches above intentionally pre-empt it because an index
        // already constrains the result set tightly enough that the per-bucket fanout is not
        // the bottleneck. Correctness is preserved either way; the optimisation is just
        // narrower on the Cypher side. If we ever want full SQL parity, the bucket prune would
        // have to feed into the index-iteration step rather than gate a separate iterator path.
        if (type != null && !pattern.getProperties().isEmpty()) {
          final Iterator<Identifiable> partitionedIter = tryPartitionPrunedIterator(type, label);
          if (partitionedIter != null)
            return partitionedIter;
        }

        // No index available - fall back to full type scan
        if (type != null) {
          @SuppressWarnings("unchecked") final Iterator<Identifiable> iter =
              (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(label, true);
          return iter;
        }
        return Collections.emptyIterator();
      }

      // Multiple labels - the iteration semantics depend on whether the labels are combined with OR
      // (disjunction, e.g. (n:A|B)) or AND (conjunction, e.g. (n:A:B)). Which types that selects is decided by
      // Labels, the one place that knows what a disjunction means, so a node pattern is scanned the same way
      // wherever it is written - here as the anchor of a MATCH, or as the start of a pattern comprehension,
      // which used to take the first label and nothing else (issues #6338, #6352).
      // <p>
      // isRowIndependentFullScan() relies on this branch - like the no-label one below - never routing
      // through the index/partition-pruning branches above (tryFindAndUseIndex,
      // tryFindAndUseIndexFromWhere, tryPartitionPrunedIterator), which is why it treats properties/WHERE
      // as safe to ignore for a multi-label or label-less pattern. If a future change ever gives this
      // branch an index/partition fast path of its own, isRowIndependentFullScan() must be revisited too.
      @SuppressWarnings("unchecked") final Iterator<Identifiable> labelled =
          (Iterator<Identifiable>) (Object) Labels.iterateMatchingVertices(context.getDatabase(), labels,
              pattern.isLabelDisjunction());
      return labelled;
    }

    // No label specified - iterate ALL vertex types, which is the same rule with nothing to satisfy - and,
    // like the multi-label branch above, relied on by isRowIndependentFullScan() for the same reason.
    @SuppressWarnings("unchecked") final Iterator<Identifiable> everyVertex =
        (Iterator<Identifiable>) (Object) Labels.iterateMatchingVertices(context.getDatabase(), labels, false);
    return everyVertex;
  }

  private Iterator<Identifiable> tryPartitionPrunedIterator(final DocumentType type, final String label) {
    final String bucketName = PartitionPruning.prunedBucketName(type, pattern.getProperties());
    if (bucketName == null)
      return null;

    usedPartitionBucket = bucketName;
    @SuppressWarnings("unchecked")
    final Iterator<Identifiable> iter = (Iterator<Identifiable>) (Object) context.getDatabase().iterateBucket(bucketName);
    return iter;
  }

  private Iterator<Identifiable> tryFindAndUseIndex(final DocumentType type, final String label,
      final Result currentInputResult) {
    // Prepare property names and values from the pattern
    final Map<String, Object> properties = new LinkedHashMap<>();
    for (final Map.Entry<String, Object> entry : pattern.getProperties().entrySet()) {
      final String propertyName = entry.getKey();
      Object propertyValue = entry.getValue();

      // Resolve parameters and dynamic expressions (e.g., e.src_id from UNWIND) against the current input
      // result. A parameter map field like $edge_data.uuid resolves from the context alone, so a bare MATCH
      // with no input row still reaches the index (issue #4909); a value that does not resolve gives up on
      // the index rather than looking up a null key, and the scan that follows filters identically.
      propertyValue = InlineProperties.resolve(propertyValue, currentInputResult, context);
      if (propertyValue == null)
        return null;

      properties.put(propertyName, propertyValue);
    }

    // Find the best index (longest leftmost prefix match)
    TypeIndex bestIndex = null;
    int bestMatchCount = 0;
    List<String> bestMatchedProperties = null;

    // Polymorphic: an index declared on a supertype is inherited by this type - the schema keeps a sub-index
    // for every bucket in the hierarchy - and is exactly as seekable from here as one this type declares
    // itself. Asking for the type's own indexes only left a child type with no usable index at all, since a
    // child cannot own a second index on a property its parent already indexes, and fell back to a full label
    // scan where SQL had always planned a fetch from the inherited index (issue #7021).
    for (final TypeIndex index : type.getAllIndexes(true)) {
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

      // Track which index was used for profiling output, named after the type that DECLARES it: an inherited
      // index reported under the queried type would name an index that does not exist (issue #7021).
      usedIndexName = bestIndex.getTypeName() + "[" + String.join(", ", propertyNames) + "]";

      return lookupByKey(bestIndex, label, propertyNames, propertyValues);
    }

    return null;
  }

  /**
   * Seeks {@code index} for the given key and, when the index is inherited from a supertype, filters its
   * cursor down to the records that really carry {@code label} - the step's own label check short-circuits a
   * single-label pattern on the grounds that the iterator already selected the type, which a polymorphic
   * index does not do (issue #7021).
   */
  private Iterator<Identifiable> lookupByKey(final TypeIndex index, final String label, final String[] propertyNames,
      final Object[] propertyValues) {
    final Iterator<Identifiable> cursor = context.getDatabase().lookupByKey(label, propertyNames, propertyValues);
    return Labels.isInheritedIndex(index, label) ? Labels.filterByLabel(cursor, context.getDatabase(), label) : cursor;
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

    // Polymorphic for the same reason as tryFindAndUseIndex above (issue #7021).
    for (final TypeIndex index : type.getAllIndexes(true)) {
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

      usedIndexName = bestIndex.getTypeName() + "[" + String.join(", ", propertyNames) + "]";

      return lookupByKey(bestIndex, label, propertyNames, propertyValues);
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
        // Resolve the value expression. Literals and $parameters resolve without an input row; a
        // predicate referencing an unbound variable (e.g. a leading MATCH with no input row) cannot
        // be resolved and is skipped for index selection - the query then falls back to the full
        // scan + row-level whereFilter, which is always correct (issue #5107).
        try {
          final Object resolvedValue = evaluator.evaluate(valueExpr, currentInputResult, context);
          if (resolvedValue != null)
            predicates.put(propertyName, resolvedValue);
        } catch (final Exception e) {
          // Unresolvable predicate value: skip it for index selection (correctness preserved by whereFilter).
        }
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
    return Labels.matches(vertex, labels, pattern.isLabelDisjunction());
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
    return Labels.matches(vertex, resolveEffectiveLabels(currentResult), pattern.isLabelDisjunction());
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
    // A row-dependent value evaluated without an input row resolves to null and therefore matches nothing,
    // which is no worse than the previous no-match behavior; a parameter still resolves from the context
    // alone, so a bare MATCH with no input row keeps binding (issue #4909).
    return InlineProperties.matches(vertex, pattern.getProperties(), currentResult, context);
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
    // The ID push-down is the single most consequential thing this step can do - it turns a full type scan (and,
    // when another MATCH NODE follows, the Cartesian product over it) into one lookupByRID - and until issue #6279
    // it was the one optimisation the plan did not name, so nothing but a stopwatch could tell whether it fired.
    // That is exactly what the resolution of issue #3216 asks a user to verify. Printed from the plan-time field
    // rather than recorded during execution, so EXPLAIN answers it without running the query; the dynamic form
    // (issue #3864) resolves per row and can only be named, not valued.
    if (idFilter != null && !idFilter.isEmpty())
      builder.append(" [id: ").append(idFilter).append("]");
    else if (dynamicIdExpression != null)
      builder.append(" [id: per-row]");
    if (usedIndexName != null)
      builder.append(" [index: ").append(usedIndexName).append("]");
    if (usedPartitionBucket != null)
      builder.append(" [partition: ").append(usedPartitionBucket).append("]");
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
