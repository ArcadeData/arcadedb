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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.opencypher.optimizer.RangePredicate;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.PhysicalOrderRidFetcher;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * Physical operator that performs an index range scan for vertices.
 * Uses a range index to efficiently find vertices matching range predicates (>, <, >=, <=).
 *
 * Examples:
 * - WHERE age > 18 AND age < 65
 * - WHERE date >= $start
 * - WHERE price <= 100
 *
 * Cost: O(log N + M) where N is index size, M is matching rows
 * Cardinality: Estimated based on range selectivity (typically 10-30%)
 */
public class NodeIndexRangeScan extends AbstractPhysicalOperator {
  private final String variable;
  private final String label;
  private final String propertyName;
  private final List<RangePredicate> predicates;  // Store predicates for parameter resolution
  private final String indexName;
  /** Every property of the chosen index, in key order: a composite index is not registered under a single one. */
  private final List<String> indexProperties;
  private boolean adaptive;
  /**
   * How the calling thread's last execution was served, for the PROFILE that follows it on the same thread. Per thread
   * because the operator belongs to a cached plan that concurrent executions share; null before it decided. Not cleared
   * on close(): the PROFILE text is rendered after the execution closes. What stays behind is one Boolean per worker
   * thread per cached plan, weakly keyed on this operator, so it goes with the plan.
   */
  private final ThreadLocal<Boolean> servedByScan = new ThreadLocal<>();

  /**
   * Create a range scan operator from range predicates.
   * Predicates may contain parameters which are resolved at execution time.
   *
   * @param variable variable name to bind results to
   * @param label vertex type/label
   * @param propertyName indexed property
   * @param predicates range predicates (may contain parameters)
   * @param indexName name of the index being used
   * @param estimatedCost estimated cost from optimizer
   * @param estimatedCardinality estimated result count
   */
  public NodeIndexRangeScan(final String variable, final String label, final String propertyName,
                           final List<RangePredicate> predicates, final String indexName,
                           final double estimatedCost, final long estimatedCardinality) {
    this(variable, label, propertyName, predicates, indexName, List.of(propertyName), estimatedCost, estimatedCardinality);
  }

  public NodeIndexRangeScan(final String variable, final String label, final String propertyName,
                           final List<RangePredicate> predicates, final String indexName,
                           final List<String> indexProperties,
                           final double estimatedCost, final long estimatedCardinality) {
    super(estimatedCost, estimatedCardinality);
    this.variable = variable;
    this.label = label;
    this.propertyName = propertyName;
    this.predicates = predicates;
    this.indexName = indexName;
    this.indexProperties = indexProperties == null || indexProperties.isEmpty() ? List.of(propertyName) : indexProperties;
  }

  /**
   * Lets the scan read the matching index entries first and then either load the records in physical order or give
   * way to a scan of the label, see {@link PhysicalOrderRidFetcher} (issue #8333). The plan enables it only where the
   * order the rows arrive in cannot show in the output, which is also where no LIMIT can stop the range early: the
   * decision reads the whole range before the first row, while a plain range scan streams the first rows at once.
   */
  public void setAdaptive(final boolean adaptive) {
    this.adaptive = adaptive;
  }

  public boolean isAdaptive() {
    return adaptive;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    // Bounds this operator's row loop by the command deadline - see WorkGuard for why between-batches is
    // not enough (issue #6266).
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    return new ResultSet() {
      private IndexCursor cursor = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      private boolean opened = false;
      private RangeIndex rangeIndex;
      // Resolved bounds (after parameter resolution)
      private Object resolvedLowerBound = null;
      private boolean resolvedLowerInclusive = false;
      private Object resolvedUpperBound = null;
      private boolean resolvedUpperInclusive = false;
      // Adaptive mode: records served in physical order, or every vertex of the label
      private PhysicalOrderRidFetcher fetcher;
      private Iterator<Record> labelScan;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size()) {
          return true;
        }

        if (finished) {
          return false;
        }

        fetchMore(nRecords > 0 ? nRecords : 100);
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

        if (!opened) {
          opened = true;
          if (!resolveIndexAndBounds()) {
            finished = true;
            return;
          }
          if (adaptive && chooseAdaptively()) {
            fetchAdaptively(n);
            return;
          }
          cursor = openCursor();
        } else if (fetcher != null || labelScan != null) {
          fetchAdaptively(n);
          return;
        }

        // Fetch up to n matching vertices, in key order
        while (buffer.size() < n && cursor.hasNext()) {
          guard.check();
          final Identifiable identifiable = cursor.next();
          addVertex(identifiable.asVertex());
        }

        if (!cursor.hasNext()) {
          finished = true;
        }
      }

      /**
       * Reads the matching entries alone and decides how to serve them.
       *
       * @return false when the decision cannot be taken (the label's buckets keep no record count), leaving the plain
       * range scan to run
       */
      private boolean chooseAdaptively() {
        // Read per execution, not at planning: the plan is cached, and buckets can be added to the type in between
        final DocumentType type = context.getDatabase().getSchema().getType(label);
        final List<Integer> bucketIds = new ArrayList<>();
        for (final Bucket bucket : type.getBuckets(true))
          bucketIds.add(bucket.getFileId());
        final long scanThreshold = PhysicalOrderRidFetcher.scanThreshold(context.getDatabase(), bucketIds);
        if (scanThreshold < 0)
          return false;

        fetcher = new PhysicalOrderRidFetcher(() -> {
          final IndexCursor pass = openCursor();
          return new PhysicalOrderRidFetcher.Source() {
            @Override
            public Object next() {
              return pass.hasNext() ? pass.next().getIdentity() : null;
            }

            @Override
            public void close() {
              pass.close();
            }
          };
        }, scanThreshold);

        if (fetcher.start(guard) == PhysicalOrderRidFetcher.Outcome.SCAN) {
          fetcher.close();
          fetcher = null;
          // Every vertex of the label: the pattern's WHERE is evaluated downstream on each of them, exactly as it is
          // on what the index returns, so the rows that survive are the same
          labelScan = context.getDatabase().iterateType(label, true);
          servedByScan.set(true);
        } else
          servedByScan.set(false);
        return true;
      }

      private void fetchAdaptively(final int n) {
        if (labelScan != null) {
          while (buffer.size() < n && labelScan.hasNext()) {
            guard.check();
            addVertex(labelScan.next().asVertex());
          }
          if (!labelScan.hasNext())
            finished = true;
          return;
        }

        while (buffer.size() < n) {
          final Object entry = fetcher.next();
          if (entry == null) {
            finished = true;
            fetcher.close();
            return;
          }
          guard.check();
          try {
            addVertex(entry instanceof RID rid ?
                context.getDatabase().lookupByRID(rid, true).asVertex() :
                ((Identifiable) entry).asVertex());
          } catch (final RecordNotFoundException e) {
            // Deleted since the index answered: nothing to match
          }
        }
      }

      private void addVertex(final Vertex vertex) {
        final ResultInternal result = new ResultInternal();
        result.setProperty(variable, vertex);
        buffer.add(result);
      }

      /**
       * Resolves the index and the bounds, with the parameters bound.
       *
       * @return false when no node can match (the label is not a vertex type, the index is not a range index)
       */
      private boolean resolveIndexAndBounds() {
        final DocumentType type = context.getDatabase().getSchema().getType(label);
        // A non-vertex type with the same name (edge/document type) matches no node pattern (issue #5194)
        if (!(type instanceof VertexType))
          return false;

        // Resolve the index by its whole key: a composite index is not registered under the single
        // anchor property, and looking it up by that property alone yielded no index and, silently,
        // no rows at all (issue #5444). Its leading column still bounds a contiguous key range, so a
        // one-element bound is a valid prefix bound.
        TypeIndex typeIndex = (TypeIndex) type.getPolymorphicIndexByProperties(indexProperties);
        if (typeIndex == null && indexProperties.size() > 1)
          typeIndex = (TypeIndex) type.getPolymorphicIndexByProperties(propertyName);
        if (typeIndex == null)
          // The planner picked an index the schema no longer offers. An empty result set here would
          // silently drop rows the query must return, so fail instead.
          throw new CommandExecutionException(
              "Index '" + indexName + "' on type '" + label + "' is no longer available: re-plan the query");

        if (!(typeIndex instanceof RangeIndex))
          return false;

        rangeIndex = (RangeIndex) typeIndex;

        // Resolve bounds from predicates (may involve parameter resolution)
        for (final RangePredicate predicate : predicates) {
          // Resolve the value (may be a parameter)
          Object value = predicate.getValue();
          if (predicate.isParameter() && context != null && context.getInputParameters() != null) {
            // Resolve parameter at execution time
            final String paramName = (String) value;
            value = context.getInputParameters().get(paramName);
          }

          if (predicate.isLowerBound()) {
            resolvedLowerBound = value;
            resolvedLowerInclusive = predicate.isInclusive();
          } else if (predicate.isUpperBound()) {
            resolvedUpperBound = value;
            resolvedUpperInclusive = predicate.isInclusive();
          }
        }

        // A bound whose type does not match the index key type cannot be pushed into the index: the
        // engine would try to coerce/compare it against the (differently typed) stored keys and throw a
        // NumberFormatException or ClassCastException (e.g. `WHERE n.val < "zzz"` on a numeric index -
        // issue #5225). In Cypher a comparison across type categories evaluates to null, so no row
        // qualifies through ordering. Fall back to a plain ascending scan and let the enclosing
        // FilterOperator apply the precise, null-aware predicate on the real record values.
        final Type indexKeyType = keyTypeOf(typeIndex);
        if (!boundMatchesIndexKey(indexKeyType, resolvedLowerBound) || !boundMatchesIndexKey(indexKeyType, resolvedUpperBound)) {
          resolvedLowerBound = null;
          resolvedUpperBound = null;
        }
        return true;
      }

      /**
       * Opens a pass over the range. Every bound is pushed into the cursor, an upper one alone included: the range
       * starts at the first key and stops at the bound by itself, rather than loading every vertex from the first key
       * on to compare it with the bound.
       */
      private IndexCursor openCursor() {
        if (resolvedLowerBound == null && resolvedUpperBound == null)
          return rangeIndex.iterator(true);
        if (resolvedUpperBound == null)
          return rangeIndex.iterator(true, new Object[] { resolvedLowerBound }, resolvedLowerInclusive);
        return rangeIndex.range(true,
            resolvedLowerBound != null ? new Object[] { resolvedLowerBound } : null, resolvedLowerInclusive,
            resolvedUpperBound != null ? new Object[] { resolvedUpperBound } : null, resolvedUpperInclusive);
      }

      @Override
      public void close() {
        // #5635: an index cursor DOES need explicit closing - a compacted-series cursor registers with its file, so a
        // scan abandoned on a LIMIT would keep a retired file alive until the next database restart.
        if (cursor != null) {
          cursor.close();
          cursor = null;
        }
        if (fetcher != null) {
          fetcher.close();
          fetcher = null;
        }
      }
    };
  }

  @Override
  public String getOperatorType() {
    return "NodeIndexRangeScan";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);

    sb.append(indent).append("+ NodeIndexRangeScan");
    sb.append("(").append(variable).append(":").append(label).append(")");
    sb.append(" [index=").append(indexName);
    sb.append(", ").append(propertyName);

    // Build range description from predicates
    for (final RangePredicate predicate : predicates) {
      sb.append(" ");
      if (predicate.isLowerBound()) {
        sb.append(predicate.isInclusive() ? ">=" : ">");
      } else {
        sb.append(predicate.isInclusive() ? "<=" : "<");
      }
      sb.append(" ");
      if (predicate.isParameter()) {
        sb.append("$").append(predicate.getValue());
      } else {
        sb.append(predicate.getValue());
      }
    }

    sb.append(", cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]");
    if (adaptive) {
      final Boolean scan = servedByScan.get();
      sb.append(scan == null ? " [physical order, or label scan on a large range]" :
          scan ? " [served by label scan: large range]" : " [served in physical order]");
    }
    sb.append("\n");

    return sb.toString();
  }

  public String getVariable() {
    return variable;
  }

  public String getLabel() {
    return label;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public List<RangePredicate> getPredicates() {
    return predicates;
  }

  public String getIndexName() {
    return indexName;
  }

  /**
   * Returns the (single-property) key {@link Type} of a range index, or {@code null} when it cannot be
   * determined. Used to detect when a query bound has a type incompatible with the index (issue #5225).
   */
  private static Type keyTypeOf(final TypeIndex index) {
    final Type[] keyTypes = index.getKeyTypes();
    return keyTypes != null && keyTypes.length == 1 ? keyTypes[0] : null;
  }

  /**
   * Tells whether a resolved range bound can be pushed into an index with the given key type. A bound is
   * compatible when it belongs to the same Cypher ordering category (numeric, string, boolean, temporal)
   * as the index key. Cross-category comparisons are undefined (null) in Cypher and, pushed into the
   * index, would throw during key coercion/comparison (issue #5225). A {@code null} bound or unknown
   * category is treated as compatible so existing typed-index paths are never over-restricted.
   */
  private static boolean boundMatchesIndexKey(final Type indexKeyType, final Object bound) {
    if (bound == null || indexKeyType == null)
      return true;
    final int keyCategory = categoryOf(indexKeyType);
    final int boundCategory = categoryOfBound(bound);
    if (keyCategory == 0 || boundCategory == 0)
      return true;
    return keyCategory == boundCategory;
  }

  private static int categoryOf(final Type type) {
    return switch (type) {
      case BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL -> 1;
      case STRING -> 2;
      case BOOLEAN -> 3;
      case DATE, DATETIME -> 4;
      default -> 0;
    };
  }

  private static int categoryOfBound(final Object bound) {
    if (bound instanceof Number)
      return 1;
    if (bound instanceof CharSequence)
      return 2;
    if (bound instanceof Boolean)
      return 3;
    if (bound instanceof Temporal || bound instanceof Date)
      return 4;
    return 0;
  }
}
