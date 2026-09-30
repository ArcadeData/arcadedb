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
import com.arcadedb.query.sql.executor.QueryHelper;
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
   * Whether the plan relies on the rows coming in index key order, in the direction {@link #ascending} says, to answer
   * the statement's ORDER BY without sorting (issue #8422).
   */
  private boolean indexOrdered;
  private boolean ascending = true;
  /** Where the vertices with no value for the key come in a scan that stands for a whole label, see {@link NullKeys}. */
  private NullKeys nullKeys = NullKeys.NONE;
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

  /**
   * Makes the scan produce its rows in index key order, which the plan then uses in place of a sort (issue #8422): no
   * adaptive serving, which loads the rows in another order.
   *
   * @param ascending    the direction to walk the index in
   * @param nullKeysLast whether the vertices of the label with no value for the key follow the index entries, so that
   *                     a scan with no bound returns the whole label in Cypher order, where null sorts last
   */
  public void setIndexOrder(final boolean ascending, final boolean nullKeysLast) {
    setIndexOrder(ascending, nullKeysLast ? NullKeys.LAST_FROM_LABEL : NullKeys.NONE);
  }

  /**
   * Where the vertices with no value for the key come in a scan that stands for a whole label (issue #8724).
   */
  public enum NullKeys {
    /** None: no vertex can lack a key, or the statement excludes them, and the index holds no null key. */
    NONE,
    /**
     * The index does not hold them and they sort last: after the index entries, found by a scan of the label. Only for an
     * ascending scan, where the first rows of the index are the answer.
     */
    LAST_FROM_LABEL,
    /** The index holds them (NULL_STRATEGY INDEX) and the statement excludes them: the entries with a null key are skipped. */
    SKIPPED_IN_INDEX,
    /**
     * The index holds them (NULL_STRATEGY INDEX) and they are part of the answer: read from the start of the index, where a
     * null key sorts lowest, they come after the values ascending and before them descending, as openCypher sorts them.
     */
    PLACED_FROM_INDEX
  }

  /**
   * Makes the scan produce its rows in index key order, which the plan then uses in place of a sort (issue #8422): no
   * adaptive serving, which loads the rows in another order.
   *
   * @param ascending the direction to walk the index in
   * @param nullKeys  where the vertices with no value for the key come, for a scan with no bound
   */
  public void setIndexOrder(final boolean ascending, final NullKeys nullKeys) {
    this.indexOrdered = true;
    this.ascending = ascending;
    this.nullKeys = nullKeys;
    this.adaptive = false;
  }

  public boolean isIndexOrdered() {
    return indexOrdered;
  }

  public boolean isAscending() {
    return ascending;
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
      // Index order with the null keys last: the label scan that follows the index, for the vertices it does not hold
      private Iterator<Record> nullKeyScan;
      // Index order of an index that holds the null keys: the pass that reads them, from the start of the index
      private IndexCursor nullKeyCursor;
      private boolean nullKeysDone = false;
      // The entries of the index with a value are all read
      private boolean valuesDone = false;

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

        final boolean nullKeysInIndex = nullKeys == NullKeys.SKIPPED_IN_INDEX || nullKeys == NullKeys.PLACED_FROM_INDEX;

        // A null key sorts lowest in the index: descending, the vertices that have one come before the values
        if (nullKeys == NullKeys.PLACED_FROM_INDEX && !ascending && !readNullKeyEntries(n))
          return;

        // Fetch up to n matching vertices, in key order
        while (!valuesDone && buffer.size() < n && cursor.hasNext()) {
          guard.check();
          final Identifiable identifiable = cursor.next();
          if (nullKeysInIndex && isNullKey(cursor.getKeys())) {
            if (ascending)
              continue; // the null keys lead the index: skip them, they are read at the end when they are part of the answer
            valuesDone = true; // descending they trail it: nothing but null keys is left
            break;
          }
          addVertex(identifiable.asVertex());
        }

        if (valuesDone || !cursor.hasNext()) {
          valuesDone = true;
          // Ascending, the vertices that have no key come after the values
          if (nullKeys == NullKeys.PLACED_FROM_INDEX && ascending) {
            if (readNullKeyEntries(n))
              finished = true;
          } else if (nullKeys == NullKeys.LAST_FROM_LABEL)
            fetchNullKeys(n);
          else
            finished = true;
        }
      }

      /**
       * Reads the entries of the index whose key is null, which lead it, into the buffer.
       *
       * @return true when they are all read, false when the buffer filled before
       */
      private boolean readNullKeyEntries(final int n) {
        if (nullKeysDone)
          return true;
        if (nullKeyCursor == null)
          nullKeyCursor = rangeIndex.iterator(true);
        boolean reachedValues = false;
        while (buffer.size() < n && nullKeyCursor.hasNext()) {
          guard.check();
          final Identifiable identifiable = nullKeyCursor.next();
          if (!isNullKey(nullKeyCursor.getKeys())) {
            reachedValues = true;
            break;
          }
          addVertex(identifiable.asVertex());
        }
        if (!reachedValues && nullKeyCursor.hasNext())
          return false; // the buffer filled first: the next call goes on from here
        nullKeyCursor.close();
        nullKeyCursor = null;
        nullKeysDone = true;
        return true;
      }

      private boolean isNullKey(final Object[] keys) {
        return keys == null || keys.length == 0 || keys[0] == null;
      }

      /**
       * The vertices of the label with no value for the key, after the index entries: the index does not hold them,
       * and a null sorts last. Only an ascending scan with no bound asks for them.
       */
      private void fetchNullKeys(final int n) {
        if (nullKeyScan == null)
          nullKeyScan = context.getDatabase().iterateType(label, true);
        while (buffer.size() < n && nullKeyScan.hasNext()) {
          guard.check();
          final Vertex vertex = nullKeyScan.next().asVertex();
          if (vertex.get(propertyName) == null)
            addVertex(vertex);
        }
        if (!nullKeyScan.hasNext())
          finished = true;
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
        // A label scan runs on this thread alone
        final long scanThreshold = PhysicalOrderRidFetcher.scanThreshold(context.getDatabase(), bucketIds, 1);
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
          // Loaded through the buckets, every page read once; a record deleted since the index answered is skipped
          final Object entry = fetcher.nextRecord(context.getDatabase());
          if (entry == null) {
            finished = true;
            fetcher.close();
            fetcher = null;
            return;
          }
          guard.check();
          try {
            addVertex(((Identifiable) entry).asVertex());
          } catch (final RecordNotFoundException e) {
            // An entry that is not a stored record's address is resolved here, and can be gone since the index answered:
            // nothing to match. A record the fetcher loaded itself arrives resolved, a deleted one already skipped
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
        final boolean foldedKeys = typeIndex.getMetadata() != null && typeIndex.getMetadata().hasAnyCaseInsensitive();
        for (final RangePredicate predicate : predicates) {
          // A STARTS WITH range is not a range of a case-insensitive index, whose keys are case-folded (issue #8666)
          if (foldedKeys && predicate.isFromPrefix())
            continue;

          // Resolve the value (may be a parameter)
          Object value = predicate.getValue();
          if (predicate.isParameter() && context != null && context.getInputParameters() != null) {
            // Resolve parameter at execution time
            final String paramName = (String) value;
            value = context.getInputParameters().get(paramName);
          }

          if (predicate.isPrefixSuccessor()) {
            // STARTS WITH (issue #8666): the bound is the string after every one with the prefix. With none (an empty or
            // non-string prefix, a prefix of characters that cannot be bumped) the range is open above, and the
            // predicate the scan stands for is evaluated on each row anyway
            if (!(value instanceof String prefix) || (value = QueryHelper.prefixSuccessor(prefix)) == null)
              continue;
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
       * on to compare it with the bound. A descending pass starts from the upper bound.
       */
      private IndexCursor openCursor() {
        if (!ascending)
          return resolvedLowerBound == null && resolvedUpperBound == null ?
              rangeIndex.iterator(false) :
              rangeIndex.range(false,
                  resolvedUpperBound != null ? new Object[] { resolvedUpperBound } : null, resolvedUpperInclusive,
                  resolvedLowerBound != null ? new Object[] { resolvedLowerBound } : null, resolvedLowerInclusive);
        if (resolvedLowerBound == null && resolvedUpperBound == null)
          // Past the null keys that lead an index holding them, when they are not read here: the first row is then the first value
          return nullKeys == NullKeys.SKIPPED_IN_INDEX || nullKeys == NullKeys.PLACED_FROM_INDEX ?
              rangeIndex.iterator(true, new Object[] { null }, false) : rangeIndex.iterator(true);
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
        if (nullKeyCursor != null) {
          nullKeyCursor.close();
          nullKeyCursor = null;
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
      if (predicate.isPrefixSuccessor())
        sb.append("next(");
      if (predicate.isParameter()) {
        sb.append("$").append(predicate.getValue());
      } else {
        sb.append(predicate.getValue());
      }
      if (predicate.isPrefixSuccessor())
        sb.append(")");
    }

    sb.append(", cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]");
    if (indexOrdered)
      sb.append(ascending ? " [index order" : " [index order, descending").append(switch (nullKeys) {
        case LAST_FROM_LABEL -> ", then null keys]";
        case PLACED_FROM_INDEX -> ", null keys from the index]";
        default -> "]";
      });
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

  public List<String> getIndexProperties() {
    return indexProperties;
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
