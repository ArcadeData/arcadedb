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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.ConsistentKeyLookup;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;

import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.executor.RowIndependentValues;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * Physical operator that performs an index seek for vertices.
 * Uses a type index to efficiently find vertices matching an equality predicate.
 *
 * This is typically 10-1000x faster than a full table scan for selective queries.
 *
 * Cost: O(log N + M) where N is index size, M is matching rows
 * Cardinality: Estimated based on selectivity
 */
public class NodeIndexSeek extends AbstractPhysicalOperator {
  private final String       variable;
  private final String       label;
  private final String       propertyName;
  private final Object       propertyValue;
  private final String       indexName;
  /** Every property of the chosen index, in key order. */
  private final List<String> indexProperties;
  /**
   * The equality values covering a leading prefix of the index key, in key order. The first element
   * corresponds to {@link #propertyName} and may be an {@link InListValues}; a list shorter than
   * {@link #indexProperties} makes this a prefix scan instead of a single-entry lookup.
   */
  private final List<Object> keyValues;
  /** Predicates reading only {@link #variable}, evaluated once per seeked vertex instead of once per expanded row. */
  private BooleanExpression whereFilter;
  /** Whether the statement can write a key of the index, see {@link #setStatementWritesKey}. */
  private boolean statementWritesKey;

  public NodeIndexSeek(final String variable, final String label, final String propertyName,
                      final Object propertyValue, final String indexName,
                      final double estimatedCost, final long estimatedCardinality) {
    this(variable, label, propertyName, propertyValue, indexName, List.of(propertyName), List.of(propertyValue),
        estimatedCost, estimatedCardinality);
  }

  public NodeIndexSeek(final String variable, final String label, final String propertyName,
                      final Object propertyValue, final String indexName, final List<String> indexProperties,
                      final List<Object> keyValues, final double estimatedCost, final long estimatedCardinality) {
    super(estimatedCost, estimatedCardinality);
    this.variable = variable;
    this.label = label;
    this.propertyName = propertyName;
    this.propertyValue = propertyValue;
    this.indexName = indexName;
    this.indexProperties = indexProperties == null || indexProperties.isEmpty() ? List.of(propertyName) : indexProperties;
    this.keyValues = keyValues == null || keyValues.isEmpty() ? List.of(propertyValue) : keyValues;
  }

  /**
   * Adds a predicate the seek can decide on its own (issue #8537). The seek already answers the equality it was built
   * from, but only through the index, whose key conversion is not Cypher's equality: a STRING parameter is converted to
   * a LONG key and finds the vertex Cypher's {@code =} would reject. The predicate is therefore still evaluated, here,
   * once per vertex the index returns, rather than dropped or left to a Filter above the expansion that re-evaluates it
   * on every row the expansion multiplies the vertex into. Several pushed predicates are ANDed together.
   */
  public void pushDownFilter(final BooleanExpression filter) {
    this.whereFilter = whereFilter == null ?
        filter : new LogicalExpression(LogicalExpression.Operator.AND, whereFilter, filter);
  }

  public BooleanExpression getWhereFilter() {
    return whereFilter;
  }

  /**
   * Tells the seek that the statement can write a key of its index (issue #10251, the Halloween problem). A key shorter
   * than the index walks the range of the entries sharing that prefix, and a write of a trailing key moves the record's
   * entry inside that range, ahead of the cursor when the key grows: the seek then serves each vertex once, as it does
   * across the values of an IN-list, and the entries it moved are passed over. That holds a RID per vertex served, for as
   * long as the seek runs, only for a prefix seek of a statement that writes a key of the index. A whole key is answered
   * at once and needs nothing.
   */
  public void setStatementWritesKey(final boolean statementWritesKey) {
    this.statementWritesKey = statementWritesKey;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    // Bounds this operator's row loop by the command deadline - see WorkGuard for why between-batches is
    // not enough (issue #6266).
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    return new ResultSet() {
      private TypeIndex index = null;
      /** The index is declared on a supertype, so its cursor carries records that are not of {@link #label}. */
      private boolean inheritedIndex = false;
      private List<Object[]> seekKeys = null; // one key per value to seek (IN-list expands to N keys)
      private int seekIndex = 0;
      private boolean wholeKey = false;        // the key covers every index property: single-entry lookup
      private IndexCursor cursor = null;
      private RidHashSet seen = null;          // de-duplicate RIDs across multiple seek values (IN-list set semantics)
      // The RIDs seen are charged to the heap budget all queries share (issue #9792)
      private OperationHeapLimit seenHeapLimit = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean initialized = false;
      private boolean finished = false;

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

      private void initialize() {
        initialized = true;

        final Seek seek = resolveSeek(context);
        if (seek == null) {
          finished = true;
          return;
        }
        index = seek.index();
        inheritedIndex = seek.inherited();
        seekKeys = seek.keys();
        wholeKey = seek.wholeKey();
        // The values of an IN-list are de-duplicated, and so are the RIDs they find, to honor set-membership semantics
        if (propertyValue instanceof InListValues)
          seen = newSeen();

        // A null parameter cut the key short of the index. A hash index answers an exact key only, and a null never equals a value
        // (issue #9236), so there is nothing to read
        if (!seek.readable())
          finished = true;
        else if (seen == null && statementWritesKey && !wholeKey)
          seen = newSeen();
      }

      private RidHashSet newSeen() {
        seenHeapLimit = OperationHeapLimit.of(context, "record addresses", "NodeIndexSeek");
        return new RidHashSet();
      }

      private void releaseSeen() {
        seen = null;
        if (seenHeapLimit != null) {
          seenHeapLimit.release();
          seenHeapLimit = null;
        }
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        if (!initialized) {
          initialize();
          if (finished)
            return;
        }

        while (buffer.size() < n) {
          guard.check();
          // Advance to the next seek value when the current cursor is exhausted.
          if (cursor == null || !cursor.hasNext()) {
            if (cursor != null) {
              // release the drained cursor before opening the next one: a compacted-series cursor holds its file's
              // retire guard, and an IN list opens one cursor per value (#5635)
              cursor.close();
              cursor = null;
            }
            if (seekIndex >= seekKeys.size()) {
              finished = true;
              releaseSeen();
              return;
            }
            if (wholeKey && seekIndex == 0) {
              // every key of the seek is read with its records against ONE committed state, so a commit that deletes and
              // re-creates a key cannot make it vanish or appear twice, nor serve a slot it freed for two keys (#9397). The records of
              // up to ConsistentKeyLookup.MAX_ENTRIES entries are loaded up front, which a LIMIT may not need: the price of one state
              cursor = ConsistentKeyLookup.lookupCursor(context.getDatabase(), index, seekKeys);
              if (cursor != null) {
                seekIndex = seekKeys.size();
                continue;
              }
            }
            final Object[] key = seekKeys.get(seekIndex++);
            cursor = openCursor(index, wholeKey, key);
            continue;
          }

          final Identifiable identifiable = cursor.next();

          // Skip already-emitted vertices when seeking multiple IN-list values (set semantics).
          if (seen != null) {
            if (!seen.add(identifiable.getIdentity()))
              continue;
            seenHeapLimit.charge(RidHashSet.BYTES_PER_ENTRY);
          }

          // Reject a record of another type in the hierarchy without loading it: the bucket the RID names
          // already says which type it belongs to (issue #7021).
          if (inheritedIndex && !Labels.carriesLabel(context.getDatabase().getSchema(), identifiable, label))
            continue;

          // Load the actual record from the identifiable (may be RID)
          final Vertex vertex = identifiable.asVertex();

          // Create result with vertex bound to variable
          final ResultInternal result = new ResultInternal();
          result.setProperty(variable, vertex);

          // Apply the pushed-down anchor predicates
          if (whereFilter != null && !whereFilter.evaluate(result, context))
            continue;

          buffer.add(result);
        }
      }

      @Override
      public void close() {
        // #5635: an index cursor DOES need explicit closing - a compacted-series cursor registers with its file, so a
        // scan abandoned on a LIMIT would keep a retired file alive until the next database restart.
        if (cursor != null) {
          cursor.close();
          cursor = null;
        }
        releaseSeen();
      }
    };
  }

  /**
   * The index a seek reads and the keys it looks up there, with the parameters bound.
   *
   * @param inherited whether the index is declared on a supertype, so its cursor carries records that are not of the label
   * @param keys      one key per value to seek: an IN-list yields one per element
   * @param wholeKey  whether the keys cover every index property, a single-entry lookup each
   */
  private record Seek(TypeIndex index, boolean inherited, List<Object[]> keys, boolean wholeKey) {
    /** Whether there is anything to read: a hash index answers a whole key only. */
    boolean readable() {
      return !keys.isEmpty() && (wholeKey || index.supportsOrderedIterations());
    }
  }

  /**
   * Resolves the index and the keys to seek, with the parameters bound.
   *
   * @return null when no node can match: the label is not a vertex type
   */
  private Seek resolveSeek(final CommandContext context) {
    final DocumentType type = context.getDatabase().getSchema().getType(label);
    // A non-vertex type with the same name (edge/document type) matches no node pattern (issue #5194)
    if (!(type instanceof VertexType))
      return null;

    // Resolve the index by its whole key: a composite index is not registered under the single
    // anchor property, and looking it up by that property alone used to yield no index and,
    // silently, no rows at all (issue #5444).
    TypeIndex index = (TypeIndex) type.getPolymorphicIndexByProperties(indexProperties);
    if (index == null && indexProperties.size() > 1)
      index = (TypeIndex) type.getPolymorphicIndexByProperties(propertyName);
    if (index == null)
      // The planner picked an index that the schema no longer offers. Returning an empty result
      // set here would silently drop rows the query must return, so fail instead.
      throw new CommandExecutionException(
          "Index '" + indexName + "' on type '" + label + "' is no longer available: re-plan the query");

    // The prefix values are shared by every seek; only the leading value varies over an IN-list.
    final List<Object> trailing = new ArrayList<>(keyValues.size() - 1);
    for (int i = 1; i < keyValues.size(); i++) {
      final Object resolved = RowIndependentValues.resolve(keyValues.get(i), context);
      if (resolved == null)
        break; // an unresolved parameter truncates the key, degrading the seek to a shorter prefix
      trailing.add(resolved);
    }

    // Resolve the seek keys at runtime (parameters change per execution). A single equality value
    // yields one key; an IN-list (issue #5306) yields one key per list element.
    final List<Object[]> keys = new ArrayList<>();
    if (propertyValue instanceof InListValues inList) {
      for (final Expression element : inList.getValues()) {
        final Object resolved = RowIndependentValues.resolve(element, context);
        if (inList.isScalar(element) || resolved == null)
          addSeekKey(keys, resolved, trailing);
        else if (inList.isWholeList(element))
          // The whole right side, x IN $ids: a list, or the refusal the scan gives a value that is not one (issue #10541). It used
          // to be taken for one key, which answered no rows where the scan refuses
          for (final Object v : InExpression.listOperand(resolved))
            addSeekKey(keys, v, trailing);
        else
          // One item of a list literal, x IN [$a, $b]: one key, whatever it resolves to (issue #10541). A list or a map is a key no
          // property holds, and the index answers it with no row, as the comparison of the scan does
          addSeekKey(keys, resolved, trailing);
      }
    } else
      addSeekKey(keys, RowIndependentValues.resolve(propertyValue, context), trailing);

    // An index inherited from a supertype spans every bucket in the hierarchy, so its cursor also yields
    // the parent's own records and every sibling child's. Those are not answers to (n:label) and are
    // dropped by the seek - the same thing the SQL plan for this query spells as FILTER ITEMS BY TYPE (#7021).
    return new Seek(index, Labels.isInheritedIndex(index, label), keys, 1 + trailing.size() == index.getPropertyNames().size());
  }

  private static void addSeekKey(final List<Object[]> keys, final Object leadingValue, final List<Object> trailing) {
    if (leadingValue == null)
      return;

    final Object[] key = new Object[1 + trailing.size()];
    key[0] = leadingValue;
    for (int i = 0; i < trailing.size(); i++)
      key[1 + i] = trailing.get(i);
    keys.add(key);
  }

  /**
   * A key covering every index property identifies at most one entry per RID; a shorter one matches a contiguous range of the
   * ordered index, which only the range cursor can walk.
   */
  private static IndexCursor openCursor(final TypeIndex index, final boolean wholeKey, final Object[] key) {
    return wholeKey ? index.get(key) : index.range(true, key, true, key, true);
  }

  /**
   * Counts the index entries the seek finds, up to a cap, without loading a record (issue #11889). The planner's estimate of an
   * equality on a NOTUNIQUE key is an average over the keys, which says little of the one sought; this reads what it finds,
   * for what reading at most {@code cap} index entries costs. An entry of an index declared on a supertype is counted only when
   * its record carries the label, read from its bucket as the seek reads it. The values of an IN-list are counted one by one,
   * a vertex two of them find twice.
   *
   * @return the entries the seek finds, or {@code cap} when it finds that many or more
   */
  public long countEntries(final CommandContext context, final long cap) {
    final Seek seek = resolveSeek(context);
    if (seek == null || !seek.readable())
      return 0L;
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    long entries = 0L;
    int read = 0;
    for (final Object[] key : seek.keys()) {
      final IndexCursor cursor = openCursor(seek.index(), seek.wholeKey(), key);
      try {
        while (entries < cap && cursor.hasNext()) {
          guard.checkPeriodically(++read);
          final Identifiable entry = cursor.next();
          if (!seek.inherited() || Labels.carriesLabel(context.getDatabase().getSchema(), entry, label))
            ++entries;
        }
      } finally {
        cursor.close();
      }
      if (entries >= cap)
        break;
    }
    return entries;
  }

  @Override
  public String getOperatorType() {
    return "NodeIndexSeek";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);

    sb.append(indent).append("+ NodeIndexSeek");
    sb.append("(").append(variable).append(":").append(label).append(")");
    sb.append(" [index=").append(indexName);
    // Show every key column the seek resolves, so a prefix seek on a composite index is recognizable
    for (int i = 0; i < keyValues.size(); i++)
      sb.append(", ").append(i < indexProperties.size() ? indexProperties.get(i) : propertyName)
          .append("=").append(keyValues.get(i) instanceof Expression expression ? expression.getText() : keyValues.get(i));
    if (whereFilter != null)
      sb.append(", filter: ").append(whereFilter.getText());
    sb.append(", cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");

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

  public List<String> getIndexProperties() {
    return indexProperties;
  }

  public Object getPropertyValue() {
    return propertyValue;
  }

  public String getIndexName() {
    return indexName;
  }
}
