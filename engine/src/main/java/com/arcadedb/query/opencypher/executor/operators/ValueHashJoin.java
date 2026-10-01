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

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.HeapEstimator;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiPredicate;

/**
 * Joins two independently matched parts of a pattern on the values of one or more {@code left = right} conjuncts of
 * the WHERE, instead of crossing them and filtering every pair (issue #8584): one input - the build side - is read
 * once into a hash table keyed by its side of the conjuncts, and each row of the other - the probe side, streamed - is
 * paired only with the build rows of its key. {@code MATCH (a:T), (b:T) WHERE a.x = b.y} costs |a| + |b| instead of
 * |a| x |b|. The build side is the right input, or the left one when that is the smaller: the planner decides.
 * <p>
 * For every probe row, in its order, the matching build rows come in the order the build input produced them: built on
 * the right, the rows come out as a {@link CartesianProduct} filtered on the conjuncts would produce them. A merged row
 * always holds the left properties first. The WHERE is still evaluated above the join, so a pair the key lets through
 * is decided by the comparison itself; the key only has to never lose a pair the comparison accepts (see
 * {@link EquiJoinKey}). A row whose key cannot be hashed is paired with every row of the other side, as a product
 * would.
 * <p>
 * The build rows are held in a {@link RowBuffer}: compact past a few thousand rows when the statement cannot change a
 * record (issue #8583), and bounded by {@link com.arcadedb.GlobalConfiguration#QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP}
 * (issue #8585). The build input is not read at all when the probe one has no row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ValueHashJoin extends AbstractPhysicalOperator {
  private final PhysicalOperator            right;
  private final EquiJoinKey[]               keys;
  private final BiPredicate<Result, Result> pairFilter;
  /** Whether the left input is the one read into the hash table, the right one streamed. */
  private final boolean                     buildLeft;
  private       int                         compactAfterRows = 0;

  /**
   * @param left       the left input
   * @param right      the right input
   * @param keys       the conjuncts the two inputs are joined on
   * @param pairFilter when non-null, tested on every (left, right) pair of the same key before it is merged
   * @param buildLeft  whether the left input is read into the hash table, rather than the right one
   */
  public ValueHashJoin(final PhysicalOperator left, final PhysicalOperator right, final EquiJoinKey[] keys,
      final double estimatedCost, final long estimatedCardinality, final BiPredicate<Result, Result> pairFilter,
      final boolean buildLeft) {
    super(left, estimatedCost, estimatedCardinality);
    this.right = right;
    this.keys = keys;
    this.pairFilter = pairFilter;
    this.buildLeft = buildLeft;
  }

  /** See {@link CartesianProduct#setCompactAfterRows(int)}. */
  public void setCompactAfterRows(final int compactAfterRows) {
    this.compactAfterRows = compactAfterRows;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    return new ResultSet() {
      private ResultSet probeResults;
      private RowBuffer buildRows;
      /** Key -> the build rows holding it: an Integer for one row, an {@link IntList} for more, ascending. */
      private Map<Object, Object> rowsByKey;
      /** The build rows whose key cannot be hashed, ascending: every probe row is paired with them. */
      private IntList unhashableRows;
      private OperationHeapLimit joinLimit;
      private boolean initialized = false;
      private boolean finished    = false;

      private Result currentProbe;
      // The build rows the current probe row is paired with: every row, or the rows of its key merged with the
      // unhashable ones, both ascending, walked side by side without building the merged list
      private boolean allCandidates;
      private int     allCount;
      private int     allIndex;
      private int[]   keyed;
      private final int[] oneKeyed = new int[1];
      private int     keyedCount;
      private int     keyedIndex;
      private int     unhashableIndex;
      private Result  pending;

      @Override
      public boolean hasNext() {
        if (pending != null)
          return true;
        if (finished)
          return false;
        if (!initialized)
          initialize();
        return advance();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result buildRow = pending;
        pending = null;

        final Result leftRow = buildLeft ? buildRow : currentProbe;
        final Result rightRow = buildLeft ? currentProbe : buildRow;
        // The two parts of a pattern bind disjoint variables, the synthetic ones included: the right properties overwrite
        // none of the left ones, unlike the rows NodeHashJoin merges, which share the variable they are joined on
        final ResultInternal merged = new ResultInternal();
        for (final String property : leftRow.getPropertyNames())
          merged.setProperty(property, leftRow.getProperty(property));
        for (final String property : rightRow.getPropertyNames())
          merged.setProperty(property, rightRow.getProperty(property));
        return merged;
      }

      private void initialize() {
        initialized = true;
        probeResults = (buildLeft ? right : child).execute(context, nRecords);
        // Nothing to pair: the build input is never read
        if (!probeResults.hasNext()) {
          finish();
          return;
        }

        // The rows and the hash table over them are charged to one operation, released with the buffer
        joinLimit = OperationHeapLimit.of(context, "hash join");
        final OperationHeapLimit limit = joinLimit;
        buildRows = new RowBuffer(compactAfterRows > 0 ? context.getDatabase() : null, limit, compactAfterRows);
        rowsByKey = new HashMap<>();
        unhashableRows = new IntList();

        final ResultSet buildResults = (buildLeft ? child : right).execute(context, nRecords);
        try {
          while (buildResults.hasNext()) {
            guard.check();
            final Result row = buildResults.next();
            final Object key = EquiJoinKey.canonicalKey(keys, buildLeft, row, context);
            // A build row no key equals is never paired
            if (key == EquiJoinKey.NO_MATCH)
              continue;

            final int index = buildRows.size();
            buildRows.add(row);
            if (key == EquiJoinKey.UNHASHABLE) {
              unhashableRows.add(index);
              limit.charge(Integer.BYTES);
            } else {
              final int sizeBefore = rowsByKey.size();
              rowsByKey.merge(key, index, ValueHashJoin::appendRow);
              if (rowsByKey.size() > sizeBefore)
                // A new key: the table grew by an entry
                limit.charge(HeapEstimator.HASH_ENTRY_BYTES + HeapEstimator.OBJECT_BYTES + HeapEstimator.estimate(key));
              else
                limit.charge(Integer.BYTES);
            }
          }
        } catch (final RuntimeException e) {
          releaseBuffer();
          throw e;
        } finally {
          buildResults.close();
        }

        if (buildRows.size() == 0)
          finish();
      }

      // Walks forward to the next (probe, build) pair of the same key the pair filter accepts
      private boolean advance() {
        while (!finished) {
          if (currentProbe != null) {
            int index;
            while ((index = nextCandidate()) >= 0) {
              guard.check();
              // A compact buffer answers null for a row whose record was deleted since it was buffered
              final Result buildRow = buildRows.get(index);
              if (buildRow != null && (pairFilter == null || (buildLeft ?
                  pairFilter.test(buildRow, currentProbe) : pairFilter.test(currentProbe, buildRow)))) {
                pending = buildRow;
                return true;
              }
            }
          }

          // Nothing left to pair with: every compact build row was found deleted since it was buffered
          if (!probeResults.hasNext() || buildRows.liveSize() == 0) {
            finish();
            break;
          }
          currentProbe = probeResults.next();
          selectCandidates(EquiJoinKey.canonicalKey(keys, !buildLeft, currentProbe, context));
        }
        return false;
      }

      private void selectCandidates(final Object key) {
        allCandidates = false;
        allCount = 0;
        allIndex = 0;
        keyedCount = 0;
        keyedIndex = 0;
        unhashableIndex = unhashableRows.size;
        if (key == EquiJoinKey.NO_MATCH)
          return;
        if (key == EquiJoinKey.UNHASHABLE) {
          allCandidates = true;
          allCount = buildRows.size();
          return;
        }

        final Object matching = rowsByKey.get(key);
        if (matching instanceof Integer single) {
          oneKeyed[0] = single;
          keyed = oneKeyed;
          keyedCount = 1;
        } else if (matching != null) {
          keyed = ((IntList) matching).values;
          keyedCount = ((IntList) matching).size;
        }
        // Every probe row is paired with the unhashable build rows too
        unhashableIndex = 0;
      }

      // The next build row to pair the current probe row with, in the order the build input produced them, or -1
      private int nextCandidate() {
        if (allCandidates)
          return allIndex < allCount ? allIndex++ : -1;
        final boolean keyedLeft = keyedIndex < keyedCount;
        final boolean unhashableLeft = unhashableIndex < unhashableRows.size;
        if (keyedLeft && (!unhashableLeft || keyed[keyedIndex] < unhashableRows.values[unhashableIndex]))
          return keyed[keyedIndex++];
        if (unhashableLeft)
          return unhashableRows.values[unhashableIndex++];
        return -1;
      }

      private void finish() {
        finished = true;
        currentProbe = null;
        if (probeResults != null) {
          probeResults.close();
          probeResults = null;
        }
        // No probe row is left to pair: the hash table goes now, not when a close() the consumer may never call comes
        releaseBuffer();
      }

      private void releaseBuffer() {
        if (buildRows != null)
          buildRows.clear();
        rowsByKey = null;
        unhashableRows = null;
        // The rows of the last key are an array of the table: it goes with it
        keyed = null;
        keyedCount = 0;
        // The hash table goes with the rows
        if (joinLimit != null)
          joinLimit.release();
      }

      @Override
      public void close() {
        pending = null;
        finish();
      }
    };
  }

  /** Adds a row to the ones of a key, keeping them in the order the build input produced them. */
  private static Object appendRow(final Object existing, final Object added) {
    if (existing instanceof IntList list) {
      list.add((Integer) added);
      return list;
    }
    final IntList list = new IntList();
    list.add((Integer) existing);
    list.add((Integer) added);
    return list;
  }

  @Override
  public String getOperatorType() {
    return "ValueHashJoin";
  }

  @Override
  public String explain(final int depth) {
    final String indent = getIndent(depth);
    final StringBuilder sb = new StringBuilder();
    sb.append(indent).append("+ ValueHashJoin [on=");
    for (int i = 0; i < keys.length; i++) {
      if (i > 0)
        sb.append(" AND ");
      sb.append(keys[i].getText());
    }
    sb.append("]");
    if (buildLeft)
      sb.append(" [build=left]");
    if (pairFilter != null)
      sb.append(" [RelationshipUniquenessFilter pushed into join]");
    if (compactAfterRows > 0)
      sb.append(" [compact buffer]");
    sb.append(" [cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");
    if (child != null)
      sb.append(child.explain(depth + 1));
    sb.append(right.explain(depth + 1));
    return sb.toString();
  }

  /** The right input; the left one is {@link #getChild()}. */
  public PhysicalOperator getRight() {
    return right;
  }

  public EquiJoinKey[] getKeys() {
    return keys;
  }

  /** A growable list of int, the rows of one key. */
  static final class IntList {
    int[] values = new int[4];
    int   size   = 0;

    void add(final int value) {
      if (size == values.length)
        values = Arrays.copyOf(values, size << 1);
      values[size++] = value;
    }
  }
}
