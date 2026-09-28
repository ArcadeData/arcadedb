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
 * the WHERE, instead of crossing them and filtering every pair (issue #8584): the right input is read once into a
 * hash table keyed by its side of the conjuncts, and each left row is paired only with the right rows of its key.
 * {@code MATCH (a:T), (b:T) WHERE a.x = b.y} costs |a| + |b| instead of |a| x |b|.
 * <p>
 * The rows come out as a {@link CartesianProduct} filtered on the conjuncts would produce them: for every left row, in
 * its order, the matching right rows in the order the right input produced them. The WHERE is still evaluated above
 * the join, so a pair the key lets through is decided by the comparison itself; the key only has to never lose a pair
 * the comparison accepts (see {@link EquiJoinKey}). A row whose key cannot be hashed is paired with every row of the
 * other side, as a product would.
 * <p>
 * The right rows are held in a {@link RowBuffer}: compact past a few thousand rows when the statement does not write
 * (issue #8583), and bounded by {@link com.arcadedb.GlobalConfiguration#QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP}
 * (issue #8585). The right input is not read at all when the left one has no row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ValueHashJoin extends AbstractPhysicalOperator {
  private final PhysicalOperator            right;
  private final EquiJoinKey[]               keys;
  private final BiPredicate<Result, Result> pairFilter;
  private       int                         compactAfterRows = 0;

  /**
   * @param left       the input streamed and probed, one row at a time
   * @param right      the input read into the hash table
   * @param keys       the conjuncts the two inputs are joined on
   * @param pairFilter when non-null, tested on every (left, right) pair of the same key before it is merged
   */
  public ValueHashJoin(final PhysicalOperator left, final PhysicalOperator right, final EquiJoinKey[] keys,
      final double estimatedCost, final long estimatedCardinality, final BiPredicate<Result, Result> pairFilter) {
    super(left, estimatedCost, estimatedCardinality);
    this.right = right;
    this.keys = keys;
    this.pairFilter = pairFilter;
  }

  /** See {@link CartesianProduct#setCompactAfterRows(int)}. */
  public void setCompactAfterRows(final int compactAfterRows) {
    this.compactAfterRows = compactAfterRows;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    return new ResultSet() {
      private ResultSet leftResults;
      private RowBuffer rightRows;
      /** Key -> the right rows holding it: an Integer for one row, an {@link IntList} for more, ascending. */
      private Map<Object, Object> rowsByKey;
      /** The right rows whose key cannot be hashed, ascending: every left row is paired with them. */
      private IntList unhashableRows;
      private boolean initialized = false;
      private boolean finished    = false;

      private Result currentLeft;
      // The right rows the current left row is paired with: either every row, or the ones listed in candidates
      private boolean allCandidates;
      private int[]   candidates;
      private final int[] oneCandidate = new int[1];
      private int     candidateCount;
      private int     candidateIndex;
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
        final Result rightRow = pending;
        pending = null;

        final ResultInternal merged = new ResultInternal();
        for (final String property : currentLeft.getPropertyNames())
          merged.setProperty(property, currentLeft.getProperty(property));
        for (final String property : rightRow.getPropertyNames())
          merged.setProperty(property, rightRow.getProperty(property));
        return merged;
      }

      private void initialize() {
        initialized = true;
        leftResults = child.execute(context, nRecords);
        // Nothing to pair: the right input is never read
        if (!leftResults.hasNext()) {
          finish();
          return;
        }

        // The rows and the hash table over them are charged to one operation, released with the buffer
        final OperationHeapLimit limit = OperationHeapLimit.of(context, "hash join");
        rightRows = new RowBuffer(compactAfterRows > 0 ? context.getDatabase() : null, limit, compactAfterRows);
        rowsByKey = new HashMap<>();
        unhashableRows = new IntList();

        final ResultSet rightResults = right.execute(context, nRecords);
        try {
          while (rightResults.hasNext()) {
            guard.check();
            final Result row = rightResults.next();
            final Object key = EquiJoinKey.canonicalKey(keys, false, row, context);
            // A right row no key equals is never paired
            if (key == EquiJoinKey.NO_MATCH)
              continue;

            final int index = rightRows.size();
            rightRows.add(row);
            if (key == EquiJoinKey.UNHASHABLE) {
              unhashableRows.add(index);
              limit.charge(Integer.BYTES);
            } else {
              final Integer boxed = index;
              // merge() hands back the very value it was given when the key is new: the table grew by an entry
              if (rowsByKey.merge(key, boxed, ValueHashJoin::appendRow) == boxed)
                limit.charge(HeapEstimator.HASH_ENTRY_BYTES + HeapEstimator.OBJECT_BYTES + HeapEstimator.estimate(key));
              else
                limit.charge(Integer.BYTES);
            }
          }
        } catch (final RuntimeException e) {
          releaseBuffer();
          throw e;
        } finally {
          rightResults.close();
        }

        if (rightRows.size() == 0)
          finish();
      }

      // Walks forward to the next (left, right) pair of the same key the pair filter accepts
      private boolean advance() {
        while (!finished) {
          if (currentLeft != null) {
            while (candidateIndex < candidateCount) {
              guard.check();
              final int index = allCandidates ? candidateIndex : candidates[candidateIndex];
              ++candidateIndex;
              // A compact buffer answers null for a row whose record was deleted since it was buffered
              final Result rightRow = rightRows.get(index);
              if (rightRow != null && (pairFilter == null || pairFilter.test(currentLeft, rightRow))) {
                pending = rightRow;
                return true;
              }
            }
          }

          // Nothing left to pair with: every compact right row was found deleted since it was buffered
          if (!leftResults.hasNext() || rightRows.liveSize() == 0) {
            finish();
            break;
          }
          currentLeft = leftResults.next();
          selectCandidates(EquiJoinKey.canonicalKey(keys, true, currentLeft, context));
        }
        return false;
      }

      private void selectCandidates(final Object key) {
        candidateIndex = 0;
        allCandidates = false;
        if (key == EquiJoinKey.NO_MATCH) {
          candidateCount = 0;
          return;
        }
        if (key == EquiJoinKey.UNHASHABLE) {
          allCandidates = true;
          candidateCount = rightRows.size();
          return;
        }

        final Object matching = rowsByKey.get(key);
        if (unhashableRows.size == 0) {
          if (matching == null)
            candidateCount = 0;
          else if (matching instanceof Integer single) {
            oneCandidate[0] = single;
            candidates = oneCandidate;
            candidateCount = 1;
          } else {
            candidates = ((IntList) matching).values;
            candidateCount = ((IntList) matching).size;
          }
          return;
        }

        // Merge the rows of the key with the unhashable ones, so the pairs keep the order of the right input
        final int[] keyed;
        final int keyedCount;
        if (matching == null) {
          keyed = null;
          keyedCount = 0;
        } else if (matching instanceof Integer single) {
          keyed = new int[] { single };
          keyedCount = 1;
        } else {
          keyed = ((IntList) matching).values;
          keyedCount = ((IntList) matching).size;
        }
        final int[] merged = new int[keyedCount + unhashableRows.size];
        int k = 0;
        int u = 0;
        int m = 0;
        while (k < keyedCount || u < unhashableRows.size)
          merged[m++] = u >= unhashableRows.size || (k < keyedCount && keyed[k] < unhashableRows.values[u]) ?
              keyed[k++] : unhashableRows.values[u++];
        candidates = merged;
        candidateCount = merged.length;
      }

      private void finish() {
        finished = true;
        currentLeft = null;
        if (leftResults != null) {
          leftResults.close();
          leftResults = null;
        }
        // No left row is left to pair: the hash table goes now, not when a close() the consumer may never call comes
        releaseBuffer();
      }

      private void releaseBuffer() {
        if (rightRows != null)
          rightRows.clear();
        rowsByKey = null;
        unhashableRows = null;
      }

      @Override
      public void close() {
        pending = null;
        finish();
      }
    };
  }

  /** Adds a row to the ones of a key, keeping them in the order the right input produced them. */
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

  /** The input read into the hash table; the streamed one is {@link #getChild()}. */
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
