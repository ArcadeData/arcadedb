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
package com.arcadedb.query.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.LocalResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * Combines the rows of the disjoint sub-patterns of a MATCH into their cartesian product, as a nested loop over the sub-plans.
 * <p>
 * The first pass of an independent sub-plan is buffered and replayed for every tuple of the levels before it. A
 * <b>correlated</b> sub-plan - one whose {@code where:} reads, through {@code $matched}, an alias bound by an earlier
 * sub-plan (issue #7434) - cannot be replayed: it is planned and executed again for every tuple of the outer levels, with
 * the {@code matched} context variable bound to that partial tuple so the predicate sees the aliases it reads. A fresh
 * plan per tuple, rather than a reset of the same one, because the fetch and filter steps of a SELECT do not restart on
 * reset (the same reason {@link LetQueryStep} plans a correlated subquery again per row). The planner orders the
 * sub-plans so that every alias a level reads is bound by a level before it.
 * <p>
 * Created by luigidellaquila on 11/10/16.
 */
public class CartesianProductStep extends AbstractExecutionStep {

  private final List<InternalExecutionPlan>           subPlans  = new ArrayList<>();
  // NON-NULL FOR A CORRELATED LEVEL: PLANS THE SUB-PATTERN AGAIN FOR EVERY OUTER TUPLE
  private final List<Supplier<InternalExecutionPlan>> factories = new ArrayList<>();

  private boolean inited = false;
  // THE ROWS OF AN INDEPENDENT LEVEL'S FIRST PASS, REPLAYED THROUGH reset() FOR EVERY LATER OUTER TUPLE
  private final List<InternalResultSet> preFetches = new ArrayList<>();
  private final List<Boolean>           firstPass  = new ArrayList<>();

  private final List<ResultSet> resultSets   = new ArrayList<>();
  private       List<Result>    currentTuple = new ArrayList<>();

  ResultInternal nextRecord;

  public CartesianProductStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    init();
    return new ResultSet() {
      int currentCount = 0;

      @Override
      public boolean hasNext() {
        if (currentCount >= nRecords) {
          return false;
        }
        return nextRecord != null;
      }

      @Override
      public Result next() {
        if (currentCount >= nRecords || nextRecord == null) {
          throw new NoSuchElementException();
        }
        final ResultInternal result = nextRecord;
        fetchNextRecord();
        currentCount++;
        return result;
      }
    };
  }

  /**
   * Best effort, like the reset of the other MATCH steps: the fetch and filter steps of a SELECT do not restart, so a
   * sub-plan that was pulled to exhaustion answers nothing again. Nothing reaches it today, since a MATCH plan is never
   * cached and a correlated subquery plans its statement again per row.
   */
  @Override
  public void reset() {
    inited = false;
    preFetches.clear();
    firstPass.clear();
    resultSets.clear();
    currentTuple = new ArrayList<>();
    nextRecord = null;
    // THE FIRST PASS OF AN INDEPENDENT LEVEL PULLS FROM THE SUB-PLAN ITSELF: ONE LEFT EXHAUSTED WOULD ANSWER NO ROW
    for (int level = 0; level < subPlans.size(); level++)
      if (factories.get(level) == null)
        subPlans.get(level).reset(context);
  }

  private void init() {
    if (subPlans.isEmpty())
      return;

    if (inited)
      return;
    inited = true;

    for (int level = 0; level < subPlans.size(); level++) {
      resultSets.add(null);
      preFetches.add(new InternalResultSet());
      firstPass.add(true);
      currentTuple.add(null);
    }

    for (int level = 0; level < subPlans.size(); level++) {
      open(level);
      if (!advance(level)) {
        nextRecord = null;
        currentTuple = null;
        return;
      }
    }
    buildNextRecord();
  }

  private void fetchNextRecord() {
    if (currentTuple != null && advance(resultSets.size() - 1))
      buildNextRecord();
    else {
      nextRecord = null;
      currentTuple = null;
    }
  }

  /**
   * Moves the given level to its next row. When the level is exhausted, the level before it is advanced and this one is
   * opened again for the new outer tuple; the loop covers a correlated level that answers no row for some outer tuples.
   *
   * @return false when the outermost level is exhausted, i.e. the product is complete
   */
  private boolean advance(final int level) {
    while (true) {
      final ResultSet rs = resultSets.get(level);
      if (rs.hasNext()) {
        final Result item = rs.next();
        currentTuple.set(level, item);
        if (firstPass.get(level) && factories.get(level) == null)
          preFetches.get(level).add(item);
        return true;
      }

      firstPass.set(level, false);
      if (level == 0 || !advance(level - 1))
        return false;
      open(level);
    }
  }

  /**
   * Opens the result set of a level for the current tuple of the levels before it: the live sub-plan on the first pass,
   * the buffered first pass afterwards, or a fresh execution when the level is correlated with the outer tuple.
   */
  private void open(final int level) {
    final Supplier<InternalExecutionPlan> factory = factories.get(level);
    if (factory != null) {
      context.setVariable("matched", partialTuple(level));
      final InternalExecutionPlan plan = factory.get();
      subPlans.set(level, plan);
      resultSets.set(level, new LocalResultSet(plan));
    } else if (firstPass.get(level))
      resultSets.set(level, new LocalResultSet(subPlans.get(level)));
    else {
      final InternalResultSet buffered = preFetches.get(level);
      buffered.reset();
      resultSets.set(level, buffered);
    }
  }

  private ResultInternal partialTuple(final int levels) {
    final ResultInternal partial = new ResultInternal(context.getDatabase());
    for (int i = 0; i < levels; i++) {
      final Result res = currentTuple.get(i);
      for (final String s : res.getPropertyNames())
        partial.setProperty(s, res.getProperty(s));
    }
    return partial;
  }

  private void buildNextRecord() {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      nextRecord = partialTuple(currentTuple.size());
    } finally {
      if (context.isProfiling()) {
        cost += System.nanoTime() - begin;
      }
    }
  }

  public void addSubPlan(final InternalExecutionPlan subPlan) {
    addSubPlan(subPlan, null);
  }

  /**
   * @param factory non-null when the sub-plan reads, through {@code $matched}, an alias bound by a sub-plan added before
   *                it: it is then planned again through the factory and executed for every tuple of those, instead of
   *                being buffered and replayed. The sub-plan given is the one EXPLAIN prints
   */
  public void addSubPlan(final InternalExecutionPlan subPlan, final Supplier<InternalExecutionPlan> factory) {
    this.subPlans.add(subPlan);
    this.factories.add(factory);
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = "";
    final String ind = ExecutionStepInternal.getIndent(depth, indent);

    final int[] blockSizes = new int[subPlans.size()];

    for (int i = 0; i < subPlans.size(); i++) {
      final InternalExecutionPlan currentPlan = subPlans.get(subPlans.size() - 1 - i);
      final String partial = currentPlan.prettyPrint(0, indent);

      final String[] partials = partial.split("\n");
      blockSizes[subPlans.size() - 1 - i] = partials.length + 2;
      result = "+-------------------------\n" + result;
      for (int j = 0; j < partials.length; j++) {
        final String p = partials[partials.length - 1 - j];
        if (result.length() > 0) {
          result = appendPipe(p) + "\n" + result;
        } else {
          result = appendPipe(p);
        }
      }
      result = "+-------------------------\n" + result;
    }
    result = addArrows(result, blockSizes);
    result += foot(blockSizes);
    result = ind + result;
    result = result.replace("\n", "\n" + ind);
    result = head(depth, indent) + "\n" + result;
    return result;
  }

  private String addArrows(final String input, final int[] blockSizes) {
    String result = "";
    final String[] rows = input.split("\n");
    int rowNum = 0;
    for (int block = 0; block < blockSizes.length; block++) {
      final int blockSize = blockSizes[block];
      for (int subRow = 0; subRow < blockSize; subRow++) {
        for (int col = 0; col < blockSizes.length * 3; col++) {
          if (isHorizontalRow(col, subRow, block, blockSize)) {
            result += "-";
          } else if (isPlus(col, subRow, block, blockSize)) {
            result += "+";
          } else if (isVerticalRow(col, subRow, block, blockSize)) {
            result += "|";
          } else {
            result += " ";
          }
        }
        result += rows[rowNum] + "\n";
        rowNum++;
      }
    }

    return result;
  }

  private boolean isHorizontalRow(final int col, final int subRow, final int block, final int blockSize) {
    if (col < block * 3 + 2) {
      return false;
    }
    return subRow == blockSize / 2;
  }

  private boolean isPlus(final int col, final int subRow, final int block, final int blockSize) {
    if (col == block * 3 + 1) {
      return subRow == blockSize / 2;
    }
    return false;
  }

  private boolean isVerticalRow(final int col, final int subRow, final int block, final int blockSize) {
    if (col == block * 3 + 1) {
      return subRow > blockSize / 2;
    } else
      return col < block * 3 + 1 && col % 3 == 1;

  }

  private String head(final int depth, final int indent) {
    final String ind = ExecutionStepInternal.getIndent(depth, indent);
    String result = ind + "+ CARTESIAN PRODUCT";
    if( context.isProfiling() ) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  private String foot(final int[] blockSizes) {
    String result = "";
    for (int i = 0; i < blockSizes.length; i++) {
      result += " V ";//TODO
    }
    return result;
  }

  private String appendPipe(final String p) {
    return "| " + p;
  }
}
