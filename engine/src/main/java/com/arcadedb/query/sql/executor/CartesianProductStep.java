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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

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
 * Many outer tuples feed a correlated level the same values: a level that reads only {@code $matched.a} is asked the
 * same question once for every tuple of the levels between {@code a} and it. When the planner can name the aliases
 * the level reads (issue #8443), the level runs with {@code $matched} bound to those aliases alone, in a context that
 * records every other outer variable it reads, and its rows are remembered per distinct binding by a
 * {@link CorrelatedSubQueryCache}, the one {@link LetQueryStep} uses for a correlated LET subquery (issue #8400).
 * <p>
 * Created by luigidellaquila on 11/10/16.
 */
public class CartesianProductStep extends AbstractExecutionStep {

  // THE PLANS AS BUILT BY THE PLANNER, WHAT EXPLAIN PRINTS; A CORRELATED LEVEL RUNS A FRESH ONE PER OUTER TUPLE INSTEAD
  private final List<InternalExecutionPlan>           subPlans  = new ArrayList<>();
  // NON-NULL FOR A CORRELATED LEVEL: PLANS THE SUB-PATTERN AGAIN FOR EVERY OUTER TUPLE, IN THE CONTEXT IT IS GIVEN
  private final List<Function<CommandContext, InternalExecutionPlan>> factories = new ArrayList<>();
  // NON-NULL FOR A CORRELATED LEVEL WHOSE ROWS MAY BE REMEMBERED: THE ALIASES OF THE OUTER TUPLE THE LEVEL READS, AND
  // THE ONLY ONES IT IS GIVEN. NULL WHEN THE PLANNER CANNOT NAME THEM ALL, WHERE THE LEVEL SEES THE WHOLE OUTER TUPLE
  private final List<String[]>                                        readAliases = new ArrayList<>();
  // THE STATEMENT THE LEVELS BELONG TO, WHAT THE RESULT CACHE CHECKS FOR PURITY. NULL: NO LEVEL IS REMEMBERED
  private       Statement                                             statement;

  private boolean inited = false;
  // THE ROWS OF AN INDEPENDENT LEVEL'S FIRST PASS, REPLAYED THROUGH reset() FOR EVERY LATER OUTER TUPLE
  private final List<InternalResultSet> preFetches = new ArrayList<>();
  /**
   * Every level's buffered rows count against queryMaxHeapElementsAllowedPerOp, as the OpenCypher product's do (#8585),
   * and charge the heap they take to the budget all the running queries share (#8591).
   */
  private final OperationHeapLimit      heapLimit;
  private final List<Boolean>           firstPass  = new ArrayList<>();

  // ONE CACHE PER REMEMBERED LEVEL (A LEVEL'S ROWS ARE ITS OWN), CREATED WHEN THE LEVEL IS FIRST OPENED. NULL ENTRIES: NONE
  private final List<CorrelatedSubQueryCache> caches       = new ArrayList<>();
  // THE RUN OF A REMEMBERED LEVEL STILL BEING PULLED: THE ROWS IT HAS ANSWERED, TO STORE ONCE IT IS EXHAUSTED
  private final List<LevelRun>                runs         = new ArrayList<>();
  private final List<ResultSet> resultSets   = new ArrayList<>();
  private       List<Result>    currentTuple = new ArrayList<>();
  // THE OUTER TUPLE A CORRELATED LEVEL IS OPEN FOR, REBOUND TO $matched BEFORE EVERY PULL FROM IT
  private final List<Result>    outerTuples  = new ArrayList<>();

  ResultInternal nextRecord;

  public CartesianProductStep(final CommandContext context) {
    super(context);
    this.heapLimit = OperationHeapLimit.of(context, "MATCH Cartesian product");
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
    releaseBuffers();
    firstPass.clear();
    resultSets.clear();
    outerTuples.clear();
    caches.clear();
    runs.clear();
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
      outerTuples.add(null);
      caches.add(null);
      runs.add(null);
    }
    initResultCaches();

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
   * opened again for the new outer tuple. The four cases, in the order the loop meets them:
   * <ol>
   *   <li>the level has a next row: bind it (and buffer it, when this is the first pass of an independent level)</li>
   *   <li>an independent level answered no row at all on its first pass: the product is empty, answer so at once</li>
   *   <li>the outermost level is exhausted: the product is complete</li>
   *   <li>otherwise advance the level before, open this one again for the new outer tuple and loop: a correlated level
   *       may answer no row for that tuple, in which case the loop backtracks once more</li>
   * </ol>
   *
   * @return false when the product is complete or empty
   */
  private boolean advance(final int level) {
    while (true) {
      final ResultSet rs = resultSets.get(level);
      // A CORRELATED LEG EVALUATES ITS FILTER LAZILY, ONE CANDIDATE PER PULL, AND THE PRODUCT PREPARES THE NEXT ROW BEFORE IT
      // HANDS OUT THE CURRENT ONE: BY THEN MatchBindMatchedStep DOWNSTREAM HAS REBOUND $matched TO A ROW OF ANOTHER OUTER
      // TUPLE, SO THE LEG'S OWN OUTER TUPLE IS BOUND AGAIN BEFORE EVERY PULL
      if (factories.get(level) != null)
        context.setVariable(MatchBindMatchedStep.MATCHED_VARIABLE, outerTuples.get(level));
      if (rs.hasNext()) {
        final Result item = rs.next();
        currentTuple.set(level, item);
        recordRow(level, item);
        if (firstPass.get(level) && factories.get(level) == null) {
          final InternalResultSet buffered = preFetches.get(level);
          buffered.add(item);
          try {
            heapLimit.add(buffered.countEntries(), item);
          } catch (final RuntimeException e) {
            // The query fails: the rows buffered so far give their heap back now
            releaseBuffers();
            throw e;
          }
        }
        return true;
      }

      // THE RUN OF A REMEMBERED LEVEL IS COMPLETE ONLY NOW: ONLY NOW DOES IT HAVE READ EVERYTHING IT WILL READ FROM THE OUTER
      // CONTEXT, WHICH IS STILL BOUND TO ITS OUTER TUPLE (BOUND AGAIN AT THE TOP OF THIS LOOP)
      storeRun(level);

      // AN INDEPENDENT LEVEL WITH NO ROW AT ALL EMPTIES THE WHOLE PRODUCT: ANSWER SO AT ONCE RATHER THAN WALKING EVERY ROW OF
      // THE LEVELS BEFORE IT TO FIND OUT. A CORRELATED LEVEL ANSWERS PER OUTER TUPLE, SO IT GETS NO SUCH SHORTCUT
      if (factories.get(level) == null && firstPass.get(level) && preFetches.get(level).countEntries() == 0)
        return false;

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
    // THE RESULT SET BEING REPLACED IS THE LIVE ONE OF A SUB-PLAN, EXHAUSTED OR SUPERSEDED: RELEASE WHAT ITS STEPS HOLD
    final ResultSet previous = resultSets.get(level);
    if (previous instanceof LocalResultSet)
      previous.close();

    final Function<CommandContext, InternalExecutionPlan> factory = factories.get(level);
    if (factory != null) {
      // THE ROOT OF THE LEG READS $matched AS ITS SEED WHEN IT IS FIRST PULLED, WHICH LocalResultSet DOES RIGHT HERE. THE
      // BINDING IS NOT RESTORED: advance() BINDS IT AGAIN BEFORE EVERY LATER PULL, AND MatchBindMatchedStep BINDS EVERY ROW
      // THE PRODUCT EMITS BEFORE THE RETURN CLAUSE READS IT. A LEVEL IS ONE CONNECTED SUB-PATTERN, NEVER A PRODUCT OF ITS
      // OWN, SO THE BINDINGS DO NOT NEST
      final ResultInternal outerTuple = partialTuple(level, readAliases.get(level));
      outerTuples.set(level, outerTuple);
      context.setVariable(MatchBindMatchedStep.MATCHED_VARIABLE, outerTuple);
      runs.set(level, null);

      final CorrelatedSubQueryCache cache = caches.get(level);
      if (cache != null && !cache.isDisabled()) {
        final DatabaseInternal database = context.getDatabase();
        final List<Result> remembered = cache.lookup(context, database);
        if (remembered != null) {
          // THE ROWS THIS LEVEL ANSWERED THE LAST TIME ITS OUTER TUPLE HELD THESE VALUES: NO PLANNING, NO EXECUTION
          final InternalResultSet replay = new InternalResultSet();
          for (final Result row : remembered)
            replay.add(row);
          resultSets.set(level, replay);
        } else {
          final CorrelatedSubQueryCache.TrackingContext tracking = cache.newContext(context);
          runs.set(level, new LevelRun(tracking));
          resultSets.set(level, new LocalResultSet(factory.apply(tracking)));
        }
      } else
        resultSets.set(level, new LocalResultSet(factory.apply(context)));
    } else if (firstPass.get(level))
      resultSets.set(level, new LocalResultSet(subPlans.get(level)));
    else {
      final InternalResultSet buffered = preFetches.get(level);
      buffered.reset();
      resultSets.set(level, buffered);
    }
  }

  @Override
  public void close() {
    for (final ResultSet rs : resultSets)
      if (rs != null)
        rs.close();
    for (int level = 0; level < subPlans.size(); level++)
      if (factories.get(level) == null)
        subPlans.get(level).close();
    releaseBuffers();
    super.close();
  }

  private void releaseBuffers() {
    preFetches.clear();
    heapLimit.release();
  }

  private ResultInternal partialTuple(final int levels) {
    return partialTuple(levels, null);
  }

  /**
   * @param aliases the only aliases to keep, or null to keep every one of the levels
   */
  private ResultInternal partialTuple(final int levels, final String[] aliases) {
    final ResultInternal partial = new ResultInternal(context.getDatabase());
    for (int i = 0; i < levels; i++) {
      final Result res = currentTuple.get(i);
      for (final String s : res.getPropertyNames())
        if (aliases == null || contains(aliases, s))
          partial.setProperty(s, res.getProperty(s));
    }
    return partial;
  }

  private static boolean contains(final String[] aliases, final String alias) {
    for (final String candidate : aliases)
      if (candidate.equals(alias))
        return true;
    return false;
  }

  /** Creates the result cache of every correlated level the planner named the aliases of, unless none can be trusted. */
  private void initResultCaches() {
    final DatabaseInternal database = context.getDatabase();
    if (statement == null || database == null)
      return;
    final int maxEntries = database.getConfiguration().getValueAsInteger(GlobalConfiguration.SQL_LET_SUBQUERY_CACHE_SIZE);
    for (int level = 0; level < factories.size(); level++)
      if (factories.get(level) != null && readAliases.get(level) != null)
        caches.set(level, CorrelatedSubQueryCache.create(statement, null, database, maxEntries));
  }

  /** Keeps a row a remembered level answers, until it turns out to be too many to remember. */
  private void recordRow(final int level, final Result row) {
    final LevelRun run = runs.get(level);
    if (run == null)
      return;
    if (run.rows.size() >= CorrelatedSubQueryCache.MAX_CACHED_ROWS_PER_ENTRY)
      // TOO MANY TO REMEMBER: THE CACHE WOULD REFUSE THEM, SO STOP HOLDING THEM
      runs.set(level, null);
    else
      run.rows.add(row);
  }

  /** Remembers the rows of a remembered level that was pulled to exhaustion, for the outer tuple it ran for. */
  private void storeRun(final int level) {
    final LevelRun run = runs.get(level);
    if (run == null)
      return;
    runs.set(level, null);
    final CorrelatedSubQueryCache cache = caches.get(level);
    if (cache != null)
      cache.store(run.tracking, context, context.getDatabase(), run.rows);
  }

  /** The rows one run of a remembered level has answered so far, and the context it reads the outer variables through. */
  private static final class LevelRun {
    final CorrelatedSubQueryCache.TrackingContext tracking;
    final List<Result>                            rows = new ArrayList<>();

    LevelRun(final CorrelatedSubQueryCache.TrackingContext tracking) {
      this.tracking = tracking;
    }
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
    addSubPlan(subPlan, null, null, null);
  }

  /**
   * @param subPlan     the sub-plan EXPLAIN prints
   * @param factory     non-null for a correlated level, one that reads through {@code $matched} an alias bound by a level
   *                    before it: plans the sub-pattern again, bound to the context it is given, for every outer tuple
   * @param readAliases the aliases of the earlier levels the correlated level reads through {@code $matched} and no
   *                    others, or null when that cannot be told: the level is then given the whole outer tuple and
   *                    its rows are never remembered
   * @param statement   the MATCH statement, which decides whether a level's rows can be remembered at all
   */
  public void addSubPlan(final InternalExecutionPlan subPlan, final Function<CommandContext, InternalExecutionPlan> factory,
      final String[] readAliases, final Statement statement) {
    this.subPlans.add(subPlan);
    this.factories.add(factory);
    this.readAliases.add(factory == null ? null : readAliases);
    if (statement != null)
      this.statement = statement;
  }

  /** The cache of a correlated level, or null when the level is not remembered. Exposed for tests. */
  CorrelatedSubQueryCache getResultCache(final int level) {
    return level < caches.size() ? caches.get(level) : null;
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
