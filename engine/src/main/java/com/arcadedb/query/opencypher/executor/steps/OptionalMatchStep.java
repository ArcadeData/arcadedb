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

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import com.arcadedb.utility.RidHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Execution step for OPTIONAL MATCH clauses.
 * Implements LEFT OUTER JOIN semantics: for each input row, try to match the pattern.
 * If matches found, return them. If no matches, return input row with NULL values.
 * <p>
 * Example: MATCH (a) OPTIONAL MATCH (a)-[r:KNOWS]->(b)
 * - For each 'a' from first MATCH, try to find (a)-[r:KNOWS]->(b)
 * - If found: return a, r, b
 * - If not found: return a, null, null
 * <p>
 * **Streaming Optimization**: To prevent OOM with large cartesian products,
 * uses bounded buffering and streams results incrementally instead of
 * loading all matches into memory at once.
 */
public class OptionalMatchStep extends AbstractExecutionStep {
  private final AbstractExecutionStep matchChainStart;
  private final AbstractExecutionStep matchChainEnd;
  private final Set<String> variableNames;

  // Buffer size limit to prevent OOM (configurable via system property)
  private static final int MAX_BUFFER_SIZE = Integer.getInteger("arcadedb.optionalMatch.maxBufferSize", 10000);

  // Past this many matches for one input row they are streamed without being remembered, so the memory of the memo stays
  // bounded: forgetting only costs running the chain again for the next input row
  private static final int MAX_REMEMBERED_MATCHES = 10_000;
  // After this many chain runs in a row whose key no input row repeated, matches stop being recorded: an input whose keys do
  // not repeat pays the comparison of each key with the one before it, not a remembered row per match
  private static final int MEMO_PROBATION = 64;
  // Marks a key name the input row does not hold, which the chain can tell from one bound to null
  private static final Object ABSENT = new Object();

  // Count-only mode: when true, only tracks unique RIDs instead of buffering full Result objects
  // This dramatically reduces memory for count() aggregations
  private boolean countOnlyMode = false;

  // The names of the input row the chain's matches are a function of, or null when they are not remembered (issue #11083)
  private String[] memoKey;
  // Profiling counters, reported by prettyPrint
  private long     chainRuns  = 0;
  private long     remembered = 0;

  /**
   * Creates an optional match step.
   *
   * @param matchChainStart first step in the optional match chain (input is fed here)
   * @param matchChainEnd   last step in the optional match chain (results are pulled from here)
   * @param variableNames   names of variables that should be set to NULL if no match
   * @param context         command context
   */
  public OptionalMatchStep(final AbstractExecutionStep matchChainStart, final AbstractExecutionStep matchChainEnd,
      final Set<String> variableNames, final CommandContext context) {
    super(context);
    this.matchChainStart = matchChainStart;
    this.matchChainEnd = matchChainEnd;
    this.variableNames = variableNames;
  }

  /**
   * Enables count-only mode for memory-efficient counting.
   * Instead of buffering full Result objects, only tracks unique RIDs.
   * Use this when the matched variables are only used for count() aggregation.
   */
  public void setCountOnlyMode(final boolean enabled) {
    this.countOnlyMode = enabled;
  }

  /**
   * Remembers the matches of an input row and hands them to the next input rows holding the same values for
   * {@code memoKey}, instead of running the chain again for each (issue #11083). The rows an expansion produces for one
   * vertex arrive one after the other, so a clause that reads only that vertex - {@code OPTIONAL MATCH (r)-[:LOW]->(l)}
   * after an {@code OPTIONAL MATCH (r)-[:HIGH]->(h)} that fanned {@code r} out to 10,000 rows - ran its expansion 10,000
   * times, each walking the whole adjacency of {@code r}. Only the last input row's matches are kept, up to
   * {@link #MAX_REMEMBERED_MATCHES}: no hashing, and no memory beyond one row's matches.
   * <p>
   * The planner asks for it only for a statement that cannot write, so nothing can change what the chain reads between
   * two input rows, and only with the key {@code DistinctSemiJoin.matchMemoKey} found complete.
   *
   * @param memoKey the names the chain reads off its input row, or null to run it for every input row
   */
  public void setMemoKey(final List<String> memoKey) {
    this.memoKey = memoKey != null ? memoKey.toArray(new String[0]) : null;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final boolean hasInput = prev != null;
    // Bounds this operator's row loop by the command deadline - see WorkGuard for why between-batches is
    // not enough (issue #6266).
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    return new ResultSet() {
      private ResultSet prevResults = null;
      private Result currentInputRow = null;
      private ResultSet currentMatchResults = null;
      // With input: whether the current input row matched. Standalone (no input): whether the
      // pattern matched at all across the whole scan, since there is no per-row input to reset it.
      private boolean foundMatchForCurrent = false;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;

      // Count-only mode: track unique RIDs instead of full Result objects
      private final RidHashSet uniqueRIDs = countOnlyMode ? new RidHashSet() : null;
      private long currentInputMatchCount = 0;

      // The memo of the last input row's matches (issue #11083)
      private final boolean memo = memoKey != null && !countOnlyMode;
      // The key values of the input row whose matches are remembered, and what the chain added to it, one row per match
      private Object[] memoValues = null;
      private List<ResultInternal> memoMatches = null;
      // The key values of the input row the chain is running for, and its matches recorded so far (null past the cap)
      private Object[] runValues = null;
      private List<ResultInternal> runMatches = null;
      // The next remembered match handed to currentInputRow, -1 when the chain runs for it instead
      private int replayIndex = -1;
      // The chain runs since an input row last repeated the key of the row before it
      private int runsWithoutReuse = 0;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size()) {
          return true;
        }

        if (finished) {
          return false;
        }

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
          // Initialize prevResults on first call
          if (prevResults == null) {
            prevResults = prev.syncPull(context, nRecords);
          }

          // Streaming mode: process matches incrementally to avoid OOM
          // Limit buffer size to prevent unbounded memory growth
          final int maxToFetch = Math.min(n, MAX_BUFFER_SIZE);
          while (buffer.size() < maxToFetch) {
            guard.check();
            // An input row answered by the memo: hand it the remembered matches
            if (replayIndex >= 0) {
              if (replayIndex < memoMatches.size()) {
                if (context.isProfiling())
                  rowCount++;
                buffer.add(withMatch(currentInputRow, memoMatches.get(replayIndex++)));
                continue;
              }
              replayIndex = -1;
              currentInputRow = null;
            }

            // If we have active match results for the current input row, continue streaming them
            if (currentMatchResults != null && currentMatchResults.hasNext()) {
              final long begin = context.isProfiling() ? System.nanoTime() : 0;
              try {
                if (context.isProfiling())
                  rowCount++;

                final Result matchResult = currentMatchResults.next();

                if (countOnlyMode) {
                  // Count-only optimization: just track unique RIDs, don't buffer full objects
                  for (final String varName : variableNames) {
                    final Object val = matchResult.getProperty(varName);
                    if (val instanceof Identifiable) {
                      uniqueRIDs.add(((Identifiable) val).getIdentity());
                    }
                  }
                  currentInputMatchCount++;
                  foundMatchForCurrent = true;
                  // Continue consuming all matches without buffering
                } else {
                  // Normal mode: buffer full Result objects
                  buffer.add(matchResult);
                  foundMatchForCurrent = true;
                  if (runMatches != null) {
                    if (runMatches.size() < MAX_REMEMBERED_MATCHES)
                      runMatches.add(addedBy(currentInputRow, matchResult));
                    else
                      runMatches = null;
                  }

                  // Stop if buffer is getting too large (prevent OOM)
                  if (buffer.size() >= Math.min(n, MAX_BUFFER_SIZE)) {
                    break;
                  }
                }
              } finally {
                if (context.isProfiling())
                  cost += System.nanoTime() - begin;
              }
            } else {
              // No more matches for current input, finalize it
              if (currentMatchResults != null) {
                currentMatchResults.close();

                if (countOnlyMode) {
                  // Count-only mode: emit a single result with count for this input
                  final ResultInternal countResult = new ResultInternal();
                  // Copy all properties from input row
                  final Set<String> inputProps = currentInputRow.getPropertyNames();
                  for (final String prop : inputProps) {
                    countResult.setProperty(prop, currentInputRow.getProperty(prop));
                  }
                  // Set count value for the matched variable (first variable name)
                  // In count-only mode, typically there's one variable being counted
                  if (!variableNames.isEmpty()) {
                    final String countVar = variableNames.iterator().next();
                    countResult.setProperty(countVar + "_count", currentInputMatchCount);
                  }
                  buffer.add(countResult);
                  currentInputMatchCount = 0; // Reset for next input
                } else {
                  // Normal mode: if no matches found for this input row, emit NULL result
                  if (!foundMatchForCurrent && currentInputRow != null)
                    buffer.add(nullRow(currentInputRow));
                }

                // Every match of the row was recorded: they answer the next rows with the same key
                if (runMatches != null) {
                  memoValues = runValues;
                  memoMatches = runMatches;
                  runMatches = null;
                }

                currentMatchResults = null;
                currentInputRow = null;
              }

              // Get next input row
              if (!prevResults.hasNext()) {
                finished = true;
                break;
              }

              currentInputRow = prevResults.next();
              foundMatchForCurrent = false;

              if (memo) {
                final Object[] values = keyValues(currentInputRow);
                if (memoMatches != null && sameKey(values, memoValues)) {
                  if (context.isProfiling())
                    remembered++;
                  runsWithoutReuse = 0;
                  if (memoMatches.isEmpty()) {
                    buffer.add(nullRow(currentInputRow));
                    currentInputRow = null;
                  } else
                    replayIndex = 0;
                  continue;
                }
                // Recording costs a row per match, so it pauses while no input row repeats the one before it, and resumes
                // as soon as one does
                if (runValues != null && sameKey(values, runValues))
                  runsWithoutReuse = 0;
                runValues = values;
                runMatches = runsWithoutReuse++ < MEMO_PROBATION ? new ArrayList<>() : null;
              }

              final long begin = context.isProfiling() ? System.nanoTime() : 0;
              try {
                if (context.isProfiling())
                  chainRuns++;
                // Feed this input row into the match chain
                final SingleRowInputStep singleRowInput = new SingleRowInputStep(currentInputRow, context);
                matchChainStart.setPrevious(singleRowInput);

                // Execute the match chain (streaming)
                currentMatchResults = matchChainEnd.syncPull(context, Math.min(100, MAX_BUFFER_SIZE));
              } finally {
                if (context.isProfiling())
                  cost += System.nanoTime() - begin;
              }
            }
          }
        } else {
          // No input: standalone OPTIONAL MATCH.
          // The match cursor is held in currentMatchResults (like the input path holds it across
          // input rows) so that it resumes where it left off on every fetchMore call instead of
          // re-running the pattern scan from scratch (issue #6668).
          if (currentMatchResults == null) {
            matchChainStart.setPrevious(null);
            currentMatchResults = matchChainEnd.syncPull(context, Math.min(n, MAX_BUFFER_SIZE));
          }

          // Collect matches with bounded buffer
          final int maxToFetch = Math.min(n, MAX_BUFFER_SIZE);
          while (buffer.size() < maxToFetch && currentMatchResults.hasNext()) {
            guard.check();
            buffer.add(currentMatchResults.next());
            foundMatchForCurrent = true;
          }

          if (!currentMatchResults.hasNext()) {
            finished = true;
            currentMatchResults.close();
            currentMatchResults = null;

            // If no matches were found across the whole pattern, return a single row with NULL values
            if (!foundMatchForCurrent) {
              final ResultInternal nullResult = new ResultInternal();
              for (final String varName : variableNames) {
                nullResult.setProperty(varName, null);
              }
              buffer.add(nullResult);
            }
          }
        }
      }

      @Override
      public void close() {
        if (currentMatchResults != null) {
          currentMatchResults.close();
          currentMatchResults = null;
        }
        OptionalMatchStep.this.close();
      }
    };
  }

  /** The input row with NULL for every variable the OPTIONAL MATCH binds and the row does not hold already. */
  private ResultInternal nullRow(final Result input) {
    final ResultInternal nullResult = new ResultInternal();
    // Copy all properties from input row
    final Set<String> inputProps = input.getPropertyNames();
    for (final String prop : inputProps)
      nullResult.setProperty(prop, input.getProperty(prop));
    // Add NULL values only for NEW optional match variables (not already bound)
    for (final String varName : variableNames)
      if (!inputProps.contains(varName))
        nullResult.setProperty(varName, null);
    return nullResult;
  }

  /**
   * What a match added to its input row: the names the chain bound. The names the input row held are left out, so the
   * match can be handed to another input row with the same key without carrying the first row's other values.
   */
  private static ResultInternal addedBy(final Result input, final Result match) {
    final ResultInternal added = new ResultInternal();
    for (final String name : match.getPropertyNames())
      if (!input.hasProperty(name))
        added.setProperty(name, match.getProperty(name));
    return added;
  }

  /** The row a remembered match produces for {@code input}. */
  private static ResultInternal withMatch(final Result input, final ResultInternal added) {
    final Set<String> names = added.getPropertyNames();
    final ResultInternal row = ResultInternal.copyBindings(input, names.size());
    for (final String name : names)
      row.setProperty(name, added.getProperty(name));
    return row;
  }

  private Object[] keyValues(final Result row) {
    final Object[] values = new Object[memoKey.length];
    for (int i = 0; i < memoKey.length; i++)
      values[i] = row.hasProperty(memoKey[i]) ? row.getProperty(memoKey[i]) : ABSENT;
    return values;
  }

  private static boolean sameKey(final Object[] values, final Object[] other) {
    for (int i = 0; i < values.length; i++)
      if (!sameValue(values[i], other[i]))
        return false;
    return true;
  }

  /**
   * Whether the chain reads the same thing from both values: a record by its RID, anything else by equality. A record
   * that has no RID matches nothing, where {@code equals} would match any other record without one: a miss only costs
   * running the chain.
   */
  private static boolean sameValue(final Object a, final Object b) {
    if (a == null || b == null)
      return a == null && b == null;
    if (a instanceof Identifiable first && b instanceof Identifiable second) {
      final RID rid = first.getIdentity();
      return rid != null && rid.equals(second.getIdentity());
    }
    return a.equals(b);
  }

  /**
   * Helper step that provides a single row as input to the match chain.
   * This allows us to feed one input row at a time into the optional match chain.
   */
  private static class SingleRowInputStep extends AbstractExecutionStep {
    private final Result singleRow;
    private boolean consumed = false;

    public SingleRowInputStep(final Result row, final CommandContext context) {
      super(context);
      this.singleRow = row;
    }

    @Override
    public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
      if (consumed) {
        return new IteratorResultSet(List.<Result>of().iterator());
      }
      consumed = true;
      return new IteratorResultSet(List.of(singleRow).iterator());
    }

    @Override
    public String prettyPrint(final int depth, final int indent) {
      return "  ".repeat(Math.max(0, depth * indent)) + "+ SINGLE ROW INPUT";
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ OPTIONAL MATCH (variables: ").append(String.join(", ", variableNames)).append(")");
    if (countOnlyMode) {
      builder.append(" [COUNT-ONLY MODE]");
    }
    if (memoKey != null)
      builder.append(" [matches reused per ").append(memoKey.length == 0 ? "query" : String.join(", ", memoKey))
          .append("]");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
      if (memoKey != null)
        builder.append(" match runs: ").append(chainRuns).append(", reused: ").append(remembered);
    }
    builder.append("\n");
    builder.append(matchChainStart.prettyPrint(depth + 1, indent));
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
