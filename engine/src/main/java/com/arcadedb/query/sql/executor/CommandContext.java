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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;
import com.arcadedb.utility.TimeBoundRegex;

import java.util.Map;

/**
 * Basic interface for commands. Manages the context variables during execution.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface CommandContext {
  Object getVariablePath(String name);

  Object getVariablePath(String name, Object iDefault);

  Object getVariable(String name);

  Object getVariable(String name, Object iDefaultValue);

  CommandContext setVariable(String name, Object iValue);

  /**
   * Returns a value from an internal cache whose key is opaque: unlike {@link #getVariable(String)}, the name is never
   * interpreted as a {@code $var.field} nested path, so it can safely embed user data (e.g. a full-text query string with
   * dots). Used to memoize per-query computations within a command execution. Returns {@code null} if absent.
   */
  Object getCachedValue(String key);

  /**
   * Stores a value in the opaque internal cache. See {@link #getCachedValue(String)}.
   */
  CommandContext setCachedValue(String key, Object value);

  /**
   * Returns an absolute {@code arcadedb.command.regexTimeout} deadline (see {@link TimeBoundRegex#newDeadline(long)})
   * shared for the lifetime of this context, computing and caching it on first call. Centralizes the
   * get-cached-or-compute-and-cache pattern every regex-bounding call site in the engine needs to share one
   * deadline across a series of related matches (issue #5886) - each call site previously hand-rolled this
   * with its own cache key, which is exactly how a real bug shipped: two independently-chosen keys collided
   * (a fixed {@code "MATCHES_DEADLINE"} key colliding with the pattern cache's {@code "MATCHES_" + <regex
   * text>} keys for the literal pattern text {@code DEADLINE}). Centralizing here still requires each caller
   * to pass a key that can't collide with anything else it puts in this same cache, but removes the need to
   * hand-write the get-or-compute logic itself at every site.
   *
   * <p>The deadline is shared across every row evaluated through <em>this</em> context, but not necessarily
   * across an entire query: {@code FetchFromTypeExecutionStep.syncPullParallel} gives each parallel bucket-scan
   * worker its own {@link #copy()} of the context (deliberately, so workers don't race on this same cache's
   * non-thread-safe backing map), so each worker computes its own deadline independently. A type scanned in
   * parallel across N buckets is therefore bounded by {@code N * timeoutMillis} overall, not one shared budget.
   * This is a much narrower gap than the one this method closes: bucket count is a schema/DDL property, not
   * attacker-controlled the way row/item counts are.
   *
   * @param cacheKey      the {@link #getCachedValue(String)} key to store the deadline under - must not collide
   *                      with any other key this context's cache holds
   * @param timeoutMillis maximum time allowed from now, in milliseconds; a value {@code <= 0} disables the bound
   */
  default long getOrComputeRegexDeadline(final String cacheKey, final long timeoutMillis) {
    Long deadline = (Long) getCachedValue(cacheKey);
    if (deadline == null) {
      deadline = TimeBoundRegex.newDeadline(timeoutMillis);
      setCachedValue(cacheKey, deadline);
    }
    return deadline;
  }

  /**
   * Convenience overload of {@link #getOrComputeRegexDeadline(String, long)} that resolves {@code timeoutMillis}
   * from {@link GlobalConfiguration#COMMAND_REGEX_TIMEOUT} for this context's database.
   */
  default long getOrComputeRegexDeadline(final String cacheKey) {
    return getOrComputeRegexDeadline(cacheKey, GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(getDatabase()));
  }

  /**
   * Returns {@code arcadedb.command.timeout} in milliseconds for this context's database, or {@code 0} when the
   * setting is disabled. Resolved once and remembered, so the hot paths that build a {@link WorkGuard} per
   * batch do not re-read the configuration.
   */
  long getCommandTimeout();

  /**
   * Returns the absolute epoch-millis instant past which this command must stop, or {@link Long#MAX_VALUE} when
   * {@code arcadedb.command.timeout} is disabled.
   * <p>
   * The deadline is computed on first use and then fixed for the lifetime of the context, and a context without
   * one of its own inherits its parent's. That is what makes the setting mean "no command runs longer than X"
   * rather than "no single step runs longer than X": a subquery, a {@code CALL} into a procedure or a per-row
   * expansion that builds its own guard all land on the same instant instead of each starting a fresh budget
   * (issue #6266).
   */
  long getCommandDeadline();

  /**
   * Names the bound {@link #getCommandDeadline()} currently expresses, for the abort message - e.g.
   * {@code "arcadedb.command.timeout of 30000ms"} or {@code "TIMEOUT clause of 50ms"}. Read only on the failure
   * path, so it costs nothing while the command is running.
   */
  String getCommandDeadlineDescription();

  /**
   * Pins this context's deadline to an already-computed instant, overriding whatever it would have resolved to,
   * and says what to call that bound when a check aborts on it.
   * <p>
   * Two callers need this. A nested plan runs with a context of its own and pins the outer command's instant, so
   * that nesting cannot buy extra budget. A SQL {@code TIMEOUT} clause is resolved by the planner rather than
   * from the configuration, so the statement's own bound has to be published here or the in-loop guards - which
   * read the deadline off this context - would never see it (issue #6266).
   * <p>
   * Every value is honoured, including {@code 0} - which pins a deadline already in the past, so the next check
   * aborts - and {@link Long#MAX_VALUE}, which lifts the bound entirely. The one exception is
   * {@link Long#MIN_VALUE}, which the implementation reserves as its "not resolved yet" marker and will
   * therefore re-resolve rather than pin. Nothing can produce it - an epoch-millis instant that far in the past
   * has no meaning and no arithmetic here reaches it - so it is named only so the next reader does not have to
   * rediscover it.
   */
  void setCommandDeadline(long deadlineEpochMillis, String description);

  CommandContext incrementVariable(String getNeighbors);

  Map<String, Object> getVariables();

  CommandContext getParent();

  CommandContext setParent(CommandContext parentContext);

  CommandContext setChild(CommandContext context);

  boolean isProfiling();

  CommandContext setProfiling(boolean profilingEnabled);

  Map<String, Object> getInputParameters();

  void setInputParameters(Map<String, Object> inputParameters);

  /**
   * Creates a copy of execution context.
   */
  CommandContext copy();

  DatabaseInternal getDatabase();

  QueryStatistics getStatistics();

  void setStatistics(QueryStatistics statistics);

  void declareScriptVariable(String varName);

  boolean isScriptVariableDeclared(String varName);

  ContextConfiguration getConfiguration();

  void setConfiguration(ContextConfiguration configuration);

  CommandContext getContextDeclaredVariable(String varName);

  /** Context variable name set to {@code true} when CSR (Graph Analytical View) acceleration was used during execution. */
  String CSR_ACCELERATED_VAR = "_csrAccelerated";

  /** Context variable set by algorithm procedures to the total number of results they will yield.
   *  Used by CallStep to optimize count-only queries by skipping per-row Result object creation. */
  String RESULT_COUNT_HINT_VAR = "_resultCountHint";

  /** Partition-pruned bucket file ids ({@link com.arcadedb.utility.IntHashSet}) for the FROM type named in {@link #PARTITION_PRUNED_TYPE_NAME_VAR}. Single-valued: only one partitioned FROM type per query (issue #4087). */
  String PARTITION_PRUNED_BUCKET_FILE_IDS_VAR = "_partitionPrunedBucketFileIds";

  /** Companion to {@link #PARTITION_PRUNED_BUCKET_FILE_IDS_VAR}: the FROM type name the prune was derived from. Consumers must match before applying. */
  String PARTITION_PRUNED_TYPE_NAME_VAR = "_partitionPrunedTypeName";
}
