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
   * Returns the absolute {@code arcadedb.command.regexTimeout} deadline (see {@link TimeBoundRegex#newDeadline(long)})
   * every regex evaluated through this command must finish by, resolved from
   * {@link GlobalConfiguration#COMMAND_REGEX_TIMEOUT} on first use and then fixed - like
   * {@link #getCommandDeadline()}, and for the same reason: a bound that restarts is not a bound.
   * <p>
   * One deadline per command rather than one per call site. Every site that bounds a regex against catastrophic
   * backtracking needs to share a budget across a series of matches (issue #5886), and the deadline used to live
   * in {@link #getCachedValue(String)} under a key each site chose for itself. That gave a query as many budgets
   * as it used regex features, and - because {@link #copy()} does not copy that cache - gave each parallel
   * bucket-scan worker one more, so a type scanned across N buckets was bounded by {@code N * timeoutMillis}
   * (issue #6304). Carried on the context and resolved before copying, it is one budget for the whole command,
   * however the command is decomposed. It also removes the key-collision hazard that shipped a real bug: a fixed
   * {@code "MATCHES_DEADLINE"} key collided with the pattern cache's {@code "MATCHES_" + <regex text>} keys for
   * the literal pattern text {@code DEADLINE}.
   * <p>
   * With the setting disabled the resolution costs no clock read at all - {@link TimeBoundRegex#newDeadline(long)}
   * answers {@link Long#MAX_VALUE} for a non-positive timeout.
   */
  long getRegexDeadline();

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
  default void setCommandDeadline(final long deadlineEpochMillis, final String description) {
    setCommandDeadline(deadlineEpochMillis, description, false);
  }

  /**
   * As {@link #setCommandDeadline(long, String)}, additionally saying what reaching the deadline means.
   *
   * @param yieldPartialResults {@code true} for a bound that asks for the rows produced so far rather than for a
   *                            failure - a SQL {@code TIMEOUT n RETURN} clause. See
   *                            {@link #isCommandDeadlinePartial()}.
   */
  void setCommandDeadline(long deadlineEpochMillis, String description, boolean yieldPartialResults);

  /**
   * Whether reaching {@link #getCommandDeadline()} means "stop and yield what you have" rather than "fail".
   * <p>
   * Only a SQL {@code TIMEOUT n RETURN} clause sets this. A guard that reaches such a deadline raises
   * {@link com.arcadedb.exception.PartialResultTimeoutException} instead of a plain
   * {@link com.arcadedb.exception.TimeoutException}, and the step owning the clause turns that into the end of its
   * result set. Without the distinction the clause could not be enforced inside a scan loop at all: a guard can
   * only stop by throwing, and throwing is exactly what {@code RETURN} promises not to do (issue #6304).
   */
  boolean isCommandDeadlinePartial();

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
