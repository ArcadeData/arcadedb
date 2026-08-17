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

import com.arcadedb.query.sql.parser.Timeout;

/**
 * Publishes a SQL {@code TIMEOUT} clause's instant onto the {@link CommandContext}, so that the in-loop
 * {@link WorkGuard} checks see the statement's own bound and not only the configured one.
 * <p>
 * Without this the two mechanisms never met. {@code arcadedb.command.timeout} is resolved from the
 * configuration by the context; a {@code TIMEOUT} clause is resolved by the planner and known only to
 * {@link TimeoutStep}. So {@code SELECT ... TIMEOUT 100} on a server that leaves the global setting at its
 * default of 0 - the common case, since a clause is how one query is normally bounded - reproduced exactly the
 * bug issue #6266 is about: a filter that rejects every record scans to the end inside one {@code hasNext()},
 * and the step that owns the clause is not re-entered until it returns.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class StatementTimeouts {
  private StatementTimeouts() {
  }

  /**
   * The step that enforces {@code clause}, or {@code null} when the clause bounds nothing.
   * <p>
   * A value of {@code 0} disables the clause rather than expiring it on the spot, which is what {@code 0} means
   * for every other timeout in the product - {@code arcadedb.command.timeout} and
   * {@code arcadedb.command.regexTimeout} are both documented as "set to 0 to disable" - and which cannot turn a
   * working statement into a failing one. SELECT used to be the exception: it chained the step for any clause at
   * all, so {@code SELECT ... TIMEOUT 0} failed on its first pull while {@code UPDATE ... TIMEOUT 0} ran
   * unbounded. Every statement kind goes through here now, so the four planners cannot drift apart again
   * (issue #6304).
   */
  static TimeoutStep stepFor(final Timeout clause, final CommandContext context) {
    if (clause == null || clause.getVal() == null || clause.getVal().longValue() <= 0)
      return null;
    return new TimeoutStep(clause, context);
  }

  /** What to call this clause when a check aborts on it - the same name whichever check does the aborting. */
  static String describe(final Timeout clause) {
    return "TIMEOUT clause of " + clause.getVal() + "ms";
  }

  /**
   * Returns the absolute instant {@code millis} from now, saturating instead of overflowing - a
   * {@code TIMEOUT 9223372036854775807} must read as "effectively never", not as an instant in the past. Kept
   * one below {@link Long#MAX_VALUE}, which is reserved for "no deadline at all".
   */
  static long deadlineIn(final long millis) {
    if (millis <= 0)
      return 0L;
    final long now = System.currentTimeMillis();
    return millis > Long.MAX_VALUE - 1 - now ? Long.MAX_VALUE - 1 : now + millis;
  }

  /**
   * Pins {@code clauseDeadline} on the context, unless the command's own bound is already the earlier of the two.
   * <p>
   * The earlier wins because both are in force: an operator's {@code arcadedb.command.timeout} is a ceiling over
   * every statement, and one asking for more time than the ceiling allows does not get it. The coarse steps
   * never applied that - a clause simply replaced the global default at planning time - and applying it here is
   * the conservative direction, since a statement can still ask for less.
   * <p>
   * A {@code RETURN} clause is published too, but marked as one that yields rather than fails. It asks for the
   * rows produced so far instead of an exception, and a guard in a scan loop knows only how to throw - so the
   * guard throws {@link com.arcadedb.exception.PartialResultTimeoutException} for such a bound and
   * {@link TimeoutStep} converts it into the end of its result set. Leaving the clause unpublished instead, as
   * this did at first, was the safe reading of "must not turn a documented return-what-you-have into a failure",
   * but it left that one clause shape with only the between-batches granularity issue #6266 was about (#6304).
   *
   * @param clauseDeadline absolute epoch-millis instant the clause expires at
   */
  static void publish(final CommandContext context, final Timeout clause, final long clauseDeadline) {
    if (context == null)
      return;

    if (context.getCommandDeadline() <= clauseDeadline)
      // The command's own bound is the stricter one, and it already carries its own description. Note this also
      // keeps a RETURN clause from softening a stricter bound in force around it: the ceiling stays as it is,
      // including what reaching it means.
      return;

    context.setCommandDeadline(clauseDeadline, describe(clause), Timeout.RETURN.equals(clause.getFailureStrategy()));
  }
}
