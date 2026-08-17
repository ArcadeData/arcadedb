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

import com.arcadedb.exception.PartialResultTimeoutException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Timeout;

import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Enforces a statement's own {@code TIMEOUT n [EXCEPTION | RETURN]} clause, for every statement kind that accepts
 * one.
 * <p>
 * <b>One meaning of the word.</b> There used to be two steps: this one, wall clock from the first pull, chained by
 * {@code UPDATE}; and an {@code AccumulatingTimeoutStep} that charged only the time spent inside the pipeline,
 * chained by {@code SELECT}. Same syntax, two bounds - a client streaming a large {@code SELECT} slowly was not
 * billed for its own pauses while the identical number on an {@code UPDATE} was - and nothing said which one a
 * statement got. Both now mean wall clock, which is what the word means everywhere else in the engine:
 * {@code arcadedb.command.timeout}, the ceiling over every statement, has been wall clock since issue #6266, so the
 * accumulating variant was the only bound in the engine that was not (issue #6304).
 * <p>
 * <b>Granularity.</b> This step is a checkpoint between two batches, which bounds nothing when the whole scan
 * happens inside one of them. What makes the clause fine-grained is
 * {@link StatementTimeouts#publish(CommandContext, Timeout, long)}: the instant is pinned on the
 * {@link CommandContext}, and the {@link WorkGuard} checks inside the scan, filter and expansion loops read it from
 * there (issue #6266).
 * <p>
 * <b>{@code RETURN} means return.</b> The clause promises the rows produced so far rather than an exception, and a
 * guard inside a loop can only stop by throwing. The two are reconciled by the guard throwing
 * {@link PartialResultTimeoutException} for a bound pinned by a {@code RETURN} clause, which this step - the one
 * that owns the clause - catches and turns into the end of its result set. Before that the deadline was simply not
 * published for {@code RETURN}, leaving that one clause shape with exactly the between-batches granularity issue
 * #6266 set out to remove.
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeoutStep extends AbstractExecutionStep {
  private final Timeout timeout;

  /** Absolute epoch-millis instant this clause expires at, pinned on the first pull of an execution. */
  private Long expiryTime;

  public TimeoutStep(final Timeout timeout, final CommandContext context) {
    super(context);
    this.timeout = timeout;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (this.expiryTime == null) {
      this.expiryTime = StatementTimeouts.deadlineIn(timeout.getVal().longValue());
      StatementTimeouts.publish(context, timeout, expiryTime);
    }
    if (expired())
      return stopped();

    final ResultSet internal;
    try {
      internal = getPrev().syncPull(context, nRecords);
    } catch (final PartialResultTimeoutException e) {
      // An in-loop guard reached this clause's deadline while producing the batch. The rows of the earlier
      // batches are already with the caller; ending the result set here is what "return what you have" means.
      return stopped();
    }

    if (getPrev().isTimedOut())
      return stopped();

    return new ResultSet() {
      /**
       * The row {@link #hasNext()} produced, held until {@link #next()} hands it over.
       * <p>
       * Both the test and the fetch happen in {@code hasNext()} so that <b>every</b> way this batch can reach
       * the deadline is one a {@code RETURN} clause can answer by ending the result set. Leaving the fetch in
       * {@code next()} does not work: a guard can fire there too - {@code MatchStep.next()} pulls the following
       * candidate before returning the current one, and that pull is guarded - and {@code next()} has no clean
       * way to say "actually, no more rows". Reporting {@code NoSuchElementException} after {@code hasNext()}
       * answered true breaks the iterator contract on a caller that is mid-iteration; raising is what the clause
       * promised not to do; and the row cannot be handed back, because the step that lost it already discarded
       * it. Fetching under the same test removes the case (issue #6304).
       */
      private Result   buffered;
      /**
       * Memoized because the answer must not change between two consecutive calls: {@code LocalResultSet.next()}
       * re-asks {@code hasNext()} after its caller already asked, and a deadline crossing in between would turn
       * a promised row into an {@code IllegalStateException} rather than into the clean end of the result set.
       */
      private Boolean available;

      @Override
      public boolean hasNext() {
        if (available == null) {
          if (timedOut || expired())
            available = stop();
          else
            try {
              available = internal.hasNext();
              buffered = available ? internal.next() : null;
            } catch (final PartialResultTimeoutException e) {
              buffered = null;
              available = stop();
            }
        }
        return available;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        available = null;
        final Result next = buffered;
        buffered = null;
        return next;
      }

      @Override
      public void close() {
        internal.close();
      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return internal.getExecutionPlan();
      }
    };
  }

  private boolean expired() {
    return System.currentTimeMillis() > expiryTime;
  }

  /**
   * Marks the whole chain timed out and either raises or, for the {@code RETURN} strategy, reports that there is
   * nothing more to produce.
   *
   * @return always {@code false}, so a caller can {@code return stop()} from {@code hasNext()}
   */
  private boolean stop() {
    this.timedOut = true;
    sendTimeout();
    if (!Timeout.RETURN.equals(this.timeout.getFailureStrategy()))
      // Named, so a run that stopped on the statement's own clause is distinguishable from one that stopped on
      // the operator's arcadedb.command.timeout - the two are both in force and either can be the one that fires.
      throw new TimeoutException("Timeout expired: " + StatementTimeouts.describe(timeout));
    return false;
  }

  /** {@link #stop()} for the call sites that owe the caller a (necessarily empty) batch. */
  private ResultSet stopped() {
    stop();
    return new InternalResultSet();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new TimeoutStep(this.timeout.copy(), context);
  }

  @Override
  public void reset() {
    // A cached plan gets re-executed: without this, the instant the first execution pinned would still be in
    // force on the next one, and at any realistic clause value it has long passed.
    this.expiryTime = null;
    this.timedOut = false;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ TIMEOUT (" + timeout.getVal().toString() + "ms)";
  }
}
