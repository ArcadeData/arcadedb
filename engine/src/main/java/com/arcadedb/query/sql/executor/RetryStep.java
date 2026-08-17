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
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.utility.RetryBackoff;

import java.util.List;
import java.util.logging.Level;

public class RetryStep extends AbstractExecutionStep {
  public        List<Statement>       body;
  public        List<Statement>       elseBody;
  public        boolean               elseFail;
  private final int                   retries;
  private final int                   retryDelay;
  private final int                   retryDelayBase;
  private       ExecutionStepInternal finalResult = null;

  public RetryStep(List<Statement> statements, int retries, List<Statement> elseStatements, Boolean elseFail, CommandContext ctx,
      boolean enableProfiling) {
    super(ctx);
    this.body = statements;
    this.retries = retries;
    this.elseBody = elseStatements;
    this.elseFail = !Boolean.FALSE.equals(elseFail);
    this.retryDelay = readRetrySetting(ctx, GlobalConfiguration.TX_RETRY_DELAY);
    this.retryDelayBase = readRetrySetting(ctx, GlobalConfiguration.TX_RETRY_DELAY_BASE);
  }

  /**
   * Resolves a retry-backoff setting from the database the script runs against. The setting is declared with
   * database scope, so the database's own configuration is the authority and it already falls back to the
   * global value when the database does not override it. The command context's configuration is deliberately
   * not used: the script engine is handed either an empty configuration (embedded API) or the server's one
   * (HTTP, Postgres and the replicated paths), and neither carries a per-database override.
   */
  private static int readRetrySetting(final CommandContext ctx, final GlobalConfiguration setting) {
    final DatabaseInternal database = ctx != null ? ctx.getDatabase() : null;
    return database != null ?
        database.getConfiguration().getValueAsInteger(setting) :
        setting.getValueAsInteger();
  }

  // @VisibleForTesting
  int getRetryDelay() {
    return retryDelay;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    if (prev != null)
      prev.syncPull(ctx, nRecords);

    if (finalResult != null) {
      return finalResult.syncPull(ctx, nRecords);
    }
    for (int attempt = 0; attempt < retries; attempt++) {
      try {
        final ScriptExecutionPlan plan = initPlan(body, ctx);
        final ExecutionStepInternal result = plan.executeFull();
        if (result != null) {
          this.finalResult = result;
          return result.syncPull(ctx, nRecords);
        }
        break;
      } catch (final TimeoutException ex) {
        // A TimeoutException is retried because the primary source under concurrent writes is
        // TransactionManager's file-lock timeout during commit (see
        // TransactionManager.lockFilesInOrder). That contention is transient and almost always
        // clears after a short backoff, so retrying inside a COMMIT RETRY block is the right
        // behavior. A TIMEOUT clause written inside the block is retried for the same reason: each
        // attempt builds its own plan on a context of its own, so the clause starts its interval again.
        // The command's own deadline is the one kind that cannot be cured by waiting, and it is
        // already the exception the caller should see, so it propagates as it is.
        //
        // Propagating rather than wrapping does mean one narrow case reports the less useful of two true
        // statements: a file-lock timeout raised at the very moment the command deadline also passes is
        // reported as the lock, though it is the deadline that ended the loop. Wrapping unconditionally
        // would fix the wording and break something worth more - a PartialResultTimeoutException that
        // reached here would stop being one, turning a TIMEOUT n RETURN clause's documented
        // return-what-you-have into a failure. The wording of a race loses to that.
        //
        // Note this leaves the block by a different door than running out of attempts does: it does not run
        // the ELSE body and it does not consult ELSE FAIL. That is deliberate. The deadline says the command
        // must stop, so an ELSE body - arbitrary statements, possibly writes, on a transaction that was just
        // rolled back - is more work the operator asked us not to do; and returning the empty result set
        // `ELSE ... AND CONTINUE` asks for would leave the caller unable to tell a block that legitimately
        // produced nothing from one the deadline cut off, which is the silent failure issue #6322 named.
        rollbackQuietly(ctx);
        if (commandDeadlineReached(ctx))
          throw ex;

        final ResultSet finished = giveUpOrBackOff(ctx, nRecords, attempt, ex);
        if (finished != null)
          return finished;
      } catch (final NeedRetryException ex) {
        // A write conflict clears on its own, so this is the retry the user asked for - unless the
        // command's deadline has passed, in which case the remaining attempts cannot even start their
        // work. The deadline, not the conflict, is what ended the block, and saying so is the whole
        // point: the conflict alone would leave a COMMIT RETRY 10 that made one attempt unexplained.
        rollbackQuietly(ctx);
        if (commandDeadlineReached(ctx))
          throw new TimeoutException(
              "COMMIT RETRY gave up after " + (attempt + 1) + " of " + retries + " attempts: the command exceeded the "
                  + ctx.getCommandDeadlineDescription(), ex);

        final ResultSet finished = giveUpOrBackOff(ctx, nRecords, attempt, ex);
        if (finished != null)
          return finished;
      }
    }

    finalResult = new EmptyStep(ctx);
    return finalResult.syncPull(ctx, nRecords);
  }

  /**
   * Ends the attempt that just failed: either the block is out of attempts - in which case the {@code ELSE}
   * body runs and {@code ELSE FAIL} decides between raising {@code ex} and answering an empty result set - or
   * the backoff interval is slept and the loop goes round again.
   *
   * @return the result set the block finished with, or {@code null} when another attempt is due
   */
  private ResultSet giveUpOrBackOff(final CommandContext ctx, final int nRecords, final int attempt,
      final ArcadeDBException ex) {
    if (attempt >= retries - 1) {
      if (elseBody != null && !elseBody.isEmpty()) {
        final ScriptExecutionPlan plan = initPlan(elseBody, ctx);
        final ExecutionStepInternal result = plan.executeFull();
        if (result != null) {
          this.finalResult = result;
          return result.syncPull(ctx, nRecords);
        }
      }
      if (elseFail)
        throw ex;
      return new InternalResultSet();
    }

    delayBetweenRetries(attempt);
    return null;
  }

  /** Discards the failed attempt's transaction. A rollback that itself fails changes nothing the caller can act on. */
  private static void rollbackQuietly(final CommandContext ctx) {
    try {
      ctx.getDatabase().rollback();
    } catch (Exception e) {
      // IGNORE IT
    }
  }

  /**
   * Whether the deadline every attempt of this block runs under has already passed. A retry is only a retry
   * when its precondition can change: the command's deadline is one instant pinned for the whole statement
   * (issue #6266) and every attempt this step starts inherits it, so once it has passed the block would spend
   * its remaining budget, plus a backoff sleep between each pair of attempts, re-reaching a bound that expired
   * before the first retry (issue #6322).
   * <p>
   * Read from the step's own context rather than from the attempt's: a {@code TIMEOUT} clause inside the body
   * publishes on the child context {@link #initPlan} builds per attempt, so it does not answer here and stays
   * retryable, while {@code arcadedb.command.timeout} and an enclosing {@code TIMEOUT} clause do.
   * <p>
   * Deliberately not consulting {@link CommandContext#isCommandDeadlinePartial()}, which everywhere else
   * separates a hard bound from a {@code TIMEOUT n RETURN} that asks for the rows produced so far. A partial
   * deadline cannot be the one in force here: it is published by {@link StatementTimeouts} onto the context of
   * the statement carrying the clause, and a script line runs on a plan of its own, so it never reaches the
   * script context this step is pulled with - verified by reading the deadline back inside a
   * {@code COMMIT RETRY} body that follows a {@code SELECT ... TIMEOUT n RETURN} line in the same script.
   */
  private static boolean commandDeadlineReached(final CommandContext ctx) {
    return ctx != null && System.currentTimeMillis() > ctx.getCommandDeadline();
  }

  public ScriptExecutionPlan initPlan(final List<Statement> body, final CommandContext ctx) {
    final BasicCommandContext subCtx1 = new BasicCommandContext();
    subCtx1.setParent(ctx);

    final ScriptExecutionPlan plan = new ScriptExecutionPlan(subCtx1);
    for (Statement stm : body)
      plan.chain(stm.createExecutionPlan(subCtx1));

    return plan;
  }

  /**
   * Sleeps an exponential-backoff-with-full-jitter interval before the next {@code COMMIT RETRY} attempt
   * (issue #5587). See {@link RetryBackoff} for the shared policy, also used by the programmatic retry loop in
   * {@link com.arcadedb.database.LocalDatabase#transaction}.
   *
   * @param attempt zero-based count of retries already performed by this statement
   */
  private void delayBetweenRetries(final int attempt) {
    if (retryDelay > 0) {
      LogManager.instance()
          .log(this, Level.FINE, "Wait up to %d ms before the next retry for transaction commit (attempt=%d, threadId=%d)",
              RetryBackoff.windowMs(attempt, retryDelayBase, retryDelay), attempt + 1, Thread.currentThread().getId());

      RetryBackoff.sleep(attempt, retryDelayBase, retryDelay);
    }
  }
}
