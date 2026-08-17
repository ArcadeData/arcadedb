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
import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for item 3 of issue #6322: {@code COMMIT RETRY n} retried a deadline that had already passed.
 * <p>
 * Retrying a {@code TimeoutException} is right for the one it was written for - {@code TransactionManager}'s
 * file-lock timeout during commit, which is transient contention that clears after a backoff. It is not right
 * for the command's own deadline: {@code arcadedb.command.timeout} and an enclosing {@code TIMEOUT} clause pin
 * one instant for the whole statement (issue #6266), and every attempt this step starts inherits it, so once it
 * has passed the remaining attempts abort on their first check. A {@code COMMIT RETRY 10} block spent all ten
 * attempts plus nine backoff sleeps re-reaching a bound that expired before the first retry, and then reported
 * the last attempt's failure rather than the deadline that actually stopped it.
 * <p>
 * The assertions are attempt counts rather than elapsed time: the counter says how many times the loop went
 * round, which is the thing that changed, and no JVM stall can push it up.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CommitRetryCommandDeadlineIssue6322Test extends TestHelper {
  /** Attempts of the retry body that actually reached the failing function. */
  private static final AtomicInteger ATTEMPTS = new AtomicInteger();

  /** Wall-clock milliseconds each attempt spends before failing, so a deadline can be made to pass. */
  private static volatile long burnMillis = 0;

  /** Whether the failure is a lock-style timeout rather than a concurrent-modification conflict. */
  private static volatile boolean failWithTimeout = false;

  /** Consumes what the burn loop produces, so the JIT cannot delete a computation nobody reads. */
  private static volatile long sink;

  @Override
  protected void beginTest() {
    ATTEMPTS.set(0);
    burnMillis = 0;
    failWithTimeout = false;
    database.getSchema().createDocumentType("Attempt");
    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(new SQLFunctionAbstract("boom6322") {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
          final Object[] params, final CommandContext context) {
        ATTEMPTS.incrementAndGet();
        // Wall clock rather than a fixed number of iterations: the point is to outlast a deadline expressed in
        // milliseconds, and a busy runner must make that more certain, not less.
        final long until = System.currentTimeMillis() + burnMillis;
        long burnt = 0;
        while (System.currentTimeMillis() < until)
          burnt = burnt * 31 + 7;
        sink += burnt;
        if (failWithTimeout)
          throw new TimeoutException("simulated file-lock timeout during commit");
        throw new ConcurrentModificationException("simulated write conflict");
      }

      @Override
      public String getSyntax() {
        return "boom6322()";
      }
    });
  }

  @AfterEach
  void restoreConfiguration() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT,
        GlobalConfiguration.COMMAND_TIMEOUT.getDefValue());
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY,
        GlobalConfiguration.TX_RETRY_DELAY.getDefValue());
  }

  /**
   * The command deadline has passed by the end of the first attempt, so the nine that were left are not started.
   * The reported failure names the bound, rather than being the conflict the last attempt happened to hit.
   */
  @Test
  void anExpiredCommandDeadlineStopsTheRetryLoop() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 300L);
    burnMillis = 900;

    assertThatThrownBy(() -> database.command("sqlscript", script(10)))
        .isInstanceOf(TimeoutException.class)
        .hasMessageContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());

    assertThat(ATTEMPTS.get()).isEqualTo(1);
  }

  /**
   * With no deadline in force the block still spends every attempt it was given - the retry the user asked for
   * is a retry, and the change above only removes the ones that cannot possibly differ.
   */
  @Test
  void withoutADeadlineEveryRetryIsStillSpent() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 0);

    assertThatThrownBy(() -> database.command("sqlscript", script(4)))
        .isInstanceOf(ConcurrentModificationException.class);

    assertThat(ATTEMPTS.get()).isEqualTo(4);
  }

  /**
   * A {@code TimeoutException} that is not the command's own - the file-lock timeout the catch was written for -
   * keeps consuming the retry budget, because waiting is exactly what clears it.
   */
  @Test
  void aLockTimeoutIsStillRetried() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 0);
    failWithTimeout = true;

    assertThatThrownBy(() -> database.command("sqlscript", script(4)))
        .isInstanceOf(TimeoutException.class)
        .hasMessageContaining("file-lock");

    assertThat(ATTEMPTS.get()).isEqualTo(4);
  }

  /**
   * A deadline generous enough to outlast the whole loop changes nothing: the guard is "has it passed", not
   * "is one configured".
   */
  @Test
  void aDeadlineThatHasNotPassedDoesNotStopTheLoop() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 3_600_000L);
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 0);

    assertThatThrownBy(() -> database.command("sqlscript", script(4)))
        .isInstanceOf(ConcurrentModificationException.class);

    assertThat(ATTEMPTS.get()).isEqualTo(4);
  }

  /**
   * {@code ELSE CONTINUE} asks for the empty result set instead of the failure when the attempts run out. An
   * expired command deadline is not "the attempts ran out": it leaves the block by a different door, without
   * running the {@code ELSE} body and without consulting {@code ELSE FAIL}, and raises.
   * <p>
   * That is the behaviour the issue asked for - it names "with no {@code ELSE FAIL}, returns an empty result
   * set instead of the error" as part of the defect - and the reason is that the alternative is unreadable at
   * the caller: an empty result set cannot be told apart from a block that legitimately produced no rows, so a
   * command cut off by its deadline would look like one that simply had nothing to say.
   */
  @Test
  void anExpiredDeadlineRaisesEvenWhenElseAsksToContinue() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 300L);
    burnMillis = 900;

    assertThatThrownBy(() -> database.command("sqlscript", scriptElseContinue(10)))
        .isInstanceOf(TimeoutException.class)
        .hasMessageContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());

    assertThat(ATTEMPTS.get()).isEqualTo(1);
  }

  /** The same block with no deadline in force: every attempt is spent and {@code ELSE CONTINUE} is honoured. */
  @Test
  void elseContinueStillAnswersAnEmptyResultSetWithoutADeadline() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 0);

    int rows = 0;
    try (final ResultSet rs = database.command("sqlscript", scriptElseContinue(3))) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }

    assertThat(rows).isEqualTo(0);
    assertThat(ATTEMPTS.get()).isEqualTo(3);
  }

  private static String script(final int retries) {
    return """
        BEGIN;
        INSERT INTO Attempt SET n = 1;
        SELECT boom6322();
        COMMIT RETRY %d;
        """.formatted(retries);
  }

  private static String scriptElseContinue(final int retries) {
    return """
        BEGIN;
        INSERT INTO Attempt SET n = 1;
        SELECT boom6322();
        COMMIT RETRY %d ELSE CONTINUE;
        """.formatted(retries);
  }
}
