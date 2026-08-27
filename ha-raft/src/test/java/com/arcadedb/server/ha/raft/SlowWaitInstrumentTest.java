/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #6343, and specifically for the way that issue nearly went wrong.
 * <p>
 * {@link BaseRaftHATest}'s slow-wait instrument exists to be read back out of CI logs: it is the only evidence
 * anyone has for how much of {@code RESYNC_RETRY_TIMEOUT_MS} the HA integration tests actually need, and every
 * cut to that budget so far - 120s to 30s, then 30s to 15s - has been decided by grepping runs on main for its
 * lines. Issue #6343 asked for exactly that, naming the thing to grep for: "collect the {@code SLOW WAIT} lines".
 * Forty full runs produced none. Not because no wait was slow, but because <em>no line the instrument has ever
 * emitted contained the string {@code SLOW WAIT}</em> - the report was a formatted sentence with no fixed token
 * in it. The grep returned zero for a report that was working and would have returned zero for a report that had
 * been deleted, and nothing in between could tell the two apart.
 * <p>
 * So the instrument now carries {@link BaseRaftHATest#SLOW_WAIT_MARKER}, and these tests are what stop that
 * marker from being reworded away again. They assert the three things a future evidence-gathering pass depends
 * on: that a slow wait produces a line at all, that the line carries the marker verbatim <em>after</em> the real
 * logging path has formatted it, and that the threshold above which a wait is reported stays a fixed fraction of
 * the budget it is evidence about - because that fraction, not the absolute millisecond value, is what bounds how
 * much of the budget a wait can burn while still reading as silence.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SlowWaitInstrumentTest {

  /**
   * The fraction of the budget at which a wait becomes worth a line. Held at a sixth since the instrument was
   * added: 10s of 120s, 5s of 30s, 2.5s of 15s. Asserted as a ratio rather than as two literals, so that
   * retuning both numbers together stays legal while letting either drift away from the other does not.
   */
  private static final long REPORT_THRESHOLD_FRACTION_OF_BUDGET = 6;

  @Test
  void aWaitBelowTheThresholdIsNotWorthALine() {
    assertThat(BaseRaftHATest.slowWaitReport("awaitValue(7)", 0, true)).isNull();
    assertThat(BaseRaftHATest.slowWaitReport("awaitValue(7)", reportThresholdMs() - 1, true)).isNull();
  }

  @Test
  void aWaitAtOrAboveTheThresholdIsReported() {
    assertThat(BaseRaftHATest.slowWaitReport("awaitValue(7)", reportThresholdMs(), true)).isNotNull();
    assertThat(BaseRaftHATest.slowWaitReport("awaitValue(7)", budgetMs(), false)).isNotNull();
  }

  @Test
  void theReportNamesTheWaitTheElapsedAndTheBudget() {
    final long budget = budgetMs();

    final String satisfied = BaseRaftHATest.slowWaitReport("withResyncRetry on server 2", budget - 1, true);
    assertThat(satisfied).contains("withResyncRetry on server 2");
    assertThat(satisfied).contains("satisfied");
    assertThat(satisfied).contains(String.valueOf(budget - 1));
    assertThat(satisfied).contains(budget + " ms budget");

    // "GAVE UP" in capitals: a wait that exhausted the budget is the one reading that argues for raising it
    // again, and it has to be distinguishable from a slow-but-fine wait at a glance in a wall of log.
    final String gaveUp = BaseRaftHATest.slowWaitReport("withResyncRetry on server 2", budget, false);
    assertThat(gaveUp).contains("GAVE UP");
    assertThat(gaveUp).doesNotContain("satisfied");
  }

  /**
   * The point of the whole exercise: the marker has to survive all the way through the real logging path, since
   * that is the only form anybody ever greps. Asserting on the return value of
   * {@code slowWaitReport} alone would not catch a {@code reportSlowWait} that dropped the report on the floor,
   * logged it below WARNING, or passed it as a format string instead of an argument.
   */
  @Test
  void theMarkerSurvivesIntoWhatIsActuallyLogged() {
    final CapturingTestLogger logger = capturing(() -> BaseRaftHATest.logSlowWait(this, "awaitValue(42)", budgetMs(), false));

    final List<String> warnings = logger.formattedAt(Level.WARNING);
    assertThat(warnings).hasSize(1);
    assertThat(warnings.getFirst()).contains(BaseRaftHATest.SLOW_WAIT_MARKER);
    // The literal token issue #6343 went looking for. Kept as a separate assertion from the marker constant on
    // purpose: renaming the constant is fine, changing the words a tracker greps for is not.
    assertThat(warnings.getFirst()).contains("SLOW WAIT");
    // Asserted as an absence too: a report demoted to INFO still says everything it used to and is still wrong,
    // because nobody scrolls an HA IT log looking for INFO lines.
    assertThat(logger.formattedAt(Level.INFO)).isEmpty();
  }

  @Test
  void aFastWaitLogsNothingAtAll() {
    final CapturingTestLogger logger = capturing(() -> BaseRaftHATest.logSlowWait(this, "awaitValue(42)", 0, true));

    assertThat(logger.formattedAt(Level.WARNING)).isEmpty();
  }

  /**
   * {@code what} is assembled from caller-supplied text ({@code "awaitValue(" + expected + ")"}), so a value
   * whose {@code toString()} contains a percent sign would be a format specifier if the report were passed as
   * the format string. The instrument must report, not throw.
   */
  @Test
  void aPercentSignInTheWaitDescriptionDoesNotBreakTheReport() {
    final CapturingTestLogger logger = capturing(
        () -> BaseRaftHATest.logSlowWait(this, "awaitValue(100% of them)", budgetMs(), true));

    final List<String> warnings = logger.formattedAt(Level.WARNING);
    assertThat(warnings).hasSize(1);
    assertThat(warnings.getFirst()).contains("awaitValue(100% of them)");
    assertThat(warnings.getFirst()).contains(BaseRaftHATest.SLOW_WAIT_MARKER);
  }

  /**
   * The invariant behind every cut so far, asserted as the equality the javadoc on those two constants claims -
   * not as an upper bound on the threshold, which is only half of it.
   * <p>
   * Too high and the budget goes back to being unmeasurable: a threshold left at 5s while the budget came down
   * to 15s would let a wait burn a third of it and still report nothing, the same blindness in miniature that
   * let 120s stand unexamined for years. Too low and the instrument stops discriminating - at 1 ms every wait
   * reports, the log fills with lines that mean nothing, and the next reader has no more idea which waits were
   * slow than if there had been no lines at all. Both directions destroy the evidence, so both are asserted.
   */
  @Test
  void theReportThresholdStaysAFixedFractionOfTheBudget() {
    final long budget = budgetMs();
    final long threshold = reportThresholdMs();

    assertThat(threshold).isPositive();
    assertThat(threshold * REPORT_THRESHOLD_FRACTION_OF_BUDGET)
        .as("the report threshold is %d ms of a %d ms budget, which is not the one-%d-th that BaseRaftHATest "
                + "documents and that all three settings of this pair have used (10s of 120s, 5s of 30s, 2.5s of "
                + "15s). Above that ratio a wait burns more of the budget in silence than it ever has; below it "
                + "the instrument reports on waits that are simply normal and stops being able to point at the "
                + "ones that are not. If the ratio itself is what you meant to change, change it here and in that "
                + "javadoc together", threshold, budget, REPORT_THRESHOLD_FRACTION_OF_BUDGET)
        .isEqualTo(budget);
  }

  /**
   * The budget, read back out of the report rather than out of a field: the report is the only form of it
   * anybody reading a CI log ever sees, so it is the form worth pinning.
   */
  private static long budgetMs() {
    final String report = BaseRaftHATest.slowWaitReport("probe", Long.MAX_VALUE / 2, true);
    assertThat(report).as("a wait of half of forever must be reported").isNotNull();

    final String tail = report.substring(report.indexOf("of the ") + "of the ".length());
    return Long.parseLong(tail.substring(0, tail.indexOf(" ms budget")));
  }

  /**
   * Finds the smallest elapsed time the instrument reports on, by bisection rather than by reading the constant.
   * A test that hardcoded 2500 would have to be edited by the same commit that retunes the threshold, which is
   * exactly the commit whose reasoning nobody would then be forced to re-examine.
   */
  private static long reportThresholdMs() {
    long silent = 0;                 // known not reported
    long reported = budgetMs() + 1;  // known reported
    assertThat(BaseRaftHATest.slowWaitReport("probe", silent, true)).isNull();
    assertThat(BaseRaftHATest.slowWaitReport("probe", reported, true)).isNotNull();

    while (reported - silent > 1) {
      final long mid = silent + (reported - silent) / 2;
      if (BaseRaftHATest.slowWaitReport("probe", mid, true) == null)
        silent = mid;
      else
        reported = mid;
    }
    return reported;
  }

  /**
   * Runs {@code body} with the module's shared {@link CapturingTestLogger} installed, and returns it. Shared
   * rather than a second Logger implementation in the same package: the 17-arg/varargs plumbing and the
   * argument substitution are fiddly enough to be worth having in exactly one place, and that helper already
   * had them.
   */
  private static CapturingTestLogger capturing(final Runnable body) {
    final CapturingTestLogger logger = CapturingTestLogger.install();
    try {
      body.run();
    } finally {
      logger.uninstall();
    }
    return logger;
  }
}
