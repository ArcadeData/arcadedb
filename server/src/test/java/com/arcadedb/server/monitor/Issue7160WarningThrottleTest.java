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
package com.arcadedb.server.monitor;

import com.arcadedb.server.monitor.ServerMonitor.SafepointSpike;
import com.arcadedb.server.monitor.ServerMonitor.SafepointSpikeDetector;
import com.arcadedb.server.monitor.ServerMonitor.WarningThrottle;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the warning rate limit of {@link ServerMonitor} (issue #7160).
 * <p>
 * The safepoint check ran on the same 10-second loop as the other two with no limit at all, so a JVM whose
 * pauses stay elevated wrote a WARNING to the server event log every interval - 8640 a day. The rule the other
 * two checks already implemented by hand is now one object all three share, taking the clock as a parameter so
 * that half an hour is something a test can state rather than wait for.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7160WarningThrottleTest {
  private static final long MINS_30 = 30 * 60 * 1_000L;

  @Test
  void theFirstReportIsAlwaysLetThrough() {
    final WarningThrottle throttle = new WarningThrottle(MINS_30);

    assertThat(throttle.reportedWithinWindow(0L)).as("nothing reported yet").isFalse();
    assertThat(throttle.tryReport(0L)).isTrue();
    assertThat(throttle.reportedWithinWindow(0L)).isTrue();
  }

  @Test
  void aSecondReportInsideTheWindowIsSuppressed() {
    final WarningThrottle throttle = new WarningThrottle(MINS_30);
    throttle.tryReport(0L);

    assertThat(throttle.tryReport(10_000L)).as("the very next 10-second sampling interval").isFalse();
    assertThat(throttle.tryReport(MINS_30 - 1)).as("and every one up to the edge of the window").isFalse();
  }

  @Test
  void aReportIsLetThroughAgainOnceTheWindowHasPassed() {
    final WarningThrottle throttle = new WarningThrottle(MINS_30);
    throttle.tryReport(0L);

    assertThat(throttle.tryReport(MINS_30)).as("the window is inclusive of its own length").isTrue();
    assertThat(throttle.tryReport(MINS_30 + 10_000L)).as("and closes again behind it").isFalse();
    assertThat(throttle.tryReport(2 * MINS_30)).isTrue();
  }

  /** A refused report must not slide the window forward, or a persistent condition would never report again. */
  @Test
  void aSuppressedReportDoesNotExtendTheWindow() {
    final WarningThrottle throttle = new WarningThrottle(MINS_30);
    throttle.tryReport(0L);

    for (long now = 10_000L; now < MINS_30; now += 10_000L)
      throttle.tryReport(now);

    assertThat(throttle.tryReport(MINS_30)).as("still due exactly one window after the report that was made")
        .isTrue();
  }

  /** {@code wouldReport} answers the same question without consuming the window. */
  @Test
  void askingDoesNotConsumeTheWindow() {
    final WarningThrottle throttle = new WarningThrottle(MINS_30);

    assertThat(throttle.wouldReport(0L)).isTrue();
    assertThat(throttle.wouldReport(0L)).as("asking twice is still a yes").isTrue();

    throttle.reported(0L);
    assertThat(throttle.wouldReport(0L)).isFalse();
  }

  /** Each kind of warning carries its own window: a day for low disk against half an hour for the others. */
  @Test
  void windowsAreIndependentPerKind() {
    final WarningThrottle daily = new WarningThrottle(24 * 60 * 60 * 1_000L);
    final WarningThrottle halfHourly = new WarningThrottle(MINS_30);

    daily.tryReport(0L);
    halfHourly.tryReport(0L);

    assertThat(halfHourly.tryReport(MINS_30)).isTrue();
    assertThat(daily.tryReport(MINS_30)).as("a day has not passed").isFalse();
  }

  /**
   * The limit is on the REPORT and never on the measurement: the detector needs every sample to keep its
   * interval baseline, so a spike suppressed by the throttle must still have advanced the detector.
   */
  @Test
  void samplingContinuesWhileReportingIsSuppressed() {
    final SafepointSpikeDetector detector = new SafepointSpikeDetector();
    final WarningThrottle throttle = new WarningThrottle(MINS_30);

    // Three samples open the first comparable interval: one to open, one to close it, one to close the next.
    detector.sample(100L, 10L);
    detector.sample(200L, 20L);

    final SafepointSpike firstSpike = detector.sample(500L, 30L);
    assertThat(firstSpike).as("10ms -> 30ms average pause").isNotNull();
    assertThat(throttle.tryReport(0L)).isTrue();

    final SafepointSpike suppressedSpike = detector.sample(1_400L, 40L);
    assertThat(suppressedSpike).as("the detector still measures it").isNotNull();
    assertThat(throttle.tryReport(10_000L)).as("the event log does not hear about it").isFalse();

    // Because the suppressed sample still advanced the baseline, the next interval is measured against IT and
    // not against the last one that happened to be reported.
    assertThat(suppressedSpike.previousIntervalAvgMs()).isEqualTo(firstSpike.currentIntervalAvgMs());
  }
}
