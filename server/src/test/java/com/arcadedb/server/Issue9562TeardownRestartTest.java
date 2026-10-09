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
package com.arcadedb.server;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9562: {@link BaseGraphServerTest#endTest()} restarts every server a test left stopped, then compares the
 * databases. It used to pin the restart to the single HTTP port the server had before, so a port another build took
 * while the server was down failed the restart; the comparison in the {@code finally} then read the stopped server's
 * stale copy from disk and its {@code DatabaseAreNotIdentical} replaced the restart failure that caused it.
 */
class Issue9562TeardownRestartTest {

  @Test
  void aRestartPrefersItsOldPortButMayMoveOnThroughTheConfiguredRange() {
    assertThat(BaseGraphServerTest.restartHttpPortSetting("2480-2489", 2481)).isEqualTo("2481-2489");
    assertThat(BaseGraphServerTest.restartHttpPortSetting(" 2482 - 2489 ", 2483)).isEqualTo("2483-2489");
  }

  @Test
  void aRestartOnTheLastPortOfTheRangeOrPastItKeepsJustThatPort() {
    assertThat(BaseGraphServerTest.restartHttpPortSetting("2480-2489", 2489)).isEqualTo("2489");
    assertThat(BaseGraphServerTest.restartHttpPortSetting("2480-2489", 2495)).isEqualTo("2495");
  }

  /** A subclass that pinned one port, or a port drawn for it, keeps exactly the port it bound, as before. */
  @Test
  void aSinglePortSettingStaysPinnedToTheBoundPort() {
    assertThat(BaseGraphServerTest.restartHttpPortSetting("31337", 31337)).isEqualTo("31337");
    assertThat(BaseGraphServerTest.restartHttpPortSetting("0", 40123)).isEqualTo("40123");
  }

  /** A setting that cannot be read as a range falls back to the bound port rather than failing the teardown. */
  @Test
  void anUnreadableRangeKeepsTheBoundPort() {
    assertThat(BaseGraphServerTest.restartHttpPortSetting("2480-abc", 2481)).isEqualTo("2481");
    assertThat(BaseGraphServerTest.restartHttpPortSetting("2480-2485-2489", 2481)).isEqualTo("2481");
  }

  /** A server that never bound a port has no old port to come back on: the configured setting is left alone. */
  @Test
  void aServerThatNeverBoundAPortKeepsItsConfiguredSetting() {
    assertThat(BaseGraphServerTest.restartHttpPortSetting("2480-2489", -1)).isEqualTo("2480-2489");
  }

  @Test
  void aComparisonFailureDoesNotHideTheRestartFailureThatCausedIt() {
    final ServerException restartFailure = new ServerException("Error on starting HTTP Server: port taken");
    final IllegalStateException comparisonFailure = new IllegalStateException("Types: DB1 8 <> DB2 7");

    BaseGraphServerTest.compareWithoutMasking(restartFailure, () -> {
      throw comparisonFailure;
    });

    assertThat(restartFailure.getSuppressed()).containsExactly(comparisonFailure);
  }

  @Test
  void aComparisonFailureWithNoRestartFailurePropagates() {
    final IllegalStateException comparisonFailure = new IllegalStateException("Types: DB1 8 <> DB2 7");

    assertThatThrownBy(() -> BaseGraphServerTest.compareWithoutMasking(null, () -> {
      throw comparisonFailure;
    })).isSameAs(comparisonFailure);
  }

  @Test
  void aPassingComparisonLeavesTheRestartFailureUntouched() {
    final ServerException restartFailure = new ServerException("Error on starting HTTP Server: port taken");
    final boolean[] compared = { false };

    BaseGraphServerTest.compareWithoutMasking(restartFailure, () -> compared[0] = true);

    assertThat(compared[0]).isTrue();
    assertThat(restartFailure.getSuppressed()).isEmpty();
  }
}
