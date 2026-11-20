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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Self-tests of the server test fixtures: the teardown path resolution, the free-port allocator, and the restart of a
 * server a test left stopped.
 */
class TestFixtureSelfTest {
  /*
   * Issue #6297: {@code TestServerHelper.deleteDatabaseFolders} resolves every path it deletes from the live
   * configuration, so a caller that resets first does not merely fail to clean up - it asks for a recursive delete of
   * absolute root-level paths.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #6297 deleteDatabaseFolders resolves every path from the live configuration
  /**
   * The trap itself, pinned so it cannot be rediscovered: {@code SERVER_DATABASE_DIRECTORY} defaults to
   * {@code ${arcadedb.server.rootPath}/databases} over a root path that defaults to {@code null}, and the resolver
   * substitutes an empty string for an unknown variable rather than failing.
   */
  @Test
  void aResetConfigurationResolvesTheDatabaseDirectoryToTheFilesystemRoot() {
    GlobalConfiguration.resetAll();
    final String resolved = GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString();

    assertThat(resolved).isEqualTo("/databases");
    assertThat(TestServerHelper.isResolvedTestPath(resolved)).isFalse();
    assertThat(TestServerHelper.isResolvedTestPath(resolved + 0 + File.separator)).isFalse();
  }

  // Regression: #6297 deleteDatabaseFolders resolves every path from the live configuration
  @Test
  void theGuardAcceptsEveryShapeATestActuallyConfigures(@TempDir final Path tempDir) {
    assertThat(TestServerHelper.isResolvedTestPath("./target/databases0" + File.separator)).isTrue();
    assertThat(TestServerHelper.isResolvedTestPath("target/databases")).isTrue();
    assertThat(TestServerHelper.isResolvedTestPath(tempDir.resolve("databases").toString())).isTrue();
    assertThat(TestServerHelper.isResolvedTestPath(tempDir.resolve("databases") + "0" + File.separator)).isTrue();

    assertThat(TestServerHelper.isResolvedTestPath(null)).isFalse();
    assertThat(TestServerHelper.isResolvedTestPath("")).isFalse();
    assertThat(TestServerHelper.isResolvedTestPath("   ")).isFalse();
    assertThat(TestServerHelper.isResolvedTestPath("/")).isFalse();
    assertThat(TestServerHelper.isResolvedTestPath("/databases")).isFalse();

    // The suffix is appended before the check, not after it, so these are the strings actually asked about: a
    // folder the collapsed placeholder named stays refused once the per-server digit and separator are on it, and
    // a root path that itself collapsed to "/" is refused for its replication folder too.
    assertThat(TestServerHelper.isResolvedTestPath("/databases" + 0 + File.separator)).isFalse();
    assertThat(TestServerHelper.isResolvedTestPath("/" + File.separator + "replication")).isFalse();
    assertThat(TestServerHelper.isResolvedTestPath("./target" + File.separator + "replication")).isTrue();
  }

  // Regression: #6297 deleteDatabaseFolders resolves every path from the live configuration
  /**
   * The two orderings, end to end: with the configuration live the per-server folder goes, and with it reset the
   * same call touches nothing at all instead of walking off to {@code /databases0}.
   */
  @Test
  void deleteDatabaseFoldersCleansWhenResolvedAndRefusesWhenNot(@TempDir final Path tempDir) throws Exception {
    final Path root = tempDir.resolve("root");
    final Path server0 = root.resolve("databases0");
    Files.createDirectories(server0);
    Files.writeString(server0.resolve("marker.txt"), "issue-6297");

    try {
      GlobalConfiguration.SERVER_ROOT_PATH.setValue(root.toString());
      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(root.resolve("databases").toString());
      TestServerHelper.deleteDatabaseFolders(1);
      assertThat(server0).as("the folder the live configuration names is the one that gets cleaned").doesNotExist();

      Files.createDirectories(server0);
      Files.writeString(server0.resolve("marker.txt"), "issue-6297");

      GlobalConfiguration.resetAll();
      TestServerHelper.deleteDatabaseFolders(1);
      assertThat(server0).as("a cleanup running after the reset must delete nothing rather than delete /databases0")
          .exists();
    } finally {
      GlobalConfiguration.resetAll();
    }
  }

  /*
   * Issue #7496: the gRPC HA integration tests bound hand-picked ports ({@code 51141 + serverIndex}), so a server from
   * an earlier class that had not released its port yet - or anything else on the runner holding it - failed
   * whichever test started next, with "Address already in use". {@link StaticBaseServerTest#allocateFreePorts} is
   * what they take their ports from now; these are the properties that makes them rely on.
   */
  // Regression: #7496 allocateFreePorts hands out ports nothing else holds
  /**
   * The ports must come from BELOW every operating system's ephemeral range. That range is where each outgoing TCP
   * connection on the host takes its local port from, so a port probed there and released can be taken by the very
   * next connect() anywhere on the machine - including the fixture's own first Raft node dialling the second one
   * before the second has bound. Ratis answers that bind failure with System.exit(1), which kills the whole test fork
   * (seen while verifying #7496 with the previous, ephemeral-range allocator).
   */
  @Test
  void allocatesBelowEveryEphemeralRange() {
    final int[] ports = StaticBaseServerTest.allocateFreePorts(16);

    for (final int port : ports)
      assertThat(port).isBetween(StaticBaseServerTest.FREE_PORT_RANGE_FIRST, StaticBaseServerTest.FREE_PORT_RANGE_LAST);
    // Linux starts its ephemeral range at 32768; macOS and Windows at 49152.
    assertThat(StaticBaseServerTest.FREE_PORT_RANGE_LAST).isLessThan(32768);
  }

  // Regression: #7496 allocateFreePorts hands out ports nothing else holds
  /**
   * The defect itself, turned around: a port somebody still holds is never handed out. A two-port range with one of
   * them held leaves exactly one right answer, so this cannot pass by the luck of a random draw.
   */
  @Test
  void neverHandsOutAPortSomebodyStillHolds() throws IOException {
    final int held = freeAdjacentPair();
    try (final ServerSocket lingering = new ServerSocket()) {
      lingering.bind(new InetSocketAddress(held));

      final int[] ports = StaticBaseServerTest.allocateFreePorts(1, held, held + 1);

      assertThat(ports).containsExactly(held + 1);
    }
  }

  /** A range with nothing free left must fail loudly, never return a port that is taken. */
  // Regression: #7496 allocateFreePorts hands out ports nothing else holds
  @Test
  void failsWhenTheRangeHasNoFreePortLeft() throws IOException {
    final int held = freeAdjacentPair();
    try (final ServerSocket lingering = new ServerSocket()) {
      lingering.bind(new InetSocketAddress(held));

      assertThatThrownBy(() -> StaticBaseServerTest.allocateFreePorts(1, held, held))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("free");
    }
  }

  // Regression: #7496 allocateFreePorts hands out ports nothing else holds
  /**
   * One fixture's servers each need their own port, and the caller must be able to bind every one of them once the
   * call returns: the probing sockets are closed, not leaked.
   */
  @Test
  void returnsDistinctPortsTheCallerCanBind() throws IOException {
    final int[] ports = StaticBaseServerTest.allocateFreePorts(6);

    assertThat(ports).hasSize(6);
    assertThat(Arrays.stream(ports).distinct().count()).isEqualTo(6);

    final ServerSocket[] bound = new ServerSocket[ports.length];
    try {
      for (int i = 0; i < ports.length; i++) {
        bound[i] = new ServerSocket();
        bound[i].bind(new InetSocketAddress(ports[i]));
        assertThat(bound[i].getLocalPort()).isEqualTo(ports[i]);
      }
    } finally {
      for (final ServerSocket socket : bound)
        if (socket != null)
          socket.close();
    }
  }

  /** The first port {@code p} of the allocation range such that {@code p} and {@code p + 1} are both free now. */
  private static int freeAdjacentPair() {
    for (int port = StaticBaseServerTest.FREE_PORT_RANGE_FIRST; port < StaticBaseServerTest.FREE_PORT_RANGE_LAST; port++)
      if (isFree(port) && isFree(port + 1))
        return port;
    throw new IllegalStateException("No two adjacent free ports in the allocation range");
  }

  private static boolean isFree(final int port) {
    try (final ServerSocket probe = new ServerSocket()) {
      probe.bind(new InetSocketAddress(port));
      return true;
    } catch (final IOException busy) {
      return false;
    }
  }

  /*
   * Issue #9562: {@link BaseGraphServerTest#endTest()} restarts every server a test left stopped, then compares the
   * databases. It used to pin the restart to the single HTTP port the server had before, so a port another build took
   * while the server was down failed the restart; the comparison in the {@code finally} then read the stopped server's
   * stale copy from disk and its {@code DatabaseAreNotIdentical} replaced the restart failure that caused it.
   */
  // Regression: #9562 endTest restarts a stopped server without pinning its old HTTP port
  @Test
  void aRestartPrefersItsOldPortButMayMoveOnThroughTheConfiguredRange() {
    assertThat(BaseGraphServerTest.restartHttpPortSetting("2480-2489", 2481)).isEqualTo("2481-2489");
    assertThat(BaseGraphServerTest.restartHttpPortSetting(" 2482 - 2489 ", 2483)).isEqualTo("2483-2489");
  }

  // Regression: #9562 endTest restarts a stopped server without pinning its old HTTP port
  @Test
  void aRestartOnTheLastPortOfTheRangeOrPastItKeepsJustThatPort() {
    assertThat(BaseGraphServerTest.restartHttpPortSetting("2480-2489", 2489)).isEqualTo("2489");
    assertThat(BaseGraphServerTest.restartHttpPortSetting("2480-2489", 2495)).isEqualTo("2495");
  }

  /** A subclass that pinned one port, or a port drawn for it, keeps exactly the port it bound, as before. */
  // Regression: #9562 endTest restarts a stopped server without pinning its old HTTP port
  @Test
  void aSinglePortSettingStaysPinnedToTheBoundPort() {
    assertThat(BaseGraphServerTest.restartHttpPortSetting("31337", 31337)).isEqualTo("31337");
    assertThat(BaseGraphServerTest.restartHttpPortSetting("0", 40123)).isEqualTo("40123");
  }

  /** A setting that cannot be read as a range falls back to the bound port rather than failing the teardown. */
  // Regression: #9562 endTest restarts a stopped server without pinning its old HTTP port
  @Test
  void anUnreadableRangeKeepsTheBoundPort() {
    assertThat(BaseGraphServerTest.restartHttpPortSetting("2480-abc", 2481)).isEqualTo("2481");
    assertThat(BaseGraphServerTest.restartHttpPortSetting("2480-2485-2489", 2481)).isEqualTo("2481");
  }

  /** A server that never bound a port has no old port to come back on: the configured setting is left alone. */
  // Regression: #9562 endTest restarts a stopped server without pinning its old HTTP port
  @Test
  void aServerThatNeverBoundAPortKeepsItsConfiguredSetting() {
    assertThat(BaseGraphServerTest.restartHttpPortSetting("2480-2489", -1)).isEqualTo("2480-2489");
  }

  // Regression: #9562 endTest restarts a stopped server without pinning its old HTTP port
  @Test
  void aComparisonFailureDoesNotHideTheRestartFailureThatCausedIt() {
    final ServerException restartFailure = new ServerException("Error on starting HTTP Server: port taken");
    final IllegalStateException comparisonFailure = new IllegalStateException("Types: DB1 8 <> DB2 7");

    BaseGraphServerTest.compareWithoutMasking(restartFailure, () -> {
      throw comparisonFailure;
    });

    assertThat(restartFailure.getSuppressed()).containsExactly(comparisonFailure);
  }

  // Regression: #9562 endTest restarts a stopped server without pinning its old HTTP port
  @Test
  void aComparisonFailureWithNoRestartFailurePropagates() {
    final IllegalStateException comparisonFailure = new IllegalStateException("Types: DB1 8 <> DB2 7");

    assertThatThrownBy(() -> BaseGraphServerTest.compareWithoutMasking(null, () -> {
      throw comparisonFailure;
    })).isSameAs(comparisonFailure);
  }

  // Regression: #9562 endTest restarts a stopped server without pinning its old HTTP port
  @Test
  void aPassingComparisonLeavesTheRestartFailureUntouched() {
    final ServerException restartFailure = new ServerException("Error on starting HTTP Server: port taken");
    final boolean[] compared = { false };

    BaseGraphServerTest.compareWithoutMasking(restartFailure, () -> compared[0] = true);

    assertThat(compared[0]).isTrue();
    assertThat(restartFailure.getSuppressed()).isEmpty();
  }
}
