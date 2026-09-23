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

import com.arcadedb.server.StaticBaseServerTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8222, the follow-up of #8203. {@link BaseRaftHATest} draws its Raft ports per test, but three Raft clusters in
 * this module do not extend it and still bound hand-picked ports: {@code RaftReplicationIT} ({@code 22424 + i} Raft,
 * {@code 22480 + i} HTTP), {@code RaftHAComprehensiveIT} ({@code 42424 + i} / {@code 42480 + i}, inside the Linux
 * ephemeral range, so any outgoing connection on the runner could hold one) and {@code RaftHAInsertBenchmark}
 * ({@code 3434 + i} / {@code 3480 + i}). {@code Issue7129PendingSnapshotStartupTest} had the same shape through a
 * different door: it asked the OS for port 0, which hands out a port from the ephemeral range. When a Raft port is
 * held, Ratis answers the bind failure with {@code System.exit(1)} and the whole test fork is lost.
 * <p>
 * They now draw their Raft AND HTTP ports from {@link StaticBaseServerTest#allocateFreePorts(int)}, made public for
 * them, in one call per cluster so the two families are distinct. The defect is a collision between two processes,
 * which no single test run can observe, so this is a source-level guard in the spirit of
 * {@link Issue8203RaftFixturePortsTest#noRaftFixtureHardCodesARaftPort()}: every JUnit class here that configures a
 * Raft port without going through {@link BaseRaftHATest} must take it from the allocator.
 */
class Issue8222StandaloneRaftFixturePortsTest {

  private static final Path TEST_SOURCES = Path.of("src", "test", "java");

  /** How a fixture tells a node which Raft port to bind. */
  private static final Pattern CONFIGURES_RAFT_PORT = Pattern.compile("setValue\\(\\s*GlobalConfiguration\\.HA_RAFT_PORT\\s*,");

  /** A port constant initialised from a literal, e.g. {@code BASE_HA_PORT = 22424} or {@code BASE_HTTP_PORT = 42480}. */
  private static final Pattern LITERAL_PORT = Pattern.compile("\\bint\\s+\\w*PORT\\w*\\s*=\\s*\\d");

  /**
   * A fixture on a base class that draws its Raft ports itself, and is guarded by {@link Issue8203RaftFixturePortsTest}.
   * Matched as a whole word so a class merely NAMED like one ({@code BaseRaftHATestV2}) is not taken for it.
   */
  private static final Pattern EXTENDS_RAFT_BASE = Pattern.compile("\\bextends\\s+BaseRaftHA(Ssl)?Test\\b");

  /** An OS-assigned port: it comes from the ephemeral range, where the next outgoing connection can take it first. */
  private static final Pattern EPHEMERAL_PORT = Pattern.compile("new\\s+ServerSocket\\(\\s*0\\s*\\)");

  /**
   * The four standalone fixtures this issue moved. The floor is what stops a moved source root or a renamed base class
   * from turning the scan into one that checks nothing and passes.
   */
  private static final int EXPECTED_MINIMUM_FIXTURES = 4;

  @Test
  void everyStandaloneRaftFixtureAllocatesItsPorts() throws IOException {
    assertThat(TEST_SOURCES).as("source root %s must exist, or the guard scans nothing", TEST_SOURCES).isDirectory();

    final List<String> fixtures = new ArrayList<>();
    final List<String> offenders = new ArrayList<>();

    try (final Stream<Path> files = Files.walk(TEST_SOURCES)) {
      for (final Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
        final String source = Files.readString(file, StandardCharsets.UTF_8);
        // Only JUnit classes run in a shared fork: RaftClusterStarter is a main() for manual Studio sessions and keeps
        // the well-known 2424/2480 on purpose. Fixtures on BaseRaftHATest are guarded by Issue8203RaftFixturePortsTest.
        if (!CONFIGURES_RAFT_PORT.matcher(source).find() || !isJUnitClass(source) || EXTENDS_RAFT_BASE.matcher(source).find())
          continue;

        final String name = file.getFileName().toString();
        fixtures.add(name);
        if (!source.contains("allocateFreePorts(") || LITERAL_PORT.matcher(source).find() || EPHEMERAL_PORT.matcher(source).find())
          offenders.add(name);
      }
    }

    assertThat(fixtures)
        .as("the scan must find the standalone Raft fixtures it guards, or it proves nothing")
        .hasSizeGreaterThanOrEqualTo(EXPECTED_MINIMUM_FIXTURES);
    assertThat(offenders)
        .as("standalone Raft fixtures binding a hand-picked or ephemeral port instead of StaticBaseServerTest.allocateFreePorts (issue #8222)")
        .isEmpty();
  }

  @Test
  void theAllocatorIsReachableFromAFixtureThatExtendsNothing() {
    // This class extends nothing, like the fixtures above: the call compiling at all is half of the fix.
    final int[] ports = StaticBaseServerTest.allocateFreePorts(6);

    assertThat(ports).hasSize(6).doesNotHaveDuplicates();
    for (final int port : ports)
      assertThat(port).as("a Raft port must stay below every ephemeral range").isBetween(15000, 32767);
  }

  private static boolean isJUnitClass(final String source) {
    return source.contains("@Test") || source.contains("@ParameterizedTest");
  }
}
