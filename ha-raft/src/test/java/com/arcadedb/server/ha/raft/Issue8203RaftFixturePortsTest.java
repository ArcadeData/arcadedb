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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8203: {@link BaseRaftHATest} pinned every Raft HA integration test to Raft port {@code 2434 + index}. When
 * that port is already held - a server lingering from an earlier class, another worktree's HA suite, any process on
 * the machine - Ratis does not throw: {@code GrpcServicesImpl} answers the bind failure with {@code System.exit(1)},
 * which kills the whole failsafe fork and every test left in it.
 * <p>
 * The base class now draws each fixture's Raft ports from {@code StaticBaseServerTest.allocateFreePorts}, keeps them
 * for the life of the test instance (a restarted node must come back on the port its peers know), and hands out every
 * other port a fixture needs (gRPC, Bolt) from the same ledger so the two families cannot collide with each other.
 * The tests here drive the base class directly, with no server started, so they are fast unit tests.
 */
class Issue8203RaftFixturePortsTest {

  private static final int FORMER_BASE_RAFT_PORT = 2434;

  /** A fixture that never starts a server: only the port bookkeeping of {@link BaseRaftHATest} runs. */
  private static class Fixture extends BaseRaftHATest {
    private final int serverCount;

    Fixture(final int serverCount) {
      this.serverCount = serverCount;
    }

    @Override
    protected int getServerCount() {
      return serverCount;
    }
  }

  /** A fixture whose "free port" draws are scripted, so a collision between two families can be forced. */
  private static final class ScriptedFixture extends Fixture {
    private final Deque<int[]> draws = new ArrayDeque<>();

    ScriptedFixture(final int serverCount, final int[]... draws) {
      super(serverCount);
      this.draws.addAll(Arrays.asList(draws));
    }

    @Override
    int[] drawFreePorts(final int count) {
      final int[] next = draws.poll();
      assertThat(next).as("the fixture asked for more draws than the test scripted").isNotNull();
      return Arrays.copyOf(next, count);
    }
  }

  @Test
  void raftPortsAreDrawnPerFixtureNotPinnedToTheDefault() {
    final Fixture fixture = new Fixture(3);

    final int[] ports = { fixture.raftPort(0), fixture.raftPort(1), fixture.raftPort(2) };

    assertThat(ports).doesNotHaveDuplicates();
    for (int i = 0; i < ports.length; i++)
      assertThat(ports[i])
          .as("node %d must not sit on the fixed port every HA suite on the machine used to share", i)
          .isNotEqualTo(FORMER_BASE_RAFT_PORT + i)
          .isBetween(15_000, 32_767);
  }

  @Test
  void aRaftPortIsStableForTheLifeOfTheFixture() {
    final Fixture fixture = new Fixture(2);
    final int first = fixture.raftPort(0);
    final int second = fixture.raftPort(1);

    // A node started after the initial cluster (a late joiner) grows the allocation without moving anyone.
    final int late = fixture.raftPort(3);

    assertThat(fixture.raftPort(0)).isEqualTo(first);
    assertThat(fixture.raftPort(1)).isEqualTo(second);
    assertThat(late).isNotIn(first, second);
    assertThat(fixture.raftPort(2)).isNotIn(first, second, late);
    assertThat(fixture.raftPort(3)).isEqualTo(late);
  }

  @Test
  void peerIdServerListAndNodeConfigurationAllNameTheSamePort() {
    final Fixture fixture = new Fixture(3);

    final String addresses = fixture.getServerAddresses();
    for (int i = 0; i < 3; i++) {
      final int port = fixture.raftPort(i);
      assertThat(fixture.peerIdForIndex(i)).isEqualTo("localhost_" + port);
      assertThat(addresses.split(",")[i]).startsWith("localhost:" + port + ":");

      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_" + i);
      fixture.onServerConfiguration(config);
      assertThat(config.getValueAsInteger(GlobalConfiguration.HA_RAFT_PORT))
          .as("node %d must bind the port its peers dial", i)
          .isEqualTo(port);
    }
  }

  @Test
  void aPortAlreadyHandedOutIsNeverHandedOutAgain() {
    // The first draw gives the gRPC ports; the second (the Raft ports) comes back with one of them, as a real
    // allocateFreePorts can once the gRPC probe sockets are closed; the third is the redraw for the collision.
    final ScriptedFixture fixture = new ScriptedFixture(2,
        new int[] { 20_000, 20_001 },
        new int[] { 20_001, 20_002 },
        new int[] { 20_003 });

    final int[] grpc = fixture.allocateFixturePorts(2);
    final int raft0 = fixture.raftPort(0);
    final int raft1 = fixture.raftPort(1);

    assertThat(grpc).containsExactly(20_000, 20_001);
    assertThat(new int[] { raft0, raft1 })
        .as("a Raft port equal to a gRPC port of the same fixture is a bind failure, and Ratis exits the JVM on it")
        .containsExactlyInAnyOrder(20_002, 20_003);
  }

  /**
   * Source-level guard: no fixture may bring the fixed port back through its own {@code getServerAddresses()} or
   * {@code onServerConfiguration()} override, as {@code RaftPriorityRejoinIT} and {@code Issue6091GrpcRoutingTableIT}
   * did. The defect is a collision between two JVMs, which no single test run can observe.
   * <p>
   * The Bolt and gRPC modules build Raft clusters on {@link BaseRaftHATest} too, and this PR moved their fixtures off
   * the literal as well, so their sources are scanned from here: they have no guard of their own for the Raft port.
   */
  @Test
  void noRaftFixtureHardCodesARaftPort() throws IOException {
    final Pattern fixedRaftPort = Pattern.compile("\\b" + FORMER_BASE_RAFT_PORT + "\\b|BASE_RAFT(_PORT)?\\s*=");
    final List<String> offenders = new ArrayList<>();

    // Surefire runs with the module directory as the working directory, so the sibling modules sit one level up.
    final int haRaftFixtures = scanRaftFixtures(Path.of("src", "test", "java"), fixedRaftPort, offenders);
    final int boltFixtures = scanRaftFixtures(Path.of("..", "bolt", "src", "test", "java"), fixedRaftPort, offenders);
    final int grpcFixtures = scanRaftFixtures(Path.of("..", "grpcw", "src", "test", "java"), fixedRaftPort, offenders);

    assertThat(haRaftFixtures).as("the scan must find the ha-raft fixtures it guards, or it proves nothing").isGreaterThan(100);
    assertThat(boltFixtures).as("the scan must find the Bolt HA fixtures it guards, or it proves nothing").isGreaterThanOrEqualTo(2);
    assertThat(grpcFixtures).as("the scan must find the gRPC HA fixtures it guards, or it proves nothing").isGreaterThanOrEqualTo(10);
    assertThat(offenders).as("Raft HA fixtures pinning a hand-picked Raft port instead of BaseRaftHATest.raftPort (issue #8203)")
        .isEmpty();
  }

  /** Adds every Raft fixture under {@code root} that matches {@code fixedRaftPort} to {@code offenders}; returns how many fixtures it saw. */
  private static int scanRaftFixtures(final Path root, final Pattern fixedRaftPort, final List<String> offenders) throws IOException {
    assertThat(root).as("source root %s must exist, or the guard silently scans less than it claims", root).isDirectory();
    int fixtures = 0;
    try (final Stream<Path> files = Files.walk(root)) {
      for (final Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
        if (file.getFileName().toString().equals(Issue8203RaftFixturePortsTest.class.getSimpleName() + ".java"))
          continue;
        final String source = Files.readString(file, StandardCharsets.UTF_8);
        if (!source.contains("extends BaseRaftHATest") && !source.contains("extends BaseRaftHASslTest")
            && !source.contains("extends BaseCompactionIndexCompletenessTest"))
          continue;
        fixtures++;
        if (fixedRaftPort.matcher(source).find())
          offenders.add(root.relativize(file).toString());
      }
    }
    return fixtures;
  }
}
