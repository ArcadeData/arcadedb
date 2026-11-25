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
import com.arcadedb.server.StaticBaseServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
 * Source-level and unit guards that no Raft test fixture binds a hand-picked port: {@link BaseRaftHATest} draws its Raft
 * ports from {@code StaticBaseServerTest.allocateFreePorts}, and the standalone Raft clusters that do not extend it do the
 * same. A Raft port that is already held makes Ratis answer the bind failure with {@code System.exit(1)}, which kills the
 * whole test fork.
 */
class RaftFixturePortsTest {

  private static final int FORMER_BASE_RAFT_PORT = 2434;

  // One regex over every base that builds a Raft cluster, not a fixed list of three names: intermediate bases such as
  // SharedClusterRaftHATest are added over time and a class on one of them must not slip past the guard.
  private static final Pattern RAFT_FIXTURE = Pattern.compile(
      "extends\\s+(BaseRaftHATest|BaseRaftHASslTest|BaseCompactionIndexCompletenessTest|SharedClusterRaftHATest|BaseMiniRaftTest)\\b");
  private static final Pattern SHARED_CLUSTER_FIXTURE = Pattern.compile("extends\\s+SharedClusterRaftHATest\\b");
  private static final Path TEST_SOURCES = Path.of("src", "test", "java");

  /** How a fixture tells a node which Raft port to bind. */
  private static final Pattern CONFIGURES_RAFT_PORT = Pattern.compile("setValue\\(\\s*GlobalConfiguration\\.HA_RAFT_PORT\\s*,");

  /** A port constant initialised from a literal, e.g. {@code BASE_HA_PORT = 22424} or {@code BASE_HTTP_PORT = 42480}. */
  private static final Pattern LITERAL_PORT = Pattern.compile("\\bint\\s+\\w*PORT\\w*\\s*=\\s*\\d");

  /**
   * A fixture on a base class that draws its Raft ports itself, and is guarded by {@link #noRaftFixtureHardCodesARaftPort()}.
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

  // Regression for #8203: the base fixture draws its Raft ports per test instead of pinning 2434 + index
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

  // Regression for #8203: the base fixture draws its Raft ports per test instead of pinning 2434 + index
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

  // Regression for #8203: the base fixture draws its Raft ports per test instead of pinning 2434 + index
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

  // Regression for #8203: the base fixture draws its Raft ports per test instead of pinning 2434 + index
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

  // Regression for #8203: the base fixture draws its Raft ports per test instead of pinning 2434 + index
  /**
   * Source-level guard: no fixture may bring the fixed port back through its own {@code getServerAddresses()} or
   * {@code onServerConfiguration()} override, as {@code RaftPriorityRejoinIT} and {@code HaPeerAddressRoutingDeclaredGrpcIT}
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
    final List<String> haRaftFixtures = scanRaftFixtures(Path.of("src", "test", "java"), fixedRaftPort, offenders);
    final List<String> boltFixtures = scanRaftFixtures(Path.of("..", "bolt", "src", "test", "java"), fixedRaftPort, offenders);
    final List<String> grpcFixtures = scanRaftFixtures(Path.of("..", "grpcw", "src", "test", "java"), fixedRaftPort, offenders);

    final int sharedClusterFixtures = countFiles(Path.of("src", "test", "java"), SHARED_CLUSTER_FIXTURE);
    assertThat(haRaftFixtures).as("the scan must find the ha-raft fixtures it guards, or it proves nothing").hasSizeGreaterThan(100);
    // The Bolt and gRPC floors are fixtures named, not file counts: consolidating the issue-named classes of a module into
    // nested classes of one topic fixture (#13233 took the gRPC fixtures from 18 files to 8) changes how many files hold
    // them, not whether the scan reaches them. A moved source root or a renamed base class still loses these names.
    assertThat(boltFixtures).as("the scan must find the Bolt HA fixtures it guards, or it proves nothing")
        .anyMatch(name -> name.endsWith("BoltFollowerForwardingIT.java"))
        .anyMatch(name -> name.endsWith("Bolt5002RoutingTableIT.java"));
    assertThat(grpcFixtures).as("the scan must find the gRPC HA fixtures it guards, or it proves nothing")
        .anyMatch(name -> name.endsWith("GrpcRaftReplicationIT.java"))
        .anyMatch(name -> name.endsWith("GrpcLeaderRoutingIT.java"))
        .anyMatch(name -> name.endsWith("GrpcFollowerForwardingIT.java"))
        .anyMatch(name -> name.endsWith("GrpcHaAcceptRpcsIT.java"));
    assertThat(sharedClusterFixtures)
        .as("the scan must reach the fixtures extending SharedClusterRaftHATest, or the next intermediate base reopens the hole")
        .isGreaterThanOrEqualTo(10);
    assertThat(offenders).as("Raft HA fixtures pinning a hand-picked Raft port instead of BaseRaftHATest.raftPort (issue #8203)")
        .isEmpty();
  }

  // Regression for #8203: the base fixture draws its Raft ports per test instead of pinning 2434 + index
  @Test
  void aFixtureOnAnIntermediateBaseIsScanned(@TempDir final Path dir) throws IOException {
    Files.writeString(dir.resolve("Probe.java"), "class Probe extends SharedClusterRaftHATest { static final int PORT = 2434; }",
        StandardCharsets.UTF_8);
    Files.writeString(dir.resolve("Clean.java"), "class Clean extends SharedClusterRaftHATest { }", StandardCharsets.UTF_8);
    final Pattern fixedRaftPort = Pattern.compile("\\b" + FORMER_BASE_RAFT_PORT + "\\b|BASE_RAFT(_PORT)?\\s*=");
    final List<String> offenders = new ArrayList<>();

    final List<String> fixtures = scanRaftFixtures(dir, fixedRaftPort, offenders);

    assertThat(fixtures).as("both classes extend a Raft fixture base and must be counted").containsExactlyInAnyOrder("Probe.java", "Clean.java");
    assertThat(offenders).containsExactly("Probe.java");
  }

  /** Counts the files under {@code root} whose source matches {@code fixture}. */
  private static int countFiles(final Path root, final Pattern fixture) throws IOException {
    int count = 0;
    try (final Stream<Path> files = Files.walk(root)) {
      for (final Path file : files.filter(p -> p.toString().endsWith(".java")).toList())
        if (fixture.matcher(Files.readString(file, StandardCharsets.UTF_8)).find())
          count++;
    }
    return count;
  }

  /**
   * Adds every Raft fixture under {@code root} that matches {@code fixedRaftPort} to {@code offenders}; returns the fixtures it
   * saw, as paths relative to {@code root}.
   */
  private static List<String> scanRaftFixtures(final Path root, final Pattern fixedRaftPort, final List<String> offenders)
      throws IOException {
    assertThat(root).as("source root %s must exist, or the guard silently scans less than it claims", root).isDirectory();
    final List<String> fixtures = new ArrayList<>();
    try (final Stream<Path> files = Files.walk(root)) {
      for (final Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
        if (file.getFileName().toString().equals(RaftFixturePortsTest.class.getSimpleName() + ".java"))
          continue;
        final String source = Files.readString(file, StandardCharsets.UTF_8);
        if (!RAFT_FIXTURE.matcher(source).find())
          continue;
        fixtures.add(root.relativize(file).toString());
        if (fixedRaftPort.matcher(source).find())
          offenders.add(root.relativize(file).toString());
      }
    }
    return fixtures;
  }

  // Regression for #8222: standalone Raft clusters allocate their Raft and HTTP ports instead of hard-coding them
  /**
   * Issue #8222, the follow-up of #8203. {@link BaseRaftHATest} draws its Raft ports per test, but three Raft clusters in
   * this module do not extend it and still bound hand-picked ports: {@code RaftReplicationIT} ({@code 22424 + i} Raft,
   * {@code 22480 + i} HTTP), {@code RaftHAComprehensiveIT} ({@code 42424 + i} / {@code 42480 + i}, inside the Linux
   * ephemeral range, so any outgoing connection on the runner could hold one) and {@code RaftHAInsertBenchmark}
   * ({@code 3434 + i} / {@code 3480 + i}). {@code PendingSnapshotStartupTest} had the same shape through a
   * different door: it asked the OS for port 0, which hands out a port from the ephemeral range. When a Raft port is
   * held, Ratis answers the bind failure with {@code System.exit(1)} and the whole test fork is lost.
   * <p>
   * They now draw their Raft AND HTTP ports from {@link StaticBaseServerTest#allocateFreePorts(int)}, made public for
   * them, in one call per cluster so the two families are distinct. The defect is a collision between two processes,
   * which no single test run can observe, so this is a source-level guard in the spirit of
   * {@link #noRaftFixtureHardCodesARaftPort()}: every JUnit class here that configures a
   * Raft port without going through {@link BaseRaftHATest} must take it from the allocator.
   */
  @Test
  void everyStandaloneRaftFixtureAllocatesItsPorts() throws IOException {
    assertThat(TEST_SOURCES).as("source root %s must exist, or the guard scans nothing", TEST_SOURCES).isDirectory();

    final List<String> fixtures = new ArrayList<>();
    final List<String> offenders = new ArrayList<>();

    try (final Stream<Path> files = Files.walk(TEST_SOURCES)) {
      for (final Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
        final String source = Files.readString(file, StandardCharsets.UTF_8);
        // Only JUnit classes run in a shared fork: RaftClusterStarter is a main() for manual Studio sessions and keeps
        // the well-known 2424/2480 on purpose. Fixtures on BaseRaftHATest are guarded by RaftFixturePortsTest.
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

  // Regression for #8222: standalone Raft clusters allocate their Raft and HTTP ports instead of hard-coding them
  @Test
  void theAllocatorIsReachableFromAFixtureThatExtendsNothing() {
    // This class extends nothing, like the fixtures above: the call compiling at all is half of the fix.
    final int[] ports = StaticBaseServerTest.allocateFreePorts(6);

    assertThat(ports).hasSize(6).doesNotHaveDuplicates();
    // StaticBaseServerTest.FREE_PORT_RANGE_FIRST/LAST, restated because they are package-private to com.arcadedb.server:
    // if that range moves, move these bounds with it.
    for (final int port : ports)
      assertThat(port).as("a Raft port must stay below every ephemeral range").isBetween(15000, 32767);
  }

  private static boolean isJUnitClass(final String source) {
    return source.contains("@Test") || source.contains("@ParameterizedTest");
  }
}
