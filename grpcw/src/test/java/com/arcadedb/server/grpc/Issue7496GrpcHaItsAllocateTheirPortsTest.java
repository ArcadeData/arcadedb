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
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.PluginPortFixtureScan;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7496: every gRPC HA integration test in this module picked a fixed gRPC base port and added the node index
 * ({@code 51141 + serverIndex}). A server from an earlier class that had not released its port yet, or any other
 * process on the runner, then failed whichever test started next - "Failed to bind to address 0.0.0.0:51142:
 * Address already in use" - so the {@code integration-tests} lane went red on a different victim each run. Two of
 * the classes even shared a base (51161) while their comments claimed to keep clear of each other: a hand-picked
 * number cannot be kept apart from every other one by review.
 * <p>
 * They now take their ports from {@code BaseRaftHATest.allocateFixturePorts}, which draws them through
 * {@code StaticBaseServerTest.allocateFreePorts} and keeps them apart from the fixture's Raft ports (issue #8203). This
 * is a source-level guard in the spirit of {@link Issue7472EveryGrpcFailureIsConcealableTest}: the defect is not
 * observable from inside one test class, only as a collision between two of them on a busy runner, so no behavioural
 * test can hold the line. It fails the moment a Raft cluster fixture in this module configures a gRPC port without
 * allocating it.
 * <p>
 * Issue #8204 extends the guard to the single-server fixtures, which #8209 moved onto {@link BaseGrpcServerTest}'s
 * ephemeral port: every class in this module that starts the gRPC plugin must now take its port from one of the two
 * fixtures, and none may pin it or dial the production default.
 */
class Issue7496GrpcHaItsAllocateTheirPortsTest {

  private static final Path TEST_SOURCES = Path.of("src", "test", "java");

  /** How a fixture tells the gRPC plugin which port to bind: the raw key, or the registered setting. */
  private static final Pattern CONFIGURES_GRPC_PORT = Pattern.compile("\"arcadedb\\.grpc\\.port\"|GRPC_PORT\\.getKey\\(\\)");

  /** A gRPC port constant initialised from a literal, e.g. {@code BASE_GRPC_PORT = 51141}. */
  private static final Pattern LITERAL_GRPC_PORT = Pattern.compile("\\bint\\s+\\w*GRPC_PORT\\w*\\s*=\\s*\\d");

  /**
   * At least this many HA fixtures configure a gRPC port today (ten when the guard was written). The floor is what
   * stops a moved source root or a renamed base class from turning the scan into one that checks nothing and passes.
   */
  private static final int EXPECTED_MINIMUM_FIXTURES = 10;

  @Test
  void everyRaftClusterFixtureAllocatesItsGrpcPorts() throws IOException {
    final List<String> fixtures = new ArrayList<>();
    final List<String> offenders = new ArrayList<>();

    try (final Stream<Path> files = Files.walk(TEST_SOURCES)) {
      for (final Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
        final String source = Files.readString(file, StandardCharsets.UTF_8);
        if (!source.contains("extends BaseRaftHATest") || !CONFIGURES_GRPC_PORT.matcher(source).find())
          continue;

        final String name = file.getFileName().toString();
        fixtures.add(name);
        if (!source.contains("allocateFixturePorts(") || LITERAL_GRPC_PORT.matcher(source).find())
          offenders.add(name);
      }
    }

    assertThat(fixtures)
        .as("the scan must find the gRPC HA fixtures it guards, or it proves nothing")
        .hasSizeGreaterThanOrEqualTo(EXPECTED_MINIMUM_FIXTURES);
    assertThat(offenders)
        .as("gRPC HA fixtures binding a hand-picked gRPC port instead of BaseRaftHATest.allocateFixturePorts (issues #7496, #8203)")
        .isEmpty();
  }

  /**
   * Every class that starts the gRPC plugin: about 60 when the guard was written. The floor keeps a moved source root
   * or a renamed plugin from turning the scan into one that checks nothing and passes.
   */
  private static final int EXPECTED_MINIMUM_PLUGIN_FIXTURES = 50;

  @Test
  void everyGrpcPluginFixtureTakesAnUnpinnedPort() throws IOException {
    final PluginPortFixtureScan.Result result = PluginPortFixtureScan.scan(TEST_SOURCES, GrpcServerPlugin.class.getName(),
        "GRPC_PORT", (Integer) GlobalConfiguration.GRPC_PORT.getDefValue(), Set.of(BaseGrpcServerTest.class.getSimpleName()));

    assertThat(result.fixtures())
        .as("the scan must find the gRPC plugin fixtures it guards, or it proves nothing")
        .hasSizeGreaterThanOrEqualTo(EXPECTED_MINIMUM_PLUGIN_FIXTURES);
    assertThat(result.offenders())
        .as("gRPC plugin fixtures on a hand-picked port instead of BaseGrpcServerTest or allocateFixturePorts (issues #8204, #8209)")
        .isEmpty();
  }
}
