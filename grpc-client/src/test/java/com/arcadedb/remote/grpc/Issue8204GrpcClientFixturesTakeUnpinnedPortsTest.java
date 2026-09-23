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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.PluginPortFixtureScan;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8204: the single-server tests of this module started the gRPC plugin on the production port 50051 and
 * connected to it by number, so a server from the previous class that had not released the port yet, or any other
 * local gRPC server, failed the next test or answered its calls. Issue #8209 moved them onto
 * {@link BaseGrpcClientServerTest}, which binds port {@code 0} and exposes the port actually bound.
 * <p>
 * The defect is not observable from inside one test class, only as a collision between two of them on a busy
 * machine, so this source-level guard holds the line: every class here that starts the gRPC plugin must extend that
 * base (or, for a Raft cluster, take its ports from {@code BaseRaftHATest.allocateFixturePorts}), and none may pin
 * the port or dial the production default. A pure client test that never starts a server may still name 50051.
 */
class Issue8204GrpcClientFixturesTakeUnpinnedPortsTest {

  private static final Path   TEST_SOURCES = Path.of("src", "test", "java");
  private static final String GRPC_PLUGIN  = "com.arcadedb.server.grpc.GrpcServerPlugin";

  /**
   * Every class that starts the gRPC plugin: about 40 when the guard was written. The floor keeps a moved source root
   * or a renamed plugin from turning the scan into one that checks nothing and passes.
   */
  private static final int EXPECTED_MINIMUM_FIXTURES = 30;

  @Test
  void everyGrpcPluginFixtureTakesAnUnpinnedPort() throws IOException {
    final PluginPortFixtureScan.Result result = PluginPortFixtureScan.scan(TEST_SOURCES, GRPC_PLUGIN, "GRPC_PORT",
        (Integer) GlobalConfiguration.GRPC_PORT.getDefValue(), Set.of(BaseGrpcClientServerTest.class.getSimpleName()));

    assertThat(result.fixtures())
        .as("the scan must find the gRPC plugin fixtures it guards, or it proves nothing")
        .hasSizeGreaterThanOrEqualTo(EXPECTED_MINIMUM_FIXTURES);
    assertThat(result.offenders())
        .as("gRPC plugin fixtures on a hand-picked port instead of BaseGrpcClientServerTest (issues #8204, #8209)")
        .isEmpty();
  }
}
