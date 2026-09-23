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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.PluginPortFixtureScan;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8204: the Bolt HA integration tests ({@code Bolt5002RoutingTableIT}, {@code BoltFollowerForwardingIT})
 * bound {@code BASE_BOLT_PORT + index}, the same shape #7496 removed from the gRPC HA tests, and the single-server
 * tests started the plugin on the production port 7687. Issue #8203 moved the HA fixtures onto
 * {@code BaseRaftHATest.allocateFixturePorts} and #8209 moved the single-server ones onto {@link BaseBoltServerTest},
 * which binds port {@code 0}.
 * <p>
 * The defect is not observable from inside one test class, only as a collision between two of them on a busy
 * machine, so this source-level guard - the Bolt counterpart of the gRPC module's
 * {@code Issue7496GrpcHaItsAllocateTheirPortsTest} - holds the line: every class here that starts the Bolt plugin must
 * take its port from one of those two fixtures, and none may pin it or dial the production default.
 */
class Issue8204BoltFixturesTakeUnpinnedPortsTest {

  private static final Path TEST_SOURCES = Path.of("src", "test", "java");

  /**
   * Every class that starts the Bolt plugin: about 25 when the guard was written, two of them Raft clusters. The floor
   * keeps a moved source root or a renamed plugin from turning the scan into one that checks nothing and passes.
   */
  private static final int EXPECTED_MINIMUM_FIXTURES = 20;

  @Test
  void everyBoltPluginFixtureTakesAnUnpinnedPort() throws IOException {
    final PluginPortFixtureScan.Result result = PluginPortFixtureScan.scan(TEST_SOURCES, BoltProtocolPlugin.class.getName(),
        "BOLT_PORT", (Integer) GlobalConfiguration.BOLT_PORT.getDefValue(), Set.of(BaseBoltServerTest.class.getSimpleName()));

    assertThat(result.fixtures())
        .as("the scan must find the Bolt plugin fixtures it guards, or it proves nothing")
        .hasSizeGreaterThanOrEqualTo(EXPECTED_MINIMUM_FIXTURES)
        .as("the Raft cluster fixtures issue #8204 named must be among them")
        .contains("Bolt5002RoutingTableIT.java", "BoltFollowerForwardingIT.java");
    assertThat(result.offenders())
        .as("Bolt plugin fixtures on a hand-picked port instead of BaseBoltServerTest or allocateFixturePorts (issues #8203, #8204)")
        .isEmpty();
  }
}
