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
package com.arcadedb.server.grpc;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7233: the gRPC row caps and stream write timeout are SCOPE.SERVER, so they live in the server's
 * {@link ContextConfiguration} - written by the server configuration file, {@code SET SERVER SETTING} and the MCP
 * tool - and used to be read off the {@link GlobalConfiguration} enum, which only a system property or an
 * environment variable ever writes.
 * <p>
 * Every one of those five reads resolves through {@code serverConfiguration()}, so asserting on it asserts on all
 * of them, including that a service built without a server still reads a value rather than throwing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7233GrpcReadsServerConfigurationTest {

  @Test
  void theSettingsResolveAgainstTheServersOwnConfiguration() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.fromJSON("{\"configuration\":{\"server.grpcQueryMaxResultRows\":4242}}");

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(configuration);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("./target/databases", server);

    assertThat(service.serverConfiguration()).isSameAs(configuration);
    assertThat(service.serverConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_GRPC_QUERY_MAX_RESULT_ROWS))
        .isEqualTo(4242);
  }

  @Test
  void aServiceWithNoServerStillReadsTheProcessWideValue() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("./target/databases", null);

    assertThat(service.serverConfiguration().getValueAsLong(GlobalConfiguration.SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS))
        .isEqualTo(GlobalConfiguration.SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS.getValueAsLong());
  }
}
