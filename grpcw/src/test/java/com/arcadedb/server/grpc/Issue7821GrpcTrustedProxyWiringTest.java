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
import com.arcadedb.server.TestServerHelper;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.util.List;

import static com.arcadedb.server.grpc.GrpcTransportSecurityInterceptorTest.PROXY_IP;
import static com.arcadedb.server.grpc.GrpcTransportSecurityInterceptorTest.decisionFor;
import static com.arcadedb.server.grpc.GrpcTransportSecurityInterceptorTest.forwardedProto;
import static com.arcadedb.server.grpc.GrpcTransportSecurityInterceptorTest.proxy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Issue #7821, reachability: the transport-security interceptor the gRPC plugin actually registers must be the one
 * that reads the server's trusted-proxy list. A plugin registering the no-argument interceptor would still pass every
 * loopback integration test while silently ignoring the list.
 */
class Issue7821GrpcTrustedProxyWiringTest {

  @TempDir
  Path tempDir;

  @Test
  void theRegisteredInterceptorHonorsTheServersTrustedProxyList() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final ArcadeDBServer server = TestServerHelper.unstartedServer(tempDir, serverConfiguration);

    final GrpcServerPlugin plugin = new GrpcServerPlugin();
    plugin.configure(server, serverConfiguration);

    final ServerBuilder<?> builder = mock(ServerBuilder.class, RETURNS_SELF);
    try {
      plugin.configureServer(builder, serverConfiguration);

      final ArgumentCaptor<ServerInterceptor> registered = ArgumentCaptor.forClass(ServerInterceptor.class);
      verify(builder, atLeastOnce()).intercept(registered.capture());
      final List<GrpcTransportSecurityInterceptor> security = registered.getAllValues().stream()
          .filter(GrpcTransportSecurityInterceptor.class::isInstance)
          .map(GrpcTransportSecurityInterceptor.class::cast)
          .toList();
      assertThat(security).hasSize(1);
      final GrpcTransportSecurityInterceptor interceptor = security.getFirst();

      assertThat(decisionFor(interceptor, proxy(), false, forwardedProto("https"))).isFalse();

      // What SET SERVER SETTING does: change the server's configuration after the interceptor was registered.
      serverConfiguration.setValue(GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES, PROXY_IP);
      assertThat(decisionFor(interceptor, proxy(), false, forwardedProto("https"))).isTrue();
    } finally {
      plugin.stopService();
    }
  }
}
