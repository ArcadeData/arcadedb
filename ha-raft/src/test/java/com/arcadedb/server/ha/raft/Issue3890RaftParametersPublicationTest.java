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
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@code RaftHAServer.raftParameters} is what the Raft client builders read to speak the same transport the
 * local Ratis server speaks, and {@code refreshRaftClient()} can read it from a leader-change callback on
 * another thread while {@code restartRatis()} is rebuilding it. So WHEN it becomes visible is a property in
 * its own right, and one that has already been got wrong twice while issue #3890 was in review:
 * <ul>
 *   <li>publishing before the TLS configuration was validated left a plaintext {@link Parameters} in the
 *       field whenever a cert/key/trust path was unusable;</li>
 *   <li>publishing between the TLS install and the peer-allowlist install left a window in which the
 *       customizer was missing from the object a reader could already see.</li>
 * </ul>
 * Both are closed by publishing on the method's last line, and this test is what keeps it there: it is
 * deliberately about the field rather than about the returned value, because the returned value was never
 * the part that was wrong.
 */
class Issue3890RaftParametersPublicationTest {

  private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481";

  @TempDir
  private Path tempDir;
  private Path certChain;
  private Path privateKey;
  private Path trustCerts;

  @BeforeEach
  void createCertificateFiles() throws Exception {
    certChain = Files.writeString(tempDir.resolve("node-cert.pem"), "cert");
    privateKey = Files.writeString(tempDir.resolve("node-key.pem"), "key");
    trustCerts = Files.writeString(tempDir.resolve("ca.pem"), "ca");
  }

  @Test
  void publishedParametersCarryBothTheTlsConfigAndTheAllowlistCustomizer() {
    final RaftHAServer server = detachedServer();
    final Parameters returned = server.buildParameters(tlsConfiguration());

    final Parameters published = server.raftParametersForTest();
    assertThat(published).isSameAs(returned);
    assertThat(GrpcConfigKeys.TLS.conf(published)).isNotNull();
    assertThat(GrpcConfigKeys.Server.servicesCustomizer(published))
        .as("the inbound-peer allowlist customizer must be installed before the object is published")
        .isNotNull();
  }

  /**
   * The restart case: a cert file that has become unreadable since the last successful start. The previous,
   * working transport configuration has to survive, because a client built from a bare {@link Parameters}
   * would silently dial the Raft port in plaintext.
   */
  @Test
  void aFailedRebuildLeavesThePreviouslyPublishedParametersInPlace() {
    final RaftHAServer server = detachedServer();
    final Parameters good = server.buildParameters(tlsConfiguration());

    final ContextConfiguration broken = tlsConfiguration();
    broken.setValue(GlobalConfiguration.HA_TLS_CERT_CHAIN_FILE, tempDir.resolve("vanished.pem").toString());

    assertThatThrownBy(() -> server.buildParameters(broken))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("arcadedb.ha.tls.certChainFile");

    assertThat(server.raftParametersForTest()).isSameAs(good);
    assertThat(GrpcConfigKeys.TLS.conf(server.raftParametersForTest())).isNotNull();
  }

  @Test
  void aPlaintextClusterPublishesUsableParametersWithNoTlsConfig() {
    final RaftHAServer server = detachedServer();
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);

    server.buildParameters(config);

    final Parameters published = server.raftParametersForTest();
    assertThat(published).isNotNull();
    assertThat(GrpcConfigKeys.TLS.conf(published)).isNull();
    assertThat(GrpcConfigKeys.Server.servicesCustomizer(published)).isNotNull();
  }

  private ContextConfiguration tlsConfiguration() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
    config.setValue(GlobalConfiguration.HA_TLS_ENABLED, true);
    config.setValue(GlobalConfiguration.HA_TLS_CERT_CHAIN_FILE, certChain.toString());
    config.setValue(GlobalConfiguration.HA_TLS_PRIVATE_KEY_FILE, privateKey.toString());
    config.setValue(GlobalConfiguration.HA_TLS_TRUST_CERT_COLLECTION_FILE, trustCerts.toString());
    return config;
  }

  /**
   * A {@link RaftHAServer} whose constructor has run but whose Ratis server was never started: the peer group
   * is all {@code buildParameters} reads, so nothing has to be bound or written to disk.
   */
  private static RaftHAServer detachedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);

    final ArcadeDBServer arcadeServer = mock(ArcadeDBServer.class);
    when(arcadeServer.getServerName()).thenReturn("ArcadeDB_0");

    return new RaftHAServer(arcadeServer, config);
  }
}
