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
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Kubernetes auto-join probe opens its own short-lived {@code RaftClient} against a peer's Raft port.
 * Ratis carries the gRPC TLS configuration on {@link Parameters}, not on {@link RaftProperties}, so cloning
 * the server's properties - which is all the probe used to do - leaves that client speaking plaintext. On a
 * cluster with {@code arcadedb.ha.tls.enabled=true} every probe would then fail its handshake, and because
 * the probe swallows per-peer failures the node would simply report "no existing cluster found" and never
 * join (issue #3890).
 */
class KubernetesAutoJoinTlsInheritanceTest {

  @TempDir
  private Path tempDir;

  @Test
  void probeClientInheritsTheServerTlsConfiguration() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_TLS_ENABLED, true);
    config.setValue(GlobalConfiguration.HA_TLS_CERT_CHAIN_FILE,
        Files.writeString(tempDir.resolve("node-cert.pem"), "cert").toString());
    config.setValue(GlobalConfiguration.HA_TLS_PRIVATE_KEY_FILE,
        Files.writeString(tempDir.resolve("node-key.pem"), "key").toString());
    config.setValue(GlobalConfiguration.HA_TLS_TRUST_CERT_COLLECTION_FILE,
        Files.writeString(tempDir.resolve("ca.pem"), "ca").toString());

    final Parameters serverParameters = new Parameters();
    final GrpcTlsConfig tlsConfig = RaftPropertiesBuilder.applyTls(config, serverParameters);
    assertThat(tlsConfig).isNotNull();

    final KubernetesAutoJoin autoJoin = new KubernetesAutoJoin(null, singlePeerGroup(),
        RaftPeerId.valueOf("localhost_2434"), new RaftProperties(), serverParameters);

    assertThat(GrpcConfigKeys.TLS.conf(autoJoin.probeParametersForTest())).isSameAs(tlsConfig);
  }

  /**
   * The legacy constructor still has to produce a usable, plaintext-dialling probe rather than a
   * {@link NullPointerException} the first time a peer is contacted.
   */
  @Test
  void probeClientWithoutTransportParametersStaysPlaintext() {
    final KubernetesAutoJoin autoJoin = new KubernetesAutoJoin(null, singlePeerGroup(),
        RaftPeerId.valueOf("localhost_2434"), new RaftProperties());

    assertThat(autoJoin.probeParametersForTest()).isNotNull();
    assertThat(GrpcConfigKeys.TLS.conf(autoJoin.probeParametersForTest())).isNull();
  }

  private static RaftGroup singlePeerGroup() {
    return RaftGroup.valueOf(RaftGroupId.randomId(),
        List.of(RaftPeer.newBuilder().setId("localhost_2434").setAddress("localhost:2434").build()));
  }
}
