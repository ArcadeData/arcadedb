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
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the {@code arcadedb.ha.tls.*} settings introduced by issue #3890: the Raft gRPC transport must
 * stay plaintext by default, must carry a {@link GrpcTlsConfig} on the Ratis {@link Parameters} once
 * enabled, and must refuse to start - naming the offending setting - when enabled with a cert, key or trust
 * path that cannot be read.
 * <p>
 * The PEM contents are irrelevant here; only the plumbing and the validation are under test, so ordinary
 * files stand in for certificates. {@link Issue3890RaftMtlsIT} covers the handshake itself with a real CA.
 */
class RaftGrpcTlsConfigTest {

  @TempDir
  private Path   tempDir;
  private Path   certChain;
  private Path   privateKey;
  private Path   trustCerts;

  @BeforeEach
  void createFiles() throws Exception {
    certChain = Files.writeString(tempDir.resolve("node-cert.pem"), "cert");
    privateKey = Files.writeString(tempDir.resolve("node-key.pem"), "key");
    trustCerts = Files.writeString(tempDir.resolve("ca.pem"), "ca");
  }

  @Test
  void transportStaysPlaintextByDefault() {
    final Parameters parameters = new Parameters();
    assertThat(RaftPropertiesBuilder.applyTls(new ContextConfiguration(), parameters)).isNull();
    assertThat(GrpcConfigKeys.TLS.conf(parameters)).isNull();
  }

  /**
   * The paths are only read when TLS is on, so a stale or templated path left behind in a configuration
   * file must not stop a plaintext node from starting.
   */
  @Test
  void disabledTlsIgnoresUnusablePaths() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_TLS_CERT_CHAIN_FILE, tempDir.resolve("nope.pem").toString());

    final Parameters parameters = new Parameters();
    assertThatCode(() -> RaftPropertiesBuilder.applyTls(config, parameters)).doesNotThrowAnyException();
    assertThat(GrpcConfigKeys.TLS.conf(parameters)).isNull();
  }

  @Test
  void enabledTlsAttachesFileBasedConfigToParameters() {
    final Parameters parameters = new Parameters();
    final GrpcTlsConfig returned = RaftPropertiesBuilder.applyTls(enabledConfiguration(), parameters);

    // The single TLS entry is what Ratis's GrpcFactory reads as the default for the admin, client and
    // server-to-server endpoints alike, so this one assertion covers all three.
    final GrpcTlsConfig attached = GrpcConfigKeys.TLS.conf(parameters);
    assertThat(attached).isNotNull().isSameAs(returned);
    assertThat(attached.isFileBasedConfig()).isTrue();
    assertThat(attached.getCertChainFile()).isEqualTo(certChain.toFile());
    assertThat(attached.getPrivateKeyFile()).isEqualTo(privateKey.toFile());
    assertThat(attached.getTrustStoreFile()).isEqualTo(trustCerts.toFile());
    assertThat(attached.getMtlsEnabled()).isTrue();
  }

  @Test
  void mutualAuthenticationCanBeTurnedOff() {
    final ContextConfiguration config = enabledConfiguration();
    config.setValue(GlobalConfiguration.HA_TLS_MUTUAL_AUTH, false);

    final GrpcTlsConfig tlsConfig = RaftPropertiesBuilder.buildTlsConfig(config);
    assertThat(tlsConfig).isNotNull();
    assertThat(tlsConfig.getMtlsEnabled()).isFalse();
  }

  @Test
  void unsetCertChainFileFailsFast() {
    final ContextConfiguration config = enabledConfiguration();
    config.setValue(GlobalConfiguration.HA_TLS_CERT_CHAIN_FILE, "");

    assertThatThrownBy(() -> RaftPropertiesBuilder.buildTlsConfig(config))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("arcadedb.ha.tls.certChainFile")
        .hasMessageContaining("arcadedb.ha.tls.enabled");
  }

  @Test
  void unsetPrivateKeyFileFailsFast() {
    final ContextConfiguration config = enabledConfiguration();
    config.setValue(GlobalConfiguration.HA_TLS_PRIVATE_KEY_FILE, "");

    assertThatThrownBy(() -> RaftPropertiesBuilder.buildTlsConfig(config))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("arcadedb.ha.tls.privateKeyFile");
  }

  @Test
  void unsetTrustCertCollectionFileFailsFast() {
    final ContextConfiguration config = enabledConfiguration();
    config.setValue(GlobalConfiguration.HA_TLS_TRUST_CERT_COLLECTION_FILE, "   ");

    assertThatThrownBy(() -> RaftPropertiesBuilder.buildTlsConfig(config))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("arcadedb.ha.tls.trustCertCollectionFile");
  }

  /**
   * The shape an unmounted Kubernetes secret volume takes: the path is configured, nothing is there.
   */
  @Test
  void missingCertificateFileFailsFast() {
    final ContextConfiguration config = enabledConfiguration();
    config.setValue(GlobalConfiguration.HA_TLS_CERT_CHAIN_FILE, tempDir.resolve("absent.pem").toString());

    assertThatThrownBy(() -> RaftPropertiesBuilder.buildTlsConfig(config))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("arcadedb.ha.tls.certChainFile")
        .hasMessageContaining("is not a readable file");
  }

  /**
   * The shape a secret volume mounted at the wrong depth takes: the path exists, but it is the directory
   * holding the certificate rather than the certificate.
   */
  @Test
  void directoryInPlaceOfCertificateFailsFast() {
    final ContextConfiguration config = enabledConfiguration();
    config.setValue(GlobalConfiguration.HA_TLS_TRUST_CERT_COLLECTION_FILE, tempDir.toString());

    assertThatThrownBy(() -> RaftPropertiesBuilder.buildTlsConfig(config))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("arcadedb.ha.tls.trustCertCollectionFile")
        .hasMessageContaining("is not a readable file");
  }

  private ContextConfiguration enabledConfiguration() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_TLS_ENABLED, true);
    config.setValue(GlobalConfiguration.HA_TLS_CERT_CHAIN_FILE, certChain.toString());
    config.setValue(GlobalConfiguration.HA_TLS_PRIVATE_KEY_FILE, privateKey.toString());
    config.setValue(GlobalConfiguration.HA_TLS_TRUST_CERT_COLLECTION_FILE, trustCerts.toString());
    return config;
  }
}
