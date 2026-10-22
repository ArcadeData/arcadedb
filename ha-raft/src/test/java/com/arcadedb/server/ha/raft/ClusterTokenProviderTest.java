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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterTokenProviderTest {

  @Test
  void consistentTokenDerivation() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    final ClusterTokenProvider p1 = new ClusterTokenProvider(config);
    final ClusterTokenProvider p2 = new ClusterTokenProvider(config);
    assertThat(p1.getClusterToken()).isEqualTo(p2.getClusterToken());
  }

  @Test
  void differentClusterNamesDifferentTokens() {
    final ContextConfiguration c1 = new ContextConfiguration();
    c1.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "cluster-a");
    c1.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "same-password");
    final ContextConfiguration c2 = new ContextConfiguration();
    c2.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "cluster-b");
    c2.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "same-password");
    assertThat(new ClusterTokenProvider(c1).getClusterToken())
        .isNotEqualTo(new ClusterTokenProvider(c2).getClusterToken());
  }

  @Test
  void missingClusterNameThrows() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    assertThatThrownBy(() -> new ClusterTokenProvider(config).getClusterToken())
        .isInstanceOf(ConfigurationException.class);
  }

  @Test
  void missingRootPasswordThrows() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "");
    assertThatThrownBy(() -> new ClusterTokenProvider(config).getClusterToken())
        .isInstanceOf(ConfigurationException.class);
  }

  @Test
  void explicitTokenTakesPrecedence() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "explicit-token");
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    assertThat(new ClusterTokenProvider(config).getClusterToken()).isEqualTo("explicit-token");
  }

  @Test
  void tokenReadFromFile(@TempDir final Path tmp) throws Exception {
    final Path tokenFile = tmp.resolve("cluster.token");
    Files.writeString(tokenFile, "file-token");
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH, tokenFile.toString());
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    assertThat(new ClusterTokenProvider(config).getClusterToken()).isEqualTo("file-token");
  }

  @Test
  void tokenFromFileIsTrimmed(@TempDir final Path tmp) throws Exception {
    // Most tooling (echo, editors, k8s secrets) appends a trailing newline that must not leak into the token.
    final Path tokenFile = tmp.resolve("cluster.token");
    Files.writeString(tokenFile, "  file-token\n");
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH, tokenFile.toString());
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    assertThat(new ClusterTokenProvider(config).getClusterToken()).isEqualTo("file-token");
  }

  @Test
  void explicitTokenTakesPrecedenceOverFile(@TempDir final Path tmp) throws Exception {
    final Path tokenFile = tmp.resolve("cluster.token");
    Files.writeString(tokenFile, "file-token");
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "explicit-token");
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH, tokenFile.toString());
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    assertThat(new ClusterTokenProvider(config).getClusterToken()).isEqualTo("explicit-token");
  }

  @Test
  void unreadableTokenFileThrows() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH, "/non/existent/cluster.token");
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    assertThatThrownBy(() -> new ClusterTokenProvider(config).getClusterToken())
        .isInstanceOf(ConfigurationException.class);
  }

  @Test
  void emptyTokenFileThrows(@TempDir final Path tmp) throws Exception {
    final Path tokenFile = tmp.resolve("cluster.token");
    Files.writeString(tokenFile, "  \n");
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH, tokenFile.toString());
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    assertThatThrownBy(() -> new ClusterTokenProvider(config).getClusterToken())
        .isInstanceOf(ConfigurationException.class);
  }

  // Issue #10462: a server given its root password as a file (the shipped StatefulSet mounts a Secret and passes only
  // arcadedb.server.rootPasswordPath) has a root password, so HA must derive its cluster token from that file too.

  @Test
  void tokenDerivedFromRootPasswordFile(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "test-password");
    final ContextConfiguration fromFile = new ContextConfiguration();
    fromFile.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    fromFile.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());

    final ContextConfiguration inline = new ContextConfiguration();
    inline.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    inline.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");

    assertThat(new ClusterTokenProvider(fromFile).getClusterToken())
        .isEqualTo(new ClusterTokenProvider(inline).getClusterToken());
  }

  @Test
  void rootPasswordFileDropsOnlyTheTrailingNewlineLikeServerSecurity(@TempDir final Path tmp) throws Exception {
    // `kubectl create secret --from-file`, a here-doc or an editor append a newline that is not part of the password:
    // the token must match the one a node given the same password inline derives. Trailing spaces are part of the
    // password, exactly as ServerSecurity reads the same file into the root user.
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "test-password \r\n");
    final ContextConfiguration fromFile = new ContextConfiguration();
    fromFile.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    fromFile.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());

    final ContextConfiguration inline = new ContextConfiguration();
    inline.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    inline.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password ");

    assertThat(new ClusterTokenProvider(fromFile).getClusterToken())
        .isEqualTo(new ClusterTokenProvider(inline).getClusterToken());
  }

  @Test
  void inlineRootPasswordTakesPrecedenceOverRootPasswordFile(@TempDir final Path tmp) throws Exception {
    // Same precedence as ServerSecurity.askForRootPassword(): the file is read only when no inline password is set.
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "file-password");
    final ContextConfiguration both = new ContextConfiguration();
    both.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    both.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "inline-password");
    both.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());

    final ContextConfiguration inline = new ContextConfiguration();
    inline.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    inline.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "inline-password");

    assertThat(new ClusterTokenProvider(both).getClusterToken())
        .isEqualTo(new ClusterTokenProvider(inline).getClusterToken());
  }

  @Test
  void explicitTokenPathTakesPrecedenceOverRootPasswordFile(@TempDir final Path tmp) throws Exception {
    final Path tokenFile = tmp.resolve("clusterToken");
    Files.writeString(tokenFile, "file-token\n");
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "file-password");
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH, tokenFile.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());
    assertThat(new ClusterTokenProvider(config).getClusterToken()).isEqualTo("file-token");
  }

  @Test
  void unreadableRootPasswordFileThrowsNamingThePath() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, "/non/existent/rootPassword");
    assertThatThrownBy(() -> new ClusterTokenProvider(config).getClusterToken())
        .isInstanceOf(ConfigurationException.class).hasMessageContaining("/non/existent/rootPassword");
  }

  @Test
  void emptyRootPasswordFileThrows(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "\n");
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());
    assertThatThrownBy(() -> new ClusterTokenProvider(config).getClusterToken())
        .isInstanceOf(ConfigurationException.class).hasMessageContaining("empty");
  }

  @Test
  void missingRootPasswordErrorNamesThePasswordFileSetting() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    assertThatThrownBy(() -> new ClusterTokenProvider(config).getClusterToken())
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH.getKey())
        .hasMessageContaining(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH.getKey());
  }

  @Test
  void testHelperDerivesFromRootPasswordFile(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "test-password\n");
    final ContextConfiguration fromFile = new ContextConfiguration();
    fromFile.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    fromFile.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());
    ClusterTokenProvider.initClusterTokenForTest(fromFile);

    final ContextConfiguration inline = new ContextConfiguration();
    inline.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    inline.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");

    assertThat(fromFile.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN))
        .isEqualTo(new ClusterTokenProvider(inline).getClusterToken());
  }
}
