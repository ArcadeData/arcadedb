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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10601 end to end, on the start path a real node takes: a one-voter cluster whose only password is the file at
 * {@code arcadedb.server.rootPasswordPath}, as the shipped StatefulSet mounts it. Rewriting that file and restarting the
 * node - what a rotated Kubernetes Secret and a pod restart do - must refuse the start rather than bring the node up on a
 * token its peers do not hold; {@code arcadedb.ha.clusterTokenAllowRotation} lets the operator rotate on purpose.
 */
class Issue10601DerivedClusterTokenRotationIT extends BaseRaftHATest {

  private Path passwordFile;

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, getServerAddresses());
    config.setValue(GlobalConfiguration.HA_ENABLED, true);
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "issue10601");
    try {
      passwordFile = Files.createTempFile("issue10601-rootPassword", "");
      Files.writeString(passwordFile, DEFAULT_PASSWORD_FOR_TESTS + "\n");
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    passwordFile.toFile().deleteOnExit();
    // The password reaches this node through the file only: neither the server's setting nor the global one carries it.
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, null);
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());
  }

  @Test
  @Timeout(180)
  void aRewrittenPasswordFileRefusesTheRestartUntilTheRotationIsAllowed() throws Exception {
    final RaftHAServer raft = getRaftPlugin(0).getRaftHAServer();
    final String tokenBefore = raft.getClusterToken();
    final File fingerprint = RaftHAServer.clusterTokenFingerprintFile(raft.getRaftStorageDir());
    assertThat(fingerprint).as("a node whose token is derived from a file records it").exists();
    assertThat(Files.readString(fingerprint.toPath())).isEqualTo(ClusterTokenProvider.tokenFingerprint(tokenBefore));

    // A restart on the same file comes back on the same token.
    getServer(0).stop();
    startServer(0);
    assertThat(getRaftPlugin(0).getRaftHAServer().getClusterToken()).isEqualTo(tokenBefore);

    // The Secret is rotated in place, then the pod restarts.
    getServer(0).stop();
    Files.writeString(passwordFile, "Rotated-" + DEFAULT_PASSWORD_FOR_TESTS + "\n");
    assertThatThrownBy(() -> startServer(0))
        .as("a node must not come up on a token its peers do not hold")
        .hasStackTraceContaining("Refusing to start")
        .hasStackTraceContaining("arcadedb.ha.clusterTokenAllowRotation");
    assertThat(getServer(0).isStarted()).isFalse();
    assertThat(Files.readString(fingerprint.toPath())).isEqualTo(ClusterTokenProvider.tokenFingerprint(tokenBefore));

    // The operator rotates on purpose. A pod whose start failed is a new process; here the same server object is reused,
    // so it is stopped first to leave the failed start behind.
    getServer(0).stop();
    getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN_ALLOW_ROTATION, true);
    startServer(0);
    final String tokenAfter = getRaftPlugin(0).getRaftHAServer().getClusterToken();
    assertThat(tokenAfter).isNotEqualTo(tokenBefore);
    assertThat(Files.readString(fingerprint.toPath())).isEqualTo(ClusterTokenProvider.tokenFingerprint(tokenAfter));
  }
}
