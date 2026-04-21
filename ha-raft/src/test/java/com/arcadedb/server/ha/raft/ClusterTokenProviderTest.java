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
}
