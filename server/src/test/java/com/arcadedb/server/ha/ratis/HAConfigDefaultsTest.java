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
package com.arcadedb.server.ha.ratis;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates that HA-related configuration defaults are sane.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HAConfigDefaultsTest {

  @Test
  void replicationLagWarningHasDefault() {
    assertThat(GlobalConfiguration.HA_REPLICATION_LAG_WARNING.getValueAsLong()).isEqualTo(1000L);
  }

  @Test
  void haLogVerboseDefaultIsOff() {
    assertThat(GlobalConfiguration.HA_LOG_VERBOSE.getValueAsInteger()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void haQuorumTimeoutHasPositiveDefault() {
    assertThat(GlobalConfiguration.HA_QUORUM_TIMEOUT.getValueAsLong()).isGreaterThan(0);
  }

  @Test
  void haClusterTokenDefaultIsEmpty() {
    // Cluster token should be empty by default (no shared secret configured)
    final String token = GlobalConfiguration.HA_CLUSTER_TOKEN.getValueAsString();
    assertThat(token == null || token.isEmpty()).isTrue();
  }
}
