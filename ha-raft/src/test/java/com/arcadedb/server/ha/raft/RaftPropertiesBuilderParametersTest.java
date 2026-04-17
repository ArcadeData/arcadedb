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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.server.GrpcServices;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link RaftPropertiesBuilder#buildParameters(ContextConfiguration)} wires the
 * gRPC services customizer correctly based on the peer-allowlist configuration.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftPropertiesBuilderParametersTest {

  @Test
  void allowlistEnabledInstallsCustomizer() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED, true);
    cfg.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2424,localhost:2425,localhost:2426");

    final Parameters parameters = RaftPropertiesBuilder.buildParameters(cfg);

    final GrpcServices.Customizer customizer = GrpcConfigKeys.Server.servicesCustomizer(parameters);
    assertThat(customizer).isInstanceOf(RaftGrpcServicesCustomizer.class);
  }

  @Test
  void allowlistDisabledLeavesCustomizerUnset() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED, false);
    cfg.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2424,localhost:2425,localhost:2426");

    final Parameters parameters = RaftPropertiesBuilder.buildParameters(cfg);

    assertThat(GrpcConfigKeys.Server.servicesCustomizer(parameters)).isNull();
  }

  @Test
  void allowlistEnabledWithEmptyServerListSkipsCustomizer() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED, true);
    cfg.setValue(GlobalConfiguration.HA_SERVER_LIST, "");

    final Parameters parameters = RaftPropertiesBuilder.buildParameters(cfg);

    assertThat(GrpcConfigKeys.Server.servicesCustomizer(parameters)).isNull();
  }
}
