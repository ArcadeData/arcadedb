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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9464: {@link TestServerHelper#unstartedServer} is what replaces a Mockito mock of {@link ArcadeDBServer} that
 * only stubbed getters, so it has to answer those getters the way the stubs did and have no side effect a mock lacked.
 */
class UnstartedServerTest {
  @TempDir
  Path root;

  @Test
  void answersFromTheGivenConfigurationAndRootWithoutTouchingTheDisk() throws IOException {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_MODE, "production");

    final ArcadeDBServer server = TestServerHelper.unstartedServer(root, configuration);

    assertThat(server.getConfiguration()).isSameAs(configuration);
    assertThat(server.getRootPath()).isEqualTo(root.toString());
    assertThat(server.getConfigPath()).isEqualTo(root.resolve("config").toString());
    assertThat(server.isProductionMode()).isTrue();
    assertThat(server.getHA()).isNull();
    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.OFFLINE);
    try (final Stream<Path> files = Files.list(root)) {
      assertThat(files).as("construction must not create the log, config or database directories").isEmpty();
    }
  }

  @Test
  void theDefaultModeIsNotProduction() {
    assertThat(TestServerHelper.unstartedServer(root, new ContextConfiguration()).isProductionMode()).isFalse();
  }
}
