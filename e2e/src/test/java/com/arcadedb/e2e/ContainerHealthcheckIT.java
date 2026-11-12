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
package com.arcadedb.e2e;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The healthcheck shipped in docker-compose.yml runs inside the container through {@code CMD-SHELL}, so the image needs a
 * shell and {@code wget}. The JVM image gets both from its Alpine base; the native images (FROM scratch / distroless) ship
 * a static BusyBox for it (#11546). The command is the one in docker-compose.yml, so a drift between the two shows here.
 */
class ContainerHealthcheckIT extends ArcadeContainerTemplate {

  private static final String COMPOSE_HEALTHCHECK = "wget -qO- http://localhost:2480/api/v1/ready || exit 1";

  @Test
  void composeHealthcheckCommandSucceedsInsideTheImage() throws Exception {
    final ExecResult result = ARCADE.execInContainer("/bin/sh", "-c", COMPOSE_HEALTHCHECK);
    assertThat(result.getExitCode()).as("healthcheck stderr: %s", result.getStderr()).isZero();
  }

  @Test
  void healthcheckFailsWhenTheEndpointIsUnreachable() throws Exception {
    final ExecResult result = ARCADE.execInContainer("/bin/sh", "-c", "wget -qO- http://localhost:1/api/v1/ready || exit 1");
    assertThat(result.getExitCode()).isNotZero();
  }

  @Test
  void imageShipsAShellForDockerExec() throws Exception {
    final ExecResult result = ARCADE.execInContainer("/bin/sh", "-c", "ls /home/arcadedb/databases && cat /home/arcadedb/config/arcadedb-log.properties");
    assertThat(result.getExitCode()).as("stderr: %s", result.getStderr()).isZero();
  }
}
