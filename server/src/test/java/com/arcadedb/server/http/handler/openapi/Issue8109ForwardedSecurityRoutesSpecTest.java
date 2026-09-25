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
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8109: the group and API-token write routes now forward to the HA leader like the user routes, so they can
 * answer the bounded forward's 504 (issue #7507), and the spec has to say so. The reads do not forward.
 */
class Issue8109ForwardedSecurityRoutesSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new SecurityAdminApiSpec().contribute(openAPI);
  }

  @Test
  void theGroupWritesDeclareTheLeaderForwardTimeout() {
    assertThat(openAPI.getPaths().get("/api/v1/server/groups").getPost().getResponses()).containsKey("504");
    assertThat(openAPI.getPaths().get("/api/v1/server/groups").getDelete().getResponses()).containsKey("504");
    assertThat(openAPI.getPaths().get("/api/v1/server/groups").getGet().getResponses()).doesNotContainKey("504");
  }

  @Test
  void theApiTokenWritesDeclareTheLeaderForwardTimeout() {
    assertThat(openAPI.getPaths().get("/api/v1/server/api-tokens").getPost().getResponses())
        .containsKey("504").containsKey("412").containsKey("201");
    assertThat(openAPI.getPaths().get("/api/v1/server/api-tokens").getDelete().getResponses()).containsKey("504");
    assertThat(openAPI.getPaths().get("/api/v1/server/api-tokens").getGet().getResponses()).doesNotContainKey("504");
  }
}
