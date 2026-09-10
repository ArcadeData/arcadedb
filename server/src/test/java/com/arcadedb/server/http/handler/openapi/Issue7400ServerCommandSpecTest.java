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
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.Paths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7400, contract surface 2: the cluster pair is dispatched by
 * {@code PostServerCommandHandler} but neither half appeared in the {@code POST /api/v1/server}
 * command list the spec publishes, so a client reading only the spec could not know the verbs exist
 * on this transport at all.
 * <p>
 * The pair is asserted together rather than {@code connect cluster} alone: the omission this catches
 * is a verb dispatched by the handler and missing from the spec, and that was true of both halves.
 */
class Issue7400ServerCommandSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new CoreApiSpec().contribute(openAPI);
  }

  @Test
  void theServerCommandListNamesTheClusterPair() {
    final Operation post = openAPI.getPaths().get("/api/v1/server").getPost();

    assertThat(post.getOperationId()).isEqualTo("executeServerCommand");
    assertThat(post.getDescription())
        .as("both halves of the cluster pair are dispatched by PostServerCommandHandler and must be documented")
        .contains("connect cluster")
        .contains("disconnect cluster");
  }
}
