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
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10075: {@code POST /api/v1/server/api-tokens} answers 409 when the token name is taken, and now names the
 * exception in the body so a client can type the conflict. The generated OpenAPI document did not mention the 409 at all,
 * and a generated SDK builds its error handling from that document.
 * <p>
 * Only the mint can find a name taken: the list and the delete on the same path create nothing.
 */
class Issue10075ApiTokenTakenNameIsSpecifiedTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new SecurityAdminApiSpec().contribute(openAPI);
  }

  @Test
  void theMintDocumentsTheConflictItAnswersForATakenName() {
    final PathItem tokens = openAPI.getPaths().get("/api/v1/server/api-tokens");

    assertThat(tokens.getPost().getResponses().get("409")).as("PostApiTokenHandler answers 409 for a taken name").isNotNull();
    assertThat(tokens.getPost().getResponses().get("409").getDescription()).containsIgnoringCase("already issued")
        .contains("RemoteAlreadyExistsException");
  }

  @Test
  void theRoutesThatCreateNothingDoNotClaimIt() {
    final PathItem tokens = openAPI.getPaths().get("/api/v1/server/api-tokens");

    assertThat(tokens.getGet().getResponses().get("409")).isNull();
    assertThat(tokens.getDelete().getResponses().get("409")).isNull();
  }
}
