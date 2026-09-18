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
 * Issue #7804: the transport refusal #7372 added to {@code POST /api/v1/server/api-tokens} answers 412, and
 * the generated OpenAPI document did not mention it. Studio is not the only client of this route - a
 * generated SDK builds its error handling from this document, and a status absent from the spec is a status
 * the SDK turns into an unhandled fault.
 * <p>
 * Only the mint is affected. The list and the delete on the same path apply no transport check, because
 * neither returns token material, and asserting that here keeps a later "add 412 everywhere on this path"
 * from widening a contract nothing enforces.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7804ApiTokenMintRefusalIsSpecifiedTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new SecurityAdminApiSpec().contribute(openAPI);
  }

  @Test
  void theMintDocumentsThe412ItCanAnswer() {
    final PathItem tokens = openAPI.getPaths().get("/api/v1/server/api-tokens");

    assertThat(tokens.getPost().getResponses().get("412"))
        .as("PostApiTokenHandler.checkTransport answers 412 and a client has to be told it can")
        .isNotNull();
    assertThat(tokens.getPost().getResponses().get("412").getDescription())
        .containsIgnoringCase("transport");
  }

  /**
   * The 412 means "reconnect over TLS", which is only worth saying to a caller that is being handed a secret.
   */
  @Test
  void theRoutesThatReturnNoTokenMaterialDoNotClaimIt() {
    final PathItem tokens = openAPI.getPaths().get("/api/v1/server/api-tokens");

    assertThat(tokens.getGet().getResponses().get("412")).isNull();
    assertThat(tokens.getDelete().getResponses().get("412")).isNull();
  }

  /**
   * The mint keeps every response it already declared: the 412 is an addition, not a replacement.
   */
  @Test
  void theExistingMintResponsesSurvive() {
    final PathItem tokens = openAPI.getPaths().get("/api/v1/server/api-tokens");

    assertThat(tokens.getPost().getResponses().keySet())
        .contains("201", "400", "401", "403", "500");
    assertThat(tokens.getPost().getOperationId()).isEqualTo("createApiToken");
  }
}
