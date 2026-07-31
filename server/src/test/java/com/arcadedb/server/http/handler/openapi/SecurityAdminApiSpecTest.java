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
import io.swagger.v3.oas.models.parameters.Parameter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SecurityAdminApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new SecurityAdminApiSpec().contribute(openAPI);
  }

  @Test
  void usersPathKeepsAllFourMethodsAndOperationIds() {
    final PathItem users = openAPI.getPaths().get("/api/v1/server/users");
    assertThat(users.getGet().getOperationId()).isEqualTo("listUsers");
    assertThat(users.getPost().getOperationId()).isEqualTo("createUser");
    assertThat(users.getPut().getOperationId()).isEqualTo("updateUser");
    assertThat(users.getDelete().getOperationId()).isEqualTo("deleteUser");
  }

  @Test
  void groupsPathKeepsThreeMethods() {
    final PathItem groups = openAPI.getPaths().get("/api/v1/server/groups");
    assertThat(groups.getGet().getOperationId()).isEqualTo("listGroups");
    assertThat(groups.getPost().getOperationId()).isEqualTo("createOrUpdateGroup");
    assertThat(groups.getDelete().getOperationId()).isEqualTo("deleteGroup");
    assertThat(groups.getPut()).isNull();
  }

  @Test
  void apiTokensPathKeepsThreeMethods() {
    final PathItem tokens = openAPI.getPaths().get("/api/v1/server/api-tokens");
    assertThat(tokens.getGet().getOperationId()).isEqualTo("listApiTokens");
    assertThat(tokens.getPost().getOperationId()).isEqualTo("createApiToken");
    assertThat(tokens.getDelete().getOperationId()).isEqualTo("deleteApiToken");
  }

  @Test
  void createUserAnswers201() {
    assertThat(openAPI.getPaths().get("/api/v1/server/users").getPost().getResponses())
        .containsKey("201");
  }

  @Test
  void createApiTokenAnswers201() {
    assertThat(openAPI.getPaths().get("/api/v1/server/api-tokens").getPost().getResponses())
        .containsKey("201");
  }

  @Test
  void deleteOperationsDeclareTheirQueryParameters() {
    assertThat(openAPI.getPaths().get("/api/v1/server/users").getDelete().getParameters()
        .stream().map(Parameter::getName).toList()).containsExactly("name");
    assertThat(openAPI.getPaths().get("/api/v1/server/groups").getDelete().getParameters()
        .stream().map(Parameter::getName).toList()).containsExactlyInAnyOrder("database", "name");
    assertThat(openAPI.getPaths().get("/api/v1/server/api-tokens").getDelete().getParameters()
        .stream().map(Parameter::getName).toList()).containsExactly("token");
  }

  @Test
  void everyOperationIsTaggedSecurity() {
    for (final String path : List.of("/api/v1/server/users", "/api/v1/server/groups",
        "/api/v1/server/api-tokens")) {
      final PathItem item = openAPI.getPaths().get(path);
      assertThat(item.readOperations()).allSatisfy(op ->
          assertThat(op.getTags()).as("%s", path).containsExactly("Security"));
    }
  }
}
