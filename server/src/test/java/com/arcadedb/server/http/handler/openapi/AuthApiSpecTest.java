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
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AuthApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new AuthApiSpec().contribute(openAPI);
  }

  @Test
  void loginTakesNoRequestBody() {
    final Operation post = openAPI.getPaths().get("/api/v1/login").getPost();
    assertThat(post.getOperationId()).isEqualTo("login");
    assertThat(post.getRequestBody())
        .as("login authenticates from the Authorization header and reads no body")
        .isNull();
  }

  @Test
  void loginResponseCarriesTokenAndUser() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("LoginResponse");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder("token", "user");
  }

  @Test
  void logoutReturnsNoContentAndDeclaresNoSuccessBody() {
    final Operation post = openAPI.getPaths().get("/api/v1/logout").getPost();
    assertThat(post.getOperationId()).isEqualTo("logout");
    assertThat(post.getResponses()).containsKey("204");
    assertThat(post.getResponses().get("204").getContent()).isNull();
    assertThat(post.getResponses().get("200"))
        .as("logout never answers 200")
        .isNull();
  }

  @Test
  void sessionListCarriesEveryTrackedField() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("SessionList");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder("result", "count");
    assertThat(schema.getProperties().get("result").getItems().getProperties().keySet())
        .containsExactlyInAnyOrder("token", "user", "createdAt", "lastUpdate", "elapsedMs",
            "sourceIp", "userAgent", "country", "city");
  }

  @Test
  void everyAuthOperationIsTaggedAuth() {
    for (final String path : List.of("/api/v1/login", "/api/v1/logout", "/api/v1/sessions")) {
      final PathItem item = openAPI.getPaths().get(path);
      final Operation op = item.getPost() != null ? item.getPost() : item.getGet();
      assertThat(op.getTags()).as("%s", path).containsExactly("Auth");
    }
  }
}
