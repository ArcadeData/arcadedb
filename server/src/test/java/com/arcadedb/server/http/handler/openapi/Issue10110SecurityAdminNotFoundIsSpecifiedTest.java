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

import com.arcadedb.server.http.handler.OpenApiSpecGenerator;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.responses.ApiResponse;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10110: the security admin routes that answer 404 for a name or hash nothing has did not declare it, so a
 * client generated from the document treated the status as undocumented. {@code DeleteApiTokenHandler},
 * {@code DeleteUserHandler}, {@code PutUserHandler} and {@code DeleteGroupHandler} all answer it. The second half of
 * the issue, the duplicate-name case of {@code createApiToken}'s 409, was declared by #10075; it is pinned here on the
 * generated document, where the in-flight replay description is appended to it.
 */
class Issue10110SecurityAdminNotFoundIsSpecifiedTest {
  private static final String TOKENS = "/api/v1/server/api-tokens";
  private static final String USERS  = "/api/v1/server/users";
  private static final String GROUPS = "/api/v1/server/groups";

  private final OpenAPI openAPI = new OpenApiSpecGenerator(null).generateSpec();

  @Test
  void deleteApiTokenDeclaresItsNotFound() {
    final ApiResponse notFound = path(TOKENS).getDelete().getResponses().get("404");
    assertThat(notFound).isNotNull();
    assertThat(notFound.getDescription()).contains("No API token has this hash");
  }

  @Test
  void deleteUserDeclaresItsNotFound() {
    final ApiResponse notFound = path(USERS).getDelete().getResponses().get("404");
    assertThat(notFound).isNotNull();
    assertThat(notFound.getDescription()).contains("No user has this name");
  }

  @Test
  void updateUserDeclaresItsNotFound() {
    final ApiResponse notFound = path(USERS).getPut().getResponses().get("404");
    assertThat(notFound).isNotNull();
    assertThat(notFound.getDescription()).contains("No user has this name");
  }

  @Test
  void deleteGroupDeclaresItsNotFound() {
    final ApiResponse notFound = path(GROUPS).getDelete().getResponses().get("404");
    assertThat(notFound).isNotNull();
    assertThat(notFound.getDescription()).contains("No group has this name");
  }

  @Test
  void routesThatNeverAnswerNotFoundDoNotDeclareIt() {
    // The handlers behind these create or list: nothing they look up can be missing, so a 404 here would be a promise
    // a client codes against and never receives
    assertThat(path(TOKENS).getGet().getResponses()).doesNotContainKey("404");
    assertThat(path(TOKENS).getPost().getResponses()).doesNotContainKey("404");
    assertThat(path(USERS).getGet().getResponses()).doesNotContainKey("404");
    assertThat(path(USERS).getPost().getResponses()).doesNotContainKey("404");
    assertThat(path(GROUPS).getGet().getResponses()).doesNotContainKey("404");
    assertThat(path(GROUPS).getPost().getResponses()).doesNotContainKey("404");
  }

  @Test
  void createApiTokenConflictNamesBothTheDuplicateNameAndTheInFlightReplay() {
    final String description = path(TOKENS).getPost().getResponses().get("409").getDescription();
    assertThat(description).contains("a token with this name was already issued");
    assertThat(description).contains("still executing");
  }

  private PathItem path(final String path) {
    final PathItem item = openAPI.getPaths().get(path);
    assertThat(item).as(path).isNotNull();
    return item;
  }
}
