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
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;

import static org.assertj.core.api.Assertions.assertThat;

class PostCommandHandlerForwardAuthTest {

  @Test
  void sessionTokenIsReplacedWithInternalHeaders() {
    final HttpRequest request = PostCommandHandler.buildForwardRequest(
        "leader:2480", "mydb", "{}",
        "Bearer AU-some-uuid", "alice", "cluster-secret"
    ).build();

    assertThat(request.headers().firstValue("Authorization")).isEmpty();
    assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).hasValue("alice");
    assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).hasValue("cluster-secret");
  }

  @Test
  void basicAuthIsForwardedUnchanged() {
    final HttpRequest request = PostCommandHandler.buildForwardRequest(
        "leader:2480", "mydb", "{}",
        "Basic dXNlcjpwYXNz", null, "cluster-secret"
    ).build();

    assertThat(request.headers().firstValue("Authorization")).hasValue("Basic dXNlcjpwYXNz");
    assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).isEmpty();
    assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).isEmpty();
  }

  @Test
  void apiTokenIsForwardedUnchanged() {
    final HttpRequest request = PostCommandHandler.buildForwardRequest(
        "leader:2480", "mydb", "{}",
        "Bearer at-some-api-token", null, "cluster-secret"
    ).build();

    assertThat(request.headers().firstValue("Authorization")).hasValue("Bearer at-some-api-token");
    assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).isEmpty();
    assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).isEmpty();
  }

  @Test
  void nullAuthHeaderProducesNoAuthHeaders() {
    final HttpRequest request = PostCommandHandler.buildForwardRequest(
        "leader:2480", "mydb", "{}",
        null, null, "cluster-secret"
    ).build();

    assertThat(request.headers().firstValue("Authorization")).isEmpty();
    assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).isEmpty();
    assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).isEmpty();
  }
}
