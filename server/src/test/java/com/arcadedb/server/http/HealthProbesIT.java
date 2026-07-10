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
package com.arcadedb.server.http;

import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

class HealthProbesIT extends BaseGraphServerTest {

  @Test
  void healthReturns204WhenLive() throws Exception {
    testEachServer(serverIndex -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/health").openConnection();
      connection.setRequestMethod("GET");
      try {
        connection.connect();
        assertThat(connection.getResponseCode()).isEqualTo(204);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void healthDoesNotRequireAuthentication() throws Exception {
    testEachServer(serverIndex -> {
      // No Authorization header is set on purpose: liveness must be unauthenticated.
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/health").openConnection();
      connection.setRequestMethod("GET");
      try {
        connection.connect();
        assertThat(connection.getResponseCode()).isNotEqualTo(401);
        assertThat(connection.getResponseCode()).isNotEqualTo(403);
        assertThat(connection.getResponseCode()).isEqualTo(204);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void readyUnchangedWhenReadinessFlagUnset() throws Exception {
    // No arcadedb.server.readinessRequiresHA is set in this test, so behavior must be
    // byte-identical to today: an ONLINE single-node server returns 204 with an empty body.
    testEachServer(serverIndex -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/ready").openConnection();
      connection.setRequestMethod("GET");
      try {
        connection.connect();
        assertThat(connection.getResponseCode()).isEqualTo(204);
        assertThat(connection.getContentLength()).isLessThanOrEqualTo(0);
      } finally {
        connection.disconnect();
      }
    });
  }
}
