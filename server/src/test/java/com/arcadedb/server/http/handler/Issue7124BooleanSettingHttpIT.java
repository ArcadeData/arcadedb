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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7124, the caller side of the strict boolean parse. {@code POST /api/v1/server} with
 * "set server setting &lt;key&gt; &lt;value&gt;" converted a boolean through the permissive
 * {@code GlobalConfiguration.coerce}, which reads anything it cannot parse as {@code false}. So
 * {@code ... requireAuthentication ture} was answered with a 200 and stored {@code Boolean.FALSE} - a typo that
 * turns the metrics endpoint public, reported as a success, with the text that produced it gone before anything
 * downstream could tell it from a deliberate {@code false}.
 * <p>
 * Every other setting type already refused what it could not read here (#6875); this pins that a boolean does too.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7124BooleanSettingHttpIT extends BaseGraphServerTest {
  private final HttpClient client = HttpClient.newHttpClient();

  @Test
  void refusesABooleanTypoInsteadOfStoringItAsFalse() throws Exception {
    final ContextConfiguration configuration = getServer(0).getConfiguration();
    final GlobalConfiguration setting = GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION;

    final HttpResponse<String> response = executeServerCommand("set server setting " + setting.getKey() + " ture");

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains(setting.getKey());
    assertThat(configuration.hasValue(setting.getKey())).as(
        "a refused command must not leave the setting flipped to the value the typo would have produced").isFalse();
  }

  @Test
  void stillAcceptsBothBooleanLiterals() throws Exception {
    final ContextConfiguration configuration = getServer(0).getConfiguration();
    final GlobalConfiguration setting = GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION;
    try {
      assertThat(executeServerCommand("set server setting " + setting.getKey() + " false").statusCode()).isEqualTo(200);
      assertThat((Object) configuration.getValue(setting)).isInstanceOf(Boolean.class).isEqualTo(Boolean.FALSE);

      assertThat(executeServerCommand("set server setting " + setting.getKey() + " TRUE").statusCode()).isEqualTo(200);
      assertThat((Object) configuration.getValue(setting)).isEqualTo(Boolean.TRUE);
    } finally {
      configuration.setValue(setting.getKey(), null);
    }
  }

  private HttpResponse<String> executeServerCommand(final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/server"))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    return client.send(request, BodyHandlers.ofString());
  }
}
