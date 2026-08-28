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
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Issue #6875, the HTTP twin of {@code set_server_setting}. {@code POST /api/v1/server} with
 * "set server setting &lt;key&gt; &lt;value&gt;" enforced the two-token shape and nothing about the value: it split
 * on the first space and handed both halves to {@code ContextConfiguration.setValue}, a plain map put. So the value
 * kept the separating space, a backtick-quoted key was stored under a name nothing reads, and an unparseable value
 * was answered with a 200 and threw later, inside whichever component read the setting next.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6875SetServerSettingHttpIT extends BaseGraphServerTest {
  private final HttpClient client = HttpClient.newHttpClient();

  @Test
  void rejectsAValueThatIsNotParseableAsTheSettingType() throws Exception {
    final HttpResponse<String> response = executeServerCommand("set server setting arcadedb.asyncWorkerThreads abc");

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("arcadedb.asyncWorkerThreads");

    // the rejected call is not the one that breaks a later read
    assertThatCode(() -> getServer(0).getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS))
        .doesNotThrowAnyException();
    assertThat(getServer(0).getConfiguration().hasValue(GlobalConfiguration.ASYNC_WORKER_THREADS.getKey())).isFalse();
  }

  /** The value used to arrive with the separating space still on it, and as text rather than as the setting's type. */
  @Test
  void storesTheValueCoercedAndWithoutTheSeparatingSpace() throws Exception {
    final ContextConfiguration configuration = getServer(0).getConfiguration();
    final GlobalConfiguration setting = GlobalConfiguration.SQL_STATEMENT_CACHE;
    try {
      final HttpResponse<String> response = executeServerCommand("set server setting " + setting.getKey() + " 500");

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat((Object) configuration.getValue(setting)).isInstanceOf(Integer.class).isEqualTo(500);
      assertThat(configuration.getValueAsInteger(setting)).isEqualTo(500);
    } finally {
      configuration.setValue(setting.getKey(), null);
    }
  }

  /** The documented command syntax quotes the key; the quoted form has to reach the setting it names. */
  @Test
  void resolvesABacktickQuotedKeyAndUnquotesTheValue() throws Exception {
    final ContextConfiguration configuration = getServer(0).getConfiguration();
    final GlobalConfiguration setting = GlobalConfiguration.DATE_TIME_FORMAT;
    try {
      final HttpResponse<String> response = executeServerCommand(
          "set server setting `" + setting.getKey() + "` 'yyyy-MM-dd'");

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(configuration.hasValue(setting.getKey())).isTrue();
      assertThat(configuration.getValueAsString(setting)).isEqualTo("yyyy-MM-dd");
    } finally {
      configuration.setValue(setting.getKey(), null);
    }
  }

  /** A size-suffixed value is what the integral accessors read back, so the writer takes it and stores the number. */
  @Test
  void acceptsASizeSuffixedValueForAnIntegralSetting() throws Exception {
    final ContextConfiguration configuration = getServer(0).getConfiguration();
    final GlobalConfiguration setting = GlobalConfiguration.COMMIT_LOCK_TIMEOUT;
    try {
      final HttpResponse<String> response = executeServerCommand("set server setting " + setting.getKey() + " 1KB");

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(configuration.getValueAsLong(setting)).isEqualTo(1024L);
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
