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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpResponse;
import java.util.Base64;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * {@code POST /api/v1/server} with "set server setting &lt;key&gt; &lt;value&gt;": the value is validated and coerced against
 * the setting it names, and refused rather than stored when it does not parse.
 */
class SetServerSettingHttpIT extends BaseGraphServerTest {
  /*
   * Issue #6875, the HTTP twin of {@code set_server_setting}. {@code POST /api/v1/server} with
   * "set server setting &lt;key&gt; &lt;value&gt;" enforced the two-token shape and nothing about the value: it split
   * on the first space and handed both halves to {@code ContextConfiguration.setValue}, a plain map put. So the value
   * kept the separating space, a backtick-quoted key was stored under a name nothing reads, and an unparseable value
   * was answered with a 200 and threw later, inside whichever component read the setting next.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  private final HttpClient client = HttpClient.newHttpClient();

  // Regression: #6875 the value is parsed as the setting type, unquoted and without the separating space
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

  // Regression: #6875 the value is parsed as the setting type, unquoted and without the separating space
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

  // Regression: #6875 the value is parsed as the setting type, unquoted and without the separating space
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

  // Regression: #6875 the value is parsed as the setting type, unquoted and without the separating space
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
        .uri(new URI(getServerHttpUrl("/api/v1/server")))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    return client.send(request, BodyHandlers.ofString());
  }

  /*
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
  // Regression: #7124 a boolean typo is refused instead of being stored as false
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

  // Regression: #7124 a boolean typo is refused instead of being stored as false
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

  /*
   * Issue #7296 at the transport the report actually names: {@code POST /api/v1/server} with
   * "set server setting arcadedb.server.mode prodction" answered <b>200</b> and stored {@code "prodction"}.
   * {@code AbstractServerHttpHandler} decides error-detail concealment by comparing the stored value with
   * {@code "production"}, found it different, and served the deployment the DEVELOPMENT behaviour - so an operator
   * who believed they had asked for production mode was told they had.
   * <p>
   * The refusal itself is pinned as a unit on {@code ServerControlPlane.applySetting}, the writer both HTTP and gRPC
   * share, and on {@code GlobalConfiguration.coerceFromAdminCommand} beneath it. What THIS test adds is the
   * end-to-end mapping of that refusal to a 400 with the key named in the body, which is all the caller ever sees -
   * the same thing the boolean tests of this class pin for the strict boolean parse of #7124, and the reason
   * a 200 was the whole defect here.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #7296 a value outside a setting allow-list is refused instead of stored verbatim
  @Test
  void refusesAnUnlistedServerModeInsteadOfStoringItVerbatim() throws Exception {
    final ContextConfiguration configuration = getServer(0).getConfiguration();
    final GlobalConfiguration setting = GlobalConfiguration.SERVER_MODE;

    final HttpResponse<String> response = executeServerCommand("set server setting " + setting.getKey() + " prodction");

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains(setting.getKey());
    assertThat(configuration.hasValue(setting.getKey()))
        .as("a refused command must not leave the setting holding the value the typo would have produced")
        .isFalse();
  }

  // Regression: #7296 a value outside a setting allow-list is refused instead of stored verbatim
  @Test
  void stillAcceptsAListedValueAndNormalisesIt() throws Exception {
    final ContextConfiguration configuration = getServer(0).getConfiguration();
    final GlobalConfiguration setting = GlobalConfiguration.SERVER_MODE;
    try {
      assertThat(executeServerCommand("set server setting " + setting.getKey() + " PRODUCTION").statusCode()).isEqualTo(200);
      assertThat(configuration.getValueAsString(setting))
          .as("every reader compares the stored value with \"production\"")
          .isEqualTo("production");
    } finally {
      configuration.setValue(setting.getKey(), null);
    }
  }
}
