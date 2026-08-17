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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.ResultSetTooLargeException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5719: the 413 refusal has to stay actionable in production mode, where {@code detail} - the field
 * carrying the full sentence - is concealed to avoid leaking engine internals (issue #5037). A caller that
 * cannot see WHICH setting refused it, or WHAT number to stay under, has been told nothing it can act on, so
 * the setting name travels in the {@code error} label and the ceiling in {@code exceptionArgs}, both of which
 * are emitted in every mode.
 */
class ResultSetTooLargeErrorBodyTest {
  private static final int MAX_ROWS = 1_000;

  private final TestHandler handler = new TestHandler(null);

  @Test
  void theRefusalNamesTheSettingAndTheCeilingInProductionModeToo() {
    final ResultSetTooLargeException refusal = AbstractServerHttpHandler.resultSetTooLarge(MAX_ROWS);

    final String body = handler.buildErrorBody(false,
        "Result set too large for a single response (" + GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getKey()
            + ")", refusal, String.valueOf(refusal.getMaxResultRows()), null);
    final JSONObject json = new JSONObject(body);

    assertThat(json.getString("error")).contains(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getKey());
    assertThat(json.getString("exception")).isEqualTo(ResultSetTooLargeException.class.getName());
    assertThat(json.getString("exceptionArgs")).isEqualTo(String.valueOf(MAX_ROWS));
    // The free-form chain is concealed in production, as it is for every other error.
    assertThat(json.has("detail")).isFalse();
  }

  @Test
  void theRefusalCarriesTheFullAdviceInDevelopmentMode() {
    final ResultSetTooLargeException refusal = AbstractServerHttpHandler.resultSetTooLarge(MAX_ROWS);

    final String body = handler.buildErrorBody(true, "Result set too large for a single response", refusal,
        String.valueOf(refusal.getMaxResultRows()), null);
    final JSONObject json = new JSONObject(body);

    final String detail = json.getString("detail");
    assertThat(detail).contains(String.valueOf(MAX_ROWS));
    assertThat(detail).contains(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getKey());
  }

  @Test
  void theCeilingIsCarriedOnTheExceptionItself() {
    // The number reaches the wire from the exception, not from a second read of the configuration, so the
    // value the client is told to stay under is the one that actually refused it.
    assertThat(AbstractServerHttpHandler.resultSetTooLarge(MAX_ROWS).getMaxResultRows()).isEqualTo(MAX_ROWS);
  }

  private static class TestHandler extends AbstractServerHttpHandler {
    TestHandler(final HttpServer httpServer) {
      super(httpServer);
    }

    @Override
    protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
        final JSONObject payload) {
      return null;
    }
  }
}
