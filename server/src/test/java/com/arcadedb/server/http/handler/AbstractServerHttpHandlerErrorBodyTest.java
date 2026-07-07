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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for issue #5037 item 1: production-mode HTTP error responses must not leak the free-form cause chain
 * ({@code detail}), which can carry file paths and engine internals. The bounded {@code exception} class name and
 * structured {@code exceptionArgs} are preserved in all modes because the remote driver and HA leader-exception
 * reconstruction depend on them. Development/test modes additionally keep the verbose {@code detail} for debugging.
 */
class AbstractServerHttpHandlerErrorBodyTest {

  private final TestHandler handler = new TestHandler(null);

  @Test
  void productionModeConcealsCauseChainButKeepsWireContract() {
    final IllegalArgumentException rootCause = new IllegalArgumentException("internal file path /var/lib/arcadedb/secret");
    final CommandExecutionException wrapped = new CommandExecutionException("Error executing internal command", rootCause);

    final String body = handler.buildErrorBody(false, "Cannot execute command", wrapped, "index|keys|#1:2", "req-1234");
    final JSONObject json = new JSONObject(body);

    // Generic message + correlation id.
    assertThat(json.getString("error")).isEqualTo("Cannot execute command");
    assertThat(json.getString("requestId")).isEqualTo("req-1234");

    // Wire contract preserved so the remote driver / HA can rebuild the typed exception and its structured args.
    assertThat(json.getString("exception")).isEqualTo(CommandExecutionException.class.getName());
    assertThat(json.getString("exceptionArgs")).isEqualTo("index|keys|#1:2");

    // Free-form cause chain concealed: no detail field, no leaked file path from the nested cause.
    assertThat(json.has("detail")).isFalse();
    assertThat(body).doesNotContain("secret");
    assertThat(body).doesNotContain("/var/lib/arcadedb");
  }

  @Test
  void developmentModeExposesExceptionDetails() {
    final IllegalArgumentException rootCause = new IllegalArgumentException("root cause detail");
    final CommandExecutionException wrapped = new CommandExecutionException("outer message", rootCause);

    final String body = handler.buildErrorBody(true, "Cannot execute command", wrapped, "index|keys|#1:2", "req-9999");
    final JSONObject json = new JSONObject(body);

    assertThat(json.getString("error")).isEqualTo("Cannot execute command");
    assertThat(json.getString("requestId")).isEqualTo("req-9999");
    assertThat(json.getString("exception")).isEqualTo(CommandExecutionException.class.getName());
    assertThat(json.getString("exceptionArgs")).isEqualTo("index|keys|#1:2");

    final String detail = json.getString("detail");
    assertThat(detail).contains("outer message");
    assertThat(detail).contains("->");
    assertThat(detail).contains("root cause detail");
  }

  @Test
  void productionModeWithNoExceptionStillReturnsMessage() {
    final String body = handler.buildErrorBody(false, "Record not found", null, null, null);
    final JSONObject json = new JSONObject(body);

    assertThat(json.getString("error")).isEqualTo("Record not found");
    assertThat(json.has("requestId")).isFalse();
    assertThat(json.has("exception")).isFalse();
    assertThat(json.has("detail")).isFalse();
  }

  @Test
  void buildDetailChainTerminatesOnDeepCycle() {
    // e -> a -> b -> a : the cycle closes back on an intermediate cause, not on the root or a direct
    // self-reference. The identity-based visited set must stop the walk instead of looping forever.
    final Throwable a = new RuntimeException("a-msg");
    final Throwable b = new RuntimeException("b-msg");
    final Throwable e = new RuntimeException("e-msg", a);
    a.initCause(b);
    b.initCause(a);

    final String chain = AbstractServerHttpHandler.buildDetailChain(e);

    assertThat(chain).isEqualTo("e-msg -> a-msg -> b-msg");
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
