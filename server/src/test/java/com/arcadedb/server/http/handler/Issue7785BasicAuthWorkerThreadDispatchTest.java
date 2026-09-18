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

import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7785: {@code handleRequest} took its only dispatch decision from {@code mustExecuteOnWorkerThread}, which
 * defaults to {@code false}, and then authenticated. For a Basic credential not already in the salt cache that ran
 * {@code PBKDF2WithHmacSHA256} at {@code arcadedb.server.saltIterations} - 65536 by default, tens of milliseconds
 * of deliberately expensive, CPU-bound work - directly on an Undertow IO thread. An IO thread is a shared selector:
 * while it is inside the KDF it serves no other connection multiplexed onto it, health probes and established
 * clients included. {@code POST /api/v1/login} is the route that guarantees a cache miss, because turning a fresh
 * password into a token is its whole purpose.
 * <p>
 * The assertion is the DISPATCH, not a latency: what went wrong was which thread the hash ran on, and a wall-clock
 * bound would say nothing about that while being a coin flip on a loaded machine (the same reasoning as issue
 * #7722's test).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7785BasicAuthWorkerThreadDispatchTest {

  @Test
  void aRequestCarryingBasicCredentialsIsDispatchedOffTheIoThread() {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.getRequestHeaders().put(Headers.AUTHORIZATION, "Basic " + Base64.getEncoder()
        .encodeToString("root:a-password-nobody-has-presented-yet".getBytes(StandardCharsets.UTF_8)));

    assertThat(AbstractServerHttpHandler.authenticationNeedsWorkerThread(exchange))
        .as("a first-login Basic credential costs one full PBKDF2, which must not run on a shared selector")
        .isTrue();
  }

  /**
   * The counter-cases that keep the assertion above meaningful. If "everything dispatches" were the rule, the test
   * would pass without saying anything about the KDF - and every probe and every token-authenticated request would
   * pay a thread hand-off it has no reason to.
   */
  @Test
  void everythingCheapToAuthenticateStaysOnTheIoThread() {
    final HttpServerExchange noCredentials = new HttpServerExchange(null);
    assertThat(AbstractServerHttpHandler.authenticationNeedsWorkerThread(noCredentials))
        .as("/api/v1/ready and the other unauthenticated probes do no authentication work at all").isFalse();

    final HttpServerExchange apiToken = new HttpServerExchange(null);
    apiToken.getRequestHeaders().put(Headers.AUTHORIZATION, "Bearer at-0123456789abcdef");
    assertThat(AbstractServerHttpHandler.authenticationNeedsWorkerThread(apiToken))
        .as("an API token is a hash-map lookup plus one SHA-256").isFalse();

    final HttpServerExchange sessionToken = new HttpServerExchange(null);
    sessionToken.getRequestHeaders().put(Headers.AUTHORIZATION, "Bearer AU-0123456789abcdef");
    assertThat(AbstractServerHttpHandler.authenticationNeedsWorkerThread(sessionToken))
        .as("a session token is resolved from the live users map, not from a password").isFalse();
  }

  /**
   * The routes issue #7785 names: none of them overrides {@code mustExecuteOnWorkerThread}, so before the fix the
   * whole of their Basic authentication ran on the selector. They must STILL not override it - the dispatch is
   * meant to be driven by the credential, not by turning every control-plane route into a worker-thread route,
   * which would cost a hand-off on the token-authenticated traffic that makes up the bulk of them.
   */
  @Test
  void theAffectedRoutesStillDoNotDispatchOnTheirOwnAccount() {
    assertThat(new PostLoginHandler(null).mustExecuteOnWorkerThread())
        .as("the route that guarantees a salt-cache miss").isFalse();
    assertThat(new GetDatabasesHandler(null).mustExecuteOnWorkerThread()).isFalse();
    assertThat(new GetServerHandler(null).mustExecuteOnWorkerThread()).isFalse();
    assertThat(new GetExistsDatabaseHandler(null).mustExecuteOnWorkerThread()).isFalse();
  }
}
