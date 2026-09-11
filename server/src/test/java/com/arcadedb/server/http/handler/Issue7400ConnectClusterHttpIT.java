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
 * Issue #7400, the HTTP side of the pair. The gRPC {@code ConnectCluster} RPC added by that issue
 * delegates to {@code ServerControlPlane.connectCluster}, the same method {@code POST /api/v1/server}
 * has always called - so what the two transports answer for the same input is now a property worth
 * pinning from both ends rather than from one.
 * <p>
 * The verb still refuses: the current HA stack has never implemented a client-initiated join, and
 * whether to implement it or retire the verb is issue #7401. What must hold is that the refusal is
 * the <em>same</em> refusal, naming the address the caller asked for, on whichever transport the
 * caller used.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7400ConnectClusterHttpIT extends BaseGraphServerTest {
  private static final String PEER_ADDRESS = "localhost:2425";

  private final HttpClient client = HttpClient.newHttpClient();

  /**
   * The address survives {@code extractTarget} and reaches the shared implementation, which names it
   * in the refusal. This is the HTTP half of the gRPC IT's
   * {@code theAddressReachesTheSharedImplementationUnmodified}: a handler that dropped the argument
   * on either transport would otherwise be indistinguishable from one that passed it through, since
   * nothing else in the answer depends on it.
   */
  @Test
  void connectClusterIsRefusedAndNamesTheAddress() throws Exception {
    final HttpResponse<String> response = executeServerCommand("connect cluster " + PEER_ADDRESS);

    assertThat(response.body())
        .contains(PEER_ADDRESS)
        .contains("not supported by the current HA implementation");
  }

  /**
   * A bare {@code connect cluster} yields {@code ""} from {@code extractTarget} and is refused for
   * the same reason a filled one is - the shared method never reads its argument. This is what the
   * gRPC side deliberately mirrors by not adding an {@code INVALID_ARGUMENT} gate of its own.
   */
  @Test
  void connectClusterWithNoAddressIsRefusedTheSameWay() throws Exception {
    final HttpResponse<String> response = executeServerCommand("connect cluster");

    assertThat(response.body()).contains("not supported by the current HA implementation");
  }

  private HttpResponse<String> executeServerCommand(final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        // The bound port, not the 2480 default: HttpServer takes the first free port of the
        // configured range, so a hardcoded 2480 sends the request to whatever else already holds it.
        .uri(new URI("http://localhost:" + getServer(0).getHttpServer().getPort() + "/api/v1/server"))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    return client.send(request, BodyHandlers.ofString());
  }
}
