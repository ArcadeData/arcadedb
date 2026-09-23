/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The headers a remote shutdown presents (issue #7837).
 * <p>
 * The dial used to send {@code Authorization: Bearer &lt;clusterToken&gt;}, a credential no handler
 * authenticates: {@code AbstractServerHttpHandler} reads a {@code Bearer} value as an API token
 * ({@code at-} prefix) or as a session token ({@code AU-} prefix), and a cluster token is neither. It is
 * recognised only through {@code X-ArcadeDB-Cluster-Token}, which on its own proves a hop rather than a
 * principal - so the header has to travel with {@code X-ArcadeDB-Forwarded-User}, exactly as every sibling
 * dial in this module sends it.
 * <p>
 * Pure, because the method that sends this request ends in the target node's {@code System.exit}.
 * {@link Issue7837RemoteShutdownCredentialIT} drives the same headers against a real peer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7837ShutdownRequestCredentialTest {

  private static final String URL = "http://arcadedb-1:2480" + RaftHAPlugin.SERVER_COMMAND_ROUTE;

  /** The regression: the cluster token travels in the header that authenticates it, paired with a principal. */
  @Test
  void theClusterTokenIsSentInTheHeaderThatAuthenticatesIt() {
    final HttpRequest request = RaftHAPlugin.shutdownRequest(URL, "s3cr3t-cluster-token");

    assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token"))
        .as("the cluster token is recognised only through its own header")
        .hasValue("s3cr3t-cluster-token");
    assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User"))
        .as("the cluster token proves a hop, not a principal; the shutdown command is root-only")
        .hasValue(RaftHAServer.FORWARDED_ROOT_USER);
    assertThat(request.headers().firstValue("Authorization"))
        .as("no Authorization header: a cluster token is not a credential that scheme can carry")
        .isEmpty();
  }

  /** The route and the body are the ones {@code PostServerCommandHandler} answers. */
  @Test
  void theRequestIsAPostOfTheShutdownCommandToTheServerRoute() {
    final HttpRequest request = RaftHAPlugin.shutdownRequest(URL, "token");

    assertThat(request.method()).isEqualTo("POST");
    assertThat(request.uri().getPath()).isEqualTo(RaftHAPlugin.SERVER_COMMAND_ROUTE);
    assertThat(request.headers().firstValue("Content-Type")).hasValue("application/json");
    assertThat(request.bodyPublisher()).isPresent();
  }

  /**
   * A cluster with no token sends no credential at all rather than an unauthenticatable one. On a cluster that
   * does not require authentication that is the request that works; on one that does it is answered 401, which
   * {@code shutdownRemoteServer} now raises instead of logging.
   */
  @Test
  void aClusterWithoutATokenSendsNeitherHeader() {
    for (final String noToken : new String[] { null, "" }) {
      final HttpRequest request = RaftHAPlugin.shutdownRequest(URL, noToken);

      assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).isEmpty();
      assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).isEmpty();
      assertThat(request.headers().firstValue("Authorization")).isEmpty();
    }
  }
}
