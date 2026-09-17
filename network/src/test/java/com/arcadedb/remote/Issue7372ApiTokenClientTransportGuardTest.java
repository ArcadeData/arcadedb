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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.network.HostUtil;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7372: {@link RemoteServer#createApiToken} is the one call in this client whose response carries
 * material the server cannot reissue and that authenticates its holder, so it refuses to be sent over a
 * connection that would put that material on the wire in the clear.
 * <p>
 * Every assertion here is about a decision taken before a socket is opened - which is why the host is one
 * that resolves to nothing: reaching the network at all would be the bug.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7372ApiTokenClientTransportGuardTest {

  /**
   * A host name that cannot resolve, so any request that slipped past the guard fails immediately instead
   * of waiting out the watchdog. {@code .invalid} is reserved by RFC 2606 for exactly this.
   */
  private static final String UNRESOLVABLE_HOST = "no-such-host.invalid";

  private OfflineRemoteServer server;

  /**
   * Skips the cluster-configuration round trip the constructor otherwise makes, exactly as
   * {@code RemoteServerTest} does.
   */
  static class OfflineRemoteServer extends RemoteServer {
    OfflineRemoteServer(final String serverAddress, final int port) {
      super(serverAddress, port, "root", "password", new ContextConfiguration());
    }

    @Override
    void requestClusterConfiguration() {
      // No-op: no server is listening, and none needs to be.
    }
  }

  @BeforeEach
  void setUp() {
    server = new OfflineRemoteServer("http://" + UNRESOLVABLE_HOST, 2480);
  }

  @AfterEach
  void tearDown() {
    server.close();
  }

  @Test
  void refusesToMintOverCleartextToARemoteHost() {
    assertThatThrownBy(() -> server.createApiToken("ci", "*", 0, new JSONObject()))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining(UNRESOLVABLE_HOST)
        .hasMessageContaining("setAllowInsecureApiTokenTransport");
  }

  /**
   * The refusal is the caller's to lift, and only explicitly. Asserted on the guard rather than on
   * {@code createApiToken} so the test does not then depend on what an unreachable server does.
   */
  @Test
  void theRefusalIsLiftedOnlyByTheExplicitOptIn() {
    final String url = "http://" + UNRESOLVABLE_HOST + ":2480/api/v1/server/api-tokens";

    assertThat(server.isAllowInsecureApiTokenTransport()).isFalse();
    assertThatThrownBy(() -> server.checkTransportCarriesSecrets(url)).isInstanceOf(SecurityException.class);

    server.setAllowInsecureApiTokenTransport(true);

    assertThat(server.isAllowInsecureApiTokenTransport()).isTrue();
    assertThatCode(() -> server.checkTransportCarriesSecrets(url)).doesNotThrowAnyException();
  }

  /**
   * What the guard objects to is cleartext, not distance: the same remote host over https passes.
   */
  @Test
  void httpsToARemoteHostPassesTheGuard() {
    assertThatCode(() -> server.checkTransportCarriesSecrets("https://198.51.100.7:2490/api/v1/server/api-tokens"))
        .doesNotThrowAnyException();
  }

  /**
   * Loopback passes too - the bytes never reach a network - and it is checked against the URL the request
   * would actually be sent to, not against the name the client was configured with: a STICKY pin or a
   * leader hand-off changes which host receives the token.
   */
  @Test
  void cleartextToLoopbackPassesTheGuard() {
    assertThatCode(() -> server.checkTransportCarriesSecrets("http://127.0.0.1:2480/api/v1/server/api-tokens"))
        .doesNotThrowAnyException();
    assertThatCode(() -> server.checkTransportCarriesSecrets("http://localhost:2480/api/v1/server/api-tokens"))
        .doesNotThrowAnyException();
  }

  /**
   * The loopback test the guard leans on lives in {@link HostUtil}, shared with {@code RemoteGrpcServer},
   * which asks the same question before attaching call credentials to a plaintext channel. Asserted from
   * here because this is the guard that consumes it.
   */
  @Test
  void loopbackIsRecognisedUnderEverySpelling() {
    assertThat(HostUtil.isLoopbackHost("localhost")).isTrue();
    assertThat(HostUtil.isLoopbackHost("LOCALHOST")).isTrue();
    assertThat(HostUtil.isLoopbackHost("127.0.0.1")).isTrue();
    assertThat(HostUtil.isLoopbackHost("127.1.2.3")).isTrue();
  }

  /**
   * Fails closed on everything it cannot read as a loopback address, an unresolvable name included: an
   * unresolvable host is not a host known to be local.
   */
  @Test
  void anythingNotKnownToBeLoopbackIsRefused() {
    assertThat(HostUtil.isLoopbackHost(null)).isFalse();
    assertThat(HostUtil.isLoopbackHost("")).isFalse();
    assertThat(HostUtil.isLoopbackHost("   ")).isFalse();
    assertThat(HostUtil.isLoopbackHost("198.51.100.7")).isFalse();
    assertThat(HostUtil.isLoopbackHost(UNRESOLVABLE_HOST)).isFalse();
  }

  /**
   * Exactly one method is gated. The others reach the network over the same cleartext connection to the
   * same remote host and fail as a transport error, which is what says the guard was not hoisted onto the
   * shared request helper - where it would have refused eight calls that carry no secret.
   */
  @Test
  void onlyTheMintIsGated() {
    assertThatThrownBy(server::listUsers).isInstanceOf(RemoteException.class);
    assertThatThrownBy(server::listGroups).isInstanceOf(RemoteException.class);
    assertThatThrownBy(server::listApiTokens).isInstanceOf(RemoteException.class);
    assertThatThrownBy(() -> server.deleteApiToken("abc")).isInstanceOf(RemoteException.class);
    assertThatThrownBy(() -> server.deleteGroup("*", "reader")).isInstanceOf(RemoteException.class);
    assertThatThrownBy(() -> server.saveGroup("*", "reader", new JSONObject())).isInstanceOf(RemoteException.class);
    assertThatThrownBy(() -> server.updateUserPassword("bob", "password1")).isInstanceOf(RemoteException.class);
  }

  /**
   * The arguments every one of these methods refuses locally, before a request is built. A blank name or
   * hash would otherwise reach the server as a 400 after a round trip.
   */
  @Test
  void blankIdentifiersAreRefusedWithoutAskingTheServer() {
    assertThatThrownBy(() -> server.createApiToken(" ", "*", 0, null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> server.deleteApiToken(" ")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> server.updateUser(" ", "password1", null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> server.saveGroup("", "reader", new JSONObject())).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> server.saveGroup("*", "", new JSONObject())).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> server.deleteGroup("", "reader")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> server.deleteGroup("*", "")).isInstanceOf(IllegalArgumentException.class);
  }
}
