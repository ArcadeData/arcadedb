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
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7372: the transport precondition {@code POST /server/api-tokens} applies to the one response on
 * the HTTP API that carries material the server can never reissue.
 * <p>
 * The decision is asserted here rather than end to end because the case that matters - a cleartext request
 * from a peer that is <em>not</em> on this machine - cannot be produced from a test that talks to
 * 127.0.0.1, and binding a real routable address in CI is not something a test can rely on.
 * {@code Issue7372RemoteServerSecurityControlPlaneIT} covers the loopback half against a live server.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7372ApiTokenTransportGateTest {

  @Test
  void httpsIsFitWhateverThePeer() {
    assertThat(PostApiTokenHandler.isTransportSafeForSecrets("https", remote())).isTrue();
    assertThat(PostApiTokenHandler.isTransportSafeForSecrets("HTTPS", remote())).isTrue();
    assertThat(PostApiTokenHandler.isTransportSafeForSecrets("https", loopback())).isTrue();
  }

  @Test
  void cleartextFromLoopbackIsFit() {
    assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", loopback())).isTrue();
    assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", new InetSocketAddress("127.9.9.9", 51234)))
        .isTrue();
  }

  /**
   * The case the gate exists for: a token written back in the clear to something that is not on this
   * machine.
   */
  @Test
  void cleartextToARemotePeerIsNotFit() {
    assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", remote())).isFalse();
  }

  /**
   * Fails closed on an address it cannot read. An unresolved address is not a peer known to be local, and
   * a missing one says nothing at all - answering "fit" for either is the wrong direction to guess in.
   */
  @Test
  void anUnreadableAddressIsNotFit() {
    assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", null)).isFalse();
    assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http",
        InetSocketAddress.createUnresolved("127.0.0.1", 51234))).isFalse();
    assertThat(PostApiTokenHandler.isTransportSafeForSecrets(null, remote())).isFalse();
  }

  /**
   * The setting that decides whether an unfit transport is refused or merely logged. It ships off, so an
   * upgrade does not start refusing the mints Studio's own token UI makes over plain HTTP; the assertion
   * is here so flipping the default is a deliberate edit with a test to update, not a silent one.
   */
  @Test
  void theRefusalIsOffByDefault() {
    assertThat(GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getDefValue()).isEqualTo(Boolean.FALSE);
    assertThat(GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getKey())
        .isEqualTo("arcadedb.server.apiTokenRequireSecureTransport");
  }

  /**
   * With the setting on, an unprotected transport is refused - and with a status that says the connection
   * is the problem rather than the caller, since the identical request over TLS succeeds.
   */
  @Test
  void withTheSettingOnAnUnprotectedMintIsRefusedWith412() {
    final ExecutionResponse refusal = PostApiTokenHandler.checkTransport("http", remote(), true);

    assertThat(refusal).isNotNull();
    assertThat(refusal.getCode()).isEqualTo(412);
    assertThat(new JSONObject(refusal.getResponse()).getString("error"))
        .contains("HTTPS")
        .contains(GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getKey());
  }

  /**
   * With the setting on, a fit transport is still let through: the gate refuses connections, not callers.
   */
  @Test
  void withTheSettingOnAFitTransportProceeds() {
    assertThat(PostApiTokenHandler.checkTransport("http", loopback(), true)).isNull();
    assertThat(PostApiTokenHandler.checkTransport("https", remote(), true)).isNull();
  }

  /**
   * With the setting off - the default - the same unprotected mint proceeds. It is logged at WARNING, which
   * is deliberately not asserted here: what this pins is that the default does not refuse, because that is
   * the compatibility promise the default exists to keep.
   */
  @Test
  void withTheSettingOffAnUnprotectedMintProceeds() {
    assertThat(PostApiTokenHandler.checkTransport("http", remote(), false)).isNull();
    assertThat(PostApiTokenHandler.checkTransport("http", null, false)).isNull();
  }

  private static InetSocketAddress loopback() {
    return new InetSocketAddress("127.0.0.1", 51234);
  }

  private static InetSocketAddress remote() {
    return new InetSocketAddress("198.51.100.7", 51234);
  }
}
