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
package com.arcadedb.server.http;

import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.FakeServerSecurity;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a node does with the answer of the node that issued a token when its own sessions changed while the question was
 * on its way: the answer was given before the change, so it must not be installed as it stands. The change is scripted
 * to land inside the call to the issuer, on the calling thread, so every case is deterministic.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ClusterAuthSessionChangeDuringLookupTest {
  private static final String TOKEN_FROM_A = "AU-node-a-2af64e60-8455-423a-bc64-ed0e19729f04";
  private static final String OTHER_TOKEN  = "AU-node-a-9f1c0d7e-3b52-4a86-8d1e-5c7a2b9e0f31";

  private volatile long              fakeNow;
  private HttpAuthSessionManager     sessions;
  private ScriptedAuthSessionPeer    peer;
  private FakeArcadeDBServer         server;
  private ServerSecurityUser         alice;
  private ClusterAuthSessionResolver resolver;

  @BeforeEach
  void setUp() {
    fakeNow = 1_000_000L;
    sessions = new HttpAuthSessionManager(30_000L, 0L, 0, 0, "node-b", () -> fakeNow);
    peer = new ScriptedAuthSessionPeer().vouchesFor("alice");
    alice = TestServerHelper.securityUser("alice");
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("getUser", args -> "alice".equals(args[0]) ? alice : null);
    server = FakeArcadeDBServer.create().security(security);
    server.setHA(peer);
    resolver = new ClusterAuthSessionResolver(server, sessions);
  }

  @AfterEach
  void tearDown() {
    sessions.close();
  }

  @Test
  void sessionsOfThePrincipalRemovedWhileTheIssuerIsAskedLeaveNoCopy() {
    // The issuer answers "yes" and only then learns of the change, as it applies it after this node does.
    peer.whenAsked(() -> {
      sessions.removeSessionsForUser("alice");
      peer.refuses();
    });

    assertThat(resolver.resolve(TOKEN_FROM_A)).isNull();

    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isNull();
    assertThat(peer.lookups()).as("the issuer is asked again once, and the second answer decides").hasSize(2);
  }

  @Test
  void tokenRemovedWhileTheIssuerIsAskedLeavesNoCopy() {
    peer.whenAsked(() -> {
      sessions.removeSession(TOKEN_FROM_A);
      peer.refuses();
    });

    assertThat(resolver.resolve(TOKEN_FROM_A)).isNull();

    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isNull();
    assertThat(peer.lookups()).hasSize(2);
  }

  @Test
  void issuerThatStillVouchesOnTheSecondAnswerGetsItsCopyInstalled() {
    peer.whenAsked(() -> {
      peer.whenAsked(null);
      sessions.removeSessionsForUser("alice");
    });

    final HttpAuthSession copy = resolver.resolve(TOKEN_FROM_A);

    assertThat(copy).isNotNull();
    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isSameAs(copy);
    assertThat(peer.lookups()).hasSize(2);
  }

  @Test
  void removalOfAnotherTokenWhileTheIssuerIsAskedDoesNotDelayTheCopy() {
    peer.whenAsked(() -> sessions.removeSession(OTHER_TOKEN));

    final HttpAuthSession copy = resolver.resolve(TOKEN_FROM_A);

    assertThat(copy).isNotNull();
    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isSameAs(copy);
    assertThat(peer.lookups()).hasSize(1);
  }

  @Test
  void changesDuringBothAnswersRefuseTheTokenWithoutRememberingItAsUnknown() {
    peer.whenAsked(() -> sessions.removeSessionsForUser("alice"));

    assertThat(resolver.resolve(TOKEN_FROM_A)).isNull();

    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isNull();
    assertThat(peer.lookups()).hasSize(2);
    assertThat(resolver.wouldDial(TOKEN_FROM_A)).as("not remembered as a token the issuer does not know").isTrue();

    peer.whenAsked(null);
    assertThat(resolver.resolve(TOKEN_FROM_A)).as("the next request is served").isNotNull();
  }

  @Test
  void leaseOfACopyRemovedWhileTheIssuerIsAskedIsNotConfirmed() {
    final HttpAuthSession copy = resolver.resolve(TOKEN_FROM_A);
    assertThat(copy).isNotNull();
    fakeNow += sessions.getRemoteRenewalIntervalMs();
    peer.whenAsked(() -> sessions.removeSessionsForUser("alice"));

    assertThat(resolver.renew(copy)).isFalse();

    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isNull();
    assertThat(copy.elapsedFromConfirmation()).as("the removed copy was not confirmed").isEqualTo(sessions.getRemoteRenewalIntervalMs());
  }

  @Test
  void leaseOfACopyThatStaysIsConfirmedAsBefore() {
    final HttpAuthSession copy = resolver.resolve(TOKEN_FROM_A);
    fakeNow += sessions.getRemoteRenewalIntervalMs();
    peer.whenAsked(() -> sessions.removeSession(OTHER_TOKEN));

    assertThat(resolver.renew(copy)).isTrue();

    assertThat(copy.elapsedFromConfirmation()).isZero();
    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isSameAs(copy);
  }
}
