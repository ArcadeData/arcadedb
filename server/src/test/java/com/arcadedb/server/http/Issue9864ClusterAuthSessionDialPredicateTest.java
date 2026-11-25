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
 * Issue #9864: authenticating a cluster-issued session token asks the node that issued it - a synchronous HTTP call
 * bounded only by the peer connect timeout - when this node does not hold the session, and again whenever the lease
 * of the copy it holds is due. The HTTP layer has to know that BEFORE the call, to move the request off the Undertow
 * IO thread, and has to know it exactly, or every token-authenticated request pays a thread hand-off.
 * <p>
 * This pins {@link ClusterAuthSessionResolver#wouldDial(String)} against what {@code resolve} and {@code renew}
 * really do, case by case, on a scripted clock: it answers {@code true} exactly when the peer is then asked. It also
 * pins what the two do for a caller that may not dial. The thread the call is made on, through a running server, is
 * {@code Issue9864ClusterSessionTokenOffIoThreadTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9864ClusterAuthSessionDialPredicateTest {
  private static final String TOKEN_FROM_A = "AU-node-a-2af64e60-8455-423a-bc64-ed0e19729f04";

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
  void aTokenAnotherNodeIssuedAndThisNodeDoesNotHoldWouldDial() {
    assertThat(resolver.wouldDial(TOKEN_FROM_A)).isTrue();
    assertThat(peer.lookups()).as("asking whether it would dial must not dial").isEmpty();

    assertThat(resolver.resolve(TOKEN_FROM_A)).isNotNull();
    assertThat(peer.lookups()).as("and it did dial").hasSize(1);
  }

  @Test
  void aTokenThatNamesNoPeerWouldNotDial() {
    final String legacy = "AU-2af64e60-8455-423a-bc64-ed0e19729f04";
    final String ownExpired = "AU-node-b-2af64e60-8455-423a-bc64-ed0e19729f04";

    assertThat(resolver.wouldDial(legacy)).as("a legacy token names no issuer").isFalse();
    assertThat(resolver.wouldDial(ownExpired)).as("this node issued it and no longer holds it").isFalse();
    assertThat(resolver.wouldDial("not-a-token")).isFalse();

    assertThat(resolver.resolve(legacy)).isNull();
    assertThat(resolver.resolve(ownExpired)).isNull();
    assertThat(peer.lookups()).isEmpty();
  }

  @Test
  void nothingWouldDialOnAServerThatIsNotClustered() {
    final HttpAuthSession copy = resolver.resolve(TOKEN_FROM_A);
    fakeNow += sessions.getRemoteRenewalIntervalMs();
    peer.lookups().clear();

    server.setHA(null);

    assertThat(resolver.wouldDial(TOKEN_FROM_A)).as("a copy whose lease is due").isFalse();
    assertThat(resolver.wouldDial("AU-node-c-2af64e60-8455-423a-bc64-ed0e19729f04")).as("an unknown token").isFalse();
    assertThat(resolver.renew(copy)).isTrue();
    assertThat(resolver.resolve("AU-node-c-2af64e60-8455-423a-bc64-ed0e19729f04")).isNull();
    assertThat(peer.lookups()).isEmpty();
  }

  @Test
  void aTokenTheIssuerJustRefusedWouldNotDialAgain() {
    peer.refuses();
    assertThat(resolver.wouldDial(TOKEN_FROM_A)).isTrue();
    assertThat(resolver.resolve(TOKEN_FROM_A)).isNull();

    assertThat(resolver.wouldDial(TOKEN_FROM_A)).as("the refusal is remembered, so a repeat is answered here").isFalse();
    assertThat(resolver.resolve(TOKEN_FROM_A)).isNull();
    assertThat(peer.lookups()).hasSize(1);
  }

  @Test
  void aSessionThisNodeIssuedNeverWouldDial() {
    final HttpAuthSession local = sessions.createSession(alice);
    fakeNow += 10 * sessions.getRemoteRenewalIntervalMs();

    assertThat(resolver.wouldDial(local.token)).isFalse();
    assertThat(resolver.renew(local)).isTrue();
    assertThat(peer.lookups()).isEmpty();
  }

  @Test
  void aCopyWouldDialOnlyOnceItsLeaseIsDue() {
    final HttpAuthSession copy = resolver.resolve(TOKEN_FROM_A);
    peer.lookups().clear();

    fakeNow += sessions.getRemoteRenewalIntervalMs() - 1;
    assertThat(resolver.wouldDial(TOKEN_FROM_A)).as("inside its lease").isFalse();
    assertThat(resolver.renew(copy)).isTrue();
    assertThat(peer.lookups()).isEmpty();

    fakeNow += 1;
    assertThat(resolver.wouldDial(TOKEN_FROM_A)).as("the lease is due").isTrue();
    assertThat(peer.lookups()).as("asking whether it would dial must not dial").isEmpty();
    assertThat(resolver.renew(copy)).isTrue();
    assertThat(peer.lookups()).as("and it did dial").hasSize(1);

    assertThat(resolver.wouldDial(TOKEN_FROM_A)).as("renewed, so inside its lease again").isFalse();
  }

  /**
   * The request that slipped past the hand-off - its lease fell due between the check and the renewal - is served on
   * the lease it has, as it is when the lookups are saturated, and the renewal is left to the next request.
   */
  @Test
  void aLeaseDueOnAThreadThatMayNotDialIsServedAsItStandsAndStaysDue() {
    final HttpAuthSession copy = resolver.resolve(TOKEN_FROM_A);
    peer.lookups().clear();
    fakeNow += sessions.getRemoteRenewalIntervalMs();
    // The issuer no longer holds it: had the renewal run, the copy would have been dropped.
    peer.refuses();

    assertThat(resolver.renew(copy, false)).isTrue();
    assertThat(peer.lookups()).isEmpty();
    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isSameAs(copy);
    assertThat(resolver.wouldDial(TOKEN_FROM_A)).as("still due: the next request is handed off and renews it").isTrue();

    assertThat(resolver.renew(copy, true)).as("which is when the revocation lands").isFalse();
    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isNull();
  }

  /**
   * The same for a token this node does not hold: refused without asking, and NOT remembered as refused, so the
   * retry - which is handed off - is the one the issuer answers.
   */
  @Test
  void anUnknownTokenOnAThreadThatMayNotDialIsRefusedWithoutBeingRemembered() {
    assertThat(resolver.resolve(TOKEN_FROM_A, false)).isNull();
    assertThat(peer.lookups()).isEmpty();
    assertThat(resolver.wouldDial(TOKEN_FROM_A)).as("not cached as a refusal").isTrue();

    assertThat(resolver.resolve(TOKEN_FROM_A, true)).isNotNull();
    assertThat(peer.lookups()).hasSize(1);
  }
}
