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
 * The idle limit of an authentication session is honoured by every lookup of its token, not only by the periodic
 * cleanup: a session left unused for longer than the limit is refused as soon as it is looked up, whether or not the
 * cleanup has run yet, and a refused lookup does not extend it. The clock is scripted, and the limit is far longer than
 * the test so the cleanup timer of the manager never runs in the meantime.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class HttpAuthSessionIdleLimitTest {
  private static final long   IDLE_LIMIT_MS = 30_000L;
  private static final String TOKEN_FROM_A  = "AU-node-a-2af64e60-8455-423a-bc64-ed0e19729f04";

  private volatile long          fakeNow;
  private HttpAuthSessionManager sessions;
  private ServerSecurityUser     alice;

  @BeforeEach
  void setUp() {
    fakeNow = 1_000_000L;
    sessions = new HttpAuthSessionManager(IDLE_LIMIT_MS, 0L, 0, 0, "node-b", () -> fakeNow);
    alice = TestServerHelper.securityUser("alice");
  }

  @AfterEach
  void tearDown() {
    sessions.close();
  }

  @Test
  void aSessionIdleBeyondTheLimitIsRefusedBeforeTheCleanupRuns() {
    final HttpAuthSession session = sessions.createSession(alice);

    fakeNow += IDLE_LIMIT_MS + 1;

    assertThat(sessions.getSessionByToken(session.getToken())).isNull();
    assertThat(sessions.getSessionByToken(session.getToken())).as("and it stays refused").isNull();
  }

  @Test
  void aSessionIdleForExactlyTheLimitIsStillServed() {
    final HttpAuthSession session = sessions.createSession(alice);

    fakeNow += IDLE_LIMIT_MS;

    assertThat(sessions.getSessionByToken(session.getToken())).isSameAs(session);
  }

  @Test
  void aRefusedLookupDoesNotExtendTheSession() {
    final HttpAuthSession session = sessions.createSession(alice);
    final long lastUse = session.getLastUpdate();

    fakeNow += IDLE_LIMIT_MS + 1;
    assertThat(sessions.getSessionByToken(session.getToken())).isNull();

    assertThat(session.getLastUpdate()).as("the refused lookup is not a use").isEqualTo(lastUse);
    fakeNow += 1;
    assertThat(sessions.getSessionByToken(session.getToken())).isNull();
  }

  @Test
  void aSessionUsedWithinTheLimitKeepsItsLifeFromTheLastUse() {
    final HttpAuthSession session = sessions.createSession(alice);

    fakeNow += IDLE_LIMIT_MS - 1;
    assertThat(sessions.getSessionByToken(session.getToken())).isSameAs(session);
    fakeNow += IDLE_LIMIT_MS - 1;
    assertThat(sessions.getSessionByToken(session.getToken())).isSameAs(session);

    fakeNow += IDLE_LIMIT_MS + 1;
    assertThat(sessions.getSessionByToken(session.getToken())).isNull();
  }

  @Test
  void anIdleSessionLeavesTheCountsOfItsPrincipalWhenItIsRefused() {
    final HttpAuthSession session = sessions.createSession(alice);
    final HttpAuthSession other = sessions.createSession(TestServerHelper.securityUser("bob"));
    assertThat(sessions.getActiveSessionCount()).isEqualTo(2);

    fakeNow += IDLE_LIMIT_MS - 1;
    assertThat(sessions.getSessionByToken(other.getToken())).isSameAs(other);
    fakeNow += 2;

    assertThat(sessions.getSessionByToken(session.getToken())).isNull();
    assertThat(sessions.getActiveSessionCount()).isEqualTo(1);
    assertThat(sessions.getActiveSessionCount("alice")).isZero();
    assertThat(sessions.getActiveSessionCount("bob")).isEqualTo(1);
    assertThat(sessions.getSessionByToken(other.getToken())).as("another session is left alone").isSameAs(other);
  }

  @Test
  void anIdleCopyOfAnotherNodesSessionIsRefusedToo() {
    final HttpAuthSession copy = sessions.addRemoteSession(TOKEN_FROM_A, alice, fakeNow, "node-a");
    assertThat(copy).isNotNull();

    fakeNow += IDLE_LIMIT_MS + 1;

    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isNull();
    assertThat(sessions.getActiveSessionCount("alice")).isZero();
  }

  @Test
  void aSessionPastTheAbsoluteLimitIsDroppedWhenItIsRefused() {
    sessions.close();
    sessions = new HttpAuthSessionManager(IDLE_LIMIT_MS, 10_000L, 0, 0, "node-b", () -> fakeNow);
    final HttpAuthSession session = sessions.createSession(alice);

    fakeNow += 10_001;

    assertThat(sessions.getSessionByToken(session.getToken())).isNull();
    assertThat(sessions.getActiveSessionCount()).isZero();
    assertThat(sessions.getActiveSessionCount("alice")).isZero();
  }

  @Test
  void aPeekReadsTheSessionWithoutUsingOrDroppingIt() {
    final HttpAuthSession session = sessions.createSession(alice);
    final long lastUse = session.getLastUpdate();

    fakeNow += 1_000;
    assertThat(sessions.peekSessionByToken(session.getToken())).isSameAs(session);
    assertThat(session.getLastUpdate()).as("a peek is not a use").isEqualTo(lastUse);

    fakeNow += IDLE_LIMIT_MS;
    assertThat(sessions.peekSessionByToken(session.getToken())).as("past the limit it counts as not held").isNull();
    assertThat(sessions.getActiveSessionCount()).as("and it is left to a lookup or the cleanup to drop").isEqualTo(1);
    assertThat(sessions.peekSessionByToken("AU-node-b-9f1c0d7e-3b52-4a86-8d1e-5c7a2b9e0f31")).isNull();
  }

  @Test
  void aLookupOfATokenThatIsNotHeldChangesNothing() {
    final HttpAuthSession session = sessions.createSession(alice);

    assertThat(sessions.getSessionByToken("AU-node-b-9f1c0d7e-3b52-4a86-8d1e-5c7a2b9e0f31")).isNull();

    assertThat(sessions.getActiveSessionCount()).isEqualTo(1);
    assertThat(sessions.getSessionByToken(session.getToken())).isSameAs(session);
  }

  /**
   * An idle copy is not served on trust: the node that issued the token is asked again, and it answers for the session
   * as it stands there.
   */
  @Test
  void anIdleCopyIsConfirmedWithItsIssuerAgainInsteadOfBeingServed() {
    final ScriptedAuthSessionPeer peer = new ScriptedAuthSessionPeer().vouchesFor("alice");
    final ClusterAuthSessionResolver resolver = resolverFor(peer);

    final HttpAuthSession first = resolver.resolve(TOKEN_FROM_A);
    assertThat(first).isNotNull();
    assertThat(peer.lookups()).hasSize(1);

    fakeNow += IDLE_LIMIT_MS + 1;
    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).as("not served from the local copy").isNull();

    final HttpAuthSession second = resolver.resolve(TOKEN_FROM_A);
    assertThat(second).isNotNull().isNotSameAs(first);
    assertThat(peer.lookups()).as("the issuer was asked again").hasSize(2);
    assertThat(sessions.getActiveSessionCount("alice")).isEqualTo(1);

    fakeNow += IDLE_LIMIT_MS + 1;
    peer.refuses();
    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isNull();
    assertThat(resolver.resolve(TOKEN_FROM_A)).as("an issuer that no longer holds it refuses it").isNull();
  }

  /**
   * Asking whether a token would be sent to its issuer is a question, not a use: it must not keep the session alive.
   */
  @Test
  void askingWhetherATokenWouldBeSentToItsIssuerDoesNotExtendTheSession() {
    final ScriptedAuthSessionPeer peer = new ScriptedAuthSessionPeer().vouchesFor("alice");
    final ClusterAuthSessionResolver resolver = resolverFor(peer);
    final HttpAuthSession copy = resolver.resolve(TOKEN_FROM_A);
    final long lastUse = copy.getLastUpdate();

    fakeNow += IDLE_LIMIT_MS - 1;
    assertThat(resolver.wouldDial(TOKEN_FROM_A)).as("the lease is due").isTrue();
    assertThat(copy.getLastUpdate()).as("the question is not a use").isEqualTo(lastUse);

    fakeNow += 2;
    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).as("so the copy idled out").isNull();
  }

  private ClusterAuthSessionResolver resolverFor(final ScriptedAuthSessionPeer peer) {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("getUser", args -> "alice".equals(args[0]) ? alice : null);
    final FakeArcadeDBServer server = FakeArcadeDBServer.create().security(security);
    server.setHA(peer);
    return new ClusterAuthSessionResolver(server, sessions);
  }
}
