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

import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ClusterAuthSessionResolver}: what a node does with a token it does not hold, how a
 * copy's lease is renewed, and how a logout reaches the other nodes (issue #7424). The peer is a scripted
 * {@link HAServerPlugin}; the HTTP leg is covered by {@code PostClusterAuthSessionHandlerTest} and the
 * three-node run by {@code ClusterAuthSessionTokenIT} in ha-raft.
 */
class ClusterAuthSessionResolverTest {
  private static final String TOKEN_FROM_A = "AU-node-a-2af64e60-8455-423a-bc64-ed0e19729f04";

  private volatile long          fakeNow;
  private HttpAuthSessionManager sessions;
  private ScriptedPeer           peer;
  private ArcadeDBServer         server;
  private ServerSecurityUser     alice;

  /** A peer whose answers the test scripts: a session, "unknown" (null), or "unreachable" (throws). */
  private static final class ScriptedPeer {
    final AtomicReference<Object> answer  = new AtomicReference<>();
    final List<String>            lookups = new ArrayList<>();
    final List<String>            revoked = new ArrayList<>();

    HAServerPlugin asPlugin() throws IOException {
      final HAServerPlugin plugin = mock(HAServerPlugin.class);
      // doAnswer, not when(): when() would run the interface's default method for real while stubbing.
      doAnswer(invocation -> {
        lookups.add(invocation.getArgument(0) + ":" + invocation.getArgument(1));
        final Object a = answer.get();
        if (a instanceof IOException e)
          throw e;
        return a;
      }).when(plugin).lookupAuthSession(anyString(), anyString());
      doAnswer(invocation -> {
        revoked.add(invocation.getArgument(0));
        return null;
      }).when(plugin).revokeAuthSession(anyString());
      return plugin;
    }
  }

  @BeforeEach
  void setUp() throws IOException {
    fakeNow = 1_000_000L;
    sessions = new HttpAuthSessionManager(30_000L, 0L, 0, 0, "node-b", () -> fakeNow);
    peer = new ScriptedPeer();
    alice = mock(ServerSecurityUser.class);
    when(alice.getName()).thenReturn("alice");
    when(alice.getAuthorizedDatabases()).thenReturn(Set.of());
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.getUser(anyString())).thenReturn(null);
    when(security.getUser("alice")).thenReturn(alice);
    // Built before the stubbing below opens: a mock created inside thenReturn(...) is a nested stubbing.
    final HAServerPlugin plugin = peer.asPlugin();
    server = mock(ArcadeDBServer.class);
    when(server.getHA()).thenReturn(plugin);
    when(server.getSecurity()).thenReturn(security);
  }

  @AfterEach
  void tearDown() {
    sessions.close();
  }

  private ClusterAuthSessionResolver resolver() {
    return new ClusterAuthSessionResolver(server, sessions);
  }

  @Test
  void tokenVouchedForByItsIssuerBecomesALocalCopy() {
    peer.answer.set(new HAServerPlugin.PeerAuthSession("alice", 900_000L));

    final HttpAuthSession copy = resolver().resolve(TOKEN_FROM_A);

    assertThat(copy).isNotNull();
    assertThat(copy.isRemote()).isTrue();
    assertThat(copy.getIssuer()).isEqualTo("node-a");
    assertThat(copy.getUser()).isSameAs(alice);
    assertThat(copy.getCreatedAt()).isEqualTo(900_000L);
    assertThat(peer.lookups).containsExactly("node-a:" + TOKEN_FROM_A);
    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).as("the next request hits locally").isSameAs(copy);
  }

  @Test
  void tokenNamingNoIssuerOrThisNodeOrNoClusterIsRefusedWithoutAsking() {
    peer.answer.set(new HAServerPlugin.PeerAuthSession("alice", 0L));
    final ClusterAuthSessionResolver resolver = resolver();

    assertThat(resolver.resolve("AU-2af64e60-8455-423a-bc64-ed0e19729f04")).as("legacy token").isNull();
    assertThat(resolver.resolve("AU-node-b-2af64e60-8455-423a-bc64-ed0e19729f04")).as("own token, so expired").isNull();
    assertThat(peer.lookups).isEmpty();

    when(server.getHA()).thenReturn(null);
    assertThat(resolver.resolve(TOKEN_FROM_A)).as("not clustered").isNull();
    assertThat(peer.lookups).isEmpty();
  }

  @Test
  void issuerThatDoesNotKnowTheTokenIsRememberedSoARepeatCostsNoCall() {
    peer.answer.set(null);
    final ClusterAuthSessionResolver resolver = resolver();

    assertThat(resolver.resolve(TOKEN_FROM_A)).isNull();
    assertThat(resolver.resolve(TOKEN_FROM_A)).isNull();
    assertThat(peer.lookups).as("second miss served from the refusal cache").hasSize(1);
  }

  @Test
  void unreachableIssuerRefusesTheTokenForNow() {
    peer.answer.set(new IOException("connection refused"));

    assertThat(resolver().resolve(TOKEN_FROM_A)).isNull();
    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isNull();
  }

  @Test
  void principalTheIssuerNamesMustStillExistHere() {
    peer.answer.set(new HAServerPlugin.PeerAuthSession("dropped-user", 0L));

    assertThat(resolver().resolve(TOKEN_FROM_A)).isNull();
  }

  @Test
  void copyIsServedWithoutAskingUntilTheRenewalIntervalPasses() {
    peer.answer.set(new HAServerPlugin.PeerAuthSession("alice", 0L));
    final ClusterAuthSessionResolver resolver = resolver();
    final HttpAuthSession copy = resolver.resolve(TOKEN_FROM_A);
    peer.lookups.clear();

    fakeNow += sessions.getRemoteRenewalIntervalMs() - 1;
    assertThat(resolver.renew(copy)).isTrue();
    assertThat(peer.lookups).isEmpty();

    fakeNow += 1;
    assertThat(resolver.renew(copy)).isTrue();
    assertThat(peer.lookups).as("lease renewed with the issuer").hasSize(1);
    assertThat(copy.elapsedFromConfirmation()).isZero();
  }

  @Test
  void localSessionNeverAsksAnyone() {
    final HttpAuthSession local = sessions.createSession(alice);
    fakeNow += 10 * sessions.getRemoteRenewalIntervalMs();

    assertThat(resolver().renew(local)).isTrue();
    assertThat(peer.lookups).isEmpty();
  }

  @Test
  void copyIsDroppedWhenTheIssuerNoLongerHoldsTheSession() {
    peer.answer.set(new HAServerPlugin.PeerAuthSession("alice", 0L));
    final ClusterAuthSessionResolver resolver = resolver();
    final HttpAuthSession copy = resolver.resolve(TOKEN_FROM_A);

    peer.answer.set(null);
    fakeNow += sessions.getRemoteRenewalIntervalMs();
    assertThat(resolver.renew(copy)).isFalse();
    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isNull();
  }

  @Test
  void copyOutlivesAnUnreachableIssuerForOneIdleTimeoutOnly() {
    peer.answer.set(new HAServerPlugin.PeerAuthSession("alice", 0L));
    final ClusterAuthSessionResolver resolver = resolver();
    final HttpAuthSession copy = resolver.resolve(TOKEN_FROM_A);

    peer.answer.set(new IOException("connection refused"));
    fakeNow += sessions.getRemoteRenewalIntervalMs();
    assertThat(resolver.renew(copy)).as("a blip does not log the user out").isTrue();
    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isSameAs(copy);

    fakeNow += sessions.getSessionTimeoutInMs();
    assertThat(resolver.renew(copy)).as("unconfirmed for a whole idle timeout").isFalse();
    assertThat(sessions.getSessionByToken(TOKEN_FROM_A)).isNull();
  }

  @Test
  void logoutFansOutOnlyForClusterAwareTokens() {
    final ClusterAuthSessionResolver resolver = resolver();

    resolver.revoke(TOKEN_FROM_A);
    resolver.revoke("AU-2af64e60-8455-423a-bc64-ed0e19729f04");

    assertThat(peer.revoked).containsExactly(TOKEN_FROM_A);
  }
}
