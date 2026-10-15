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
package com.arcadedb.server;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.http.HttpRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The one builder every peer-to-peer dial starts its request from (code review on PR #7854, issue #9999).
 * <p>
 * Issue #7837 was one dial spelling the credential out by hand and spelling it wrong, and issue #9999 found the sites
 * the first two helpers never reached drifting in three ways. The shape is pinned here once, in both request forms the
 * dials use: the two headers travel together or not at all, a cluster with no token sends neither, and a plain URL on
 * an SSL cluster is said - once per JVM, whichever dial is first.
 */
class PeerRequestTest {

  private static final String PLAIN_URL     = "http://arcadedb-1:2480/api/v1/cluster/capabilities";
  private static final String ENCRYPTED_URL = "https://arcadedb-1:2490/api/v1/cluster/capabilities";

  @BeforeEach
  @AfterEach
  void rearmTheNotice() {
    PeerRequest.rearmForTests();
  }

  private static HttpRequest build(final String clusterToken) {
    return PeerRequest.newRequest(PLAIN_URL, false, PeerRequestTest.class, "testing", clusterToken).GET().build();
  }

  /** The pair a peer authenticates: the token proves the hop, the forwarded user names the principal. */
  @Test
  void aTokenTravelsWithThePrincipalItAuthorizes() {
    final HttpRequest built = build("s3cr3t");

    assertThat(built.uri().toString()).isEqualTo(PLAIN_URL);
    assertThat(built.headers().firstValue(PeerRequest.CLUSTER_TOKEN_HEADER)).hasValue("s3cr3t");
    assertThat(built.headers().firstValue(PeerRequest.FORWARDED_USER_HEADER)).hasValue(PeerRequest.FORWARDED_ROOT_USER);
    assertThat(built.headers().firstValue("Authorization"))
        .as("a cluster token is not a credential that scheme can carry - this is issue #7837")
        .isEmpty();
  }

  /** The header names are the ones AbstractServerHttpHandler reads: a constant that drifted would authenticate nothing. */
  @Test
  void theHeaderNamesAreTheOnesTheReceivingHandlerReads() {
    assertThat(PeerRequest.CLUSTER_TOKEN_HEADER).isEqualTo("X-ArcadeDB-Cluster-Token");
    assertThat(PeerRequest.FORWARDED_USER_HEADER).isEqualTo("X-ArcadeDB-Forwarded-User");
  }

  /** A cluster with no token sends no credentials at all, rather than an unverifiable claim about who. */
  @Test
  void noTokenMeansNeitherHeader() {
    for (final String noToken : new String[] { null, "", "   " }) {
      final HttpRequest built = build(noToken);

      assertThat(built.headers().firstValue(PeerRequest.CLUSTER_TOKEN_HEADER)).isEmpty();
      assertThat(built.headers().firstValue(PeerRequest.FORWARDED_USER_HEADER))
          .as("a principal with no proof of the hop is worth nothing to the handler that reads it")
          .isEmpty();
      assertThat(PeerRequest.hasToken(noToken)).isFalse();
    }
    assertThat(PeerRequest.hasToken("s3cr3t")).isTrue();
  }

  /** The dials that relay the caller's own identity rather than acting for the cluster: the follower's write forward,
   * its termination relay and the verify fan-out (issue #9999 found the overload for this with no production caller). */
  @Test
  void aRelayedIdentityTravelsUnderTheSamePair() {
    final HttpRequest built = PeerRequest.newRequest(PLAIN_URL, false, PeerRequestTest.class, "testing", "s3cr3t", "reader")
        .GET().build();

    assertThat(built.headers().firstValue(PeerRequest.CLUSTER_TOKEN_HEADER)).hasValue("s3cr3t");
    assertThat(built.headers().firstValue(PeerRequest.FORWARDED_USER_HEADER)).hasValue("reader");
  }

  /**
   * The {@link HttpURLConnection} form the snapshot download, the resync order and the verify fan-out use. The snapshot
   * download sent the token with no principal (issue #9999, drift 1): the pair is now the same in both forms.
   */
  @Test
  void theConnectionFormCarriesThePairOrNothing() throws Exception {
    final HttpURLConnection withToken = PeerRequest.openConnection(PLAIN_URL, false, PeerRequestTest.class, "testing",
        "s3cr3t", PeerRequest.FORWARDED_ROOT_USER);
    try {
      assertThat(withToken.getRequestProperty(PeerRequest.CLUSTER_TOKEN_HEADER)).isEqualTo("s3cr3t");
      assertThat(withToken.getRequestProperty(PeerRequest.FORWARDED_USER_HEADER)).isEqualTo(PeerRequest.FORWARDED_ROOT_USER);
    } finally {
      withToken.disconnect();
    }

    for (final String noToken : new String[] { null, "", "   " }) {
      final HttpURLConnection without = PeerRequest.openConnection(PLAIN_URL, false, PeerRequestTest.class, "testing",
          noToken, "reader");
      try {
        assertThat(without.getRequestProperty(PeerRequest.CLUSTER_TOKEN_HEADER)).isNull();
        assertThat(without.getRequestProperty(PeerRequest.FORWARDED_USER_HEADER)).isNull();
      } finally {
        without.disconnect();
      }
    }
  }

  /** A plain URL on an SSL cluster is the fallback the notice exists for, in the builder form. */
  @Test
  void aPlainRequestOnAnSslClusterSaysTheNotice() {
    PeerRequest.newRequest(PLAIN_URL, true, PeerRequestTest.class, "testing", "s3cr3t");

    assertThat(PeerRequest.saidForTests()).isTrue();
    assertThat(PeerRequest.sayOnce(PeerRequestTest.class, "testing"))
        .as("said once per JVM, whichever dial was first")
        .isFalse();
  }

  /** And in the connection form, which the dials the old notice never reached use (issue #9999). */
  @Test
  void aPlainConnectionOnAnSslClusterSaysTheNotice() throws Exception {
    PeerRequest.openConnection(PLAIN_URL, true, PeerRequestTest.class, "testing", "s3cr3t", "root").disconnect();

    assertThat(PeerRequest.saidForTests()).isTrue();
  }

  /**
   * A token with no principal is refused, never sent as half a pair nor patched up as root: a dial relaying a caller's
   * identity that lost it must fail rather than act with more authority (PR #10069 review). Without a token the user is
   * not sent at all, so its absence is not an error.
   */
  @Test
  void aTokenWithNoPrincipalIsRefused() throws Exception {
    for (final String noUser : new String[] { null, "", "   " }) {
      assertThatThrownBy(() -> PeerRequest.newRequest(PLAIN_URL, false, PeerRequestTest.class, "testing", "s3cr3t", noUser))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("principal");
      assertThatThrownBy(() -> PeerRequest.openConnection(PLAIN_URL, false, PeerRequestTest.class, "testing", "s3cr3t", noUser))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("principal");

      final HttpRequest built = PeerRequest.newRequest(PLAIN_URL, false, PeerRequestTest.class, "testing", null, noUser)
          .GET().build();
      assertThat(built.headers().firstValue(PeerRequest.FORWARDED_USER_HEADER)).isEmpty();
    }
  }

  /** The notice is about the token on the wire: a plain dial with no token has none to leak (PR #10069 review). */
  @Test
  void aPlainDialWithNoTokenSaysNothing() throws Exception {
    PeerRequest.newRequest(PLAIN_URL, true, PeerRequestTest.class, "testing", null);
    PeerRequest.openConnection(PLAIN_URL, true, PeerRequestTest.class, "testing", "  ", "root").disconnect();

    assertThat(PeerRequest.saidForTests()).isFalse();
  }

  /** Nothing to say when the dial is encrypted, or when the cluster never asked for SSL. */
  @Test
  void noFallbackNoNotice() throws Exception {
    PeerRequest.newRequest(ENCRYPTED_URL, true, PeerRequestTest.class, "testing", "s3cr3t");
    PeerRequest.newRequest(PLAIN_URL, false, PeerRequestTest.class, "testing", "s3cr3t");
    PeerRequest.openConnection(PLAIN_URL, false, PeerRequestTest.class, "testing", "s3cr3t", "root").disconnect();

    assertThat(PeerRequest.saidForTests()).isFalse();
  }

  // ------------------------------------------------------------------ the one other shape: a forward that relays the client's own credential

  /**
   * The forward of a Basic or API-token request (issue #7516): the token proves the hop and the client's own header is relayed
   * unchanged, so the leader validates what the client actually sent. The principal header is deliberately absent - asserting a
   * name would make the leader resolve the user by name and silently discard an API token's scopes.
   */
  @Test
  void aRelayedClientCredentialTravelsWithTheHopProofAndNoPrincipal() {
    final HttpRequest built = PeerRequest.newRelayRequest(PLAIN_URL, false, PeerRequestTest.class, "testing", "s3cr3t",
        "Basic cm9vdDpwYXNz").GET().build();

    assertThat(built.headers().firstValue(PeerRequest.CLUSTER_TOKEN_HEADER)).hasValue("s3cr3t");
    assertThat(built.headers().firstValue("Authorization")).hasValue("Basic cm9vdDpwYXNz");
    assertThat(built.headers().firstValue(PeerRequest.FORWARDED_USER_HEADER))
        .as("the client's own credential carries the identity, so no principal is asserted beside it").isEmpty();
  }

  /** Without a token there is no hop to prove: only what the client sent goes, and with nothing to relay, nothing. */
  @Test
  void aRelayWithoutATokenCarriesOnlyTheClientCredential() {
    for (final String noToken : new String[] { null, "", "   " }) {
      final HttpRequest relayed = PeerRequest.newRelayRequest(PLAIN_URL, false, PeerRequestTest.class, "testing", noToken,
          "Bearer abc").GET().build();
      assertThat(relayed.headers().firstValue(PeerRequest.CLUSTER_TOKEN_HEADER)).isEmpty();
      assertThat(relayed.headers().firstValue("Authorization")).hasValue("Bearer abc");

      final HttpRequest bare = PeerRequest.newRelayRequest(PLAIN_URL, false, PeerRequestTest.class, "testing", noToken, null)
          .GET().build();
      assertThat(bare.headers().map()).as("nothing to prove and nothing to relay").isEmpty();
    }
  }

  /**
   * The relay is a plain dial like any other: on an SSL cluster it says the notice, once, when it puts the token OR the client's own
   * credential on the wire in clear text. A Basic header relayed with no cluster token configured is just as exposed.
   */
  @Test
  void aPlainRelayOnAnSslClusterSaysTheNotice() {
    PeerRequest.newRelayRequest(ENCRYPTED_URL, true, PeerRequestTest.class, "testing", "s3cr3t", "Basic x");
    assertThat(PeerRequest.saidForTests()).as("encrypted: nothing in clear").isFalse();

    PeerRequest.newRelayRequest(PLAIN_URL, true, PeerRequestTest.class, "testing", null, null);
    assertThat(PeerRequest.saidForTests()).as("neither a token nor a credential: nothing to leak").isFalse();

    PeerRequest.newRelayRequest(PLAIN_URL, false, PeerRequestTest.class, "testing", "s3cr3t", "Basic x");
    assertThat(PeerRequest.saidForTests()).as("the cluster never asked for SSL").isFalse();

    PeerRequest.newRelayRequest(PLAIN_URL, true, PeerRequestTest.class, "testing", null, "Basic x");
    assertThat(PeerRequest.saidForTests()).as("no token, but the client's credential goes out in clear").isTrue();

    PeerRequest.rearmForTests();
    PeerRequest.newRelayRequest(PLAIN_URL, true, PeerRequestTest.class, "testing", "s3cr3t", null);
    assertThat(PeerRequest.saidForTests()).as("the token alone").isTrue();
  }
}
