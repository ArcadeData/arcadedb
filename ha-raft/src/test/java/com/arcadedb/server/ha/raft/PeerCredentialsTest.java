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

import java.net.URI;
import java.net.http.HttpRequest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The credential pair every peer-to-peer dial presents (claude-review on PR #7854).
 * <p>
 * Issue #7837 was one dial spelling this out by hand and spelling it wrong, so the shape is worth pinning in
 * one place rather than once per dial: the two headers travel together or not at all, and a cluster with no
 * token sends neither rather than a principal nobody can verify.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PeerCredentialsTest {

  private static HttpRequest.Builder request() {
    return HttpRequest.newBuilder().uri(URI.create("http://arcadedb-1:2480/api/v1/cluster/capabilities")).GET();
  }

  /** The pair a peer authenticates: the token proves the hop, the forwarded user names the principal. */
  @Test
  void aTokenTravelsWithThePrincipalItAuthorizes() {
    final HttpRequest built = PeerCredentials.attach(request(), "s3cr3t").build();

    assertThat(built.headers().firstValue("X-ArcadeDB-Cluster-Token")).hasValue("s3cr3t");
    assertThat(built.headers().firstValue("X-ArcadeDB-Forwarded-User"))
        .hasValue(RaftHAServer.FORWARDED_ROOT_USER);
    assertThat(built.headers().firstValue("Authorization"))
        .as("a cluster token is not a credential that scheme can carry - this is issue #7837")
        .isEmpty();
  }

  /** A cluster with no token sends no credentials at all, rather than an unverifiable claim about who. */
  @Test
  void noTokenMeansNeitherHeader() {
    for (final String noToken : new String[] { null, "", "   " }) {
      final HttpRequest built = PeerCredentials.attach(request(), noToken).build();

      assertThat(built.headers().firstValue("X-ArcadeDB-Cluster-Token")).isEmpty();
      assertThat(built.headers().firstValue("X-ArcadeDB-Forwarded-User"))
          .as("a principal with no proof of the hop is worth nothing to the handler that reads it")
          .isEmpty();
    }
  }

  /** The one dial that relays the caller's own identity rather than acting for the cluster. */
  @Test
  void aRelayedIdentityTravelsUnderTheSamePair() {
    final HttpRequest built = PeerCredentials.attachAs(request(), "s3cr3t", "reader").build();

    assertThat(built.headers().firstValue("X-ArcadeDB-Cluster-Token")).hasValue("s3cr3t");
    assertThat(built.headers().firstValue("X-ArcadeDB-Forwarded-User")).hasValue("reader");
  }
}
