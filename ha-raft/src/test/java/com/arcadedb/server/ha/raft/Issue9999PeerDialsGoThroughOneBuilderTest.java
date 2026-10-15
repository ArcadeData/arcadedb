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

import com.arcadedb.server.PeerRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9999: two helpers extracted to make the peer-to-peer dials spell their credential and their plain-HTTP fallback
 * notice in one place had 2 and 5 adopters out of 11, and the sites left alone had drifted - a snapshot download sending
 * the token with no principal, a probe spelling the root principal as a literal, and seven dials that fell back to plain
 * HTTP on an SSL cluster without a word, the follower's write forward among them.
 * <p>
 * The two helpers are now one builder, {@link PeerRequest}, and the guarantee is a scan of this module's main sources:
 * no request is built, and neither header is spelled, anywhere in them. A helper with two adopters reads the same as a
 * helper with eleven in a diff; this is the form of the fix the twelfth dial cannot quietly bypass.
 * <p>
 * The builder lives in the {@code server} module since issue #10068, which brought that module's own dials onto it too and
 * scans its sources with {@code Issue10068ServerPeerDialsGoThroughOneBuilderTest}. This module therefore holds no builder
 * source at all: every hit of the scan is a bypass.
 * <p>
 * It is a tripwire, not a proof: a text scan cannot see a header name assembled at runtime or a call split across lines.
 * It catches the shape every dial in this module has actually taken, which is the shape the next one will copy.
 */
class Issue9999PeerDialsGoThroughOneBuilderTest {

  private static final Path MAIN_SOURCES = Path.of("src", "main", "java");

  /** Every shape a peer dial in this module could take to bypass the builder. */
  private static final String[] BYPASSES = {
      "\"X-ArcadeDB-Cluster-Token\"",   // the token, spelled by hand
      "\"X-ArcadeDB-Forwarded-User\"",  // the principal, spelled by hand
      "HttpRequest.newBuilder(",        // a request built without the credential and the notice
      ".openConnection()",              // the same, in the HttpURLConnection form
      "toURL()",                        // any URL-level dial (openConnection, openStream) starts from one
  };

  @BeforeEach
  @AfterEach
  void rearmTheNotice() {
    PeerRequest.rearmForTests();
  }

  @Test
  void noPeerDialBuildsItsRequestOrSpellsItsCredentialOutsideTheBuilder() throws IOException {
    assertThat(MAIN_SOURCES).as("the scan runs from the ha-raft module directory").isDirectory();

    final List<String> offenders = new ArrayList<>();
    int scanned = 0;
    try (final Stream<Path> files = Files.walk(MAIN_SOURCES)) {
      for (final Path file : files.filter(f -> f.toString().endsWith(".java")).toList()) {
        scanned++;
        final List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (int i = 0; i < lines.size(); i++)
          for (final String bypass : BYPASSES)
            if (lines.get(i).contains(bypass))
              offenders.add(file.getFileName() + ":" + (i + 1) + "  " + lines.get(i).strip());
      }
    }

    assertThat(scanned).as("the scan reached this module's sources, so an empty offender list means something")
        .isGreaterThan(50);
    assertThat(offenders)
        .as("every peer-to-peer dial starts its request from PeerRequest.newRequest/openConnection, which attaches the "
            + "cluster token and forwarded user together and says the plain-HTTP fallback notice (issue #9999)")
        .isEmpty();
  }

  // ------------------------------------------------------------------ the drifts the issue found, through their dials

  /** Drift 2: the bootstrap-state probe spelled the root principal as the literal "root". */
  @Test
  void theBootstrapProbeActsAsTheRootPrincipalEveryOtherDialNames() {
    final HttpRequest request = BootstrapElection.bootstrapStateRequestTo("http://arcadedb-1:2480/api/v1/cluster/bootstrap-state",
        false, "s3cr3t", 1234L);

    assertThat(request.headers().firstValue(PeerRequest.CLUSTER_TOKEN_HEADER)).hasValue("s3cr3t");
    assertThat(request.headers().firstValue(PeerRequest.FORWARDED_USER_HEADER)).hasValue(RaftHAServer.FORWARDED_ROOT_USER);
  }

  /** The probe used to send the principal even with no token: a claim with no proof, which the receiver ignores. */
  @Test
  void theBootstrapProbeSendsNoPrincipalWithoutAToken() {
    final HttpRequest request = BootstrapElection.bootstrapStateRequestTo("http://arcadedb-1:2480/api/v1/cluster/bootstrap-state",
        false, null, 1234L);

    assertThat(request.headers().firstValue(PeerRequest.CLUSTER_TOKEN_HEADER)).isEmpty();
    assertThat(request.headers().firstValue(PeerRequest.FORWARDED_USER_HEADER)).isEmpty();
  }

  /**
   * The bootstrap-state probe is also how the unverified-closed-copy check and the leader-serves check ask, which never
   * said the fallback: they now say it as the request is built, not at a fan-out some callers skipped.
   */
  @Test
  void aPlainBootstrapProbeOnAnSslClusterSaysTheNotice() {
    BootstrapElection.bootstrapStateRequestTo("http://arcadedb-1:2480/api/v1/cluster/bootstrap-state", true, "s3cr3t", 1234L);

    assertThat(PeerRequest.saidForTests()).isTrue();
  }

  /** The remote shutdown, the first dial on the shared builder, keeps saying it. */
  @Test
  void aPlainShutdownOnAnSslClusterSaysTheNotice() {
    final HttpRequest request = RaftHAPlugin.shutdownRequest("http://arcadedb-1:2480" + RaftHAPlugin.SERVER_COMMAND_ROUTE,
        true, "s3cr3t");

    assertThat(PeerRequest.saidForTests()).isTrue();
    assertThat(request.headers().firstValue(PeerRequest.FORWARDED_USER_HEADER)).hasValue(RaftHAServer.FORWARDED_ROOT_USER);
  }

  /** An encrypted dial has nothing to say. */
  @Test
  void anEncryptedProbeSaysNothing() {
    BootstrapElection.bootstrapStateRequestTo("https://arcadedb-1:2490/api/v1/cluster/bootstrap-state", true, "s3cr3t", 1234L);

    assertThat(PeerRequest.saidForTests()).isFalse();
  }
}
