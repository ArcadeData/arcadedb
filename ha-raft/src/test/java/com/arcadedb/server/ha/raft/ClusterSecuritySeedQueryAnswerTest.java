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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient.Version;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What {@link ClusterSecuritySeedQuery} makes of each answer the leader can give (code review on PR #7854).
 * <p>
 * The dial itself is covered end-to-end over a real socket by {@code Issue7835HaTlsRemainingPeerDialsIT} and
 * {@code Issue7833SecurityCatchUpIT}, but both drive the happy path. What those cannot reach is the set of
 * answers this client has to tell apart, because a healthy leader never produces them: the partial failure a
 * caller must report verbatim, the "you are not the leader" that must be re-resolved rather than reported, and
 * the malformed or unexpected answers that must become an {@code IOException} rather than a quiet empty list.
 * <p>
 * That last distinction is the one worth a test of its own: "nothing failed" and "I could not find out what
 * failed" are the same empty list to a careless reader, and the admission route turns the first into a 200.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ClusterSecuritySeedQueryAnswerTest {

  private static final String LEADER = "arcadedb-1:2480";

  // ------------------------------------------------------------------ the answers a leader can give

  /** Everything committed: the caller reports no failures, and the admission route answers 200. */
  @Test
  void aSeedThatCommittedEverythingReportsNoFailures() throws Exception {
    assertThat(ClusterSecuritySeedQuery.parse(answer(200,
        "{\"upToDate\":false,\"seeded\":true,\"failedSeeds\":[]}"), LEADER))
        .isEmpty();
  }

  /** The caller already held every document: nothing was submitted, and nothing failed. */
  @Test
  void anUpToDateAnswerReportsNoFailures() throws Exception {
    assertThat(ClusterSecuritySeedQuery.parse(answer(200,
        "{\"upToDate\":true,\"seeded\":false,\"failedSeeds\":[]}"), LEADER))
        .isEmpty();
  }

  /**
   * The partial failure, which is the one an operator acts on: issue #7521 made it a 503 naming the documents,
   * and the names have to survive the trip back rather than becoming a generic transport error.
   */
  @Test
  void aPartialFailureIsReportedWithTheDocumentsThatDidNotCommit() throws Exception {
    assertThat(ClusterSecuritySeedQuery.parse(answer(503,
        "{\"upToDate\":false,\"seeded\":true,\"failedSeeds\":[\"groups\",\"API tokens\"]}"), LEADER))
        .containsExactly("groups", "API tokens");
  }

  /**
   * A 503 that names no documents is NOT a clean seed. It is the leader saying it could not find out, and
   * reporting it as an empty failure list would tell an admitting node that everything committed.
   */
  @Test
  void aSeedThatCouldNotBeCompletedIsRaisedRatherThanReportedAsNothingFailed() {
    assertThatThrownBy(() -> ClusterSecuritySeedQuery.parse(answer(503,
        "{\"seeded\":false,\"error\":\"this node is stopping\"}"), LEADER))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("this node is stopping");
  }

  /** An answer that is not JSON at all - a proxy's error page, say - names the leader it came from. */
  @Test
  void anAnswerThatIsNotJsonIsRaisedAndNamesTheLeader() {
    assertThatThrownBy(() -> ClusterSecuritySeedQuery.parse(answer(502, "<html>Bad Gateway</html>"), LEADER))
        .isInstanceOf(IOException.class)
        .hasMessageContaining(LEADER);
  }

  /** And any other status the route never produces is an error rather than a silent success. */
  @Test
  void anUnexpectedStatusIsRaised() {
    assertThatThrownBy(() -> ClusterSecuritySeedQuery.parse(answer(404, "{\"error\":\"no such route\"}"), LEADER))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("404");
  }

  // ------------------------------------------------------------------ the deadline the request is given

  /**
   * The request's deadline is derived from the seed's own retry budget rather than fixed, so a deployment that
   * widens one widens the other: a caller that timed out inside the budget would report a failure the cluster
   * had not had.
   */
  @Test
  void theRequestDeadlineFollowsTheSeedRetryBudget() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SECURITY_SEED_RETRY_TIMEOUT, 60_000L);

    assertThat(ClusterSecuritySeedQuery.reportTimeoutMs(configuration))
        .as("the deadline must outlast the retrying it is waiting for")
        .isGreaterThan(60_000L);
  }

  // ------------------------------------------------------------------ fixtures

  /** The smallest {@link HttpResponse} {@code parse} reads: a status and a body. */
  private static HttpResponse<String> answer(final int status, final String body) {
    return new HttpResponse<>() {
      @Override
      public int statusCode() {
        return status;
      }

      @Override
      public HttpRequest request() {
        return null;
      }

      @Override
      public Optional<HttpResponse<String>> previousResponse() {
        return Optional.empty();
      }

      @Override
      public HttpHeaders headers() {
        return HttpHeaders.of(Map.of(), (a, b) -> true);
      }

      @Override
      public String body() {
        return body;
      }

      @Override
      public Optional<SSLSession> sslSession() {
        return Optional.empty();
      }

      @Override
      public URI uri() {
        return URI.create("http://" + LEADER + PostSecuritySeedHandler.ROUTE);
      }

      @Override
      public Version version() {
        return Version.HTTP_1_1;
      }
    };
  }
}
