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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8363 on a real cluster and a real wire: a client dialling a follower directly - the case the readiness gate
 * of #7519 cannot reach - is answered with a retryable 503 while that follower's copy of the database is being
 * replaced from the leader's snapshot, and is served again the moment the replacement ends.
 * <p>
 * The replacement is registered the way {@code SnapshotInstaller.install} registers it, rather than driven by a real
 * download: what is under test is what the request path does during the download, and a download fast enough for a
 * test is over before any request could observe it. The node-wide {@code snapshotInstallInProgress} flag is left
 * alone, which is the point: before this fix nothing but that flag, raised only around the swap at the very end,
 * stood between a client and the copy being discarded.
 */
class Issue8363ReplacedDatabaseRefusesClientsIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void aClientDiallingTheFollowerDirectlyIsRefusedWhileItsCopyIsReplaced() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = leader == 0 ? 1 : 0;

    final String query = "/api/v1/query/" + getDatabaseName() + "/sql/select%20count(*)%20from%20V1";
    assertThat(get(follower, query).status).as("precondition: the follower serves the database").isEqualTo(200);

    final Path followerCopy = Path.of(getServer(follower).getDatabase(getDatabaseName()).getDatabasePath());
    SnapshotInstaller.markInstallInFlightForTesting(followerCopy);
    try {
      final Response refused = get(follower, query);
      assertThat(refused.status).as("a retryable refusal, not the copy being discarded").isEqualTo(503);
      assertThat(refused.body).contains("being replaced");
      assertThat(getServer(follower).isSnapshotInstallInProgress())
          .as("the node-wide swap flag played no part in the refusal").isFalse();

      assertThat(get(leader, query).status).as("the leader's own copy is not being replaced").isEqualTo(200);
    } finally {
      SnapshotInstaller.clearInstallInFlightForTesting(followerCopy);
    }

    assertThat(get(follower, query).status).as("served again once the replacement ends").isEqualTo(200);
  }

  private Response get(final int serverIndex, final String path) throws Exception {
    final int port = getServer(serverIndex).getHttpServer().getPort();
    final HttpURLConnection conn = (HttpURLConnection) new URI("http://localhost:" + port + path).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    try {
      final int status = conn.getResponseCode();
      final var stream = status < 400 ? conn.getInputStream() : conn.getErrorStream();
      return new Response(status, stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      conn.disconnect();
    }
  }

  private record Response(int status, String body) {
  }
}
