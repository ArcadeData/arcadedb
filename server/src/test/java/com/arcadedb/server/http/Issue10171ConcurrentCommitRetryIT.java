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

import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #10171: a {@code /commit} retry that arrives while the first commit of the same session is still running waits for the
 * session lock, then finds the session ended. It answered 404 although the first commit succeeded; a retry arriving after the
 * first commit ended already answered 204 (issue #10164). Both now answer the same.
 * <p>
 * The first commit is played by the test holding the session lock and doing what {@code PostCommitHandler} does once the
 * commit succeeded - remember it, then end the session - only after the retry is known to be waiting for the lock, so the retry
 * cannot take the after-the-commit path by arriving late.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10171ConcurrentCommitRetryIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "graph";

  private String baseUrl(final int serverIndex) {
    return "http://127.0.0.1:" + getServerHttpPort(serverIndex) + "/api/v1";
  }

  private static String rootAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
  }

  private String begin(final int serverIndex) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl(serverIndex) + "/begin/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", rootAuth());
    connection.connect();
    try {
      readResponse(connection);
      assertThat(connection.getResponseCode()).isEqualTo(204);
      return connection.getHeaderField(HttpSessionManager.ARCADEDB_SESSION_ID).trim();
    } finally {
      connection.disconnect();
    }
  }

  private int commit(final int serverIndex, final String sessionId) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl(serverIndex) + "/commit/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", rootAuth());
    connection.setRequestProperty(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
    connection.connect();
    try {
      final int code = connection.getResponseCode();
      if (code >= 400)
        readError(connection);
      else
        readResponse(connection);
      return code;
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Holds the session lock as a running first request does, sends the retry, and once the retry waits for the lock ends the
   * session, remembering it as committed when {@code committed} is true.
   *
   * @return the status the retry was answered
   */
  private int retryWhileTheFirstRequestEndsTheSession(final int serverIndex, final boolean committed) throws Exception {
    final String sessionId = begin(serverIndex);
    final HttpSessionManager manager = getServer(serverIndex).getHttpServer().getSessionManager();
    final HttpSession session = manager.getSessionById(null, sessionId);
    assertThat(session).isNotNull();

    final CompletableFuture<Integer>[] retry = new CompletableFuture[1];
    session.execute(session.user, () -> {
      retry[0] = CompletableFuture.supplyAsync(() -> {
        try {
          return commit(serverIndex, sessionId);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
      await().atMost(10, TimeUnit.SECONDS).until(session::hasWaitingRequests);

      if (committed)
        manager.recordCommitted(session.id, session.user);
      manager.removeSession(session.id);
      return null;
    }, false, true);
    // What the first request leaves behind is no business of the retry
    session.cancel();

    return retry[0].get(30, TimeUnit.SECONDS);
  }

  @Test
  void aRetryWaitingOnTheFirstCommitAnswers204() throws Exception {
    testEachServer(serverIndex -> assertThat(retryWhileTheFirstRequestEndsTheSession(serverIndex, true)).isEqualTo(204));
  }

  @Test
  void aCommitWaitingOnARequestThatEndedTheSessionWithoutCommittingAnswers404() throws Exception {
    testEachServer(serverIndex -> assertThat(retryWhileTheFirstRequestEndsTheSession(serverIndex, false)).isEqualTo(404));
  }
}
