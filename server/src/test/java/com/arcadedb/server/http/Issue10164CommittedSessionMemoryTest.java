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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #10164: the session manager remembers, for a bounded time and number, which sessions a /commit ended, so a
 * retried commit of a committed session still answers 204 while a commit naming any other unresolvable session answers
 * 404. These pin the bounds and the owner gate of that memory.
 */
class Issue10164CommittedSessionMemoryTest {
  private static final long TIMEOUT_MS = 600_000L;

  private HttpSessionManager sessionManager;

  private static ServerSecurityUser user(final String name) {
    return new ServerSecurityUser(null, new JSONObject().put("name", name));
  }

  @AfterEach
  void tearDown() {
    if (sessionManager != null)
      sessionManager.close();
  }

  @Test
  void rememberedOnlyForItsOwner() {
    sessionManager = new HttpSessionManager(TIMEOUT_MS);
    sessionManager.recordCommitted("AS-1", user("alice"));

    assertThat(sessionManager.wasCommittedBy(user("alice"), "AS-1")).isTrue();
    assertThat(sessionManager.wasCommittedBy(user("bob"), "AS-1")).isFalse();
    assertThat(sessionManager.wasCommittedBy(user("alice"), "AS-2")).isFalse();
    assertThat(sessionManager.wasCommittedBy(null, "AS-1")).isFalse();
  }

  @Test
  void forgottenAfterTheSessionTimeout() {
    sessionManager = new HttpSessionManager(TIMEOUT_MS);
    sessionManager.recordCommitted("AS-1", user("alice"));

    sessionManager.pruneCommittedSessions(System.currentTimeMillis() + TIMEOUT_MS + 1);

    assertThat(sessionManager.wasCommittedBy(user("alice"), "AS-1")).isFalse();
  }

  @Test
  void theIdleSweepPrunesExpiredCommits() {
    // A short timeout, so the manager's own timer runs the sweep
    sessionManager = new HttpSessionManager(50L);
    sessionManager.recordCommitted("AS-1", user("alice"));

    await().atMost(10, TimeUnit.SECONDS).until(() -> sessionManager.getRememberedCommits() == 0);
  }

  @Test
  void boundedInNumber() {
    sessionManager = new HttpSessionManager(TIMEOUT_MS);
    for (int i = 0; i <= HttpSessionManager.MAX_REMEMBERED_COMMITS; i++)
      sessionManager.recordCommitted("AS-" + i, user("alice"));

    assertThat(sessionManager.wasCommittedBy(user("alice"), "AS-0")).as("the oldest is evicted").isFalse();
    assertThat(sessionManager.wasCommittedBy(user("alice"), "AS-1")).isTrue();
    assertThat(sessionManager.wasCommittedBy(user("alice"), "AS-" + HttpSessionManager.MAX_REMEMBERED_COMMITS)).isTrue();
  }

  @Test
  void droppingTheUserForgetsItsCommits() {
    sessionManager = new HttpSessionManager(TIMEOUT_MS);
    sessionManager.recordCommitted("AS-1", user("alice"));
    sessionManager.recordCommitted("AS-2", user("bob"));

    sessionManager.removeSessionsForUser("alice");

    assertThat(sessionManager.wasCommittedBy(user("alice"), "AS-1")).isFalse();
    assertThat(sessionManager.wasCommittedBy(user("bob"), "AS-2")).isTrue();
  }
}
