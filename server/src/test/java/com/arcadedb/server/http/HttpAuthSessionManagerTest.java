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

import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link HttpAuthSessionManager}.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/1691">GitHub Issue #1691</a>
 */
class HttpAuthSessionManagerTest {

  private HttpAuthSessionManager manager;
  private long                   fakeNow;

  @AfterEach
  void tearDown() {
    if (manager != null) {
      manager.close();
    }
  }

  private ServerSecurityUser createMockUser(String username) {
    ServerSecurityUser user = mock(ServerSecurityUser.class);
    when(user.getName()).thenReturn(username);
    when(user.getAuthorizedDatabases()).thenReturn(Set.of());
    return user;
  }

  /**
   * Builds a manager backed by a deterministic fake clock (advanced via {@link #fakeNow}) instead of
   * the wall clock, so idle/absolute timeout assertions cannot flake under a stop-the-world pause (#6398).
   */
  private HttpAuthSessionManager createManagerWithFakeClock(final long sessionTimeoutInMs, final long absoluteTimeoutInMs) {
    fakeNow = 0L;
    return new HttpAuthSessionManager(sessionTimeoutInMs, absoluteTimeoutInMs, () -> fakeNow);
  }

  @Test
  void createAndGetSession() {
    manager = new HttpAuthSessionManager(30_000L); // 30 second idle timeout
    ServerSecurityUser user = createMockUser("testuser");

    HttpAuthSession session = manager.createSession(user);

    assertThat(session).isNotNull();
    assertThat(session.getToken()).startsWith("AU-");
    assertThat(session.getUser().getName()).isEqualTo("testuser");
    assertThat(manager.getActiveSessionCount()).isEqualTo(1);

    // Should be able to get the session back
    HttpAuthSession retrieved = manager.getSessionByToken(session.getToken());
    assertThat(retrieved).isNotNull();
    assertThat(retrieved.getToken()).isEqualTo(session.getToken());
  }

  @Test
  void sessionIdleTimeout() throws Exception {
    manager = createManagerWithFakeClock(100L, 0); // 100ms idle timeout
    ServerSecurityUser user = createMockUser("testuser");

    HttpAuthSession session = manager.createSession(user);

    // Don't call getSessionByToken here as it would reset the idle timer via touch()
    assertThat(session).isNotNull();
    String token = session.getToken();

    // Advance the fake clock well past the idle timeout; no wall-clock wait needed
    fakeNow += 300;

    manager.checkSessionsValidity();
    assertThat(manager.getActiveSessionCount()).isEqualTo(0);
    assertThat(manager.getSessionByToken(token)).isNull();
  }

  @Test
  void sessionIdleTimeoutResetByAccess() throws Exception {
    manager = createManagerWithFakeClock(200L, 0); // 200ms idle timeout
    ServerSecurityUser user = createMockUser("testuser");

    HttpAuthSession session = manager.createSession(user);

    // Access session before it times out (resets idle timer)
    fakeNow += 100;
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();

    // Access again
    fakeNow += 100;
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();

    // Session should still be valid because we kept accessing it
    int expired = manager.checkSessionsValidity();
    assertThat(expired).isEqualTo(0);
  }

  @Test
  void absoluteTimeoutZeroMeansUnlimited() throws Exception {
    manager = createManagerWithFakeClock(10_000L, 0); // 10s idle timeout, 0 = unlimited absolute
    ServerSecurityUser user = createMockUser("testuser");

    HttpAuthSession session = manager.createSession(user);

    fakeNow += 100;

    // Session should still be valid (no absolute timeout)
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();
    int expired = manager.checkSessionsValidity();
    assertThat(expired).isEqualTo(0);
  }

  @Test
  void absoluteTimeoutExpiresSession() throws Exception {
    manager = createManagerWithFakeClock(10_000L, 100L); // 10s idle timeout, 100ms absolute timeout
    ServerSecurityUser user = createMockUser("testuser");

    HttpAuthSession session = manager.createSession(user);
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();

    // Advance the fake clock past the absolute timeout
    fakeNow += 150;

    // Session should be rejected even though idle timeout hasn't expired
    assertThat(manager.getSessionByToken(session.getToken())).isNull();

    // Cleanup should also remove it
    int expired = manager.checkSessionsValidity();
    assertThat(expired).isEqualTo(1);
  }

  @Test
  void absoluteTimeoutNotResetByAccess() throws Exception {
    manager = createManagerWithFakeClock(10_000L, 200L); // 10s idle timeout, 200ms absolute timeout
    ServerSecurityUser user = createMockUser("testuser");

    HttpAuthSession session = manager.createSession(user);

    // Access session multiple times (this should reset idle timeout but NOT absolute)
    fakeNow += 80;
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();

    fakeNow += 80;
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();

    // Advance the fake clock past the absolute timeout
    fakeNow += 100;

    // Session should now be rejected because absolute timeout expired
    assertThat(manager.getSessionByToken(session.getToken())).isNull();
  }

  @Test
  void removeSession() {
    manager = new HttpAuthSessionManager(30_000L);
    ServerSecurityUser user = createMockUser("testuser");

    HttpAuthSession session = manager.createSession(user);
    assertThat(manager.getActiveSessionCount()).isEqualTo(1);

    boolean removed = manager.removeSession(session.getToken());
    assertThat(removed).isTrue();
    assertThat(manager.getActiveSessionCount()).isEqualTo(0);
    assertThat(manager.getSessionByToken(session.getToken())).isNull();

    // Removing again should return false
    removed = manager.removeSession(session.getToken());
    assertThat(removed).isFalse();
  }

  @Test
  void getSessionByInvalidToken() {
    manager = new HttpAuthSessionManager(30_000L);

    HttpAuthSession session = manager.getSessionByToken("AU-invalid-token");
    assertThat(session).isNull();
  }

  @Test
  void elapsedFromCreation() throws Exception {
    manager = createManagerWithFakeClock(30_000L, 0);
    ServerSecurityUser user = createMockUser("testuser");

    HttpAuthSession session = manager.createSession(user);
    assertThat(session.elapsedFromCreation()).isEqualTo(0);

    fakeNow += 100;

    assertThat(session.elapsedFromCreation()).isEqualTo(100);
  }

  @Test
  void multipleSessions() {
    manager = new HttpAuthSessionManager(30_000L);
    ServerSecurityUser user1 = createMockUser("user1");
    ServerSecurityUser user2 = createMockUser("user2");

    HttpAuthSession session1 = manager.createSession(user1);
    HttpAuthSession session2 = manager.createSession(user2);

    assertThat(manager.getActiveSessionCount()).isEqualTo(2);
    assertThat(session1.getToken()).isNotEqualTo(session2.getToken());

    assertThat(manager.getSessionByToken(session1.getToken())).isNotNull();
    assertThat(manager.getSessionByToken(session2.getToken())).isNotNull();

    manager.removeSession(session1.getToken());
    assertThat(manager.getActiveSessionCount()).isEqualTo(1);
    assertThat(manager.getSessionByToken(session1.getToken())).isNull();
    assertThat(manager.getSessionByToken(session2.getToken())).isNotNull();
  }
}
