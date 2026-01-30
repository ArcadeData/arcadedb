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
    manager = new HttpAuthSessionManager(100L); // 100ms idle timeout
    ServerSecurityUser user = createMockUser("testuser");

    HttpAuthSession session = manager.createSession(user);

    // Don't call getSessionByToken here as it would reset the idle timer via touch()
    assertThat(session).isNotNull();
    String token = session.getToken();

    // Wait for idle timeout + timer to run (timer runs every 100ms)
    Thread.sleep(300);

    // Session should be cleaned up by the background timer or manually
    // Either the timer already ran, or we trigger it now
    manager.checkSessionsValidity();
    assertThat(manager.getActiveSessionCount()).isEqualTo(0);
    assertThat(manager.getSessionByToken(token)).isNull();
  }

  @Test
  void sessionIdleTimeoutResetByAccess() throws Exception {
    manager = new HttpAuthSessionManager(200L); // 200ms idle timeout
    ServerSecurityUser user = createMockUser("testuser");

    HttpAuthSession session = manager.createSession(user);

    // Access session before it times out (resets idle timer)
    Thread.sleep(100);
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();

    // Access again
    Thread.sleep(100);
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();

    // Session should still be valid because we kept accessing it
    int expired = manager.checkSessionsValidity();
    assertThat(expired).isEqualTo(0);
  }

  @Test
  void absoluteTimeoutZeroMeansUnlimited() throws Exception {
    manager = new HttpAuthSessionManager(10_000L, 0); // 10s idle timeout, 0 = unlimited absolute
    ServerSecurityUser user = createMockUser("testuser");

    HttpAuthSession session = manager.createSession(user);

    // Wait a bit
    Thread.sleep(100);

    // Session should still be valid (no absolute timeout)
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();
    int expired = manager.checkSessionsValidity();
    assertThat(expired).isEqualTo(0);
  }

  @Test
  void absoluteTimeoutExpiresSession() throws Exception {
    manager = new HttpAuthSessionManager(10_000L, 100L); // 10s idle timeout, 100ms absolute timeout
    ServerSecurityUser user = createMockUser("testuser");

    HttpAuthSession session = manager.createSession(user);
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();

    // Wait for absolute timeout to expire
    Thread.sleep(150);

    // Session should be rejected even though idle timeout hasn't expired
    assertThat(manager.getSessionByToken(session.getToken())).isNull();

    // Cleanup should also remove it
    int expired = manager.checkSessionsValidity();
    assertThat(expired).isEqualTo(1);
  }

  @Test
  void absoluteTimeoutNotResetByAccess() throws Exception {
    manager = new HttpAuthSessionManager(10_000L, 200L); // 10s idle timeout, 200ms absolute timeout
    ServerSecurityUser user = createMockUser("testuser");

    HttpAuthSession session = manager.createSession(user);

    // Access session multiple times (this should reset idle timeout but NOT absolute)
    Thread.sleep(80);
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();

    Thread.sleep(80);
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();

    // Wait for absolute timeout to expire
    Thread.sleep(100);

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
    manager = new HttpAuthSessionManager(30_000L);
    ServerSecurityUser user = createMockUser("testuser");

    HttpAuthSession session = manager.createSession(user);
    long initialElapsed = session.elapsedFromCreation();
    assertThat(initialElapsed).isGreaterThanOrEqualTo(0);
    assertThat(initialElapsed).isLessThan(100);

    Thread.sleep(100);

    long laterElapsed = session.elapsedFromCreation();
    assertThat(laterElapsed).isGreaterThanOrEqualTo(100);
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
