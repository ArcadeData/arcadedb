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
  // Also read by the manager's background cleanup Timer thread via the () -> fakeNow supplier passed to
  // createManagerWithFakeClock(): volatile so that read is guaranteed to see this thread's writes, even though
  // every test only asserts after advancing the clock and calling checkSessionsValidity() itself.
  private volatile long          fakeNow;

  @AfterEach
  void tearDown() {
    if (manager != null) {
      manager.close();
    }
  }

  private ServerSecurityUser createMockUser(final String username) {
    final ServerSecurityUser user = mock(ServerSecurityUser.class);
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
    final ServerSecurityUser user = createMockUser("testuser");

    final HttpAuthSession session = manager.createSession(user);

    assertThat(session).isNotNull();
    assertThat(session.getToken()).startsWith("AU-");
    assertThat(session.getUser().getName()).isEqualTo("testuser");
    assertThat(manager.getActiveSessionCount()).isEqualTo(1);

    // Should be able to get the session back
    final HttpAuthSession retrieved = manager.getSessionByToken(session.getToken());
    assertThat(retrieved).isNotNull();
    assertThat(retrieved.getToken()).isEqualTo(session.getToken());
  }

  @Test
  void sessionIdleTimeout() throws Exception {
    manager = createManagerWithFakeClock(100L, 0); // 100ms idle timeout
    final ServerSecurityUser user = createMockUser("testuser");

    final HttpAuthSession session = manager.createSession(user);

    // Don't call getSessionByToken here as it would reset the idle timer via touch()
    assertThat(session).isNotNull();
    final String token = session.getToken();

    // Advance the fake clock well past the idle timeout; no wall-clock wait needed
    fakeNow += 300;

    manager.checkSessionsValidity();
    assertThat(manager.getActiveSessionCount()).isEqualTo(0);
    assertThat(manager.getSessionByToken(token)).isNull();
  }

  @Test
  void sessionIdleTimeoutResetByAccess() throws Exception {
    manager = createManagerWithFakeClock(200L, 0); // 200ms idle timeout
    final ServerSecurityUser user = createMockUser("testuser");

    final HttpAuthSession session = manager.createSession(user);

    // Access session before it times out (resets idle timer)
    fakeNow += 100;
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();

    // Access again
    fakeNow += 100;
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();

    // Session should still be valid because we kept accessing it
    final int expired = manager.checkSessionsValidity();
    assertThat(expired).isEqualTo(0);
  }

  @Test
  void absoluteTimeoutZeroMeansUnlimited() throws Exception {
    manager = createManagerWithFakeClock(10_000L, 0); // 10s idle timeout, 0 = unlimited absolute
    final ServerSecurityUser user = createMockUser("testuser");

    final HttpAuthSession session = manager.createSession(user);

    fakeNow += 100;

    // Session should still be valid (no absolute timeout)
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();
    final int expired = manager.checkSessionsValidity();
    assertThat(expired).isEqualTo(0);
  }

  @Test
  void absoluteTimeoutExpiresSession() throws Exception {
    manager = createManagerWithFakeClock(10_000L, 100L); // 10s idle timeout, 100ms absolute timeout
    final ServerSecurityUser user = createMockUser("testuser");

    final HttpAuthSession session = manager.createSession(user);
    assertThat(manager.getSessionByToken(session.getToken())).isNotNull();

    // Advance the fake clock past the absolute timeout
    fakeNow += 150;

    // Session should be rejected even though idle timeout hasn't expired
    assertThat(manager.getSessionByToken(session.getToken())).isNull();

    // Cleanup should also remove it
    final int expired = manager.checkSessionsValidity();
    assertThat(expired).isEqualTo(1);
  }

  @Test
  void absoluteTimeoutNotResetByAccess() throws Exception {
    manager = createManagerWithFakeClock(10_000L, 200L); // 10s idle timeout, 200ms absolute timeout
    final ServerSecurityUser user = createMockUser("testuser");

    final HttpAuthSession session = manager.createSession(user);

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
    final ServerSecurityUser user = createMockUser("testuser");

    final HttpAuthSession session = manager.createSession(user);
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

    final HttpAuthSession session = manager.getSessionByToken("AU-invalid-token");
    assertThat(session).isNull();
  }

  @Test
  void elapsedFromCreation() throws Exception {
    manager = createManagerWithFakeClock(30_000L, 0);
    final ServerSecurityUser user = createMockUser("testuser");

    final HttpAuthSession session = manager.createSession(user);
    assertThat(session.elapsedFromCreation()).isEqualTo(0);

    fakeNow += 100;

    assertThat(session.elapsedFromCreation()).isEqualTo(100);
  }

  @Test
  void multipleSessions() {
    manager = new HttpAuthSessionManager(30_000L);
    final ServerSecurityUser user1 = createMockUser("user1");
    final ServerSecurityUser user2 = createMockUser("user2");

    final HttpAuthSession session1 = manager.createSession(user1);
    final HttpAuthSession session2 = manager.createSession(user2);

    assertThat(manager.getActiveSessionCount()).isEqualTo(2);
    assertThat(session1.getToken()).isNotEqualTo(session2.getToken());

    assertThat(manager.getSessionByToken(session1.getToken())).isNotNull();
    assertThat(manager.getSessionByToken(session2.getToken())).isNotNull();

    manager.removeSession(session1.getToken());
    assertThat(manager.getActiveSessionCount()).isEqualTo(1);
    assertThat(manager.getSessionByToken(session1.getToken())).isNull();
    assertThat(manager.getSessionByToken(session2.getToken())).isNotNull();
  }

  // ---------------------------------------------------------------------------------------------------
  // Issue #6809: the session map was an unbounded HashMap with no global and no per-principal cap, and it
  // stored four untruncated client-controlled header values per entry, each retained for the whole idle
  // timeout (30 minutes by default).
  // ---------------------------------------------------------------------------------------------------

  @Test
  void perPrincipalCapEvictsThatPrincipalsOldestSession() {
    manager = new HttpAuthSessionManager(30_000L, 0, 1_000, 3, () -> fakeNow);
    final ServerSecurityUser user = createMockUser("looper");

    final HttpAuthSession first = manager.createSession(user);
    final HttpAuthSession second = manager.createSession(user);
    final HttpAuthSession third = manager.createSession(user);
    assertThat(manager.getActiveSessionCount()).isEqualTo(3);

    // The fourth login evicts the oldest of this principal's sessions instead of growing the map.
    final HttpAuthSession fourth = manager.createSession(user);
    assertThat(fourth).isNotNull();
    assertThat(manager.getActiveSessionCount()).isEqualTo(3);
    assertThat(manager.getActiveSessionCount("looper")).isEqualTo(3);

    assertThat(manager.getSessionByToken(first.getToken())).isNull();
    assertThat(manager.getSessionByToken(second.getToken())).isNotNull();
    assertThat(manager.getSessionByToken(third.getToken())).isNotNull();
    assertThat(manager.getSessionByToken(fourth.getToken())).isNotNull();

    // 1000 more logins by the same principal must not move the total.
    for (int i = 0; i < 1_000; i++)
      manager.createSession(user);
    assertThat(manager.getActiveSessionCount()).isEqualTo(3);
  }

  @Test
  void perPrincipalCapNeverEvictsAnotherPrincipalsSession() {
    manager = new HttpAuthSessionManager(30_000L, 0, 1_000, 2, () -> fakeNow);
    final HttpAuthSession victim = manager.createSession(createMockUser("victim"));

    final ServerSecurityUser attacker = createMockUser("attacker");
    for (int i = 0; i < 100; i++)
      manager.createSession(attacker);

    assertThat(manager.getActiveSessionCount("attacker")).isEqualTo(2);
    assertThat(manager.getSessionByToken(victim.getToken())).isNotNull();
    assertThat(manager.getActiveSessionCount()).isEqualTo(3);
  }

  @Test
  void globalCapRefusesInsteadOfGrowingTheMap() {
    // Per-principal cap disabled so only the global one is under test.
    manager = new HttpAuthSessionManager(30_000L, 0, 2, 0, () -> fakeNow);

    assertThat(manager.createSession(createMockUser("u1"))).isNotNull();
    assertThat(manager.createSession(createMockUser("u2"))).isNotNull();

    // Third distinct principal: the map is full, so the login is refused (the handler answers 503).
    assertThat(manager.createSession(createMockUser("u3"))).isNull();
    assertThat(manager.getActiveSessionCount()).isEqualTo(2);
  }

  @Test
  void globalCapReclaimsIdleExpiredSessionsBeforeRefusing() {
    manager = new HttpAuthSessionManager(100L, 0, 2, 0, () -> fakeNow);

    manager.createSession(createMockUser("u1"));
    manager.createSession(createMockUser("u2"));
    assertThat(manager.getActiveSessionCount()).isEqualTo(2);

    // Both are now idle-expired: a legitimate login must not be refused just because the background sweep
    // has not fired yet.
    fakeNow += 300;
    assertThat(manager.createSession(createMockUser("u3"))).isNotNull();
    assertThat(manager.getActiveSessionCount()).isEqualTo(1);
  }

  @Test
  void aPrincipalAtItsOwnCapIsStillAdmittedWhenTheMapIsGloballyFull() {
    // Evicting frees exactly one slot, so this login costs the server nothing - and the decision must be
    // taken BEFORE the eviction, or the refusal would destroy a live session of the principal it refuses.
    manager = new HttpAuthSessionManager(30_000L, 0, 2, 2, () -> fakeNow);
    final ServerSecurityUser user = createMockUser("regular");

    final HttpAuthSession first = manager.createSession(user);
    final HttpAuthSession second = manager.createSession(user);
    assertThat(manager.getActiveSessionCount()).isEqualTo(2);

    final HttpAuthSession third = manager.createSession(user);
    assertThat(third).isNotNull();
    assertThat(manager.getActiveSessionCount()).isEqualTo(2);
    assertThat(manager.getSessionByToken(first.getToken())).isNull();
    assertThat(manager.getSessionByToken(second.getToken())).isNotNull();
    assertThat(manager.getSessionByToken(third.getToken())).isNotNull();

    // A different principal, however, is refused - and the refusal must not disturb anybody's sessions.
    assertThat(manager.createSession(createMockUser("newcomer"))).isNull();
    assertThat(manager.getActiveSessionCount()).isEqualTo(2);
    assertThat(manager.getActiveSessionCount("regular")).isEqualTo(2);
    assertThat(manager.getActiveSessionCount("newcomer")).isZero();
  }

  @Test
  void zeroOrNegativeCapsMeanUnlimited() {
    manager = new HttpAuthSessionManager(30_000L, 0, 0, 0, () -> fakeNow);
    final ServerSecurityUser user = createMockUser("testuser");

    for (int i = 0; i < 200; i++)
      assertThat(manager.createSession(user)).isNotNull();

    assertThat(manager.getActiveSessionCount()).isEqualTo(200);
  }

  @Test
  void clientSuppliedMetadataIsTruncated() {
    manager = new HttpAuthSessionManager(30_000L);
    final String oversized = "x".repeat(1_000_000);

    final HttpAuthSession session = manager.createSession(createMockUser("testuser"), oversized, oversized,
        oversized, oversized);

    assertThat(session.getSourceIp()).hasSize(HttpAuthSession.MAX_METADATA_LENGTH);
    assertThat(session.getUserAgent()).hasSize(HttpAuthSession.MAX_METADATA_LENGTH);
    assertThat(session.getCountry()).hasSize(HttpAuthSession.MAX_METADATA_LENGTH);
    assertThat(session.getCity()).hasSize(HttpAuthSession.MAX_METADATA_LENGTH);
  }

  @Test
  void truncationNeverSplitsASurrogatePair() {
    // A supplementary-plane character straddling the cut must be dropped whole, not left as an unpaired
    // half that renders as a replacement character wherever the metadata is displayed.
    final String emoji = "😀"; // U+1F600, two chars
    final String straddling = "x".repeat(HttpAuthSession.MAX_METADATA_LENGTH - 1) + emoji.repeat(10);
    assertThat(HttpAuthSession.truncate(straddling))
        .hasSize(HttpAuthSession.MAX_METADATA_LENGTH - 1)
        .doesNotContain("\uD83D");

    // An emoji that ends exactly on the boundary is kept whole.
    final String aligned = "x".repeat(HttpAuthSession.MAX_METADATA_LENGTH - 2) + emoji.repeat(10);
    assertThat(HttpAuthSession.truncate(aligned))
        .hasSize(HttpAuthSession.MAX_METADATA_LENGTH)
        .endsWith(emoji);
  }

  @Test
  void metadataThatFitsIsKeptVerbatimAndNullStaysNull() {
    manager = new HttpAuthSessionManager(30_000L);

    final HttpAuthSession session = manager.createSession(createMockUser("testuser"), "10.0.0.1", "curl/8.4.0",
        "IT", null);

    assertThat(session.getSourceIp()).isEqualTo("10.0.0.1");
    assertThat(session.getUserAgent()).isEqualTo("curl/8.4.0");
    assertThat(session.getCountry()).isEqualTo("IT");
    assertThat(session.getCity()).isNull();
  }

  @Test
  void removeSessionsForUserDropsEveryTokenOfThatPrincipalOnly() {
    // A dropped user (or one whose password changed) must not keep authenticating with a token minted
    // before the change. The per-principal index added for the cap is what makes this cheap.
    manager = new HttpAuthSessionManager(30_000L, 0, 1_000, 0, () -> fakeNow);
    final ServerSecurityUser revoked = createMockUser("revoked");
    final ServerSecurityUser other = createMockUser("other");

    final HttpAuthSession first = manager.createSession(revoked);
    final HttpAuthSession second = manager.createSession(revoked);
    final HttpAuthSession survivor = manager.createSession(other);

    assertThat(manager.removeSessionsForUser("revoked")).isEqualTo(2);

    assertThat(manager.getSessionByToken(first.getToken())).isNull();
    assertThat(manager.getSessionByToken(second.getToken())).isNull();
    assertThat(manager.getSessionByToken(survivor.getToken())).isNotNull();
    assertThat(manager.getActiveSessionCount()).isEqualTo(1);
    assertThat(manager.getActiveSessionCount("revoked")).isZero();

    // Idempotent, and null-safe for callers that do not know whether the principal had any session.
    assertThat(manager.removeSessionsForUser("revoked")).isZero();
    assertThat(manager.removeSessionsForUser("neverLoggedIn")).isZero();
    assertThat(manager.removeSessionsForUser(null)).isZero();
  }

  @Test
  void removedAndExpiredSessionsAreDroppedFromThePerPrincipalIndex() {
    manager = new HttpAuthSessionManager(100L, 0, 1_000, 2, () -> fakeNow);
    final ServerSecurityUser user = createMockUser("testuser");

    final HttpAuthSession session = manager.createSession(user);
    manager.removeSession(session.getToken());
    assertThat(manager.getActiveSessionCount("testuser")).isZero();

    // A logged-out session must not keep consuming the principal's quota.
    assertThat(manager.createSession(user)).isNotNull();
    assertThat(manager.createSession(user)).isNotNull();
    assertThat(manager.getActiveSessionCount("testuser")).isEqualTo(2);

    fakeNow += 300;
    assertThat(manager.checkSessionsValidity()).isEqualTo(2);
    assertThat(manager.getActiveSessionCount("testuser")).isZero();
  }
}
