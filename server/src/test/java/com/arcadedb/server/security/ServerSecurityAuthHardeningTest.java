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
package com.arcadedb.server.security;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5029: brute-force lockout on password authentication and constant-time
 * password comparison.
 */
class ServerSecurityAuthHardeningTest {

  private static final String USER     = "testuser";
  private static final String PASSWORD = "SuperSecretPwd12345";

  @BeforeEach
  void setUp() {
    // keep PBKDF2 iterations small so the test runs fast
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
  }

  private ServerSecurity newSecurity() {
    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");
    security.createUser(USER, PASSWORD);
    return security;
  }

  @Test
  void shouldLockOutAfterRepeatedPasswordFailures() {
    final ServerSecurity security = newSecurity();

    // Five wrong-password attempts are rejected as invalid credentials.
    for (int i = 0; i < 5; i++)
      assertThatThrownBy(() -> security.authenticate(USER, "wrong-password", null))
          .isInstanceOf(ServerSecurityException.class)
          .hasMessageContaining("User/Password not valid");

    // The sixth attempt - even with the CORRECT password - is throttled by the lockout.
    assertThatThrownBy(() -> security.authenticate(USER, PASSWORD, null))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("Too many failed authentication attempts");
  }

  @Test
  void shouldNotLockOutOtherUsers() {
    final ServerSecurity security = newSecurity();
    security.createUser("otheruser", "AnotherStrongPwd99");

    // Exhaust the lockout for USER.
    for (int i = 0; i < 6; i++)
      assertThatThrownBy(() -> security.authenticate(USER, "wrong-password", null))
          .isInstanceOf(ServerSecurityException.class);

    // A different principal is unaffected and still authenticates.
    assertThat(security.authenticate("otheruser", "AnotherStrongPwd99", null)).isNotNull();
  }

  @Test
  void shouldLockOutUnknownUserWithGenericMessage() {
    final ServerSecurity security = newSecurity();

    // Attempts against a non-existent user must return the SAME generic message as a wrong password,
    // so the lockout path does not reveal whether the principal exists.
    for (int i = 0; i < 5; i++)
      assertThatThrownBy(() -> security.authenticate("ghost", "whatever", null))
          .isInstanceOf(ServerSecurityException.class)
          .hasMessageContaining("User/Password not valid");

    // Unknown users are also throttled after the threshold.
    assertThatThrownBy(() -> security.authenticate("ghost", "whatever", null))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("Too many failed authentication attempts");
  }

  @Test
  void shouldClearFailureCounterOnSuccessfulAuthentication() {
    final ServerSecurity security = newSecurity();

    // Four failures - below the threshold.
    for (int i = 0; i < 4; i++)
      assertThatThrownBy(() -> security.authenticate(USER, "wrong-password", null))
          .isInstanceOf(ServerSecurityException.class);

    // A successful authentication resets the counter.
    assertThat(security.authenticate(USER, PASSWORD, null)).isNotNull();

    // After the reset, four more failures still do not trip the lockout, and the correct
    // password is accepted (would be throttled if the counter had not been cleared).
    for (int i = 0; i < 4; i++)
      assertThatThrownBy(() -> security.authenticate(USER, "wrong-password", null))
          .isInstanceOf(ServerSecurityException.class);

    assertThat(security.authenticate(USER, PASSWORD, null)).isNotNull();
  }

  @Test
  void shouldMatchPasswordConstantTimeCorrectly() {
    final ServerSecurity security = newSecurity();

    final String encoded = security.encodePassword(PASSWORD, ServerSecurity.generateRandomSalt());

    assertThat(security.passwordMatch(PASSWORD, encoded)).isTrue();
    assertThat(security.passwordMatch("wrong-password", encoded)).isFalse();
    // Malformed stored hashes must not match and must not throw, for either failure mode:
    // wrong number of '$'-separated parts, or a non-numeric iteration count.
    assertThat(security.passwordMatch(PASSWORD, "not-a-valid-hash")).isFalse();
    assertThat(security.passwordMatch(PASSWORD, "PBKDF2$notanumber$salt$hash")).isFalse();
  }
}
