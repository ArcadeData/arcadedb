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
package com.arcadedb.server.security.credential;

import com.arcadedb.server.security.ServerSecurityException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for DefaultCredentialsValidator.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DefaultCredentialsValidatorTest {

  @Test
  void shouldValidateCorrectCredentials() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    // Should not throw exception
    validator.validateCredentials("testuser", "testpass123");
  }

  @Test
  void shouldRejectNullUserName() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    assertThatThrownBy(() -> validator.validateCredentials(null, "testpass123"))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("Empty user name");
  }

  @Test
  void shouldRejectEmptyUserName() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    assertThatThrownBy(() -> validator.validateCredentials("", "testpass123"))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("Empty user name");
  }

  @Test
  void shouldRejectShortUserName() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    assertThatThrownBy(() -> validator.validateCredentials("abc", "testpass123"))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("User name too short");
  }

  @Test
  void shouldRejectLongUserName() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();
    final String longUserName = "a".repeat(257);

    assertThatThrownBy(() -> validator.validateCredentials(longUserName, "testpass123"))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("User name too long");
  }

  @Test
  void shouldAcceptMinimumLengthUserName() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    validator.validateCredentials("abcd", "testpass123");
  }

  @Test
  void shouldAcceptMaximumLengthUserName() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();
    final String maxUserName = "a".repeat(256);

    validator.validateCredentials(maxUserName, "testpass123");
  }

  @Test
  void shouldRejectNullPassword() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    assertThatThrownBy(() -> validator.validateCredentials("testuser", null))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("Empty user password");
  }

  @Test
  void shouldRejectEmptyPassword() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    assertThatThrownBy(() -> validator.validateCredentials("testuser", ""))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("Empty user password");
  }

  @Test
  void shouldRejectShortPassword() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    assertThatThrownBy(() -> validator.validateCredentials("testuser", "short"))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("User password too short");
  }

  @Test
  void shouldRejectLongPassword() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();
    final String longPassword = "a".repeat(257);

    assertThatThrownBy(() -> validator.validateCredentials("testuser", longPassword))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("User password too long");
  }

  @Test
  void shouldAcceptMinimumLengthPassword() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    validator.validateCredentials("testuser", "12345678");
  }

  @Test
  void shouldAcceptMaximumLengthPassword() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();
    final String maxPassword = "a".repeat(256);

    validator.validateCredentials("testuser", maxPassword);
  }

  @Test
  void shouldGenerateRandomPassword() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    final String password = validator.generateRandomPassword();

    assertThat(password).isNotNull();
    assertThat(password).hasSize(8);
  }

  @Test
  void shouldGenerateUniquePasswords() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    final String password1 = validator.generateRandomPassword();
    final String password2 = validator.generateRandomPassword();
    final String password3 = validator.generateRandomPassword();

    assertThat(password1).isNotEqualTo(password2);
    assertThat(password2).isNotEqualTo(password3);
    assertThat(password1).isNotEqualTo(password3);
  }

  @Test
  void shouldGeneratePasswordsWithMixedCase() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    // Generate multiple passwords to increase chance of mixed case
    boolean hasMixedCase = false;
    for (int i = 0; i < 10; i++) {
      final String password = validator.generateRandomPassword();
      boolean hasUpper = false;
      boolean hasLower = false;

      for (final char c : password.toCharArray()) {
        if (Character.isUpperCase(c))
          hasUpper = true;
        if (Character.isLowerCase(c))
          hasLower = true;
      }

      if (hasUpper && hasLower) {
        hasMixedCase = true;
        break;
      }
    }

    assertThat(hasMixedCase).isTrue();
  }

  @Test
  void shouldValidateGeneratedPassword() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    final String password = validator.generateRandomPassword();

    // Generated password should be valid
    validator.validateCredentials("testuser", password);
  }

  @Test
  void shouldAcceptSpecialCharactersInUserName() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    validator.validateCredentials("test.user@example", "testpass123");
  }

  @Test
  void shouldAcceptSpecialCharactersInPassword() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    validator.validateCredentials("testuser", "test@Pass#123!");
  }

  @Test
  void shouldAcceptNumericUserName() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    validator.validateCredentials("12345678", "testpass123");
  }

  @Test
  void shouldAcceptNumericPassword() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    validator.validateCredentials("testuser", "12345678");
  }

  @Test
  void shouldHandleUnicodeCharacters() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    validator.validateCredentials("user中文", "password中文");
  }

  @Test
  void shouldRejectBothNullCredentials() {
    final DefaultCredentialsValidator validator = new DefaultCredentialsValidator();

    assertThatThrownBy(() -> validator.validateCredentials(null, null))
        .isInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("Empty user name");
  }
}
