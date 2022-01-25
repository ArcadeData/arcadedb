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

import java.util.Random;
import java.util.UUID;

/**
 * Default implementation for validating users. The requirements are quite minimalistic: user name must be between 4 and 256 character and the password between 8 and 256.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DefaultCredentialsValidator implements CredentialsValidator {
  protected int userMinLength     = 4;
  protected int userMaxLength     = 256;
  protected int passwordMinLength = 8;
  protected int passwordMaxLength = 256;

  @Override
  public void validateCredentials(final String userName, final String userPassword) throws ServerSecurityException {
    if (userName == null || userName.isEmpty())
      throw new ServerSecurityException("Empty user name");
    if (userName.length() < userMinLength)
      throw new ServerSecurityException("User name too short (<" + userMinLength + " characters)");
    if (userName.length() > userMaxLength)
      throw new ServerSecurityException("User name too long (>" + userMaxLength + " characters)");

    if (userPassword == null || userPassword.isEmpty())
      throw new ServerSecurityException("Empty user password");
    if (userPassword.length() < passwordMinLength)
      throw new ServerSecurityException("User password too short (<" + passwordMinLength + " characters)");
    if (userPassword.length() > passwordMaxLength)
      throw new ServerSecurityException("User password too long (>" + passwordMaxLength + " characters)");
  }

  @Override
  public String generateRandomPassword() {
    String password = UUID.randomUUID().toString().substring(0, 8);
    for (int i = 0; i < password.length() - 1; i++) {
      final char c = password.charAt(i);
      if (Character.isLetter(c)) {
        if (new Random().nextInt(2) == 0)
          password = password.substring(0, i) + Character.toUpperCase(c) + password.substring(i + 1);
      }
    }

    return password;
  }
}
