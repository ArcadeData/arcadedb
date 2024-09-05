/*
 * Copyright © 2024-present Arcade Data Ltd (info@arcadedata.com)
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
 * SPDX-FileCopyrightText: 2024-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.database;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * @author Pawel Maslej
 * @since 1 Jul 2024
 */
class DefaultDataEncryptionTest {
  static SecretKey key;

  @BeforeAll
  public static void beforeAll() throws NoSuchAlgorithmException, InvalidKeySpecException {
    String password = "password";
    String salt = "salt";
    key = DefaultDataEncryption.getSecretKeyFromPasswordUsingDefaults(password, salt);
  }

  @Test
  void testEncryptionOfString() throws NoSuchAlgorithmException, NoSuchPaddingException {
    var dde = DefaultDataEncryption.useDefaults(key);
    String data = "data";
    byte[] encryptedData = dde.encrypt(data.getBytes());
    String decryptedData = new String(dde.decrypt(encryptedData));
    assertEquals(data, decryptedData);
  }

  @Test
  void testEncryptionOfDouble() throws NoSuchAlgorithmException, NoSuchPaddingException {
    var dde = DefaultDataEncryption.useDefaults(key);
    double data = 1000000d;
    byte[] encryptedData = dde.encrypt(ByteBuffer.allocate(8).putLong(Double.doubleToLongBits(data)).array());
    var decryptedData = Double.longBitsToDouble(ByteBuffer.wrap(dde.decrypt(encryptedData)).getLong());
    assertEquals(data, decryptedData);
  }
}
