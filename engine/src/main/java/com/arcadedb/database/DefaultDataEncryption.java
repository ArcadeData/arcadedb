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

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import com.arcadedb.exception.EncryptionException;

/**
 * Provides configurable default with implementation for data encryption and decryption.
 *
 * @author Pawel Maslej
 * @since 27 Jun 2024
 */
public class DefaultDataEncryption implements DataEncryption {
  public static final int DEFAULT_SALT_ITERATIONS = 65536;
  public static final int DEFAULT_KEY_LENGTH = 256;
  public static final String DEFAULT_PASSWORD_ALGORITHM = "PBKDF2WithHmacSHA256";
  public static final String DEFAULT_SECRET_KEY_ALGORITHM = "AES";

  public static final String DEFAULT_ALGORITHM = "AES/GCM/NoPadding";
  public static final int DEFAULT_IV_SIZE = 12;
  public static final int DEFAULT_TAG_SIZE = 128;

  private SecretKey secretKey;
  private String algorithm;
  private int ivSize;
  private int tagSize;

  public DefaultDataEncryption(SecretKey secretKey, String algorithm, int ivSize, int tagSize) throws NoSuchAlgorithmException, NoSuchPaddingException {
    this.secretKey = secretKey;
    this.algorithm = algorithm;
    this.ivSize = ivSize;
    this.tagSize = tagSize;
    Cipher.getInstance(algorithm); // validates before execution
  }

  @Override
  public byte[] encrypt(byte[] data) {
    try {
      var ivBytes = generateIv(ivSize);
      var cipher = Cipher.getInstance(algorithm);
      cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(tagSize, ivBytes));
      byte[] encryptedData = cipher.doFinal(data);
      byte[] ivAndEncryptedData = new byte[ivBytes.length + encryptedData.length];
      System.arraycopy(ivBytes, 0, ivAndEncryptedData, 0, ivBytes.length);
      System.arraycopy(encryptedData, 0, ivAndEncryptedData, ivBytes.length, encryptedData.length);
      return ivAndEncryptedData;
    } catch (Exception e) {
      throw new EncryptionException("Error while encrypting data", e);
    }
  }

  @Override
  public byte[] decrypt(byte[] data) {
    try {
      byte[] ivBytes = new byte[ivSize];
      byte[] encryptedData = new byte[data.length - ivSize];
      System.arraycopy(data, 0, ivBytes, 0, ivSize);
      System.arraycopy(data, ivSize, encryptedData, 0, encryptedData.length);
      var decipher = Cipher.getInstance(algorithm);
      decipher.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(tagSize, ivBytes));
      return decipher.doFinal(encryptedData);
    } catch (Exception e) {
      throw new EncryptionException("Error while decrypting data", e);
    }
  }

  public static DefaultDataEncryption useDefaults(SecretKey secretKey) throws NoSuchAlgorithmException, NoSuchPaddingException {
    return new DefaultDataEncryption(secretKey, DEFAULT_ALGORITHM, DEFAULT_IV_SIZE, DEFAULT_TAG_SIZE);
  }

  public static SecretKey generateRandomSecretKeyUsingDefaults() throws NoSuchAlgorithmException {
    KeyGenerator keyGen = KeyGenerator.getInstance(DEFAULT_SECRET_KEY_ALGORITHM);
    keyGen.init(DEFAULT_KEY_LENGTH);
    return keyGen.generateKey();
  }

  public static SecretKey getSecretKeyFromPasswordUsingDefaults(String password, String salt) throws NoSuchAlgorithmException, InvalidKeySpecException {
    return getKeyFromPassword(password, salt, DEFAULT_PASSWORD_ALGORITHM, DEFAULT_SECRET_KEY_ALGORITHM, DEFAULT_SALT_ITERATIONS, DEFAULT_KEY_LENGTH);
  }

  private static byte[] generateIv(int ivSize) {
    final byte[] iv = new byte[ivSize];
    new SecureRandom().nextBytes(iv);
    return iv;
  }

  public static SecretKey getKeyFromPassword(String password, String salt, String passwordAlgorithm, String secretKeyAlgorithm, int saltIterations, int keyLength)
      throws NoSuchAlgorithmException, InvalidKeySpecException {
    final SecretKeyFactory factory = SecretKeyFactory.getInstance(passwordAlgorithm);
    final KeySpec spec = new PBEKeySpec(password.toCharArray(), salt.getBytes(), saltIterations, keyLength);
    return new SecretKeySpec(factory.generateSecret(spec).getEncoded(), secretKeyAlgorithm);
  }
}