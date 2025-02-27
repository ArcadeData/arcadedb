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
package com.arcadedb.event;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Implements record encryption by using the database events.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RecordEncryptionTest extends TestHelper
    implements BeforeRecordCreateListener, AfterRecordReadListener, BeforeRecordUpdateListener {
  private final static String          password           = "JustAPassword";
  private final static String          PASSWORD_ALGORITHM = "PBKDF2WithHmacSHA256";
  private final static String          ALGORITHM          = "AES/CBC/PKCS5Padding";
  private static final int             SALT_ITERATIONS    = 65536;
  private static final int             KEY_LENGTH         = 256;
  private              SecretKey       key;
  private              IvParameterSpec ivParameterSpec;
  private final        AtomicInteger   creates            = new AtomicInteger();
  private final        AtomicInteger   reads              = new AtomicInteger();
  private final        AtomicInteger   updates            = new AtomicInteger();

  @Override
  public void beginTest() {
    final VertexType backAccount = database.getSchema().createVertexType("BackAccount");
    backAccount.getEvents().registerListener((BeforeRecordCreateListener) this);
    backAccount.getEvents().registerListener((AfterRecordReadListener) this);
    backAccount.getEvents().registerListener((BeforeRecordUpdateListener) this);

    try {
      key = getKeyFromPassword(password, "salt");
      // Generate IV once during initialization
      byte[] iv = new byte[16];
      new SecureRandom().nextBytes(iv);
      ivParameterSpec = new IvParameterSpec(iv);
    } catch (Exception e) {
      throw new SecurityException(e);
    }
  }

  @Test
  public void testEncryption() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex("BackAccount")
          .set("secret", "Nobody must know John and Zuck are brothers")
          .save();
    });

    assertThat(creates.get()).isEqualTo(1);

    database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    database.transaction(() -> {
      final Vertex v1 = database.iterateType("BackAccount", true).next().asVertex();
      assertThat(v1.getString("secret")).isEqualTo("Nobody must know John and Zuck are brothers");
    });

    assertThat(reads.get()).isEqualTo(1);

    database.transaction(() -> {
      final MutableVertex v1 = database.iterateType("BackAccount", true).next().asVertex().modify();
      v1.set("secret", "Tool late, everybody knows it").save();
    });

    assertThat(updates.get()).isEqualTo(1);
    assertThat(reads.get()).isEqualTo(2);

    database.transaction(() -> {
      final Vertex v1 = database.iterateType("BackAccount", true).next().asVertex();
      assertThat(v1.getString("secret")).isEqualTo("Tool late, everybody knows it");
    });

    assertThat(reads.get()).isEqualTo(3);
  }

  @Override
  public Record onAfterRead(Record record) {
    final MutableVertex doc = record.asVertex().modify();
    try {
      byte[] ivBytes = Base64.getDecoder().decode(doc.getString("iv"));
      IvParameterSpec iv = new IvParameterSpec(ivBytes);
      doc.set("secret", decrypt(ALGORITHM, doc.getString("secret"), key, iv));
      reads.incrementAndGet();
      return doc;
    } catch (Exception e) {
      throw new SecurityException(e);
    }
  }

  @Override
  public boolean onBeforeCreate(Record record) {
    final MutableVertex doc = record.asVertex().modify();
    try {
      String encrypted = encrypt(ALGORITHM, doc.getString("secret"), key, ivParameterSpec);
      doc.set("secret", encrypted);
      doc.set("iv", Base64.getEncoder().encodeToString(ivParameterSpec.getIV()));
      creates.incrementAndGet();
    } catch (Exception e) {
      throw new SecurityException(e);
    }
    return true;
  }

  @Override
  public boolean onBeforeUpdate(Record record) {
    final MutableVertex doc = record.asVertex().modify();
    try {
      String encrypted = encrypt(ALGORITHM, doc.getString("secret"), key, ivParameterSpec);
      doc.set("secret", encrypted);
      doc.set("iv", Base64.getEncoder().encodeToString(ivParameterSpec.getIV()));
      updates.incrementAndGet();
    } catch (Exception e) {
      throw new SecurityException(e);
    }
    return true;
  }

  public static String encrypt(String algorithm, String input, SecretKey key, IvParameterSpec iv)
      throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException,
      InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
    final Cipher cipher = Cipher.getInstance(algorithm);
    cipher.init(Cipher.ENCRYPT_MODE, key, iv);
    final byte[] cipherText = cipher.doFinal(input.getBytes());
    return Base64.getEncoder().encodeToString(cipherText);
  }

  public static String decrypt(String algorithm, String cipherText, SecretKey key, IvParameterSpec iv)
      throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException,
      InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
    final Cipher cipher = Cipher.getInstance(algorithm);
    cipher.init(Cipher.DECRYPT_MODE, key, iv);
    final byte[] plainText = cipher.doFinal(Base64.getDecoder().decode(cipherText));
    return new String(plainText);
  }

  public static SecretKey getKeyFromPassword(final String password, final String salt)
      throws NoSuchAlgorithmException, InvalidKeySpecException {
    final SecretKeyFactory factory = SecretKeyFactory.getInstance(PASSWORD_ALGORITHM);
    final KeySpec spec = new PBEKeySpec(password.toCharArray(), salt.getBytes(), SALT_ITERATIONS, KEY_LENGTH);
    return new SecretKeySpec(factory.generateSecret(spec).getEncoded(), "AES");
  }
}
