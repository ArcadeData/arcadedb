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
package com.arcadedb.function.util;

import com.arcadedb.function.StatelessFunction;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Abstract base class for utility functions.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractUtilFunction implements StatelessFunction {
  private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

  @Override
  public String getName() {
    return "util." + getSimpleName();
  }

  protected abstract String getSimpleName();

  /**
   * Computes a hash of the given value using the specified algorithm.
   */
  protected String computeHash(final Object value, final String algorithm) {
    if (value == null)
      return null;

    try {
      final MessageDigest digest = MessageDigest.getInstance(algorithm);
      final byte[] hash = digest.digest(value.toString().getBytes(StandardCharsets.UTF_8));
      return bytesToHex(hash);
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("Hash algorithm not available: " + algorithm, e);
    }
  }

  /**
   * Converts a byte array to a hexadecimal string.
   */
  protected String bytesToHex(final byte[] bytes) {
    final char[] hexChars = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      final int v = bytes[i] & 0xFF;
      hexChars[i * 2] = HEX_CHARS[v >>> 4];
      hexChars[i * 2 + 1] = HEX_CHARS[v & 0x0F];
    }
    return new String(hexChars);
  }
}
