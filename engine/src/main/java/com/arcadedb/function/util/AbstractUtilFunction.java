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
import com.arcadedb.query.sql.executor.MultiValue;

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
   * Computes the hex hash of {@code values} as APOC's {@code apoc.util.md5} / {@code sha*} do (issue #12108): the hash of the
   * concatenation of each element's string form, a {@code null} element contributing nothing, so {@code md5(['a', 'b'])} is
   * the MD5 of {@code ab} and {@code md5([])} the MD5 of the empty string. A single value that is not a list is hashed as its
   * string form (an ArcadeDB extension: APOC accepts only a list), and {@code null} answers {@code null}.
   */
  protected String computeHash(final Object values, final String algorithm) {
    if (values == null)
      return null;

    final StringBuilder text = new StringBuilder();
    if (values instanceof Iterable<?> iterable) {
      for (final Object value : iterable)
        appendText(value, text);
    } else if (MultiValue.isSequenceArray(values)) {
      for (final Object value : MultiValue.getMultiValueAsList(values))
        appendText(value, text);
    } else
      appendText(values, text);

    try {
      final MessageDigest digest = MessageDigest.getInstance(algorithm);
      return bytesToHex(digest.digest(text.toString().getBytes(StandardCharsets.UTF_8)));
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("Hash algorithm not available: " + algorithm, e);
    }
  }

  private static void appendText(final Object value, final StringBuilder text) {
    if (value == null)
      return;
    // a stored ARRAY_OF_LONGS / ARRAY_OF_DOUBLES reads as the list it holds, not as the Java identity string "[J@..."
    if (MultiValue.isSequenceArray(value))
      text.append(MultiValue.getMultiValueAsList(value));
    else
      text.append(value);
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
