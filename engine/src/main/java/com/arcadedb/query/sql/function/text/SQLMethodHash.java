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
package com.arcadedb.query.sql.function.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.misc.AbstractSQLMethod;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Hash a string supporting multiple algorithm, all those supported by JVM
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodHash extends AbstractSQLMethod {

  public static final String NAME = "hash";

  public static final String HASH_ALGORITHM = "SHA-256";

  public SQLMethodHash() {
    super(NAME, 0, 1);
  }

  @Override
  public String getSyntax() {
    return "hash([<algorithm>])";
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final CommandContext iContext, final Object ioResult,
      final Object[] iParams) {
    if (iThis == null)
      return null;

    final String algorithm = iParams.length > 0 ? iParams[0].toString() : HASH_ALGORITHM;
    try {
      return createHash(iThis.toString(), algorithm);

    } catch (NoSuchAlgorithmException e) {
      throw new CommandExecutionException("hash(): algorithm '" + algorithm + "' is not supported", e);
    } catch (UnsupportedEncodingException e) {
      throw new CommandExecutionException("hash(): encoding 'UTF-8' is not supported", e);
    }
  }

  public static String createHash(final String iInput, String algorithm)
      throws NoSuchAlgorithmException, UnsupportedEncodingException {
    if (algorithm == null)
      algorithm = HASH_ALGORITHM;

    final MessageDigest msgDigest = MessageDigest.getInstance(algorithm);

    return byteArrayToHexStr(msgDigest.digest(iInput.getBytes(StandardCharsets.UTF_8)));
  }

  public static String byteArrayToHexStr(final byte[] data) {
    if (data == null)
      return null;

    final char[] chars = new char[data.length * 2];
    for (int i = 0; i < data.length; i++) {
      final byte current = data[i];
      final int hi = (current & 0xF0) >> 4;
      final int lo = current & 0x0F;
      chars[2 * i] = (char) (hi < 10 ? ('0' + hi) : ('A' + hi - 10));
      chars[2 * i + 1] = (char) (lo < 10 ? ('0' + lo) : ('A' + lo - 10));
    }
    return new String(chars);
  }

}
