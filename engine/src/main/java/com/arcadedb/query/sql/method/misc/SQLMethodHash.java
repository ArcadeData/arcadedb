/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.query.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

import java.nio.charset.*;
import java.security.*;

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
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context,
      final Object[] params) {
    if (value == null)
      return null;

    final String algorithm = params.length > 0 ? params[0].toString() : HASH_ALGORITHM;
    try {
      return createHash(value.toString(), algorithm);

    } catch (final NoSuchAlgorithmException e) {
      throw new CommandExecutionException("hash(): algorithm '" + algorithm + "' is not supported", e);
    }
  }

  public static String createHash(final String iInput, String algorithm)
      throws NoSuchAlgorithmException {
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
