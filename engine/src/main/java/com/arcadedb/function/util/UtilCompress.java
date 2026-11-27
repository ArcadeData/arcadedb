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

import com.arcadedb.query.sql.executor.CommandContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * util.compress(data, [config]) - Compress a string into a BYTEARRAY, as APOC's {@code apoc.util.compress} (issue #12108).
 * The config map is {@code {compression: 'GZIP' (default) | 'DEFLATE' | 'NONE', charset: 'UTF-8' (default)}}; see
 * {@link CompressionConfig}.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class UtilCompress extends AbstractUtilFunction {
  /** Largest payload this function will compress; anything beyond it is refused. */
  public static final int MAX_INPUT_SIZE = 10 * 1024 * 1024; // 10MB maximum input size

  @Override
  protected String getSimpleName() {
    return "compress";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Compress a string into a byte array, config {compression: 'GZIP' (default), 'DEFLATE' or 'NONE', charset: 'UTF-8'}";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final CompressionConfig config = CompressionConfig.of(args.length > 1 ? args[1] : null);
    final byte[] inputBytes = args[0].toString().getBytes(config.charset());

    // Check input size to prevent DoS attacks
    if (inputBytes.length > MAX_INPUT_SIZE)
      throw new IllegalArgumentException(
          "Input size exceeds maximum allowed (" + MAX_INPUT_SIZE + " bytes): " + inputBytes.length + " bytes");

    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();

      // Arrow form, as on the decompress side: a colon switch stays correct only while every branch keeps its
      // break, so a line added to one of them could fall through and compress the payload twice, with the second
      // algorithm wrapping the output of the first.
      switch (config.algorithm()) {
        case GZIP -> {
          try (final GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
            gzos.write(inputBytes);
          }
        }
        case DEFLATE -> {
          try (final DeflaterOutputStream dos = new DeflaterOutputStream(baos)) {
            dos.write(inputBytes);
          }
        }
        case NONE -> {
          return inputBytes;
        }
      }

      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException("Error compressing data", e);
    }
  }
}
