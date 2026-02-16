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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * util.compress(data, algorithm) - Compress data using the specified algorithm.
 * Supported algorithms: "gzip", "deflate" (default: "gzip").
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class UtilCompress extends AbstractUtilFunction {
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
    return "Compress data using the specified algorithm (gzip or deflate), returns base64-encoded string";
  }

  private static final int MAX_INPUT_SIZE = 10 * 1024 * 1024; // 10MB maximum input size

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final String data = args[0].toString();
    final String algorithm = args.length > 1 && args[1] != null ? args[1].toString().toLowerCase() : "gzip";

    try {
      final byte[] inputBytes = data.getBytes(StandardCharsets.UTF_8);

      // Check input size to prevent DoS attacks
      if (inputBytes.length > MAX_INPUT_SIZE) {
        throw new IllegalArgumentException(
            "Input size exceeds maximum allowed (" + MAX_INPUT_SIZE + " bytes): " + inputBytes.length + " bytes");
      }

      final ByteArrayOutputStream baos = new ByteArrayOutputStream();

      switch (algorithm) {
      case "gzip":
        try (final GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
          gzos.write(inputBytes);
        }
        break;
      case "deflate":
        try (final DeflaterOutputStream dos = new DeflaterOutputStream(baos, new Deflater(Deflater.DEFAULT_COMPRESSION))) {
          dos.write(inputBytes);
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported compression algorithm: " + algorithm);
      }

      return Base64.getEncoder().encodeToString(baos.toByteArray());
    } catch (final IOException e) {
      throw new RuntimeException("Error compressing data", e);
    }
  }
}
