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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

/**
 * util.decompress(data, algorithm) - Decompress data using the specified algorithm.
 * Supported algorithms: "gzip", "deflate" (default: "gzip").
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class UtilDecompress extends AbstractUtilFunction {
  @Override
  protected String getSimpleName() {
    return "decompress";
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
    return "Decompress base64-encoded data using the specified algorithm (gzip or deflate)";
  }

  private static final int MAX_OUTPUT_SIZE = 100 * 1024 * 1024; // 100MB maximum output size

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final String base64Data = args[0].toString();
    final String algorithm = args.length > 1 && args[1] != null ? args[1].toString().toLowerCase() : "gzip";

    try {
      final byte[] compressedBytes = Base64.getDecoder().decode(base64Data);
      final ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();

      switch (algorithm) {
      case "gzip":
        try (final GZIPInputStream gzis = new GZIPInputStream(bais)) {
          final byte[] buffer = new byte[1024];
          int len;
          long totalBytesRead = 0;
          while ((len = gzis.read(buffer)) != -1) {
            totalBytesRead += len;
            if (totalBytesRead > MAX_OUTPUT_SIZE) {
              throw new IllegalArgumentException(
                  "Decompressed output size exceeds maximum allowed (" + MAX_OUTPUT_SIZE + " bytes). Potential zip bomb attack.");
            }
            baos.write(buffer, 0, len);
          }
        }
        break;
      case "deflate":
        try (final InflaterInputStream iis = new InflaterInputStream(bais)) {
          final byte[] buffer = new byte[1024];
          int len;
          long totalBytesRead = 0;
          while ((len = iis.read(buffer)) != -1) {
            totalBytesRead += len;
            if (totalBytesRead > MAX_OUTPUT_SIZE) {
              throw new IllegalArgumentException(
                  "Decompressed output size exceeds maximum allowed (" + MAX_OUTPUT_SIZE + " bytes). Potential zip bomb attack.");
            }
            baos.write(buffer, 0, len);
          }
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported compression algorithm: " + algorithm);
      }

      return baos.toString(StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new RuntimeException("Error decompressing data", e);
    }
  }
}
