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

import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.query.sql.executor.CommandContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

/**
 * util.decompress(data, [config]) - Decompress the BYTEARRAY {@code util.compress} answers back into a string, as APOC's
 * {@code apoc.util.decompress} (issue #12108). The config map is the one {@code util.compress} takes (see
 * {@link CompressionConfig}). A string argument is read as base64, the form {@code util.compress} answered before it followed
 * APOC, so a value stored from it can still be read back.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class UtilDecompress extends AbstractUtilFunction {
  /** Largest decompressed payload this function will buffer; anything beyond it is refused as a zip bomb. */
  public static final int MAX_OUTPUT_SIZE = 100 * 1024 * 1024; // 100MB maximum output size

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
    return "Decompress a byte array written by util.compress back into a string, with the same config map";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final CompressionConfig config = CompressionConfig.of(args.length > 1 ? args[1] : null);
    final byte[] compressedBytes = switch (args[0]) {
      case byte[] bytes -> bytes;
      case String base64 -> Base64.getDecoder().decode(base64);
      default -> throw new IllegalArgumentException(
          "util.decompress expects the byte array util.compress returns, got: " + args[0].getClass().getSimpleName());
    };

    try {
      final ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);

      // Both algorithms share one bounded read loop: a second copy of it would be free to drift away from the
      // zip-bomb guard, and only one of the two would then be covered by any given test (issue #7142). The arrow
      // form is deliberate - a colon switch would be correct only while every branch happens to return, so adding
      // a line to one of them could silently fall through into the next algorithm.
      return switch (config.algorithm()) {
        case GZIP -> {
          try (final GZIPInputStream gzis = new GZIPInputStream(bais)) {
            yield readBounded(gzis, config.charset());
          }
        }
        case DEFLATE -> {
          try (final InflaterInputStream iis = new InflaterInputStream(bais)) {
            yield readBounded(iis, config.charset());
          }
        }
        case NONE -> new String(compressedBytes, config.charset());
      };
    } catch (final IOException e) {
      // issue #11589: bytes that are not what the algorithm wrote are the caller's mistake
      throw new InvalidArgumentException("Error decompressing data: " + e.getMessage(), e);
    }
  }

  /**
   * Drains {@code in} into a string of {@code charset}, refusing to buffer more than {@link #MAX_OUTPUT_SIZE} bytes. The
   * cap is what stops a zip bomb - a few hundred bytes of input that inflate to gigabytes - from exhausting the heap, so
   * it is checked before each chunk is buffered rather than after the whole stream has been read.
   */
  private static String readBounded(final InputStream in, final Charset charset) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final byte[] buffer = new byte[8192];
    long totalBytesRead = 0;
    int len;
    while ((len = in.read(buffer)) != -1) {
      totalBytesRead += len;
      if (totalBytesRead > MAX_OUTPUT_SIZE)
        throw new IllegalArgumentException(
            "Decompressed output size exceeds maximum allowed (" + MAX_OUTPUT_SIZE + " bytes). Potential zip bomb attack.");
      baos.write(buffer, 0, len);
    }
    return baos.toString(charset);
  }
}
