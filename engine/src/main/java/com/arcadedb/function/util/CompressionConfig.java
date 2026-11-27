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

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Locale;
import java.util.Map;

/**
 * The second argument of {@code util.compress} / {@code util.decompress}: APOC's config map
 * {@code {compression: 'GZIP' | 'DEFLATE' | 'NONE', charset: 'UTF-8'}} (issue #12108). The algorithm name is matched in any
 * letter case. A plain string is still read as the algorithm name, the form these functions took before they followed APOC.
 * APOC also offers {@code BZIP2}, {@code BLOCK_LZ4} and {@code FRAMED_SNAPPY}, which need codecs the engine does not ship:
 * they are refused with an error that says so.
 */
record CompressionConfig(Algorithm algorithm, Charset charset) {
  enum Algorithm {GZIP, DEFLATE, NONE}

  static final String KEY_COMPRESSION = "compression";
  static final String KEY_CHARSET     = "charset";

  static CompressionConfig of(final Object config) {
    return switch (config) {
      case null -> new CompressionConfig(Algorithm.GZIP, StandardCharsets.UTF_8);
      case Map<?, ?> map -> new CompressionConfig(algorithmOf(map.get(KEY_COMPRESSION)), charsetOf(map.get(KEY_CHARSET)));
      case String name -> new CompressionConfig(algorithmOf(name), StandardCharsets.UTF_8);
      default -> throw new IllegalArgumentException(
          "Compression config must be a map like {compression: 'GZIP', charset: 'UTF-8'}, got: " + config);
    };
  }

  private static Algorithm algorithmOf(final Object name) {
    if (name == null)
      return Algorithm.GZIP;
    final String upper = name.toString().toUpperCase(Locale.ROOT);
    return switch (upper) {
      case "GZIP" -> Algorithm.GZIP;
      case "DEFLATE" -> Algorithm.DEFLATE;
      case "NONE" -> Algorithm.NONE;
      case "BZIP2", "BLOCK_LZ4", "FRAMED_SNAPPY" -> throw new IllegalArgumentException(
          "Compression algorithm " + upper + " is not available in ArcadeDB; supported: GZIP, DEFLATE, NONE");
      default -> throw new IllegalArgumentException("Unsupported compression algorithm: " + name);
    };
  }

  private static Charset charsetOf(final Object name) {
    if (name == null)
      return StandardCharsets.UTF_8;
    try {
      return Charset.forName(name.toString());
    } catch (final IllegalCharsetNameException | UnsupportedCharsetException e) {
      throw new IllegalArgumentException("Unsupported charset: " + name, e);
    }
  }
}
