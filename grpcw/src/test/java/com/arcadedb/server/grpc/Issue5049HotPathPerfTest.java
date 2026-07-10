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
package com.arcadedb.server.grpc;

import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5049 (gRPC hot-path performance).
 *
 * <p>PERF-L: {@link GrpcTypeConverter#bytesOf(String)} must return the exact UTF-8 byte
 * length without allocating a throwaway {@code byte[]}, matching
 * {@code String.getBytes(UTF_8).length} for every input class including unpaired surrogates.
 *
 * <p>PERF-1: a converter-level sanity check that toggling the {@code debug} flag never changes
 * conversion output. This exercises {@link GrpcTypeConverter} (which itself contains no logging);
 * the guarded per-value logging added by this fix lives in the private
 * {@code ArcadeDbGrpcService.toGrpcValue}/{@code convert*} paths, which are exercised end-to-end
 * (at the default debug-off level) by {@code ArcadeDbGrpcServiceExtendedTest} and
 * {@code ArcadeDbGrpcServiceCoverageIT}. The guards are behavior-neutral by construction: each
 * only wraps a {@code Level.FINE} log call, never a value-producing statement.
 */
class Issue5049HotPathPerfTest {

  @Test
  void bytesOfMatchesGetBytesForAsciiAndMultiByte() {
    final String[] samples = {
        "",
        "a",
        "hello world",
        "café",                       // 2-byte sequence (é)
        "日本語",                      // 3-byte sequences
        "€uro",                       // 3-byte sequence (€)
        "smile 😀 end",     // valid surrogate pair -> 4 bytes
        "😀😁",   // two valid surrogate pairs
    };

    for (final String s : samples) {
      final int expected = s.getBytes(StandardCharsets.UTF_8).length;
      assertThat(GrpcTypeConverter.bytesOf(s)).as("bytesOf(\"%s\")", s).isEqualTo(expected);
    }
  }

  @Test
  void bytesOfHandlesNull() {
    assertThat(GrpcTypeConverter.bytesOf(null)).isEqualTo(0);
  }

  @Test
  void bytesOfMatchesGetBytesForUnpairedSurrogates() {
    // Unpaired surrogates are encoded by String.getBytes(UTF_8) as the replacement byte '?'
    // (a single byte). The allocation-free counter must reproduce that exactly.
    final String loneHigh = "x\uD83Dy";                 // high surrogate with no low following
    final String loneLow = "x\uDE00y";                  // low surrogate with no high preceding
    final String highThenHigh = "\uD83D\uD83D";         // high followed by another high
    final String trailingHigh = "abc\uD83D";            // high surrogate at end of string

    assertThat(GrpcTypeConverter.bytesOf(loneHigh)).isEqualTo(loneHigh.getBytes(StandardCharsets.UTF_8).length);
    assertThat(GrpcTypeConverter.bytesOf(loneLow)).isEqualTo(loneLow.getBytes(StandardCharsets.UTF_8).length);
    assertThat(GrpcTypeConverter.bytesOf(highThenHigh)).isEqualTo(highThenHigh.getBytes(StandardCharsets.UTF_8).length);
    assertThat(GrpcTypeConverter.bytesOf(trailingHigh)).isEqualTo(trailingHigh.getBytes(StandardCharsets.UTF_8).length);
  }

  @Test
  void bytesOfMatchesGetBytesAcrossFullCodePointRange() {
    // Exhaustively cover every BMP code point plus a spread of supplementary code points,
    // guaranteeing byte-for-byte parity with the JDK encoder on the whole boundary space.
    final StringBuilder sb = new StringBuilder();
    for (int cp = 0; cp <= 0xFFFF; cp++) {
      sb.append((char) cp);
    }
    for (int cp = 0x10000; cp <= 0x10FFFF; cp += 0x137) {
      sb.appendCodePoint(cp);
    }
    final String s = sb.toString();
    assertThat(GrpcTypeConverter.bytesOf(s)).isEqualTo(s.getBytes(StandardCharsets.UTF_8).length);
  }

  @Test
  void conversionResultIsIndependentOfDebugFlag() {
    final boolean previous = LogManager.instance().isDebugEnabled();
    try {
      final Object[] values = { true, 42, 123456789012345L, 3.14d, "text", 2.5f };

      for (final Object v : values) {
        LogManager.instance().setDebugEnabled(false);
        final GrpcValue off = GrpcTypeConverter.toGrpcValue(v);
        final Object offBack = GrpcTypeConverter.fromGrpcValue(off);

        LogManager.instance().setDebugEnabled(true);
        final GrpcValue on = GrpcTypeConverter.toGrpcValue(v);
        final Object onBack = GrpcTypeConverter.fromGrpcValue(on);

        assertThat(on).as("encoded value for %s must not depend on debug flag", v).isEqualTo(off);
        assertThat(onBack).as("decoded value for %s must not depend on debug flag", v).isEqualTo(offBack);
      }
    } finally {
      LogManager.instance().setDebugEnabled(previous);
    }
  }
}
