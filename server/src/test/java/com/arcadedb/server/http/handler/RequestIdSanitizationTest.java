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
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AbstractServerHttpHandler#sanitizeRequestId(String)}: a client-supplied
 * X-Request-Id must be bounded (length-capped) and stripped of control characters before being echoed
 * and logged, with no change for already-clean values.
 */
class RequestIdSanitizationTest {

  private static final char NL  = '\n';
  private static final char CR  = '\r';
  private static final char TAB = '\t';
  private static final char DEL = '\u007F';

  @Test
  void nullOrEmptyYieldsNull() {
    assertThat(AbstractServerHttpHandler.sanitizeRequestId(null)).isNull();
    assertThat(AbstractServerHttpHandler.sanitizeRequestId("")).isNull();
  }

  @Test
  void cleanValueIsReturnedUnchanged() {
    final String id = "550e8400-e29b-41d4-a716-446655440000";
    assertThat(AbstractServerHttpHandler.sanitizeRequestId(id)).isSameAs(id);
  }

  @Test
  void lengthIsCappedAt128() {
    assertThat(AbstractServerHttpHandler.sanitizeRequestId("a".repeat(500))).hasSize(128);
  }

  @Test
  void controlCharactersAreStrippedButPrintablesKept() {
    // NL CR TAB and DEL (0x7F) are dropped; the space (0x20) is printable and kept.
    final String raw = "ab" + NL + "cd" + CR + "e" + TAB + "fg h" + DEL;
    assertThat(AbstractServerHttpHandler.sanitizeRequestId(raw)).isEqualTo("abcdefg h");
  }

  @Test
  void valueOfOnlyControlCharsYieldsNull() {
    final String raw = "" + NL + CR + TAB;
    assertThat(AbstractServerHttpHandler.sanitizeRequestId(raw)).isNull();
  }

  @Test
  void capAppliesBeforeStrippingSoControlCharsBeyond128AreIgnored() {
    // 128 valid chars then control chars: only the first 128 are scanned and kept (already clean).
    final String raw = "x".repeat(128) + NL + NL;
    assertThat(AbstractServerHttpHandler.sanitizeRequestId(raw)).isEqualTo("x".repeat(128));
  }
}
