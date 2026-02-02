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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AnsiCodeTest {

  @Test
  void ansiCodesExist() {
    // Basic color codes should exist
    assertThat(AnsiCode.RED).isNotNull();
    assertThat(AnsiCode.GREEN).isNotNull();
    assertThat(AnsiCode.YELLOW).isNotNull();
    assertThat(AnsiCode.BLUE).isNotNull();
    assertThat(AnsiCode.RESET).isNotNull();
  }

  @Test
  void formatWithAnsiVariables() {
    // format() uses $ANSI{...} variable syntax
    final String formatted = AnsiCode.format("$ANSI{RED Hello}");
    // Should process the ANSI variable
    assertThat(formatted).isNotNull();
  }

  @Test
  void formatWithColorAndText() {
    final String formatted = AnsiCode.format("$ANSI{green Text}", true);
    assertThat(formatted).contains("Text");
  }

  @Test
  void formatWithMultipleCodes() {
    final String formatted = AnsiCode.format("$ANSI{red:high_intensity Warning}", true);
    assertThat(formatted).contains("Warning");
  }

  @Test
  void supportsColorsReturnsBoolean() {
    // Just verify it doesn't throw and returns a boolean
    final boolean supportsColors = AnsiCode.supportsColors();
    assertThat(supportsColors).isIn(true, false);
  }

  @Test
  void resetCodeClearsFormatting() {
    final String reset = AnsiCode.RESET.toString();
    assertThat(reset).isNotNull();
    assertThat(reset).contains("\u001B[0m");
  }

  @Test
  void highIntensityAndUnderlineExist() {
    assertThat(AnsiCode.HIGH_INTENSITY).isNotNull();
    assertThat(AnsiCode.UNDERLINE).isNotNull();
  }

  @Test
  void formatPlainTextWithoutAnsiVariables() {
    final String formatted = AnsiCode.format("Plain text without ANSI");
    assertThat(formatted).isEqualTo("Plain text without ANSI");
  }

  @Test
  void formatWithColorsDisabled() {
    // When colors are disabled, the text inside ANSI block should be returned
    final String formatted = AnsiCode.format("$ANSI{red Error}", false);
    assertThat(formatted).isEqualTo("Error");
  }

  @Test
  void allBasicColorsAvailable() {
    // Verify all standard colors are available
    assertThat(AnsiCode.BLACK).isNotNull();
    assertThat(AnsiCode.RED).isNotNull();
    assertThat(AnsiCode.GREEN).isNotNull();
    assertThat(AnsiCode.YELLOW).isNotNull();
    assertThat(AnsiCode.BLUE).isNotNull();
    assertThat(AnsiCode.MAGENTA).isNotNull();
    assertThat(AnsiCode.CYAN).isNotNull();
    assertThat(AnsiCode.WHITE).isNotNull();
  }

  @Test
  void backgroundColorsExist() {
    assertThat(AnsiCode.BACKGROUND_RED).isNotNull();
    assertThat(AnsiCode.BACKGROUND_GREEN).isNotNull();
    assertThat(AnsiCode.BACKGROUND_BLUE).isNotNull();
    assertThat(AnsiCode.BACKGROUND_WHITE).isNotNull();
  }

  @Test
  void styleCodesExist() {
    assertThat(AnsiCode.ITALIC).isNotNull();
    assertThat(AnsiCode.BLINK).isNotNull();
    assertThat(AnsiCode.REVERSE_VIDEO).isNotNull();
    assertThat(AnsiCode.INVISIBLE_TEXT).isNotNull();
  }

  @Test
  void nullCodeReturnsEmptyString() {
    assertThat(AnsiCode.NULL.toString()).isEmpty();
  }

  @Test
  void ansiCodeToStringReturnsEscapeSequence() {
    // All non-NULL codes should contain escape sequence
    assertThat(AnsiCode.RED.toString()).contains("\u001B[");
    assertThat(AnsiCode.GREEN.toString()).contains("\u001B[");
    assertThat(AnsiCode.BLUE.toString()).contains("\u001B[");
  }
}
