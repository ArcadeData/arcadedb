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
package com.arcadedb.query.sql.method.string;

import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodCapitalizeTest {
  private SQLMethod method;

  @BeforeEach
  void setUp() {
    method = new SQLMethodCapitalize();
  }

  @Test
  void nullReturnedAsNull() {
    final Object result = method.execute(null, null, null, null);
    assertThat(result).isNull();
  }

  @Test
  void capitalizeEmpty() {
    final Object result = method.execute("", null, null, null);
    assertThat(result).isEqualTo("");
  }

  @Test
  void capitalizeIdentical() {
    final Object result = method.execute("Capitalize This", null, null, null);
    assertThat(result).isEqualTo("Capitalize This");
  }

  @Test
  void capitalizeLower() {
    final Object result = method.execute("capitalize this", null, null, null);
    assertThat(result).isEqualTo("Capitalize This");
  }

  @Test
  void capitalizeUpper() {
    final Object result = method.execute("CAPITALIZE THIS", null, null, null);
    assertThat(result).isEqualTo("Capitalize This");
  }

  @Test
  void capitalizeSingle() {
    final Object result = method.execute("c t", null, null, null);
    assertThat(result).isEqualTo("C T");
  }

  @Test
  void capitalizeNumbers() {
    final Object result = method.execute("111 222", null, null, null);
    assertThat(result).isEqualTo("111 222");
  }

  @Test
  void capitalizeKeepsTheAsciiWordBoundaries() {
    assertThat(method.execute("don't stop-me now_ok 1st", null, null, null)).isEqualTo("Don'T Stop-Me Now_ok 1st");
    assertThat(method.execute("  two  spaces\nnew\tline  ", null, null, null)).isEqualTo("  Two  Spaces\nNew\tLine  ");
  }

  /**
   * Issue #12869: a word was cut at ASCII-only boundaries, so {@code café} became {@code CafÉ} and Cyrillic was left untouched.
   */
  @Test
  void capitalizeANonAsciiWordAsAWhole() {
    assertThat(method.execute("café au lait", null, null, null)).isEqualTo("Café Au Lait");
    assertThat(method.execute("über cool", null, null, null)).isEqualTo("Über Cool");
    assertThat(method.execute("ÉLAN VITAL", null, null, null)).isEqualTo("Élan Vital");
    assertThat(method.execute("naïve résumé", null, null, null)).isEqualTo("Naïve Résumé");
    assertThat(method.execute("привет мир", null, null, null)).isEqualTo("Привет Мир");
    assertThat(method.execute("straße", null, null, null)).isEqualTo("Straße");
    // a combining mark belongs to its word: "e" + U+0301 is one word, not "e" then a new word
    assertThat(method.execute("e\u0301TAT", null, null, null)).isEqualTo("E\u0301tat");
  }

  /**
   * Issue #12869: the word was lower-cased without its first letter, so the Greek final sigma lost its context.
   */
  @Test
  void capitalizeKeepsTheGreekFinalSigma() {
    assertThat(method.execute("ΟΣ", null, null, null)).isEqualTo("Ος");
    assertThat(method.execute("ΟΔΟΣ ΣΟΦΙΑΣ", null, null, null)).isEqualTo("Οδος Σοφιας");
  }

  /**
   * Capitalizing takes the title case of the first letter: {@code ǆ} is {@code ǅ}, not {@code Ǆ}, and {@code ß} has no single
   * upper-case letter, so it is kept rather than becoming {@code SS}.
   */
  @Test
  void capitalizeTitleCasesTheFirstLetter() {
    assertThat(method.execute("ǆungla", null, null, null)).isEqualTo("ǅungla");
    assertThat(method.execute("ßtraße", null, null, null)).isEqualTo("ßtraße");
    assertThat(method.execute("İSTANBUL", null, null, null)).isEqualTo("İstanbul");
    // a letter outside the BMP is two UTF-16 units: "𐐨" (U+10428) title-cases to "𐐀" (U+10400)
    assertThat(method.execute("\uD801\uDC28\uD801\uDC00", null, null, null)).isEqualTo("\uD801\uDC00\uD801\uDC28");
  }

  /**
   * Issue #12869: the built text was a {@code Matcher} replacement string, so a {@code $} or a backslash in the data threw.
   */
  @Test
  void capitalizeTreatsADollarOrABackslashAsData() {
    assertThat(method.execute("a$b", null, null, null)).isEqualTo("A$B");
    assertThat(method.execute("price: $5 each", null, null, null)).isEqualTo("Price: $5 Each");
    assertThat(method.execute("x ${y} z", null, null, null)).isEqualTo("X ${Y} Z");
    assertThat(method.execute("c:\\temp", null, null, null)).isEqualTo("C:\\Temp");
  }
}
