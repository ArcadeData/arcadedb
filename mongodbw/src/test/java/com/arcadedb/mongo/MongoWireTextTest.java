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
package com.arcadedb.mongo;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The text a client sends is shown in messages and log lines with printable characters only, and never longer than
 * {@link MongoWireText#MAX_LENGTH}.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MongoWireTextTest {

  @Test
  void aPrintableTextIsReturnedAsItIs() {
    final String text = "listDatabases";

    assertThat(MongoWireText.clean(text)).isSameAs(text);
    assertThat(MongoWireText.clean("")).isEmpty();
  }

  @Test
  void nonAsciiLettersAreKept() {
    assertThat(MongoWireText.clean("città-日本語-😀")).isEqualTo("città-日本語-😀");
  }

  @Test
  void aNullIsShownAsNull() {
    assertThat(MongoWireText.clean(null)).isEqualTo("null");
    assertThat(MongoWireText.describe(null)).isEqualTo("null");
  }

  @Test
  void controlCharactersAreReplaced() {
    assertThat(MongoWireText.clean("a\nb\rc\td\u0000e\u001b[31mf\u007fg\u0085h")).isEqualTo("a?b?c?d?e?[31mf?g?h");
  }

  @Test
  void lineSeparatorsAndFormattingCharactersAreReplaced() {
    assertThat(MongoWireText.clean("a b c‮d​e")).isEqualTo("a?b?c?d?e");
  }

  @Test
  void aLongTextIsCut() {
    final String text = "x".repeat(MongoWireText.MAX_LENGTH + 50);

    final String cleaned = MongoWireText.clean(text);

    assertThat(cleaned).isEqualTo("x".repeat(MongoWireText.MAX_LENGTH) + "...");
  }

  @Test
  void aTextOfTheLimitLengthIsNotCut() {
    final String text = "x".repeat(MongoWireText.MAX_LENGTH);

    assertThat(MongoWireText.clean(text)).isSameAs(text);
  }

  @Test
  void aSurrogatePairIsNotSplitByTheCut() {
    // the emoji straddles the limit: its first half is the last char kept, so it goes too
    final String text = "x".repeat(MongoWireText.MAX_LENGTH - 1) + "😀tail";

    final String cleaned = MongoWireText.clean(text);

    assertThat(cleaned).isEqualTo("x".repeat(MongoWireText.MAX_LENGTH - 1) + "...");
  }

  @Test
  void aCutAndAReplacementTogether() {
    final String text = "\n" + "y".repeat(MongoWireText.MAX_LENGTH * 2);

    assertThat(MongoWireText.clean(text)).isEqualTo("?" + "y".repeat(MongoWireText.MAX_LENGTH - 1) + "...");
  }

  @Test
  void theLimitCanBeGiven() {
    final String message = "m".repeat(MongoWireText.MAX_LENGTH + 1);

    assertThat(MongoWireText.clean(message, MongoWireText.MAX_MESSAGE_LENGTH)).isSameAs(message);
    assertThat(MongoWireText.clean("abcdef", 3)).isEqualTo("abc...");
    assertThat(MongoWireText.clean("abc", 3)).isEqualTo("abc");
  }

  @Test
  void aValueThatIsNotATextIsNamedByItsType() {
    assertThat(MongoWireText.describe("PLAIN")).isEqualTo("PLAIN");
    assertThat(MongoWireText.describe("PL\nAIN")).isEqualTo("PL?AIN");
    assertThat(MongoWireText.describe(42)).isEqualTo("Integer");
    assertThat(MongoWireText.describe(new Object())).isEqualTo("Object");
  }
}
