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

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The text a client sends is shown in messages and log lines with printable characters only, and never longer than
 * {@link MongoWireText#MAX_LENGTH}.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MongoWireTextTest {
  private static final char ESCAPE = (char) 0x1B;

  @Test
  void aPrintableTextIsReturnedAsItIs() {
    final String text = "listDatabases";

    assertThat(MongoWireText.clean(text)).isSameAs(text);
    assertThat(MongoWireText.clean("")).isEmpty();
  }

  @Test
  void nonAsciiLettersAreKept() {
    final String emoji = new String(Character.toChars(0x1F600));
    final String text = "citt" + (char) 0xE0 + "-" + (char) 0x65E5 + (char) 0x672C + "-" + emoji;

    assertThat(MongoWireText.clean(text)).isEqualTo(text);
  }

  @Test
  void aNullIsShownAsNull() {
    assertThat(MongoWireText.clean(null)).isEqualTo("null");
    assertThat(MongoWireText.describe(null)).isEqualTo("null");
  }

  @Test
  void controlCharactersAreReplaced() {
    final String text = "a\nb\rc\td" + (char) 0 + "e" + ESCAPE + "[31mf" + (char) 0x7F + "g" + (char) 0x85 + "h";

    assertThat(MongoWireText.clean(text)).isEqualTo("a?b?c?d?e?[31mf?g?h");
  }

  @Test
  void lineSeparatorsAndFormattingCharactersAreReplaced() {
    final char lineSeparator = (char) 0x2028;
    final char paragraphSeparator = (char) 0x2029;
    final char rightToLeftOverride = (char) 0x202E;
    final char zeroWidthSpace = (char) 0x200B;

    assertThat(MongoWireText.clean("a" + lineSeparator + "b" + paragraphSeparator + "c" + rightToLeftOverride + "d" + zeroWidthSpace + "e"))
        .isEqualTo("a?b?c?d?e");
  }

  @Test
  void anInvisibleCharacterOfTheSupplementaryPlanesIsReplacedOnce() {
    // a language tag is a format character made of two chars (a surrogate pair): one replacement, and the emoji next to it stays
    final String languageTag = new String(Character.toChars(0xE0001));
    final String emoji = new String(Character.toChars(0x1F600));

    assertThat(MongoWireText.clean("a" + languageTag + "b" + emoji + "c")).isEqualTo("a?b" + emoji + "c");
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
    final String text = "x".repeat(MongoWireText.MAX_LENGTH - 1) + new String(Character.toChars(0x1F600)) + "tail";

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

  @Test
  void aLoggedThrowableKeepsItsFramesAndShowsEachMessageCleaned() {
    final Throwable cause = new IllegalStateException("cause\nsecond-line " + "y".repeat(MongoWireText.MAX_MESSAGE_LENGTH * 2));
    final Throwable original = new IllegalArgumentException("outer\nsecond-line" + ESCAPE + "[31m", cause);

    final Throwable logged = MongoWireText.forLog(original);

    assertThat(logged.getStackTrace()).isEqualTo(original.getStackTrace());
    assertThat(logged.getMessage()).isEqualTo("java.lang.IllegalArgumentException: outer?second-line?[31m");
    assertThat(logged.getCause()).isNotNull();
    assertThat(logged.getCause().getStackTrace()).isEqualTo(cause.getStackTrace());
    assertThat(logged.getCause().getMessage()).startsWith("java.lang.IllegalStateException: cause?second-line yyy").endsWith("...")
        .hasSizeLessThan(MongoWireText.MAX_MESSAGE_LENGTH + 64);

    // what the log prints: no escape, no text of the original on a line of its own
    final StringWriter rendered = new StringWriter();
    logged.printStackTrace(new PrintWriter(rendered));
    assertThat(rendered.toString().chars()).noneMatch(c -> c == ESCAPE || c == '\r');
    assertThat(rendered.toString().lines()).noneMatch(line -> line.startsWith("second-line"));
  }

  @Test
  void aLoggedThrowableWithoutAMessageIsNamedByItsClass() {
    assertThat(MongoWireText.forLog(new IllegalStateException()).getMessage()).isEqualTo("java.lang.IllegalStateException");
    assertThat(MongoWireText.forLog(null)).isNull();
  }

  @Test
  void aLongCauseChainIsCut() {
    Throwable chain = new IllegalStateException("root");
    for (int i = 0; i < 50; i++)
      chain = new IllegalStateException("level " + i, chain);

    int depth = 0;
    for (Throwable t = MongoWireText.forLog(chain); t != null; t = t.getCause())
      depth++;

    assertThat(depth).isBetween(2, 20);
  }
}
