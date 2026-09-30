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
package com.arcadedb.query.sql.executor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8666: the two pure helpers that turn a {@code LIKE} pattern into an index range.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class QueryHelperPrefixTest {
  @Test
  void successorBumpsTheLastCodePoint() {
    assertThat(QueryHelper.prefixSuccessor("abc")).isEqualTo("abd");
    assertThat(QueryHelper.prefixSuccessor("a")).isEqualTo("b");
    assertThat(QueryHelper.prefixSuccessor("ab\uFFFE")).isEqualTo("ab\uFFFF");
    assertThat(QueryHelper.prefixSuccessor("\uD83D\uDE00")).isEqualTo("\uD83D\uDE01");
    assertThat(QueryHelper.prefixSuccessor("a\uD83D\uDFFF")).isEqualTo("a\uD83E\uDC00");
  }

  @Test
  void successorStepsOverCodePointsThatCannotBeBumped() {
    // U+D7FF would become a surrogate, U+FFFF a supplementary character (UTF-16 sorts it lower), U+10FFFF nothing
    assertThat(QueryHelper.prefixSuccessor("ab\uD7FF")).isEqualTo("ac");
    assertThat(QueryHelper.prefixSuccessor("ab\uFFFF\uFFFF")).isEqualTo("ac");
    assertThat(QueryHelper.prefixSuccessor("ab\uDBFF\uDFFF")).isEqualTo("ac");
    assertThat(QueryHelper.prefixSuccessor("ab\uD83D")).isEqualTo("ac");
    assertThat(QueryHelper.prefixSuccessor("ab\uDE00")).isEqualTo("ac");
  }

  @Test
  void successorOfNothingBumpable() {
    assertThat(QueryHelper.prefixSuccessor("")).isNull();
    assertThat(QueryHelper.prefixSuccessor("\uFFFF")).isNull();
    assertThat(QueryHelper.prefixSuccessor("\uD7FF\uFFFF")).isNull();
    assertThat(QueryHelper.prefixSuccessor("\uDBFF\uDFFF")).isNull();
  }

  @Test
  void everyKeyWithThePrefixSortsBelowTheSuccessor() {
    for (final String prefix : new String[] { "a", "ab", "ab\uD7FF", "\uFFFFa", "\uD83D\uDE00", "z\uD83D\uDFFF" }) {
      final String successor = QueryHelper.prefixSuccessor(prefix);
      assertThat(successor).isNotNull();
      for (final String tail : new String[] { "", "\u0000", "z", "\uFFFF", "\uD83D\uDE00", "\uFFFF\uFFFF\uFFFF" })
        assertThat((prefix + tail).compareTo(successor)).as(prefix + tail).isNegative();
    }
  }

  @Test
  void literalPrefixStopsAtTheFirstWildcard() {
    assertThat(QueryHelper.likeLiteralPrefix("abc%")).isEqualTo("abc");
    assertThat(QueryHelper.likeLiteralPrefix("abc%def")).isEqualTo("abc");
    assertThat(QueryHelper.likeLiteralPrefix("ab?d%")).isEqualTo("ab");
    assertThat(QueryHelper.likeLiteralPrefix("abc")).isEqualTo("abc");
    assertThat(QueryHelper.likeLiteralPrefix("%abc")).isEmpty();
    assertThat(QueryHelper.likeLiteralPrefix("?abc")).isEmpty();
    assertThat(QueryHelper.likeLiteralPrefix("")).isEmpty();
  }

  @Test
  void literalPrefixReadsEscapesLikeTheMatcher() {
    assertThat(QueryHelper.likeLiteralPrefix("ab\\%c%")).isEqualTo("ab%c");
    assertThat(QueryHelper.likeLiteralPrefix("ab\\?c%")).isEqualTo("ab?c");
    assertThat(QueryHelper.likeLiteralPrefix("ab\\c%")).isEqualTo("ab\\c");
    // a backslash, then an escaped percent: both literal, and the pattern has no wildcard left
    assertThat(QueryHelper.likeLiteralPrefix("ab\\\\%")).isEqualTo("ab\\%");
    assertThat(QueryHelper.likeLiteralPrefix("ab\\")).isEqualTo("ab\\");
    assertThat(QueryHelper.likeLiteralPrefix("\\%")).isEqualTo("%");
  }
}
