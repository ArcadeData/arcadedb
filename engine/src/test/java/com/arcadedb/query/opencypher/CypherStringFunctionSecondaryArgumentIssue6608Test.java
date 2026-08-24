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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6608: issue #5798 (fixed by PR #5901) made several string functions reject a
 * non-STRING value in their PRIMARY argument position, but the remaining STRING-typed argument positions of
 * the same functions - {@code split()}'s delimiter, {@code replace()}'s search and replacement text,
 * {@code trim()}/{@code btrim()}/{@code lTrim()}/{@code rTrim()}'s trim-character argument - still accepted
 * any value and silently converted it via {@code Object.toString()}. {@code char.length()} (a.k.a.
 * {@code char_length()}) was not covered by #5798 at all, in any argument position.
 * <p>
 * Handing a non-STRING value to one of these positions is the client's mistake and must be a
 * {@link CommandSemanticException} (400), not a silently "successful" textual conversion, exactly as the
 * primary argument position already behaves since #5798.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherStringFunctionSecondaryArgumentIssue6608Test extends TestHelper {

  // ===================== the reproducers from the issue =====================

  @Test
  void replaceOfNonStringSearchOrReplacementArgumentIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN replace('a', 1, 'b') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("replace()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("RETURN replace('a', 1, []) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("replace()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("RETURN replace('a', 'x', []) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("replace()")
        .hasMessageContaining("STRING");
  }

  @Test
  void splitOfNonStringDelimiterIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN split('a', []) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("split()")
        .hasMessageContaining("STRING");
  }

  @Test
  void lTrimOfNonStringTrimCharacterIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN lTrim('a', []) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("lTrim()")
        .hasMessageContaining("STRING");
  }

  @Test
  void rTrimOfNonStringTrimCharacterIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN rTrim('a', []) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("rTrim()")
        .hasMessageContaining("STRING");
  }

  @Test
  void btrimOfNonStringTrimCharacterIsAClientTypeError() {
    // 2-argument form: btrim(source, trimCharacter).
    assertThatThrownBy(() -> consume("RETURN btrim('a', []) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("trim()")
        .hasMessageContaining("STRING");
  }

  @Test
  void theSqlStyleThreeArgumentTrimFormRejectsANonStringTrimCharacter() {
    // trim(BOTH/LEADING/TRAILING char FROM string): char is args[1], source is args[2].
    assertThatThrownBy(() -> consume("RETURN trim(BOTH [] FROM 'a') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("trim()")
        .hasMessageContaining("STRING");
  }

  @Test
  void charLengthOfANonStringArgumentIsAClientTypeError() {
    // char.length() and char_length() are aliases resolving to the same executor (CypherFunctionFactory),
    // so both report under the executor's own getName() spelling, same convention as the toUpper()/upper()
    // aliases in issue #5798.
    assertThatThrownBy(() -> consume("RETURN char.length([]) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("char_length()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("RETURN char_length(5) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("char_length()")
        .hasMessageContaining("STRING");
  }

  // ===================== the internal consistency controls from the issue still pass =====================

  @Test
  void thePrimaryArgumentPositionsFixedByIssue5798StillReject() {
    assertThatThrownBy(() -> consume("RETURN split(5, ',') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("split()");
    assertThatThrownBy(() -> consume("RETURN replace(5, 'a', 'b') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("replace()");
    assertThatThrownBy(() -> consume("RETURN btrim('a', 1, []) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("trim()");
  }

  // ===================== null propagation is not a type error =====================

  @Test
  void nullPropagationIsNotATypeError() {
    assertThat(single("RETURN split('a,b', null) AS r")).isNull();
    assertThat(single("RETURN replace('a', null, 'b') AS r")).isNull();
    assertThat(single("RETURN replace('a', 'a', null) AS r")).isNull();
    assertThat(single("RETURN lTrim('a', null) AS r")).isNull();
    assertThat(single("RETURN rTrim('a', null) AS r")).isNull();
    assertThat(single("RETURN btrim('a', null) AS r")).isNull();
    assertThat(single("RETURN char.length(null) AS r")).isNull();
  }

  // ===================== explicit conversion still works =====================

  @Test
  void explicitToStringConversionStillWorks() {
    assertThat(single("RETURN split('a1b', toString(1)) AS r").toString()).isEqualTo("[a, b]");
    assertThat(single("RETURN replace('a1b', toString(1), 'x') AS r")).isEqualTo("axb");
    assertThat(single("RETURN char.length(toString(42)) AS r")).isEqualTo(2L);
  }

  // ===================== valid arguments still work (no regression) =====================

  @Test
  void validArgumentsStillWork() {
    assertThat(single("RETURN split('a,b', ',') AS r").toString()).isEqualTo("[a, b]");
    assertThat(single("RETURN replace('abc', 'b', 'x') AS r")).isEqualTo("axc");
    assertThat(single("RETURN lTrim('xxabc', 'x') AS r")).isEqualTo("abc");
    assertThat(single("RETURN rTrim('abcxx', 'x') AS r")).isEqualTo("abc");
    assertThat(single("RETURN btrim('xxabcxx', 'x') AS r")).isEqualTo("abc");
    assertThat(single("RETURN trim(BOTH 'x' FROM 'xxabcxx') AS r")).isEqualTo("abc");
    assertThat(single("RETURN char.length('ab') AS r")).isEqualTo(2L);
    assertThat(single("RETURN char_length('ab') AS r")).isEqualTo(2L);
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("r");
    }
  }

  private void consume(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
