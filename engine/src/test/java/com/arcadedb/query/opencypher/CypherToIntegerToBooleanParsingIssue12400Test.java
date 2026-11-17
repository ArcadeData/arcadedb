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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12400: the text parsing of {@code toInteger()} and {@code toBoolean()} differed from
 * Neo4j 5 on the surrounding whitespace and on the integers out of the 64-bit range. The expected values are the ones
 * Neo4j 5.26.33 answers (measured for this issue):
 * <ul>
 *   <li>{@code toInteger(' 12 ')} is {@code null}, a text is parsed as it is written</li>
 *   <li>{@code toInteger('12.5')} is {@code 12}, a fraction is truncated</li>
 *   <li>{@code toInteger('9223372036854775808')} raises "integer, 9223372036854775808, is too large"</li>
 *   <li>{@code toIntegerOrNull('9223372036854775808')} is {@code null}</li>
 *   <li>{@code toBoolean(' true ')} is {@code true}, a boolean text is trimmed</li>
 * </ul>
 */
class CypherToIntegerToBooleanParsingIssue12400Test extends TestHelper {

  @Test
  void anIntegerTextIsNotTrimmed() {
    assertThat(single("RETURN toInteger(' 12 ') AS r")).isNull();
    assertThat(single("RETURN toInteger('\\t12\\n') AS r")).isNull();
    assertThat(single("RETURN toInteger(' 12.5 ') AS r")).isNull();
    assertThat(single("RETURN toInteger('  1e3  ') AS r")).isNull();
    assertThat(single("RETURN toInteger('12 ') AS r")).isNull();
  }

  @Test
  void anIntegerTextIsParsedAsItIsWritten() {
    assertThat(single("RETURN toInteger('12') AS r")).isEqualTo(12L);
    assertThat(single("RETURN toInteger('+12') AS r")).isEqualTo(12L);
    assertThat(single("RETURN toInteger('12.5') AS r")).isEqualTo(12L);
    assertThat(single("RETURN toInteger('-12.5') AS r")).isEqualTo(-12L);
    assertThat(single("RETURN toInteger('1e3') AS r")).isEqualTo(1000L);
    assertThat(single("RETURN toInteger('9223372036854775807') AS r")).isEqualTo(Long.MAX_VALUE);
    assertThat(single("RETURN toInteger('-9223372036854775808') AS r")).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  void aTextThatIsNotANumberIsNull() {
    assertThat(single("RETURN toInteger('not a number') AS r")).isNull();
    assertThat(single("RETURN toInteger('12abc') AS r")).isNull();
    assertThat(single("RETURN toInteger('') AS r")).isNull();
  }

  @Test
  void anIntegerTextOutOfThe64BitRangeIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN toInteger('9223372036854775808') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("too large");
    assertThatThrownBy(() -> consume("RETURN toInteger('99999999999999999999') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("too large");
    assertThatThrownBy(() -> consume("RETURN toInteger('1e19') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("too large");
    assertThatThrownBy(() -> consume("RETURN toInteger('12.5e30') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("too large");
    assertThatThrownBy(() -> consume("RETURN toInteger('-9223372036854775809') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("too large");
    // the range is checked on the value as it is written, before the truncation
    assertThatThrownBy(() -> consume("RETURN toInteger('9223372036854775807.9') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("too large");
    assertThatThrownBy(() -> consume("RETURN toInteger('-9223372036854775808.9') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("too large");
  }

  @Test
  void aTextThatFitsIsTruncatedTowardsZero() {
    assertThat(single("RETURN toInteger('9223372036854775806.9') AS r")).isEqualTo(9223372036854775806L);
    assertThat(single("RETURN toInteger('9223372036854775807.0') AS r")).isEqualTo(Long.MAX_VALUE);
    assertThat(single("RETURN toInteger('1e18') AS r")).isEqualTo(1000000000000000000L);
    assertThat(single("RETURN toInteger('1e-30') AS r")).isEqualTo(0L);
  }

  @Test
  void theOrNullAndListFormsAnswerNullForATextOutOfRange() {
    assertThat(single("RETURN toIntegerOrNull('9223372036854775808') AS r")).isNull();
    assertThat(single("RETURN toIntegerOrNull(' 12 ') AS r")).isNull();
    assertThat(list("RETURN toIntegerList([' 12 ']) AS r")).containsExactly((Object) null);
    assertThat(list("RETURN toIntegerList(['9223372036854775808']) AS r")).containsExactly((Object) null);
    assertThat(list("RETURN toIntegerList(['12','13']) AS r")).containsExactly(12L, 13L);
  }

  @Test
  void aBooleanTextIsTrimmed() {
    assertThat(single("RETURN toBoolean(' true ') AS r")).isEqualTo(Boolean.TRUE);
    assertThat(single("RETURN toBoolean('true ') AS r")).isEqualTo(Boolean.TRUE);
    assertThat(single("RETURN toBoolean(' false') AS r")).isEqualTo(Boolean.FALSE);
    assertThat(single("RETURN toBoolean('FALSE ') AS r")).isEqualTo(Boolean.FALSE);
    assertThat(single("RETURN toBoolean('TrUe') AS r")).isEqualTo(Boolean.TRUE);
  }

  @Test
  void aTextThatIsNotABooleanIsNull() {
    assertThat(single("RETURN toBoolean('yes') AS r")).isNull();
    assertThat(single("RETURN toBoolean('1') AS r")).isNull();
    assertThat(single("RETURN toBoolean('  ') AS r")).isNull();
  }

  @Test
  void theOrNullAndListFormsOfToBooleanTrimAsWell() {
    assertThat(single("RETURN toBooleanOrNull(' true ') AS r")).isEqualTo(Boolean.TRUE);
    assertThat(list("RETURN toBooleanList([' true ']) AS r")).containsExactly(Boolean.TRUE);
    assertThat(list("RETURN toBooleanList(['bogus']) AS r")).containsExactly((Object) null);
  }

  @Test
  void toFloatIsNotAffected() {
    assertThat(single("RETURN toFloat(' 1.5 ') AS r")).isEqualTo(1.5d);
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("r");
    }
  }

  @SuppressWarnings("unchecked")
  private List<Object> list(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return (List<Object>) rs.next().getProperty("r");
    }
  }

  private void consume(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
