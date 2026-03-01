/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.api.Assertions;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive tests for OpenCypher String functions based on Neo4j Cypher documentation.
 * Tests cover all 18 string functions with their various parameters and edge cases.
 */
class OpenCypherStringFunctionsComprehensiveTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./databases/test-cypher-string-functions");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  // ==================== btrim() Tests ====================

  @Test
  void btrimBasicWhitespace() {
    final ResultSet result = database.command("opencypher", "RETURN btrim('   hello    ') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hello");
  }

  @Test
  void btrimWithCustomCharacter() {
    final ResultSet result = database.command("opencypher", "RETURN btrim('xxyyhelloxyxy', 'xy') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hello");
  }

  @Test
  void btrimNullHandling() {
    ResultSet result = database.command("opencypher", "RETURN btrim(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN btrim(null, null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN btrim('hello', null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN btrim(null, ' ') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void btrimEdgeCases() {
    ResultSet result = database.command("opencypher", "RETURN btrim('') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("");

    result = database.command("opencypher", "RETURN btrim('hello') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hello");

    result = database.command("opencypher", "RETURN btrim('   ') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("");
  }

  // ==================== left() Tests ====================

  @Test
  void leftBasic() {
    final ResultSet result = database.command("opencypher", "RETURN left('hello', 3) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hel");
  }

  @Test
  void leftExceedsLength() {
    final ResultSet result = database.command("opencypher", "RETURN left('hi', 10) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hi");
  }

  @Test
  void leftZeroLength() {
    final ResultSet result = database.command("opencypher", "RETURN left('hello', 0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("");
  }

  @Test
  void leftNullHandling() {
    ResultSet result = database.command("opencypher", "RETURN left(null, 3) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN left(null, null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void leftNullLengthRaisesError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN left('hello', null) AS result"))
        .hasMessageContaining("null");
  }

  @Test
  void leftNegativeLengthRaisesError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN left('hello', -1) AS result"))
        .hasMessageContaining("negative");
  }

  // ==================== lower() and toLower() Tests ====================

  @Test
  void lowerBasic() {
    final ResultSet result = database.command("opencypher", "RETURN lower('HELLO') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hello");
  }

  @Test
  void toLowerBasic() {
    final ResultSet result = database.command("opencypher", "RETURN toLower('HELLO') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hello");
  }

  @Test
  void lowerNullHandling() {
    final ResultSet result = database.command("opencypher", "RETURN lower(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void lowerMixedCase() {
    final ResultSet result = database.command("opencypher", "RETURN lower('HeLLo WoRLd') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hello world");
  }

  // ==================== ltrim() Tests ====================

  @Test
  void ltrimBasicWhitespace() {
    final ResultSet result = database.command("opencypher", "RETURN ltrim('   hello') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hello");
  }

  @Test
  void ltrimWithCustomCharacter() {
    final ResultSet result = database.command("opencypher", "RETURN ltrim('xxyyhelloxyxy', 'xy') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("helloxyxy");
  }

  @Test
  void ltrimNullHandling() {
    ResultSet result = database.command("opencypher", "RETURN ltrim(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN ltrim(null, null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN ltrim('hello', null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN ltrim(null, ' ') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== normalize() Tests ====================

  @Test
  void normalizeBasicNFC() {
    // Unicode normalization: Angstrom sign (\u212B) should equal Latin capital letter A with ring above (\u00C5)
    final ResultSet result = database.command("opencypher", "RETURN normalize('\\u212B') = '\\u00C5' AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isTrue();
  }

  @Test
  void normalizeWithNFKC() {
    // Compatibility normalization: Small less-than sign (\uFE64) normalizes to less-than sign (\u003C)
    final ResultSet result = database.command("opencypher", "RETURN normalize('\\uFE64', NFKC) = '\\u003C' AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isTrue();
  }

  @Test
  void normalizeWithNFD() {
    final ResultSet result = database.command("opencypher", "RETURN normalize('é', NFD) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void normalizeWithNFKD() {
    final ResultSet result = database.command("opencypher", "RETURN normalize('test', NFKD) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("test");
  }

  @Test
  void normalizeNullHandling() {
    final ResultSet result = database.command("opencypher", "RETURN normalize(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== replace() Tests ====================

  @Test
  void replaceBasic() {
    final ResultSet result = database.command("opencypher", "RETURN replace('hello', 'l', 'w') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hewwo");
  }

  @Test
  void replaceWithLimit() {
    final ResultSet result = database.command("opencypher", "RETURN replace('hello', 'l', 'w', 1) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hewlo");
  }

  @Test
  void replaceNotFound() {
    final ResultSet result = database.command("opencypher", "RETURN replace('hello', 'x', 'y') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hello");
  }

  @Test
  void replaceNullHandling() {
    ResultSet result = database.command("opencypher", "RETURN replace(null, 'a', 'b') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN replace('hello', null, 'b') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN replace('hello', 'a', null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void replaceMultipleOccurrences() {
    final ResultSet result = database.command("opencypher", "RETURN replace('banana', 'a', 'o') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("bonono");
  }

  // ==================== reverse() Tests ====================

  @Test
  void reverseBasic() {
    final ResultSet result = database.command("opencypher", "RETURN reverse('palindrome') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("emordnilap");
  }

  @Test
  void reverseSingleCharacter() {
    final ResultSet result = database.command("opencypher", "RETURN reverse('a') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("a");
  }

  @Test
  void reverseEmpty() {
    final ResultSet result = database.command("opencypher", "RETURN reverse('') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("");
  }

  @Test
  void reverseNullHandling() {
    final ResultSet result = database.command("opencypher", "RETURN reverse(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== right() Tests ====================

  @Test
  void rightBasic() {
    final ResultSet result = database.command("opencypher", "RETURN right('hello', 3) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("llo");
  }

  @Test
  void rightExceedsLength() {
    final ResultSet result = database.command("opencypher", "RETURN right('hi', 10) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hi");
  }

  @Test
  void rightZeroLength() {
    final ResultSet result = database.command("opencypher", "RETURN right('hello', 0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("");
  }

  @Test
  void rightNullHandling() {
    ResultSet result = database.command("opencypher", "RETURN right(null, 3) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN right(null, null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void rightNullLengthRaisesError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN right('hello', null) AS result"))
        .hasMessageContaining("null");
  }

  @Test
  void rightNegativeLengthRaisesError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN right('hello', -1) AS result"))
        .hasMessageContaining("negative");
  }

  // ==================== rtrim() Tests ====================

  @Test
  void rtrimBasicWhitespace() {
    final ResultSet result = database.command("opencypher", "RETURN rtrim('hello   ') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hello");
  }

  @Test
  void rtrimWithCustomCharacter() {
    final ResultSet result = database.command("opencypher", "RETURN rtrim('xxyyhelloxyxy', 'xy') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("xxyyhello");
  }

  @Test
  void rtrimNullHandling() {
    ResultSet result = database.command("opencypher", "RETURN rtrim(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN rtrim(null, null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN rtrim('hello', null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN rtrim(null, ' ') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== split() Tests ====================

  @Test
  void splitBasic() {
    final ResultSet result = database.command("opencypher", "RETURN split('one,two', ',') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<String> parts = (List<String>) result.next().getProperty("result");
    assertThat(parts).containsExactly("one", "two");
  }

  @Test
  void splitMultipleDelimiters() {
    final ResultSet result = database.command("opencypher", "RETURN split('a,b,c,d', ',') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<String> parts = (List<String>) result.next().getProperty("result");
    assertThat(parts).containsExactly("a", "b", "c", "d");
  }

  @Test
  void splitEmptyString() {
    final ResultSet result = database.command("opencypher", "RETURN split('', ',') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<String> parts = (List<String>) result.next().getProperty("result");
    assertThat(parts).hasSize(1);
  }

  @Test
  void splitNullHandling() {
    ResultSet result = database.command("opencypher", "RETURN split(null, ',') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN split('hello', null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== substring() Tests ====================

  @Test
  void substringBasic() {
    final ResultSet result = database.command("opencypher", "RETURN substring('hello', 1, 3) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("ell");
  }

  @Test
  void substringWithoutLength() {
    final ResultSet result = database.command("opencypher", "RETURN substring('hello', 2) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("llo");
  }

  @Test
  void substringFromStart() {
    final ResultSet result = database.command("opencypher", "RETURN substring('hello', 0, 2) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("he");
  }

  @Test
  void substringZeroLength() {
    final ResultSet result = database.command("opencypher", "RETURN substring('hello', 1, 0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("");
  }

  @Test
  void substringNullHandling() {
    final ResultSet result = database.command("opencypher", "RETURN substring(null, 1, 2) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void substringNullStartRaisesError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN substring('hello', null, 2) AS result"))
        .hasMessageContaining("null");
  }

  @Test
  void substringNegativeStartRaisesError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN substring('hello', -1, 2) AS result"))
        .hasMessageContaining("negative");
  }

  @Test
  void substringNegativeLengthRaisesError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN substring('hello', 1, -1) AS result"))
        .hasMessageContaining("negative");
  }

  // ==================== toString() Tests ====================

  @Test
  void toStringFromInteger() {
    final ResultSet result = database.command("opencypher", "RETURN toString(123) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("123");
  }

  @Test
  void toStringFromFloat() {
    final ResultSet result = database.command("opencypher", "RETURN toString(11.5) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("11.5");
  }

  @Test
  void toStringFromBoolean() {
    final ResultSet result = database.command("opencypher", "RETURN toString(true) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("true");
  }

  @Test
  void toStringFromString() {
    final ResultSet result = database.command("opencypher", "RETURN toString('already a string') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("already a string");
  }

  @Test
  void toStringNullHandling() {
    final ResultSet result = database.command("opencypher", "RETURN toString(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== toStringOrNull() Tests ====================

  @Test
  void toStringOrNullFromInteger() {
    final ResultSet result = database.command("opencypher", "RETURN toStringOrNull(123) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("123");
  }

  @Test
  void toStringOrNullFromFloat() {
    final ResultSet result = database.command("opencypher", "RETURN toStringOrNull(11.5) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("11.5");
  }

  @Test
  void toStringOrNullFromBoolean() {
    final ResultSet result = database.command("opencypher", "RETURN toStringOrNull(true) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("true");
  }

  @Test
  void toStringOrNullFromInvalidType() {
    final ResultSet result = database.command("opencypher", "RETURN toStringOrNull(['A', 'B', 'C']) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void toStringOrNullNullHandling() {
    final ResultSet result = database.command("opencypher", "RETURN toStringOrNull(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== toUpper() and upper() Tests ====================

  @Test
  void toUpperBasic() {
    final ResultSet result = database.command("opencypher", "RETURN toUpper('hello') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("HELLO");
  }

  @Test
  void upperBasic() {
    final ResultSet result = database.command("opencypher", "RETURN upper('hello') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("HELLO");
  }

  @Test
  void toUpperNullHandling() {
    final ResultSet result = database.command("opencypher", "RETURN toUpper(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void toUpperMixedCase() {
    final ResultSet result = database.command("opencypher", "RETURN toUpper('HeLLo WoRLd') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("HELLO WORLD");
  }

  // ==================== trim() Tests ====================

  @Test
  void trimBasicWhitespace() {
    final ResultSet result = database.command("opencypher", "RETURN trim('   hello   ') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hello");
  }

  @Test
  void trimWithBothSpecification() {
    final ResultSet result = database.command("opencypher", "RETURN trim(BOTH 'x' FROM 'xxxhelloxxx') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hello");
  }

  @Test
  void trimWithLeadingSpecification() {
    final ResultSet result = database.command("opencypher", "RETURN trim(LEADING 'x' FROM 'xxxhelloxxx') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("helloxxx");
  }

  @Test
  void trimWithTrailingSpecification() {
    final ResultSet result = database.command("opencypher", "RETURN trim(TRAILING 'x' FROM 'xxxhelloxxx') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("xxxhello");
  }

  @Test
  void trimNullHandling() {
    ResultSet result = database.command("opencypher", "RETURN trim(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN trim(' ' FROM null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN trim(null FROM 'hello') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN trim(BOTH null FROM null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== Combined/Integration Tests ====================

  @Test
  void stringFunctionsCombination() {
    final ResultSet result = database.command("opencypher",
        "RETURN toUpper(left('hello world', 5)) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("HELLO");
  }

  @Test
  void stringFunctionsChaining() {
    final ResultSet result = database.command("opencypher",
        "RETURN trim(toLower('  HELLO WORLD  ')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hello world");
  }

  @Test
  void stringFunctionsWithReplace() {
    final ResultSet result = database.command("opencypher",
        "RETURN toUpper(replace('hello world', 'world', 'cypher')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("HELLO CYPHER");
  }
}
